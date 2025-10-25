use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use aes::Aes128;
use async_trait::async_trait;
use bytes::Bytes;
use cbc::{
    cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit},
    Decryptor,
};
use futures::{future::join_all, TryStreamExt};
use hls_m3u8::{types::DecryptionKey, Decryptable, MediaPlaylist};
use reqwest::StatusCode;
use reqwest::{Client, Url};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::{
    config::Config,
    errors::{HLSDecoderError, StreamItemError},
    segment::{SegmentList, SegmentResolver, StreamSegment},
    utils::{get_segment_uri, ReqwestStream, ReqwestStreamItem},
};

// Callback for transforming or decrypting HLS AES-128 keys.
// This can be used for in-house DRM, key wrapping, or custom modifications.
pub type KeyProcessorCallback = dyn Fn(Bytes) -> Bytes + Send + Sync;
pub type KeyQueryParams = HashMap<String, String>;
pub type KeyRequestHeaders = HashMap<String, String>;

#[derive(Default, Clone)]
pub struct AesConfig {
    pub(crate) key_query_params: Option<KeyQueryParams>,
    pub(crate) key_request_headers: Option<KeyRequestHeaders>,
    pub(crate) key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
}

impl AesConfig {
    pub fn new(
        key_query_params: Option<KeyQueryParams>,
        key_request_headers: Option<KeyRequestHeaders>,
        key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
    ) -> Self {
        Self {
            key_query_params,
            key_request_headers,
            key_processor_cb,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SegmentEncryptionData {
    pub(crate) key: [u8; 16],
    pub(crate) iv: Option<[u8; 16]>,
    pub(crate) media_sequence: u128,
    pub(crate) length: Arc<AtomicU64>,
}

impl SegmentEncryptionData {
    pub(crate) fn new(key: [u8; 16], iv: Option<[u8; 16]>, media_sequence: u128) -> Self {
        let length = Arc::new(AtomicU64::new(0));
        Self {
            key,
            iv,
            media_sequence,
            length,
        }
    }

    #[inline(always)]
    pub(crate) fn decrypted_content_length(&self) -> u64 {
        self.length.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub(crate) struct SegmentListDecryptionState {
    key: [u8; 16],
    iv: [u8; 16],
    buffer: Vec<u8>,
    decryptor: Decryptor<Aes128>,
}

impl SegmentListDecryptionState {
    fn new(key: [u8; 16], iv: [u8; 16]) -> Self {
        Self {
            key,
            iv,
            buffer: Vec::with_capacity(1024),
            decryptor: Decryptor::<Aes128>::new((&key).into(), (&iv).into()),
        }
    }

    fn update_decryption_state(&mut self, key: [u8; 16], iv: [u8; 16]) {
        if self.key != key || self.iv != iv {
            self.key = key;
            self.iv = iv;
            self.decryptor = Decryptor::<Aes128>::new((&key).into(), (&iv).into());
        }
    }
}

// A helper struct for downloading segments.
pub(crate) struct DownloadAesSegmentParams {
    client: Client,
    uri: String,
    key: Option<[u8; 16]>,
    iv: Option<[u8; 16]>,
    media_sequence: u128,
}

// A helper struct to unify the processing of initialization and media segments.
struct SegmentMeta<'a> {
    uri: &'a str,
    key_info: Option<&'a DecryptionKey<'a>>,
    media_sequence: usize,
}

async fn get_key_iv(
    client: Client,
    mut key_uri: String,
    key_iv: Option<[u8; 16]>,
    key_query_params: Option<HashMap<String, String>>,
    key_request_headers: Option<HashMap<String, String>>,
    key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
) -> Result<(String, Option<[u8; 16]>, Option<[u8; 16]>), HLSDecoderError> {
    if let Some(params) = key_query_params {
        let mut url_obj = Url::parse(&key_uri)?;
        {
            let mut query_pairs = url_obj.query_pairs_mut();
            for (k, v) in params {
                query_pairs.append_pair(k.as_str(), v.as_str());
            }
        }
        key_uri = url_obj.to_string();
    }

    // Create a request builder.
    let mut request_builder = client.get(&key_uri);
    // Add custom headers if they are provided.
    if let Some(headers) = key_request_headers {
        for (name, value) in headers {
            request_builder = request_builder.header(name, value);
        }
    }

    // Send the request.
    let resp = request_builder.send().await?;
    if resp.status() != StatusCode::OK {
        return Err(HLSDecoderError::Other(format!(
            "Failed to fetch key: {}",
            resp.status()
        )));
    }

    let raw_bytes = resp.bytes().await?;
    #[cfg(feature = "tracing")]
    tracing::debug!(
        "Fetched raw key ({} bytes): {:?}",
        raw_bytes.len(),
        raw_bytes
    );

    let processed_bytes = match key_processor_cb {
        Some(cb) => (cb)(raw_bytes),
        None => raw_bytes,
    };

    #[cfg(feature = "tracing")]
    tracing::debug!(
        "Processed key ({} bytes): {:?}",
        processed_bytes.len(),
        processed_bytes
    );

    // The processed key must be 16 bytes
    if processed_bytes.len() != 16 {
        return Err(HLSDecoderError::Other(format!(
            "Invalid key length: expected 16, got {}",
            processed_bytes.len()
        )));
    }

    let mut key_arr = [0u8; 16];
    key_arr.copy_from_slice(&processed_bytes[0..16]);
    Ok((key_uri, Some(key_arr), key_iv))
}

#[async_trait]
impl<'a> SegmentResolver<DownloadAesSegmentParams, 'a> for SegmentList {
    fn poll_stream(&self, cx: &mut Context<'_>, index: usize) -> Poll<Option<ReqwestStreamItem>> {
        if let Some(segment_arc) = self.segments.get(index) {
            // Try to get a read lock on the segment without blocking.
            match segment_arc.try_read() {
                Ok(segment) => {
                    if let Some(data) = &segment.encryption_data {
                        let key = data.key;
                        let iv = data.iv.unwrap_or_else(|| data.media_sequence.to_be_bytes());

                        let mut state_guard = self.decryption_state.lock().unwrap();
                        let reinitialize = match &*state_guard {
                            Some(state) => state.key != key || state.iv != iv,
                            None => true,
                        };
                        if reinitialize {
                            *state_guard = Some(SegmentListDecryptionState::new(key, iv));
                        }

                        let state = state_guard.as_mut().unwrap();
                        state.update_decryption_state(key, iv);

                        loop {
                            match segment.poll_stream(cx) {
                                Poll::Ready(Some(Ok(new_bytes))) => {
                                    state.buffer.extend_from_slice(&new_bytes)
                                }
                                Poll::Ready(None) => {
                                    if state.buffer.is_empty() {
                                        return Poll::Ready(None);
                                    }
                                    let final_chunk_res = state
                                        .decryptor
                                        .clone()
                                        .decrypt_padded_mut::<Pkcs7>(&mut state.buffer)
                                        .map_err(|e| {
                                            StreamItemError::Decryption(format!(
                                                "Final block decryption failed: {:?}",
                                                e
                                            ))
                                        })
                                        .map(Bytes::copy_from_slice);

                                    if let Ok(ref final_chunk) = final_chunk_res {
                                        data.length
                                            .fetch_add(final_chunk.len() as u64, Ordering::SeqCst);
                                    }

                                    state.buffer.clear();
                                    return Poll::Ready(Some(final_chunk_res));
                                }
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                    } else {
                        // If segment is not encrypted just poll its inner stream.
                        return segment.poll_stream(cx);
                    }
                }
                Err(_) => {
                    // If the lock is held (e.g., for writing during a seek),
                    // we cannot proceed. Return Pending to be polled again later.
                    Poll::Pending
                }
            }
        } else {
            return Poll::Pending;
        }
    }

    async fn resolve_segments(
        client: Client,
        config: &Config,
        media_playlist: &MediaPlaylist<'a>,
        playlist_url: &Url,
    ) -> Result<SegmentList, HLSDecoderError> {
        let base_url = config.get_base_url();
        let key_query_params = config.get_key_query_params();
        let key_request_headers = config.get_key_request_headers();
        let key_processor_cb = config.get_key_processor_cb();

        let base = if let Some(base) = base_url.as_ref() {
            base.as_str()
        } else {
            playlist_url.as_str().trim_end_matches(".m3u8")
        };

        // Collect metadata for all segments into a single list.
        let mut segment_metas: Vec<SegmentMeta> = Vec::new();
        let starting_sequence = media_playlist.media_sequence;
        if let Some(first_segment) = media_playlist.segments.get(0) {
            if let Some(ref map) = first_segment.map {
                segment_metas.push(SegmentMeta {
                    uri: map.uri(),
                    key_info: map.keys().get(0).cloned(),
                    media_sequence: starting_sequence,
                });
            }
        }
        for (i, (_, segment)) in media_playlist.segments.iter().enumerate() {
            segment_metas.push(SegmentMeta {
                uri: segment.uri(),
                key_info: segment.keys().get(0).cloned(),
                media_sequence: starting_sequence + i,
            });
        }

        // Collect all unique encryption key URIs.
        let mut key_requests = HashMap::new();
        for meta in &segment_metas {
            if let Some(key) = meta.key_info {
                if let Entry::Vacant(entry) = key_requests.entry(key.uri().to_string()) {
                    entry.insert(key.iv.to_slice());
                }
            }
        }

        // Fetch all unique keys in parallel.
        let key_futs: Vec<_> = key_requests
            .into_iter()
            .map(|(key_uri, key_iv)| {
                get_key_iv(
                    client.clone(),
                    key_uri,
                    key_iv,
                    key_query_params.clone(),
                    key_request_headers.clone(),
                    key_processor_cb.clone(),
                )
            })
            .collect();

        let mut cached_keys = HashMap::new();
        for result in join_all(key_futs).await {
            let (key_uri, key_data, _) = result?;
            cached_keys.insert(key_uri, key_data);
        }

        // Create futures to download all segments using the fetched keys.
        let segment_futs: Vec<_> = segment_metas
            .into_iter()
            .map(|meta| {
                let uri = get_segment_uri(meta.uri, base);

                let (key, iv) = if let Some(key_info) = meta.key_info {
                    let key_uri = key_info.uri().to_string();
                    let decrypted_key = cached_keys.get(&key_uri).cloned().flatten();
                    (decrypted_key, key_info.iv.to_slice())
                } else {
                    (None, None)
                };
                Self::download_segment(DownloadAesSegmentParams {
                    client: client.clone(),
                    media_sequence: meta.media_sequence as u128,
                    uri,
                    key,
                    iv,
                })
            })
            .collect();

        // Execute segment downloads in parallel and collect the results.
        let segments = join_all(segment_futs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SegmentList::new(segments))
    }

    async fn download_segment(
        params: DownloadAesSegmentParams,
    ) -> Result<Arc<RwLock<StreamSegment>>, HLSDecoderError> {
        let client = params.client;
        let uri = params.uri;
        let key = params.key;
        let iv = params.iv;
        let media_sequence = params.media_sequence;

        let resp = client.get(&uri).send().await?;
        let headers = resp.headers().clone();
        let content_length = resp.content_length().unwrap_or(0);

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|val| val.to_str().ok())
            .and_then(|ct| mediatype::MediaTypeBuf::from_string(ct.to_string()).ok())
            .map(|ct| ct.to_string())
            .unwrap_or_default();

        let stream = resp.bytes_stream().map_err(StreamItemError::from);
        let boxed_stream: ReqwestStream = Box::new(stream);
        let stream = Arc::new(RwLock::new(boxed_stream));

        let encryption_data = {
            if let Some(key) = key {
                Some(Arc::new(Box::new(SegmentEncryptionData::new(
                    key,
                    iv,
                    media_sequence,
                ))))
            } else {
                None
            }
        };
        let segment = StreamSegment::new(
            uri,
            stream,
            headers,
            content_type,
            content_length,
            encryption_data,
        );
        Ok(Arc::new(RwLock::new(segment)))
    }
}
