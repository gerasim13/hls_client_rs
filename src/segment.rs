use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    io,
    ops::ControlFlow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{future::join_all, Stream, TryStreamExt};
use hls_m3u8::{types::DecryptionKey, Decryptable, MediaPlaylist};
use reqwest::{header::HeaderMap, Client, Url};
use tokio::sync::RwLock;

#[cfg(feature = "aes-encryption")]
use {
    crate::config::KeyProcessorCallback,
    aes::Aes128,
    bytes::Bytes,
    cbc::{
        cipher::{block_padding::Pkcs7, Block, BlockDecryptMut, KeyIvInit},
        Decryptor,
    },
    futures::StreamExt,
    reqwest::StatusCode,
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use crate::{
    errors::{HLSDecoderError, StreamItemError},
    utils::{format_range_header_bytes, ReqwestStream, ReqwestStreamItem, RANGE_HEADER_KEY},
};

#[cfg(feature = "aes-encryption")]
#[derive(Debug)]
struct DecryptionState {
    key: [u8; 16],
    iv: [u8; 16],
    buffer: Vec<u8>,
    decryptor: Decryptor<Aes128>,
}

#[derive(Debug)]
pub(crate) struct SegmentList {
    segments: Vec<Arc<RwLock<StreamSegment>>>,
    #[cfg(feature = "aes-encryption")]
    decryption_state: Mutex<Option<DecryptionState>>,
}

#[derive(Clone)]
pub(crate) struct StreamSegment {
    pub(crate) uri: String,
    pub(crate) stream: Arc<RwLock<ReqwestStream>>,
    pub(crate) content_type: String,
    pub(crate) content_length: u64,
    pub(crate) headers: HeaderMap,
    #[cfg(feature = "aes-encryption")]
    pub(crate) key: Option<[u8; 16]>,
    #[cfg(feature = "aes-encryption")]
    pub(crate) iv: Option<[u8; 16]>,
    #[cfg(feature = "aes-encryption")]
    pub(crate) media_sequence: u128,
    #[cfg(feature = "aes-encryption")]
    pub(crate) decrypted_content_length: Arc<AtomicU64>,
}

impl Debug for StreamSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("StreamSegment");
        debug_struct
            .field("uri", &self.uri)
            .field("content_type", &self.content_type)
            .field("content_length", &self.content_length);
        #[cfg(feature = "aes-encryption")]
        {
            debug_struct
                .field("decrypted_content_length", &self.decrypted_content_length)
                .field("media_sequence", &self.media_sequence)
                .field("key", &self.key)
                .field("iv", &self.iv);
        }
        debug_struct.finish()
    }
}

impl PartialEq for StreamSegment {
    fn eq(&self, other: &Self) -> bool {
        self.uri == other.uri
    }
}

impl StreamSegment {
    #[cfg(not(feature = "aes-encryption"))]
    pub fn new(
        uri: String,
        stream: Arc<RwLock<ReqwestStream>>,
        headers: HeaderMap,
        content_type: String,
        content_length: u64,
    ) -> Self {
        Self {
            uri,
            stream,
            content_type,
            content_length,
            headers,
        }
    }

    #[cfg(feature = "aes-encryption")]
    pub fn new(
        uri: String,
        key: Option<[u8; 16]>,
        iv: Option<[u8; 16]>,
        stream: Arc<RwLock<ReqwestStream>>,
        headers: HeaderMap,
        content_type: String,
        content_length: u64,
        media_sequence: u128,
    ) -> Self {
        let decrypted_content_length = Arc::new(AtomicU64::new(0));
        Self {
            uri,
            key,
            iv,
            stream,
            content_type,
            content_length,
            headers,
            decrypted_content_length,
            media_sequence,
        }
    }

    fn poll_stream(&self, cx: &mut Context<'_>) -> Poll<Option<ReqwestStreamItem>> {
        let stream_arc = &self.stream;
        if let Ok(mut guard) = stream_arc.try_write() {
            let pinned_stream = Pin::new(&mut *guard);
            pinned_stream.poll_next(cx)
        } else {
            Poll::Pending
        }
    }

    pub(crate) async fn replace_stream(&mut self, new_stream: ReqwestStream) {
        *self.stream.write().await = new_stream;
    }

    pub(crate) async fn reconnect(
        &mut self,
        start: Option<u64>,
        end: Option<u64>,
    ) -> io::Result<()> {
        let client = Client::new();
        let segment_uri = self.uri.clone();
        #[cfg(feature = "tracing")]
        let request_start = std::time::Instant::now();
        let resp = {
            if let Some(start) = start {
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    "sending HTTP range request to {} (local offset: {}, local end: {:?})",
                    segment_uri,
                    start,
                    end
                );
                client
                    .get(segment_uri)
                    .header(RANGE_HEADER_KEY, format_range_header_bytes(start, end))
                    .send()
                    .await
            } else {
                client.get(segment_uri).send().await
            }
        }
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        #[cfg(feature = "tracing")]
        tracing::trace!(
            "HTTP range request finished in {:?}",
            request_start.elapsed()
        );

        let stream = resp.bytes_stream().map_err(StreamItemError::from);
        let boxed_stream: ReqwestStream = Box::new(stream);
        let stream = Arc::new(RwLock::new(boxed_stream));

        self.stream = stream;
        Ok(())
    }
}

fn get_segment_uri(uri: &str, base_url: &str) -> String {
    if uri.starts_with("http") {
        uri.to_string()
    } else {
        format!("{}/{}", base_url, uri)
    }
}

/// Downloads a single segment, returning a fully constructed `StreamSegment`.
#[cfg(not(feature = "aes-encryption"))]
async fn download_segment(
    client: Client,
    uri: String,
) -> Result<Arc<RwLock<StreamSegment>>, HLSDecoderError> {
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

    let segment = StreamSegment::new(uri, stream, headers, content_type, content_length);
    Ok(Arc::new(RwLock::new(segment)))
}

#[cfg(feature = "aes-encryption")]
async fn download_segment(
    client: Client,
    uri: String,
    key: Option<[u8; 16]>,
    iv: Option<[u8; 16]>,
    media_sequence: u128,
) -> Result<Arc<RwLock<StreamSegment>>, HLSDecoderError> {
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

    let segment = StreamSegment::new(
        uri,
        key,
        iv,
        stream,
        headers,
        content_type,
        content_length,
        media_sequence,
    );
    Ok(Arc::new(RwLock::new(segment)))
}

#[cfg(feature = "aes-encryption")]
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

/// Resolves all segments from a media playlist, including downloading them.
#[cfg(not(feature = "aes-encryption"))]
pub(crate) async fn resolve_segments<'a>(
    client: Client,
    media_playlist: &MediaPlaylist<'a>,
    playlist_url: &Url,
    base_url: Option<&Url>,
) -> Result<SegmentList, HLSDecoderError> {
    let base = if let Some(base) = base_url {
        base.as_str()
    } else {
        playlist_url.as_str().trim_end_matches(".m3u8")
    };

    // Collect metadata for all segments into a single list.
    let mut segment_metas: Vec<SegmentMeta> = Vec::new();
    if let Some(first_segment) = media_playlist.segments.get(0) {
        if let Some(ref map) = first_segment.map {
            segment_metas.push(SegmentMeta {
                uri: map.uri(),
                key_info: map.keys().get(0).cloned(),
            });
        }
    }
    for (_, segment) in media_playlist.segments.iter() {
        segment_metas.push(SegmentMeta {
            uri: segment.uri(),
            key_info: segment.keys().get(0).cloned(),
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

    // Create futures to download all segments using the fetched keys.
    let segment_futs: Vec<_> = segment_metas
        .into_iter()
        .map(|meta| {
            let uri = get_segment_uri(meta.uri, base);
            download_segment(client.clone(), uri)
        })
        .collect();

    // Execute segment downloads in parallel and collect the results.
    let segments = join_all(segment_futs)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SegmentList::new(segments))
}

#[cfg(feature = "aes-encryption")]
pub(crate) async fn resolve_segments<'a>(
    client: Client,
    media_playlist: &MediaPlaylist<'a>,
    playlist_url: &Url,
    base_url: Option<&Url>,
    key_query_params: Option<HashMap<String, String>>,
    key_request_headers: Option<HashMap<String, String>>,
    key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
) -> Result<SegmentList, HLSDecoderError> {
    let base = if let Some(base) = base_url {
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
            download_segment(client.clone(), uri, key, iv, meta.media_sequence as u128)
        })
        .collect();

    // Execute segment downloads in parallel and collect the results.
    let segments = join_all(segment_futs)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SegmentList::new(segments))
}

// A helper struct to unify the processing of initialization and media segments.
struct SegmentMeta<'a> {
    uri: &'a str,
    key_info: Option<&'a DecryptionKey<'a>>,
    #[cfg(feature = "aes-encryption")]
    media_sequence: usize,
}

// A helper enum to represent the result of seeking within a segment.
#[derive(Debug, PartialEq, Eq)]
pub enum SeekResult {
    Located {
        index: usize,
        local_start: Option<u64>,
        local_end: Option<u64>,
    },
    EndOfStream,
    OutOfBounds,
}

// SegmentList implementation
impl SegmentList {
    fn new(segments: Vec<Arc<RwLock<StreamSegment>>>) -> Self {
        Self {
            segments,
            #[cfg(feature = "aes-encryption")]
            decryption_state: Mutex::new(None),
        }
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn get(&self, index: usize) -> Option<&Arc<RwLock<StreamSegment>>> {
        self.segments.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Arc<RwLock<StreamSegment>>> {
        self.segments.get_mut(index)
    }

    /// Returns the content type of the first segment, if available.
    fn content_type(&self) -> Option<String> {
        self.segments
            .first()
            .and_then(|s| s.try_read().ok())
            .map(|s| s.content_type.clone())
    }

    /// Returns the total content length of all segments combined.
    pub fn content_length(&self) -> Option<u64> {
        Some(self.content_lengths().sum())
    }

    /// Returns an iterator of content lengths for each segment.
    fn content_lengths(&self) -> impl Iterator<Item = u64> + '_ {
        self.segments
            .iter()
            .filter_map(|s| s.try_read().ok())
            .map(|s| {
                #[cfg(feature = "aes-encryption")]
                {
                    let decrypted_content_length =
                        s.decrypted_content_length.load(Ordering::SeqCst);
                    if decrypted_content_length > 0 {
                        return decrypted_content_length;
                    }
                }
                s.content_length
            })
    }

    /// Returns whether the segments support range requests.
    pub fn supports_range_request(&self) -> Option<bool> {
        let result = self.for_each_header(|h| {
            if h.contains_key("Accept-Ranges") {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });
        match result {
            Some(ControlFlow::Continue(_)) => Some(true),
            Some(ControlFlow::Break(_)) => Some(false),
            None => None,
        }
    }

    /// Returns an iterator of headers for each segment.
    fn headers(&self) -> impl Iterator<Item = HeaderMap> + '_ {
        self.segments
            .iter()
            .filter_map(|s| s.try_read().ok())
            .map(|s| s.headers.clone())
    }

    /// Iterates over each segment's headers and applies a function to them.
    fn for_each_header<B, F>(&self, mut f: F) -> Option<ControlFlow<B>>
    where
        F: FnMut(HeaderMap) -> ControlFlow<B>,
    {
        for header in self.headers() {
            if let ControlFlow::Break(b) = f(header) {
                return Some(ControlFlow::Break(b));
            }
        }
        Some(ControlFlow::Continue(()))
    }

    /// Seeks to a specific segment within the list.
    pub fn seek_segment(&self, start: u64, end: Option<u64>) -> SeekResult {
        if let Some(total_length) = self.content_length() {
            if start == total_length && total_length != 0 {
                return SeekResult::EndOfStream;
            }
        }

        let mut cumulative = 0;
        let mut segment_end = 0;
        let mut selected_index = None;
        for (i, len) in self.content_lengths().enumerate() {
            if start < cumulative + len {
                selected_index = Some(i);
                segment_end = cumulative + len;
                break;
            }
            cumulative += len;
        }

        if let Some(idx) = selected_index {
            let local_start = start - cumulative;
            let local_end = end.map(|e| e.min(segment_end) - cumulative);
            SeekResult::Located {
                index: idx,
                local_start: Some(local_start),
                local_end: local_end,
            }
        } else {
            SeekResult::OutOfBounds
        }
    }

    /// Finds a segment by its URI.
    pub fn find_segment(&self, uri: String) -> Option<usize> {
        self.segments
            .iter()
            .position(|s| s.try_read().map_or(false, |guard| guard.uri == uri))
    }

    /// Replaces a segment with a new one.
    pub async fn replace_segment(&mut self, new_segment: Arc<RwLock<StreamSegment>>) {
        let index = self
            .find_segment(new_segment.read().await.uri.clone())
            .unwrap();
        let current_segment = self.segments.get_mut(index).expect("Segment should exist");

        #[cfg(feature = "tracing")]
        tracing::debug!("Restoring old stream at index {}", index);
        *current_segment = new_segment;
    }

    /// Polls the current stream for the next item.
    #[cfg(not(feature = "aes-encryption"))]
    pub fn poll_stream(
        &self,
        cx: &mut Context<'_>,
        index: usize,
    ) -> Option<Poll<Option<ReqwestStreamItem>>> {
        let segment_arc = self.segments.get(index)?;
        // Try to get a read lock on the segment without blocking.
        match segment_arc.try_read() {
            Ok(segment) => Some(segment.poll_stream(cx)),
            Err(_) => {
                // If the lock is held (e.g., for writing during a seek),
                // we cannot proceed. Return Pending to be polled again later.
                Some(Poll::Pending)
            }
        }
    }

    #[cfg(feature = "aes-encryption")]
    pub fn poll_stream(
        &self,
        cx: &mut Context<'_>,
        index: usize,
    ) -> Option<Poll<Option<ReqwestStreamItem>>> {
        let segment_arc = self.segments.get(index)?;
        // Try to get a read lock on the segment without blocking.
        match segment_arc.try_read() {
            Ok(segment) => {
                // If segment is not encrypted just poll its inner stream.
                if segment.key.is_none() {
                    *self.decryption_state.lock().unwrap() = None;
                    return Some(segment.poll_stream(cx));
                }
                let (key, iv_from_playlist) = (segment.key.unwrap(), segment.iv);
                let final_iv =
                    iv_from_playlist.unwrap_or_else(|| segment.media_sequence.to_be_bytes());

                let mut state_guard = self.decryption_state.lock().unwrap();
                let reinitialize = match &*state_guard {
                    Some(state) => state.key != key || state.iv != final_iv,
                    None => true,
                };
                if reinitialize {
                    *state_guard = Some(DecryptionState {
                        key,
                        iv: final_iv,
                        buffer: Vec::with_capacity(32),
                        decryptor: Decryptor::<Aes128>::new(&key.into(), &final_iv.into()),
                    });
                }

                let state = state_guard.as_mut().unwrap();

                loop {
                    match segment.poll_stream(cx) {
                        Poll::Ready(Some(Ok(new_bytes))) => {
                            state.buffer.extend_from_slice(&new_bytes)
                        }
                        Poll::Ready(None) => {
                            if state.buffer.is_empty() {
                                return Some(Poll::Ready(None));
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
                                segment
                                    .decrypted_content_length
                                    .fetch_add(final_chunk.len() as u64, Ordering::SeqCst);
                            }

                            state.buffer.clear();
                            return Some(Poll::Ready(Some(final_chunk_res)));
                        }
                        Poll::Pending => return Some(Poll::Pending),
                        Poll::Ready(Some(Err(e))) => return Some(Poll::Ready(Some(Err(e)))),
                    }
                }
            }
            Err(_) => {
                // If the lock is held (e.g., for writing during a seek),
                // we cannot proceed. Return Pending to be polled again later.
                Some(Poll::Pending)
            }
        }
    }
}
