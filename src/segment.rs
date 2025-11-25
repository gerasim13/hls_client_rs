use std::{
    fmt::Debug,
    io,
    ops::ControlFlow,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use hls_m3u8::MediaPlaylist;
use reqwest::{header::HeaderMap, Client, Url};
use tokio::sync::RwLock;

use crate::{
    config::Config,
    errors::{HLSDecoderError, StreamItemError},
    utils::{format_range_header_bytes, ReqwestStream, ReqwestStreamItem, RANGE_HEADER_KEY},
};

#[cfg(not(feature = "aes-encryption"))]
use {
    futures::future::join_all,
    hls_m3u8::{types::DecryptionKey, Decryptable},
    std::collections::{hash_map::Entry, HashMap},
};

#[cfg(feature = "aes-encryption")]
use crate::aes::{SegmentEncryptionData, SegmentListDecryptionState};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct StreamLength {
    pub(crate) reported: u64,
    pub(crate) gathered: Option<u64>,
}

#[async_trait]
pub(crate) trait SegmentResolver<T, 'a>
where
    T: Send + Sync + 'static,
{
    async fn resolve_segments(
        client: Client,
        config: &Config,
        media_playlist: &MediaPlaylist<'a>,
        playlist_url: &Url,
    ) -> Result<SegmentList, HLSDecoderError>;
    async fn download_segment(params: T) -> Result<Arc<RwLock<StreamSegment>>, HLSDecoderError>;
    fn poll_stream(&self, cx: &mut Context<'_>, index: usize) -> Poll<Option<ReqwestStreamItem>>;
}

#[derive(Clone)]
pub(crate) struct StreamSegment {
    pub(crate) uri: Url,
    pub(crate) stream: Arc<RwLock<ReqwestStream>>,
    pub(crate) content_type: String,
    pub(crate) content_length: u64,
    pub(crate) headers: HeaderMap,
    #[cfg(feature = "aes-encryption")]
    pub(crate) encryption_data: Option<Arc<Box<SegmentEncryptionData>>>,
}

pub(crate) struct SegmentList {
    pub(crate) cached_content_length: Mutex<StreamLength>,
    pub(crate) segments: Vec<Arc<RwLock<StreamSegment>>>,
    #[cfg(feature = "aes-encryption")]
    pub(crate) decryption_state: Mutex<SegmentListDecryptionState>,
}

impl Debug for StreamSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("StreamSegment");
        debug_struct
            .field("uri", &self.uri)
            .field("content_type", &self.content_type)
            .field("content_length", &self.content_length);
        // #[cfg(feature = "aes-encryption")]
        // debug_struct.field("encryption_data", &self.encryption_data);
        debug_struct.finish()
    }
}

impl Debug for SegmentList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("SegmentList");
        debug_struct
            .field("cached_content_length", &self.cached_content_length)
            .field("segments", &self.segments);
        // #[cfg(feature = "aes-encryption")]
        // debug_struct.field("decryption_state", &self.decryption_state);
        debug_struct.finish()
    }
}

impl PartialEq for StreamSegment {
    fn eq(&self, other: &Self) -> bool {
        self.uri == other.uri
    }
}

impl StreamSegment {
    pub fn new(
        uri: Url,
        stream: Arc<RwLock<ReqwestStream>>,
        headers: HeaderMap,
        content_type: String,
        content_length: u64,
        #[cfg(feature = "aes-encryption")] encryption_data: Option<Arc<Box<SegmentEncryptionData>>>,
    ) -> Self {
        Self {
            uri,
            stream,
            content_type,
            content_length,
            headers,
            #[cfg(feature = "aes-encryption")]
            encryption_data,
        }
    }

    pub(crate) fn poll_stream(&self, cx: &mut Context<'_>) -> Poll<Option<ReqwestStreamItem>> {
        let stream_arc = &self.stream;
        if let Ok(mut guard) = stream_arc.try_write() {
            let pinned_stream = Pin::new(&mut *guard);
            pinned_stream.poll_next(cx)
        } else {
            Poll::Pending
        }
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
        *self.stream.write().await = boxed_stream;
        Ok(())
    }
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
    pub fn new(segments: Vec<Arc<RwLock<StreamSegment>>>) -> Self {
        let content_length = StreamLength {
            gathered: None,
            reported: segments
                .iter()
                .filter_map(|s| s.try_read().ok())
                .map(|s| s.content_length)
                .sum(),
        };
        Self {
            segments,
            cached_content_length: Mutex::new(content_length),
            #[cfg(feature = "aes-encryption")]
            decryption_state: Mutex::new(SegmentListDecryptionState::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn get(&self, index: usize) -> Option<&Arc<RwLock<StreamSegment>>> {
        self.segments.get(index)
    }

    /// Returns an iterator of headers for each segment.
    pub fn headers(&self) -> impl Iterator<Item = HeaderMap> + '_ {
        self.segments
            .iter()
            .filter_map(|s| s.try_read().ok())
            .map(|s| s.headers.clone())
    }

    /// Returns the content type of the first segment, if available.
    pub fn content_type(&self) -> Option<String> {
        self.segments
            .first()
            .and_then(|s| s.try_read().ok())
            .map(|s| s.content_type.clone())
    }

    /// Returns the total content length of all segments combined.
    pub fn content_length(&self) -> StreamLength {
        let cached_content_lenght_guard = self.cached_content_length.lock().unwrap();
        cached_content_lenght_guard.clone()
    }

    /// Returns an iterator of content lengths for each segment.
    pub fn content_lengths(&self) -> impl Iterator<Item = u64> + '_ {
        self.segments
            .iter()
            .filter_map(|s| s.try_read().ok())
            .map(|s| {
                #[cfg(feature = "aes-encryption")]
                {
                    if let Some(data) = &s.encryption_data {
                        let decrypted_length = data.decrypted_content_length();
                        if decrypted_length > 0 {
                            return decrypted_length;
                        }
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
        let content_length = self.content_length();
        if let Some(total_length) = content_length.gathered {
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
    pub fn find_segment(&self, uri: Url) -> Option<usize> {
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

    /// Updates the cached content length.
    pub(crate) fn update_cached_content_length(&self) {
        let mut cached_content_lenght_guard = self.cached_content_length.lock().unwrap();
        cached_content_lenght_guard.gathered = Some(self.content_lengths().sum());
    }
}

// A helper struct for downloading segments.
#[cfg(not(feature = "aes-encryption"))]
pub(crate) struct DownloadSegmentParams {
    client: Client,
    uri: Url,
}

// A helper struct to unify the processing of initialization and media segments.
#[cfg(not(feature = "aes-encryption"))]
struct SegmentMeta<'a> {
    uri: &'a str,
    key_info: Option<&'a DecryptionKey<'a>>,
}

#[cfg(not(feature = "aes-encryption"))]
#[async_trait]
impl<'a> SegmentResolver<DownloadSegmentParams, 'a> for SegmentList {
    /// Resolves all segments from a media playlist, including downloading them.
    async fn resolve_segments(
        client: Client,
        config: &Config,
        media_playlist: &MediaPlaylist<'a>,
        playlist_url: &Url,
    ) -> Result<SegmentList, HLSDecoderError> {
        let base_url = config.get_base_url();
        let base = if let Some(base) = base_url {
            base
        } else {
            Url::parse(playlist_url.as_str().trim_end_matches(".m3u8"))?
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
                let uri = base.join(meta.uri).unwrap();
                Self::download_segment(DownloadSegmentParams {
                    client: client.clone(),
                    uri,
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

    /// Downloads a single segment, returning a fully constructed `StreamSegment`.
    async fn download_segment(
        params: DownloadSegmentParams,
    ) -> Result<Arc<RwLock<StreamSegment>>, HLSDecoderError> {
        let client = params.client;
        let uri = params.uri;

        let resp = client.get(uri.clone()).send().await?;
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

    /// Polls the current stream for the next item.
    fn poll_stream(&self, cx: &mut Context<'_>, index: usize) -> Poll<Option<ReqwestStreamItem>> {
        if let Some(segment_arc) = self.segments.get(index) {
            // Try to get a read lock on the segment without blocking.
            match segment_arc.try_read() {
                Ok(segment) => match segment.poll_stream(cx) {
                    Poll::Ready(None) => {
                        self.update_cached_content_length();
                        Poll::Ready(None)
                    }
                    r => r,
                },
                Err(_) => {
                    // If the lock is held (e.g., for writing during a seek),
                    // we cannot proceed. Return Pending to be polled again later.
                    Poll::Pending
                }
            }
        } else {
            Poll::Pending
        }
    }
}
