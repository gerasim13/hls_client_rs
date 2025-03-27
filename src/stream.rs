use std::{
    io,
    ops::Div,
    pin::Pin,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use bytes::Bytes;
use futures::Stream;
use hls_m3u8::{tags::VariantStream, types::PlaylistType, MasterPlaylist, MediaPlaylist};
use mediatype::MediaTypeBuf;
use reqwest::{
    header::{self, HeaderMap},
    IntoUrl, Url,
};

#[cfg(feature = "stream_download")]
use stream_download::source::SourceStream;

use crate::{config::Config, errors::HLSDecoderError};

pub type ReqwestStreamItem = Result<Bytes, reqwest::Error>;
pub type ReqwestStream = Box<dyn Stream<Item = ReqwestStreamItem> + Unpin + Send + Sync>;

struct StreamDetails {
    content_length: Option<u64>,
    content_type: Option<String>,
    headers: Vec<HeaderMap>,
    segments: Vec<String>,
    content_lengths: Vec<u64>,
    streams: Vec<Arc<RwLock<ReqwestStream>>>,
    current_index: usize,
    target_duration: Duration,
    finished: bool,
}

pub struct HLSStream {
    media_playlist_url: Url,
    stream_details: Arc<RwLock<StreamDetails>>,
}

impl HLSStream {
    async fn handle_master_playlist(
        playlist: &str,
        src: &str,
        stream_selection_cb: Option<
            Arc<Box<dyn Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync>>,
        >,
    ) -> Result<Option<String>, HLSDecoderError> {
        if let Ok(master_playlist) = hls_m3u8::MasterPlaylist::try_from(playlist) {
            #[cfg(feature = "tracing")]
            tracing::info!("Master playlist detected");

            let stream_selection_cb = stream_selection_cb
                .unwrap_or(Arc::new(Box::new(|p| p.variant_streams.first().cloned())));

            if let Some(variant) = stream_selection_cb(master_playlist) {
                let uri = match variant {
                    VariantStream::ExtXStreamInf { uri, .. } => uri,
                    VariantStream::ExtXIFrame { uri, .. } => uri,
                };

                let stream_url = if uri.starts_with("http") {
                    uri.to_string()
                } else {
                    // Resolve relative path
                    let base_url = src.trim_end_matches(".m3u8");
                    format!("{}/{}", base_url, uri)
                };

                #[cfg(feature = "tracing")]
                tracing::info!("Switching to media playlist: {}", stream_url);
                return Ok(Some(stream_url));
            } else {
                return Err(HLSDecoderError::NoStreamsError);
            }
        }

        Ok(None)
    }

    async fn handle_media_playlist<'a>(
        playlist: &'a str,
    ) -> Result<MediaPlaylist<'a>, HLSDecoderError> {
        let playlist = hls_m3u8::MediaPlaylist::try_from(playlist)?;

        #[cfg(feature = "tracing")]
        tracing::info!("Media playlist detected");

        Ok(playlist)
    }

    async fn parse_playlist(media_playlist_url: &Url) -> Result<StreamDetails, HLSDecoderError> {
        let playlist = reqwest::get(media_playlist_url.as_str())
            .await?
            .text()
            .await?;
        let media_playlist = Self::handle_media_playlist(playlist.as_str()).await?;

        // By default don't treat stream as Event but as VOD
        let is_infinite_stream =
            media_playlist.playlist_type.unwrap_or(PlaylistType::Event) == PlaylistType::Event;

        #[cfg(feature = "tracing")]
        tracing::debug!("Stream is infinite {}", is_infinite_stream);

        let segments = media_playlist
            .segments
            .into_iter()
            .map(|(_, segment)| {
                if segment.uri().starts_with("http") {
                    segment.uri().to_string()
                } else {
                    // Resolve relative path
                    let base_url = media_playlist_url.as_str().trim_end_matches(".m3u8");
                    format!("{}/{}", base_url, segment.uri())
                }
            })
            .collect::<Vec<_>>();

        let futures = segments
            .clone()
            .into_iter()
            .map(reqwest::get)
            .collect::<Vec<_>>();

        let segment_resps = futures::future::join_all(futures)
            .await
            .into_iter()
            .map(|resp| resp.map_err(HLSDecoderError::from))
            .collect::<Vec<_>>();

        let segment_streams = segment_resps.into_iter().flatten().collect::<Vec<_>>();

        let content_lengths = segment_streams
            .iter()
            .map(|resp| resp.content_length().unwrap_or(0))
            .collect();

        let content_length = if is_infinite_stream {
            None
        } else {
            segment_streams
                .iter()
                .filter_map(|resp| resp.content_length())
                .reduce(|acc, content_len| acc + content_len)
        };

        let content_type = segment_streams
            .first()
            .and_then(|r| {
                r.headers().get(header::CONTENT_TYPE).and_then(|val| {
                    val.to_str()
                        .inspect_err(|_e| {
                            #[cfg(feature = "tracing")]
                            tracing::warn!("error converting header value: {_e:?}")
                        })
                        .ok()
                })
            })
            .map_or_else(
                || {
                    #[cfg(feature = "tracing")]
                    tracing::warn!("content type header missing");
                    None
                },
                |content_type| {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(content_type, "received content type");
                    match content_type.parse::<MediaTypeBuf>() {
                        Ok(content_type) => Some(content_type.to_string()),
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!("error parsing content type: {_e:?}");
                            None
                        }
                    }
                },
            );

        let headers = segment_streams
            .iter()
            .map(|resp| resp.headers().clone())
            .collect();

        let streams = segment_streams
            .into_iter()
            .map(|resp| Arc::new(RwLock::new(Box::new(resp.bytes_stream()) as ReqwestStream)))
            .collect::<Vec<_>>();

        Ok(StreamDetails {
            content_length,
            content_type,
            headers,
            segments,
            content_lengths,
            streams,
            target_duration: media_playlist.target_duration,
            current_index: 0,
            finished: false,
        })
    }

    async fn reload_playlist(
        media_playlist_url: &Url,
        stream_details: Arc<RwLock<StreamDetails>>,
        reconnect: bool,
    ) -> Result<bool, HLSDecoderError> {
        #[cfg(feature = "tracing")]
        tracing::debug!("Reloading playlist");

        let resp = Self::parse_playlist(media_playlist_url).await?;

        let should_seek = {
            let mut stream_details = stream_details.write().unwrap();
            let old_index = stream_details.current_index;
            let old_active_stream = stream_details.streams.get(old_index).cloned();
            let old_url = if stream_details.finished {
                None
            } else {
                stream_details.segments.get(old_index).cloned()
            };

            *stream_details = resp;

            // If our old url exists in the new URL list, restore the position, otherwise start from 0
            let (new_current_index, should_seek) = if let Some(old_url) = old_url.as_ref() {
                if let Some(new_index) = stream_details
                    .segments
                    .iter()
                    .position(|url| url == old_url)
                {
                    (new_index, true)
                } else {
                    (0, false)
                }
            } else {
                (0, false)
            };

            #[cfg(feature = "tracing")]
            tracing::debug!(
                "old_url: {:?}, old_index: {}, newsegments: {:?}",
                old_url,
                old_index,
                stream_details.segments
            );
            stream_details.current_index = new_current_index;
            stream_details.finished = false;
            // If we don't need to reconnect, restore the old stream which was ongoing so we have continuity
            if should_seek && !reconnect {
                if let Some(old_active_stream) = old_active_stream {
                    let item = stream_details
                        .streams
                        .get_mut(new_current_index)
                        .expect("Stream should exist");

                    #[cfg(feature = "tracing")]
                    tracing::debug!("Restoring old stream at index {}", new_current_index);
                    *item = old_active_stream;

                    // No need to seek if we're replacing the old active stream
                    return Ok(false);
                }
            }
            should_seek
        };

        Ok(should_seek)
    }

    fn spawn_reload_loop(&self) {
        let stream_details = self.stream_details.clone();
        let media_playlist = self.media_playlist_url.clone();
        tokio::spawn(async move {
            let stream_details = stream_details.clone();
            let mut target_duration = stream_details.read().unwrap().target_duration.clone();
            loop {
                tokio::time::sleep(target_duration).await;
                let resp =
                    Self::reload_playlist(&media_playlist, stream_details.clone(), false).await;
                if let Ok(_) = resp {
                    target_duration = stream_details.read().unwrap().target_duration.clone();
                } else {
                    break;
                }
            }
        });
    }

    pub async fn try_new(config: Config) -> Result<Self, HLSDecoderError> {
        let resp = reqwest::get(config.get_url()).await?;
        let playlist_str = resp.text().await?;

        let playlist = if let Some(media_playlist) = Self::handle_master_playlist(
            playlist_str.as_str(),
            config.get_url().as_str(),
            config.get_stream_selection_cb(),
        )
        .await?
        {
            media_playlist
        } else {
            config.get_url().as_str().to_string()
        };

        let resp = Self::parse_playlist(&playlist.parse()?).await?;
        let stream_details = Arc::new(RwLock::new(resp));

        let ret = Self {
            media_playlist_url: playlist.parse()?,
            stream_details,
        };

        ret.spawn_reload_loop();
        Ok(ret)
    }

    pub fn content_type(&self) -> Option<String> {
        self.stream_details.read().unwrap().content_type.clone()
    }

    pub fn headers(&self) -> Vec<HeaderMap> {
        self.stream_details.read().unwrap().headers.clone()
    }

    fn supports_range_request(&self) -> bool {
        self.headers()
            .iter()
            .all(|res| res.contains_key("Accept-Ranges"))
    }

    async fn new_req_with_range(
        url: impl IntoUrl,
        start: u64,
        end: Option<u64>,
    ) -> Result<reqwest::Response, HLSDecoderError> {
        let client = reqwest::Client::new();
        Ok(client
            .get(url)
            .header(RANGE_HEADER_KEY, format_range_header_bytes(start, end))
            .send()
            .await?)
    }

    pub async fn handle_seek(&mut self, start: u64, end: Option<u64>) -> io::Result<()> {
        let content_lengths = self.stream_details.read().unwrap().content_lengths.clone();
        let total_length: u64 = content_lengths.iter().sum();
        if start == total_length && total_length != 0 {
            #[cfg(feature = "tracing")]
            tracing::trace!("seek position is at total length; setting stream to empty");
            let mut stream_details = self.stream_details.write().unwrap();
            stream_details.current_index = 0;
            stream_details.finished = true;
            return Ok(());
        }

        if !self.supports_range_request() {
            #[cfg(feature = "tracing")]
            tracing::warn!("Accept-Ranges header not present. Attempting seek anyway.");
        }

        let mut cumulative = 0;
        let mut selected_index = None;
        for (i, &len) in content_lengths.iter().enumerate() {
            if start < cumulative + len {
                selected_index = Some(i);
                break;
            }
            cumulative += len;
        }
        let idx = selected_index
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "start offset out of range"))?;

        let local_start = start - cumulative;
        let local_end = end.map(|e| e - cumulative);

        let selected_url = &self.stream_details.read().unwrap().segments[idx].clone();
        #[cfg(feature = "tracing")]
        tracing::trace!(
            "sending HTTP range request to {} (local offset: {}, local end: {:?})",
            selected_url,
            local_start,
            local_end
        );

        #[cfg(feature = "tracing")]
        let request_start = std::time::Instant::now();
        let response = Self::new_req_with_range(
            selected_url,
            local_start,
            local_end.map(|le| le.saturating_sub(1)),
        )
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        #[cfg(feature = "tracing")]
        tracing::trace!(
            "HTTP range request finished in {:?}",
            request_start.elapsed()
        );

        // Replace the stream for the selected segment with the new one.
        let mut stream_details = self.stream_details.write().unwrap();
        stream_details.streams[idx] = Arc::new(RwLock::new(
            Box::new(response.bytes_stream()) as ReqwestStream
        ));
        stream_details.current_index = idx;

        Ok(())
    }

    pub fn content_length(&self) -> Option<u64> {
        self.stream_details.read().unwrap().content_length.clone()
    }
}

impl Stream for HLSStream {
    type Item = ReqwestStreamItem;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        #[cfg(feature = "tracing")]
        tracing::trace!(
            "poll_next called, current_index: {}",
            this.stream_details.read().unwrap().current_index
        );

        let mut stream_details = this.stream_details.read().unwrap();

        // If the stream is inifinite, always return pending. Otherwise tell the reader that we've exhausted the streams
        let finished_return = if stream_details.content_length.is_none_or(|c| c == 0) {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(None)
        };

        // Return back pending if we're supposed to wait for a playlist reload
        if stream_details.finished {
            return finished_return;
        }
        while stream_details.current_index < stream_details.streams.len() {
            let idx = stream_details.current_index;
            {
                let stream_arc = &stream_details.streams[idx];
                if let Ok(mut guard) = stream_arc.try_write() {
                    let pinned_stream = Pin::new(&mut *guard);
                    match pinned_stream.poll_next(cx) {
                        std::task::Poll::Ready(Some(item)) => {
                            match &item {
                                Ok(_bytes) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::trace!(
                                        "Stream at index {} yielded {} bytes",
                                        idx,
                                        _bytes.len()
                                    );
                                }
                                Err(_e) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::trace!(
                                        "Stream at index {} yielded error: {:?}",
                                        idx,
                                        _e
                                    );
                                }
                            }
                            return std::task::Poll::Ready(Some(item));
                        }
                        std::task::Poll::Ready(None) => {
                            // Move to the next stream if the current one is exhausted
                            #[cfg(feature = "tracing")]
                            tracing::trace!("Stream at index {} is exhausted", idx);
                        }
                        std::task::Poll::Pending => {
                            return std::task::Poll::Pending;
                        }
                    }
                } else {
                    // Unable to acquire the lock immediately, so yield Pending.
                    return std::task::Poll::Pending;
                }
            }

            drop(stream_details);

            this.stream_details.write().unwrap().current_index += 1;
            stream_details = this.stream_details.read().unwrap();
        }

        #[cfg(feature = "tracing")]
        tracing::trace!(
            "Readable streams ended. Returning {}",
            if finished_return.is_pending() {
                "pending"
            } else {
                "exhausted"
            }
        );
        this.stream_details.write().unwrap().finished = true;
        finished_return
    }
}

#[cfg(feature = "stream_download")]
impl SourceStream for HLSStream {
    type Params = Config;

    type StreamCreationError = HLSDecoderError;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Self::try_new(params).await
    }

    fn content_length(&self) -> Option<u64> {
        self.content_length()
    }

    async fn seek_range(&mut self, start: u64, end: Option<u64>) -> io::Result<()> {
        self.handle_seek(start, end).await
    }

    async fn reconnect(&mut self, current_position: u64) -> Result<(), io::Error> {
        let should_seek =
            Self::reload_playlist(&self.media_playlist_url, self.stream_details.clone(), true)
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        if should_seek && self.supports_range_request() {
            self.seek_range(current_position, None).await?;
        }
        Ok(())
    }

    fn supports_seek(&self) -> bool {
        true
    }
}

/// Reference: https://github.com/aschey/stream-download-rs/blob/74bd011587b2a4f67c12f870f92ca8368991ec99/src/http/mod.rs#L363C1-L374C2
/// HTTP range header key
pub const RANGE_HEADER_KEY: &str = "Range";

/// Utility function to format a range header for requesting bytes.
///
/// ex: `bytes=200-400`
pub fn format_range_header_bytes(start: u64, end: Option<u64>) -> String {
    format!(
        "bytes={start}-{}",
        end.map(|e| e.to_string()).unwrap_or_default()
    )
}
