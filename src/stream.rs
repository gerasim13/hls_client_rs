use std::{
    fs, io,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use futures::Stream;
use hls_m3u8::{tags::VariantStream, MasterPlaylist, MediaPlaylist};
use reqwest::{header::HeaderMap, IntoUrl, Url};

use crate::{
    config::Config,
    errors::HLSDecoderError,
    utils::{
        build_streams, compute_content_lengths, compute_overall_content_length,
        extract_content_type, extract_headers, fetch_playlist, fetch_segment_responses,
        is_infinite_stream, parse_media_playlist, resolve_segment_urls, ReqwestStream,
        ReqwestStreamItem,
    },
};

pub(crate) struct StreamDetails {
    media_playlist: MediaPlaylist<'static>,
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
    pub(crate) media_playlist_url: Url,
    pub(crate) base_url: Option<Url>,
    pub(crate) stream_details: Arc<RwLock<StreamDetails>>,
}

impl HLSStream {
    pub async fn try_new(config: Config) -> Result<Self, HLSDecoderError> {
        let url = config.get_url();

        let playlist_contents = match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| HLSDecoderError::MissingURLError)?;

                fs::read_to_string(&path).map_err(|e| HLSDecoderError::IoError(e))?
            }
            _ => {
                let resp = reqwest::get(url).await.map_err(HLSDecoderError::from)?;
                resp.text().await.map_err(HLSDecoderError::from)?
            }
        };

        let playlist = if let Some(media_playlist) = Self::handle_master_playlist(
            &playlist_contents,
            config.get_url().as_str(),
            config.get_base_url(),
            config.get_stream_selection_cb(),
        )
        .await?
        {
            media_playlist
        } else {
            config.get_url().as_str().to_string()
        };

        let resp = Self::parse_playlist(&playlist.parse()?, config.get_base_url().as_ref()).await?;
        let stream_details = Arc::new(RwLock::new(resp));

        let ret = Self {
            media_playlist_url: playlist.parse()?,
            base_url: config.get_base_url(),
            stream_details,
        };

        // Reload playlist only for infinite streams
        if ret.content_length().is_none() {
            ret.spawn_reload_loop();
        }

        Ok(ret)
    }

    async fn handle_master_playlist(
        playlist: &str,
        src: &str,
        base_url: Option<Url>,
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
                } else if let Some(base) = base_url {
                    format!("{}/{}", base, uri)
                } else {
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

    async fn parse_playlist(
        media_playlist_url: &Url,
        base_url: Option<&Url>,
    ) -> Result<StreamDetails, HLSDecoderError> {
        let playlist_text = fetch_playlist(media_playlist_url).await?;
        let media_playlist = parse_media_playlist(&playlist_text).await?.into_owned();
        let is_infinite_stream = is_infinite_stream(&media_playlist);
        let segment_urls = resolve_segment_urls(&media_playlist, media_playlist_url, base_url);
        let segment_responses = fetch_segment_responses(&segment_urls).await;
        let content_lengths = compute_content_lengths(&segment_responses);
        let content_length = if is_infinite_stream {
            None
        } else {
            compute_overall_content_length(&segment_responses)
        };
        let content_type = extract_content_type(segment_responses.first());
        let headers = extract_headers(&segment_responses);
        let streams = build_streams(segment_responses);

        Ok(StreamDetails {
            content_length,
            content_type,
            headers,
            segments: segment_urls,
            content_lengths,
            streams,
            target_duration: media_playlist.target_duration,
            current_index: 0,
            finished: false,
            media_playlist,
        })
    }

    pub(crate) async fn reload_playlist(
        media_playlist_url: &Url,
        stream_details: Arc<RwLock<StreamDetails>>,
        base_url: Option<&Url>,
        reconnect: bool,
    ) -> Result<bool, HLSDecoderError> {
        #[cfg(feature = "tracing")]
        tracing::debug!("Reloading playlist");

        let resp = Self::parse_playlist(media_playlist_url, base_url).await?;

        let should_seek = {
            let mut stream_details = stream_details.write().unwrap();
            let old_index = stream_details.current_index;
            let old_active_stream = stream_details.streams.get(old_index).cloned();
            let old_url = if stream_details.finished {
                None
            } else {
                stream_details.segments.get(old_index).cloned()
            };

            #[cfg(feature = "tracing")]
            tracing::debug!(
                "old_url: {:?}, old_index: {}, newsegments: {:?}",
                old_url,
                old_index,
                stream_details.segments
            );

            *stream_details = resp;

            // Find the old URL in the new segments list to maintain playback position
            let (new_current_index, should_seek) = match old_url.as_ref() {
                Some(url) => stream_details
                    .segments
                    .iter()
                    .position(|segment_url| segment_url == url)
                    .map_or((0, false), |idx| (idx, true)),
                None => (0, false),
            };

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
        let base_url = self.base_url.clone();
        tokio::spawn(async move {
            let stream_details = stream_details.clone();
            let mut target_duration = stream_details.read().unwrap().target_duration;
            loop {
                tokio::time::sleep(target_duration).await;
                let resp = Self::reload_playlist(
                    &media_playlist,
                    stream_details.clone(),
                    base_url.as_ref(),
                    false,
                )
                .await;

                if resp.is_ok() {
                    target_duration = stream_details.read().unwrap().target_duration;
                } else {
                    break;
                }
            }
        });
    }

    pub fn content_type(&self) -> Option<String> {
        self.stream_details.read().unwrap().content_type.clone()
    }

    pub fn headers(&self) -> Vec<HeaderMap> {
        self.stream_details.read().unwrap().headers.clone()
    }

    pub(crate) fn supports_range_request(&self) -> bool {
        self.headers()
            .iter()
            .all(|res| res.contains_key("Accept-Ranges"))
    }

    pub fn get_media_playlist(&self) -> MediaPlaylist<'static> {
        return self.stream_details.read().unwrap().media_playlist.clone();
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
        let mut segment_end = 0;
        let mut selected_index = None;
        for (i, &len) in content_lengths.iter().enumerate() {
            if start < cumulative + len {
                selected_index = Some(i);
                segment_end = cumulative + len;
                break;
            }
            cumulative += len;
        }
        let idx = selected_index
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "start offset out of range"))?;

        let local_start = start - cumulative;
        let local_end = end.map(|e| e.min(segment_end) - cumulative);

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
        stream_details.finished = false;

        Ok(())
    }

    pub fn content_length(&self) -> Option<u64> {
        self.stream_details.read().unwrap().content_length
    }
}

impl Stream for HLSStream {
    type Item = ReqwestStreamItem;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        let mut stream_details = this.stream_details.read().unwrap();

        // Check if we're already finished
        if stream_details.finished {
            return get_finished_poll_result(&stream_details);
        }

        // Process current streams
        while stream_details.current_index < stream_details.streams.len() {
            if let Some(poll_result) = poll_current_stream(&mut stream_details, cx) {
                return poll_result;
            }

            // Move to next stream
            drop(stream_details);
            this.stream_details.write().unwrap().current_index += 1;
            stream_details = this.stream_details.read().unwrap();
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Finished all streams, ending stream if finite");

        // After the loop ends, stream_details will always be locked by the last iteration
        let res = get_finished_poll_result(&stream_details);
        drop(stream_details);

        // Mark as finished if we've exhausted all streams
        // This doesn't always mean that the stream has ended. In case of infinite streams, we just have to wait for a playlist refresh
        this.stream_details.write().unwrap().finished = true;
        return res;
    }
}

fn get_finished_poll_result(
    stream_details: &StreamDetails,
) -> std::task::Poll<Option<ReqwestStreamItem>> {
    // If the stream is infinite, always return pending. Otherwise tell the reader that we've exhausted the streams
    if stream_details.content_length.is_none_or(|c| c == 0) {
        std::task::Poll::Pending
    } else {
        std::task::Poll::Ready(None)
    }
}

fn poll_current_stream(
    stream_details: &mut std::sync::RwLockReadGuard<'_, StreamDetails>,
    cx: &mut std::task::Context<'_>,
) -> Option<std::task::Poll<Option<ReqwestStreamItem>>> {
    let idx = stream_details.current_index;
    let stream_arc = &stream_details.streams[idx];

    if let Ok(mut guard) = stream_arc.try_write() {
        let pinned_stream = Pin::new(&mut *guard);
        match pinned_stream.poll_next(cx) {
            std::task::Poll::Ready(Some(item)) => Some(std::task::Poll::Ready(Some(item))),
            std::task::Poll::Ready(None) => {
                // Move to the next stream if the current one is exhausted
                None
            }
            std::task::Poll::Pending => Some(std::task::Poll::Pending),
        }
    } else {
        // Unable to acquire the lock immediately, so yield Pending.
        Some(std::task::Poll::Pending)
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
