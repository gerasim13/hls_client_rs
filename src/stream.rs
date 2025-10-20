use std::{fs, io, pin::Pin, sync::Arc, time::Duration};

use futures::Stream;
use hls_m3u8::{tags::VariantStream, MasterPlaylist, MediaPlaylist};
use reqwest::{Client, Url};
use tokio::sync::RwLock;

#[cfg(feature = "aes-encryption")]
use {crate::config::KeyProcessorCallback, std::collections::HashMap};

use crate::{
    config::{Config, VariantStreamSelector},
    errors::HLSDecoderError,
    segment::{resolve_segments, SeekResult, SegmentList},
    utils::{fetch_playlist, is_infinite_stream, parse_media_playlist, ReqwestStreamItem},
};

pub(crate) struct StreamDetails {
    media_playlist: MediaPlaylist<'static>,
    segments: SegmentList,
    current_index: usize,
    target_duration: Duration,
    finished: bool,
}

pub struct HLSStream {
    pub(crate) config: Arc<Box<Config>>,
    pub(crate) stream_details: Arc<RwLock<StreamDetails>>,
    pub(crate) media_playlist_url: Url,
}

impl HLSStream {
    pub async fn try_new(config: Config) -> Result<Self, HLSDecoderError> {
        let url = config.get_url();
        let client = Client::new();

        let playlist_contents = match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| HLSDecoderError::MissingURLError)?;

                fs::read_to_string(&path).map_err(|e| HLSDecoderError::IoError(e))?
            }
            _ => {
                let resp = client
                    .get(url)
                    .send()
                    .await
                    .map_err(HLSDecoderError::from)?;
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

        #[cfg(feature = "aes-encryption")]
        let details = Self::parse_playlist(
            client,
            &playlist.parse()?,
            config.get_base_url().as_ref(),
            config.get_key_query_params(),
            config.get_key_request_headers(),
            config.get_key_processor_cb(),
        )
        .await?;

        #[cfg(not(feature = "aes-encryption"))]
        let details =
            Self::parse_playlist(client, &playlist.parse()?, config.get_base_url().as_ref())
                .await?;

        let content_length = details.segments.content_length();
        let ret = Self {
            media_playlist_url: playlist.parse()?,
            config: Arc::new(Box::new(config)),
            stream_details: Arc::new(RwLock::new(details)),
        };

        if content_length.is_none() {
            ret.spawn_reload_loop();
        }

        Ok(ret)
    }

    async fn handle_master_playlist(
        playlist: &str,
        src: &str,
        base_url: Option<Url>,
        stream_selection_cb: Option<Arc<Box<VariantStreamSelector>>>,
    ) -> Result<Option<String>, HLSDecoderError> {
        if let Ok(master_playlist) = MasterPlaylist::try_from(playlist) {
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

    #[cfg(not(feature = "aes-encryption"))]
    async fn parse_playlist(
        client: Client,
        media_playlist_url: &Url,
        base_url: Option<&Url>,
    ) -> Result<StreamDetails, HLSDecoderError> {
        let playlist_text = fetch_playlist(media_playlist_url).await?;
        let media_playlist = parse_media_playlist(&playlist_text).await?.into_owned();
        let segments =
            resolve_segments(client, &media_playlist, media_playlist_url, base_url).await?;
        let target_duration = media_playlist.target_duration;

        Ok(StreamDetails {
            segments,
            media_playlist,
            target_duration,
            finished: false,
            current_index: 0,
        })
    }

    #[cfg(feature = "aes-encryption")]
    async fn parse_playlist(
        client: Client,
        media_playlist_url: &Url,
        base_url: Option<&Url>,
        key_query_params: Option<HashMap<String, String>>,
        key_request_headers: Option<HashMap<String, String>>,
        key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
    ) -> Result<StreamDetails, HLSDecoderError> {
        let playlist_text = fetch_playlist(media_playlist_url).await?;
        let media_playlist = parse_media_playlist(&playlist_text).await?.into_owned();
        let segments = resolve_segments(
            client,
            &media_playlist,
            media_playlist_url,
            base_url,
            key_query_params,
            key_request_headers,
            key_processor_cb,
        )
        .await?;
        let target_duration = media_playlist.target_duration;

        Ok(StreamDetails {
            segments,
            media_playlist,
            target_duration,
            finished: false,
            current_index: 0,
        })
    }

    async fn update_stream_details(
        new_details: StreamDetails,
        old_stream_details: Arc<RwLock<StreamDetails>>,
        reconnect: bool,
    ) -> Result<bool, HLSDecoderError> {
        #[cfg(feature = "tracing")]
        tracing::debug!("Reloading playlist");
        let should_seek = {
            let mut stream_details = old_stream_details.write().await;
            let old_index = stream_details.current_index;
            let old_seg = if stream_details.finished {
                None
            } else {
                stream_details.segments.get(old_index).cloned()
            };

            #[cfg(feature = "tracing")]
            tracing::debug!(
                "old_seg: {:?}, old_index: {}, newsegments: {:?}",
                old_seg,
                old_index,
                stream_details.segments
            );

            *stream_details = new_details;

            // Find the old URL in the new segments list to maintain playback position
            let (new_current_index, should_seek) = match old_seg.as_ref() {
                Some(seg) => stream_details
                    .segments
                    .find_segment(seg.read().await.uri.clone())
                    .map_or((0, false), |idx| (idx, true)),
                None => (0, false),
            };

            stream_details.current_index = new_current_index;
            stream_details.finished = false;

            // If we don't need to reconnect, restore the old stream which was ongoing so we have continuity
            if should_seek && !reconnect {
                let new_segment = stream_details
                    .segments
                    .get_mut(new_current_index)
                    .expect("Segment should exist");

                if let Some(old_active_segment) = old_seg {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Restoring old stream at index {}", new_current_index);
                    *new_segment = old_active_segment;
                    // No need to seek if we're replacing the old active stream
                    return Ok(false);
                }
            }
            should_seek
        };

        Ok(should_seek)
    }

    #[cfg(not(feature = "aes-encryption"))]
    pub(crate) async fn reload_playlist(
        media_playlist_url: &Url,
        stream_details: Arc<RwLock<StreamDetails>>,
        base_url: Option<&Url>,
        reconnect: bool,
    ) -> Result<bool, HLSDecoderError> {
        let client = Client::new();
        let new_details = Self::parse_playlist(client, media_playlist_url, base_url).await?;
        Self::update_stream_details(new_details, stream_details, reconnect).await
    }

    #[cfg(feature = "aes-encryption")]
    pub(crate) async fn reload_playlist(
        media_playlist_url: &Url,
        stream_details: Arc<RwLock<StreamDetails>>,
        base_url: Option<&Url>,
        key_query_params: Option<HashMap<String, String>>,
        key_request_headers: Option<HashMap<String, String>>,
        key_processor_cb: Option<Arc<Box<KeyProcessorCallback>>>,
        reconnect: bool,
    ) -> Result<bool, HLSDecoderError> {
        let client = Client::new();
        let new_details = Self::parse_playlist(
            client,
            media_playlist_url,
            base_url,
            key_query_params,
            key_request_headers,
            key_processor_cb,
        )
        .await?;
        Self::update_stream_details(new_details, stream_details, reconnect).await
    }

    fn spawn_reload_loop(&self) {
        let stream_details = self.stream_details.clone();
        let media_playlist = self.media_playlist_url.clone();
        let base_url = self.config.get_base_url();

        #[cfg(feature = "aes-encryption")]
        let key_processor_cb = self.config.get_key_processor_cb();
        #[cfg(feature = "aes-encryption")]
        let key_query_params = self.config.get_key_query_params();
        #[cfg(feature = "aes-encryption")]
        let key_request_headers = self.config.get_key_request_headers();

        tokio::spawn(async move {
            let stream_details = stream_details.clone();
            let mut target_duration = stream_details.read().await.target_duration;
            loop {
                tokio::time::sleep(target_duration).await;

                #[cfg(feature = "aes-encryption")]
                let resp = Self::reload_playlist(
                    &media_playlist,
                    stream_details.clone(),
                    base_url.as_ref(),
                    key_query_params.clone(),
                    key_request_headers.clone(),
                    key_processor_cb.clone(),
                    false,
                )
                .await;

                #[cfg(not(feature = "aes-encryption"))]
                let resp = Self::reload_playlist(
                    &media_playlist,
                    stream_details.clone(),
                    base_url.as_ref(),
                    false,
                )
                .await;

                if resp.is_ok() {
                    target_duration = stream_details.read().await.target_duration;
                } else {
                    break;
                }
            }
        });
    }

    pub(crate) fn content_length(&self) -> Option<u64> {
        match self.stream_details.try_read() {
            Ok(guard) => {
                let is_infinite_stream = is_infinite_stream(&guard.media_playlist);
                if is_infinite_stream {
                    None
                } else {
                    guard.segments.content_length()
                }
            }
            Err(_) => None,
        }
    }

    pub(crate) async fn supports_range_request(&self) -> bool {
        self.stream_details
            .read()
            .await
            .segments
            .supports_range_request()
            .is_some_and(|supports_range_request| supports_range_request)
    }

    pub async fn handle_seek(&mut self, start: u64, end: Option<u64>) -> io::Result<()> {
        let seek_result = self
            .stream_details
            .read()
            .await
            .segments
            .seek_segment(start, end);
        match seek_result {
            SeekResult::EndOfStream => {
                #[cfg(feature = "tracing")]
                tracing::trace!("seek position is at total length; setting stream to empty");
                let mut stream_details = self.stream_details.write().await;
                stream_details.current_index = 0;
                stream_details.finished = true;
                Ok(())
            }
            SeekResult::OutOfBounds => Err(io::Error::new(
                io::ErrorKind::Other,
                "start offset out of range",
            )),
            SeekResult::Located {
                index,
                local_start,
                local_end,
            } => {
                #[cfg(feature = "tracing")]
                if !self.supports_range_request().await {
                    tracing::warn!("Accept-Ranges header not present. Attempting seek anyway.");
                }
                let mut stream_details = self.stream_details.write().await;
                if let Some(selected_segment) = stream_details.segments.get(index).cloned() {
                    selected_segment
                        .write()
                        .await
                        .reconnect(local_start, local_end)
                        .await?;
                }
                stream_details.current_index = index;
                stream_details.finished = false;
                Ok(())
            }
        }
    }
}

impl Stream for HLSStream {
    type Item = ReqwestStreamItem;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        loop {
            let stream_details = match this.stream_details.try_read() {
                Ok(guard) => guard,
                Err(_) => {
                    // Lock is currently held for writing, so we can't proceed.
                    // We return Pending and the runtime will poll us again later.
                    return std::task::Poll::Pending;
                }
            };

            // Check if we're already finished
            if stream_details.finished {
                return get_finished_poll_result(&stream_details);
            }

            // Check if we've finished all available segments.
            if stream_details.current_index >= stream_details.segments.len() {
                // Release the read lock so we can get a write lock.
                drop(stream_details);

                // Get a write lock to mark the stream as finished.
                let mut write_guard = match this.stream_details.try_write() {
                    Ok(guard) => guard,
                    Err(_) => return std::task::Poll::Pending, // Can't get write lock, try later.
                };

                write_guard.finished = true;
                return get_finished_poll_result(&write_guard);
            }

            // Poll the stream of the current segment.
            match stream_details
                .segments
                .poll_stream(cx, stream_details.current_index)
            {
                Some(std::task::Poll::Ready(Some(item))) => {
                    // We got data from the current segment. Return it.
                    return std::task::Poll::Ready(Some(item));
                }
                Some(std::task::Poll::Pending) => {
                    // The underlying segment stream is waiting on I/O.
                    // Propagate the Pending state.
                    return std::task::Poll::Pending;
                }
                Some(std::task::Poll::Ready(None)) | None => {
                    // The current segment has finished. We need to advance the index.
                    // First, release the read lock.
                    drop(stream_details);

                    // Now, acquire a write lock to increment the index.
                    let mut write_guard = match this.stream_details.try_write() {
                        Ok(guard) => guard,
                        Err(_) => return std::task::Poll::Pending, // Try again later.
                    };

                    if write_guard.current_index < write_guard.segments.len() {
                        write_guard.current_index += 1;
                    }

                    // Instead of returning, we `continue` the loop to immediately
                    // try polling the *next* segment in the same poll cycle.
                    continue;
                }
            }
        }
    }
}

fn get_finished_poll_result(
    stream_details: &StreamDetails,
) -> std::task::Poll<Option<ReqwestStreamItem>> {
    // If the stream is infinite, always return pending. Otherwise tell the reader that we've exhausted the streams
    if stream_details
        .segments
        .content_length()
        .is_none_or(|c| c == 0)
    {
        std::task::Poll::Pending
    } else {
        std::task::Poll::Ready(None)
    }
}
