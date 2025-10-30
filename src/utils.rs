use bytes::Bytes;
use futures::Stream;
use hls_m3u8::{types::PlaylistType, MediaPlaylist};
use url::Url;

use crate::errors::{HLSDecoderError, StreamItemError};

pub type ReqwestStreamItem = Result<Bytes, StreamItemError>;
pub type ReqwestStream = Box<dyn Stream<Item = ReqwestStreamItem> + Unpin + Send + Sync>;

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

pub(crate) fn is_infinite_stream(media_playlist: &MediaPlaylist) -> bool {
    media_playlist.playlist_type.unwrap_or(PlaylistType::Event) == PlaylistType::Event
}

pub(crate) async fn fetch_playlist(url: &Url) -> Result<String, HLSDecoderError> {
    let response = reqwest::get(url.as_str()).await?;
    let text = response.text().await?;
    Ok(text)
}

pub(crate) fn parse_media_playlist(
    playlist: &str,
) -> Result<MediaPlaylist<'static>, HLSDecoderError> {
    let playlist = MediaPlaylist::try_from(playlist)?.into_owned();

    #[cfg(feature = "tracing")]
    tracing::info!("Media playlist detected");

    Ok(playlist)
}
