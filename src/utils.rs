use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::{future, Stream};
use hls_m3u8::{types::PlaylistType, MediaPlaylist};
use mediatype::MediaTypeBuf;
use reqwest::{header, Response};
use url::Url;

use crate::errors::HLSDecoderError;

pub type ReqwestStreamItem = Result<Bytes, reqwest::Error>;
pub type ReqwestStream = Box<dyn Stream<Item = ReqwestStreamItem> + Unpin + Send + Sync>;

pub(crate) async fn fetch_playlist(url: &Url) -> Result<String, HLSDecoderError> {
    let response = reqwest::get(url.as_str()).await?;
    let text = response.text().await?;
    Ok(text)
}

pub(crate) async fn parse_media_playlist(
    playlist_text: &str,
) -> Result<MediaPlaylist, HLSDecoderError> {
    handle_media_playlist(playlist_text).await
}

pub(crate) fn is_infinite_stream(media_playlist: &MediaPlaylist) -> bool {
    media_playlist.playlist_type.unwrap_or(PlaylistType::Event) == PlaylistType::Event
}

pub(crate) fn resolve_segment_urls(
    media_playlist: &MediaPlaylist,
    playlist_url: &Url,
    base_url: Option<&Url>,
) -> Vec<String> {
    let mut urls = Vec::new();
    // Use base_url from config if present, otherwise derive from playlist URL.
    let base = if let Some(base) = base_url {
        base.as_str()
    } else {
        playlist_url.as_str().trim_end_matches(".m3u8")
    };
    // If the first segment has an init section, add its URI as the first URL.
    if let Some(first_segment) = media_playlist.segments.get(0) {
        if let Some(ref map) = first_segment.map {
            let init_uri = map.uri();
            let init_url = if init_uri.starts_with("http") {
                init_uri.to_string()
            } else {
                format!("{}/{}", base, init_uri)
            };
            urls.push(init_url);
        }
    }
    // Add all segment URIs (media segments) to the list.
    urls.extend(media_playlist.segments.iter().map(|(_, segment)| {
        let seg_uri = segment.uri();
        if seg_uri.starts_with("http") {
            seg_uri.to_string()
        } else {
            format!("{}/{}", base, seg_uri)
        }
    }));
    urls
}

pub(crate) async fn fetch_segment_responses(segment_urls: &[String]) -> Vec<Response> {
    let futures = segment_urls.iter().map(reqwest::get);
    let responses = future::join_all(futures).await;
    responses.into_iter().filter_map(Result::ok).collect()
}

pub(crate) fn compute_content_lengths(responses: &[Response]) -> Vec<u64> {
    responses
        .iter()
        .map(|resp| resp.content_length().unwrap_or(0))
        .collect()
}

pub(crate) fn compute_overall_content_length(responses: &[Response]) -> Option<u64> {
    responses
        .iter()
        .filter_map(|resp| resp.content_length())
        .reduce(|acc, len| acc + len)
}

pub(crate) fn extract_content_type(response: Option<&Response>) -> Option<String> {
    response
        .and_then(|r| {
            r.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|val| val.to_str().ok())
        })
        .and_then(|ct| match ct.parse::<MediaTypeBuf>() {
            Ok(parsed) => Some(parsed.to_string()),
            Err(_) => None,
        })
}

pub(crate) fn extract_headers(responses: &[Response]) -> Vec<reqwest::header::HeaderMap> {
    responses.iter().map(|r| r.headers().clone()).collect()
}

pub(crate) fn build_streams(responses: Vec<Response>) -> Vec<Arc<RwLock<ReqwestStream>>> {
    responses
        .into_iter()
        .map(|resp| Arc::new(RwLock::new(Box::new(resp.bytes_stream()) as ReqwestStream)))
        .collect()
}

pub(crate) async fn handle_media_playlist(
    playlist: &str,
) -> Result<MediaPlaylist, HLSDecoderError> {
    let playlist = hls_m3u8::MediaPlaylist::try_from(playlist)?.into_owned();

    #[cfg(feature = "tracing")]
    tracing::info!("Media playlist detected");

    Ok(playlist)
}
