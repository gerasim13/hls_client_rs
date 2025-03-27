#[cfg(feature = "stream_download")]
use stream_download::source::DecodeError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum HLSDecoderError {
    #[error("Failed to fetch: {0}")]
    FetchFailure(#[from] reqwest::Error),

    #[error("Failed to parse playlist: {0}")]
    PlaylistParseError(#[from] hls_m3u8::Error),

    #[error("No streams found in master playlist")]
    NoStreamsError,

    #[error("Stream does not have a content length")]
    NoContentLength,

    #[error("Cannot seek to a negative pos")]
    NegativeSeek,

    #[error("Failed to parse URL: {0}")]
    URLParseError(#[from] url::ParseError),

    #[error("URL must be provided before building Config")]
    MissingURLError,
}

#[cfg(feature = "stream_download")]
impl DecodeError for HLSDecoderError {}
