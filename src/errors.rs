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

    #[error("URL conversion error: {0}")]
    URLConversionError(#[from] std::convert::Infallible),

    #[error("URL must be provided before building Config")]
    MissingURLError,

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// A general-purpose error for miscellaneous issues.
    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug)]
pub enum StreamItemError {
    #[error("Network error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Decryption error: {0}")]
    Decryption(String),
}

#[cfg(feature = "stream_download")]
impl DecodeError for HLSDecoderError {}
