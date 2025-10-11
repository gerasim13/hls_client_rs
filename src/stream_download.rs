use std::io;

use stream_download::source::SourceStream;

use crate::{config::Config, errors::HLSDecoderError, stream::HLSStream};

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
        let should_seek = Self::reload_playlist(
            &self.media_playlist_url,
            self.stream_details.clone(),
            self.base_url.as_ref(),
            true,
        )
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
