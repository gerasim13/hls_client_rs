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
        let stream_details = self.stream_details.clone();
        let media_playlist = self.media_playlist_url.clone();
        let config = self.config.clone();

        let should_seek = Self::reload_playlist(&config, &media_playlist, stream_details, true)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        if should_seek && self.supports_range_request().await {
            self.seek_range(current_position, None).await?;
        }
        Ok(())
    }

    fn supports_seek(&self) -> bool {
        true
    }
}
