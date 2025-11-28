use std::io;

use stream_download::source::SourceStream;
use stream_download::storage::{ContentLength, DynamicLength};

use crate::{config::Config, errors::HLSDecoderError, stream::HLSStream};

impl SourceStream for HLSStream {
    type Params = Config;
    type StreamCreationError = HLSDecoderError;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Self::try_new(params).await
    }

    fn content_length(&self) -> ContentLength {
        let stream_length = self.content_length();
        match stream_length.reported {
            None => ContentLength::Unknown,
            Some(reported_length) => match stream_length.gathered {
                Some(gathered_length) if gathered_length == reported_length => {
                    ContentLength::Static(reported_length)
                }
                _ => ContentLength::Dynamic(DynamicLength {
                    reported: reported_length,
                    gathered: stream_length.gathered,
                }),
            },
        }
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
