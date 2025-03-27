use std::io::{Read, Seek};

use bytes::{Buf, Bytes};
use futures::executor::{block_on, block_on_stream, BlockingStream};
use reqwest::IntoUrl;

use crate::{errors::HLSDecoderError, stream::HLSStream};

pub struct HLSDecoder {
    stream: BlockingStream<HLSStream>,
    buffer: Option<Bytes>,
    current_pos: usize,
}

impl HLSDecoder {
    pub async fn new(url: impl IntoUrl) -> Result<Self, HLSDecoderError> {
        Ok(Self {
            stream: block_on_stream(HLSStream::try_new(url).await?),
            buffer: None,
            current_pos: 0,
        })
    }
}

impl Read for HLSDecoder {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let mut currently_read = 0usize;

        #[cfg(feature = "tracing")]
        tracing::trace!("Reading pos {} {}", buf_len, currently_read);
        while currently_read < buf_len {
            if self.buffer.as_ref().is_none_or(|buf| buf.is_empty()) {
                if let Some(data) = self.stream.next() {
                    match data {
                        Ok(bytes) => {
                            #[cfg(feature = "tracing")]
                            tracing::trace!("Got data read size {}", bytes.len());
                            self.buffer = Some(bytes)
                        }
                        Err(err) => {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, err))
                        }
                    }
                } else {
                    // If we have nothing to read, the stream likely ended. Return whatever we have at this point
                    break;
                }
            }

            if let Some(buffer) = self.buffer.as_mut() {
                let available = buffer.len();
                let to_copy = buf_len - currently_read;

                let can_copy = available.min(to_copy);

                // Copy whatever we can
                buf[currently_read..currently_read + can_copy]
                    .copy_from_slice(&buffer[0..can_copy]);
                currently_read += can_copy;

                buffer.advance(can_copy);
            }
        }

        self.current_pos += currently_read;
        Ok(currently_read)
    }
}

impl Seek for HLSDecoder {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let content_len = self.stream.content_length();
        let abs_pos = match pos {
            std::io::SeekFrom::Start(pos) => pos,
            std::io::SeekFrom::End(pos) => {
                if let Some(content_len) = content_len {
                    let tmp_pos = (content_len as i64) + pos;
                    if tmp_pos < 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            HLSDecoderError::NegativeSeek,
                        ));
                    }

                    tmp_pos as u64
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        HLSDecoderError::NoContentLength,
                    ));
                }
            }
            std::io::SeekFrom::Current(pos) => {
                let tmp_pos = self.current_pos as i64 + pos;
                if tmp_pos < 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        HLSDecoderError::NegativeSeek,
                    ));
                }
                tmp_pos as u64
            }
        };

        let normal_pos = abs_pos.min(content_len.unwrap_or(0));
        block_on(self.stream.handle_seek(normal_pos, None))?;

        Ok(normal_pos)
    }
}
