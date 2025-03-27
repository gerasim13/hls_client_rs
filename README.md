# HLS client

This library provides a client for HLS (HTTP Live Streaming) protocol. It is still a work in progress and does not completely implement RFC 8216.

## Use cases

Normally, HLS playback can be achieved by individually loading each segment as a different media source in a player. However in most cases, this adds a minor delay between 2 segments introduced by the media player application preparing a new decoder for the next segment.

This library combines all segments into a single stream. This approach only works when all the segments are of the same type (e.g., same container and codec).

## Goals

This library aims to provide a simple interface for usage with rodio player or other media players which accept a single stream.

It does not and will not provide preloading or caching of segments. This can be achieved by using a wrapper around streams such as [stream-download-rs](https://github.com/aschey/stream-download-rs). We do provide an interface which can be used to use this library with stream-download-rs without any additional setup.

## Usage with rodio

### Without stream-download-rs

This can be achieved by using HLSDecoder directly.
```rust
use std::error::Error;

use hls_client::{config::ConfigBuilder, decoder::HLSDecoder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let decoder = HLSDecoder::new(
        ConfigBuilder::new()
            .url("https://streams.radiomast.io/ref-128k-mp3-stereo/hls.m3u8")?
            .build()?,
    )
    .await?;

    let handle = tokio::task::spawn_blocking(move || {
        let (_stream, handle) = rodio::OutputStream::try_default()?;
        let sink = rodio::Sink::try_new(&handle)?;
        sink.append(rodio::Decoder::new(decoder)?);
        sink.sleep_until_end();

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    handle.await??;

    Ok(())
}
```

### With stream-download-rs

This can be achieved using HLSStream directly.

```rust
use std::{error::Error, num::NonZeroUsize, path::PathBuf, str::FromStr};

use hls_client::{config::ConfigBuilder, stream::HLSStream};
use stream_download::{
    storage::{adaptive::AdaptiveStorageProvider, temp::TempStorageProvider},
    Settings, StreamDownload,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let settings = Settings::default();
    let decoder = StreamDownload::new::<HLSStream>(
        ConfigBuilder::new()
            .url("https://streams.radiomast.io/ref-128k-mp3-stereo/hls.m3u8")?
            .build()?,
        AdaptiveStorageProvider::new(
            TempStorageProvider::new(),
            NonZeroUsize::new((settings.get_prefetch_bytes() * 2) as usize).unwrap(),
        ),
        settings,
    )
    .await?;

    let handle = tokio::task::spawn_blocking(move || {
        let (_stream, handle) = rodio::OutputStream::try_default()?;
        let sink = rodio::Sink::try_new(&handle)?;
        sink.append(rodio::Decoder::new(decoder)?);
        sink.sleep_until_end();

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    handle.await??;

    Ok(())
}
```
