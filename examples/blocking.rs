use std::error::Error;

use hls_client::{config::ConfigBuilder, decoder::HLSDecoder};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("hls_client=trace"))
        .with_line_number(true)
        .with_file(true)
        .init();

    let decoder = HLSDecoder::new(
        ConfigBuilder::new()
            .url("https://streams.radiomast.io/ref-128k-mp3-stereo/hls.m3u8")?
            .variant_stream_selector(|playlist| playlist.variant_streams.first().cloned())
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
