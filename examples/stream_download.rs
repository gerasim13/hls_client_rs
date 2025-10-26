use std::{error::Error, num::NonZeroUsize};

use hls_client::{config::ConfigBuilder, stream::HLSStream};
use stream_download::{
    storage::{adaptive::AdaptiveStorageProvider, temp::TempStorageProvider},
    Settings, StreamDownload,
};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("hls_client=trace"))
        .with_line_number(true)
        .with_file(true)
        .init();

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

    let stream_handle =
        rodio::OutputStreamBuilder::open_default_stream().expect("open default audio stream");
    let sink = rodio::Sink::connect_new(&stream_handle.mixer());
    sink.append(rodio::Decoder::new(decoder)?);

    sink.sleep_until_end();

    Ok(())
}
