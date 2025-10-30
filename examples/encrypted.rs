use std::error::Error;

use hls_client::{config::ConfigBuilder, decoder::HLSDecoder};
use tracing_subscriber::EnvFilter;
use url::Url;

fn get_base_url(url: Url) -> Url {
    let segments = url.path_segments().unwrap();
    let base_segments: Vec<_> = segments.clone().collect();

    let base_url_str = {
        if base_segments.is_empty() {
            url[..url::Position::BeforePath].to_string()
        } else {
            let base_path = base_segments[..base_segments.len() - 1].join("/");
            let mut base_url = url[..url::Position::BeforePath].to_string();
            base_url.push('/');
            base_url.push_str(&base_path);
            base_url.push('/');
            base_url
        }
    };

    base_url_str.parse::<Url>().unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("hls_client=trace"))
        .with_line_number(true)
        .with_file(true)
        .init();

    let url = Url::parse("https://stream.silvercomet.top/drm/master.m3u8")?;
    let base_url = get_base_url(url.clone());
    let decoder = HLSDecoder::new(
        ConfigBuilder::new()
            .url(url)?
            .base_url(base_url)?
            .variant_stream_selector(|playlist| playlist.variant_streams.last().cloned())
            .build()?,
    )
    .await?;

    let handle = tokio::task::spawn_blocking(move || {
        let stream_handle =
            rodio::OutputStreamBuilder::open_default_stream().expect("open default audio stream");
        let sink = rodio::Sink::connect_new(&stream_handle.mixer());
        sink.append(rodio::Decoder::new(decoder)?);
        sink.sleep_until_end();

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    handle.await??;

    Ok(())
}
