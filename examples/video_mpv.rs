use std::env::args;
use std::error::Error;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::slice;
use std::sync::OnceLock;

use hls_client::config::ConfigBuilder;
use hls_client::stream::HLSStream;
use libmpv2::events::Event;
use libmpv2::protocol::Protocol;
use libmpv2::Mpv;
use stream_download::storage::adaptive::AdaptiveStorageProvider;
use stream_download::storage::temp::TempStorageProvider;
use stream_download::{Settings, StreamDownload};
use tracing_subscriber::EnvFilter;
use url::Url;

struct Stream {
    reader: StreamDownload<AdaptiveStorageProvider<TempStorageProvider, TempStorageProvider>>,
    content_length: u64,
}

static HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

// NOTE: this requires having libmpv installed already
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // The stream needs to be created in a function invoked by libmpv.
    // We need to store a reference to the tokio runtime here to access it from that callback.

    // We could pass it in the first parameter to Protocol::new, but the handle is not unwind safe
    // and the open() function could panic since we're not able to return a Result.
    HANDLE.set(tokio::runtime::Handle::current()).unwrap();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_line_number(true)
        .with_file(true)
        .init();

    let url = args()
        .nth(1)
        .unwrap_or_else(|| "https://ezharjan.github.io/M3U8Example/ene.m3u8".to_string());
    // SAFETY: we don't call any libmpv functions in the provided callback functions
    let mpv = Mpv::new()?;
    let protocol = unsafe {
        Protocol::new(
            &mpv,
            "stream".into(),
            (),
            open,
            close,
            read,
            Some(seek),
            Some(size),
        )
    };
    protocol.register()?;
    mpv.command("loadfile", &[&format!("stream://{url}"), "append-play"])?;

    let mut mpv_client = mpv.create_client("client".into())?;
    tokio::task::spawn_blocking(move || loop {
        let ev = mpv_client
            .wait_event(600.)
            .unwrap_or(Err(libmpv2::Error::Null));
        if let Ok(Event::EndFile(_)) = ev {
            return;
        }
    })
    .await?;

    Ok(())
}

fn open(_: &mut (), uri: &str) -> Stream {
    let handle = HANDLE.get().unwrap();
    let _guard = handle.enter();
    handle
        .block_on(async move {
            let settings = Settings::default();
            let reader = StreamDownload::new::<HLSStream>(
                ConfigBuilder::new()
                    .url(uri.replace("stream://", "").parse::<Url>().unwrap())?
                    .build()?,
                AdaptiveStorageProvider::new(
                    TempStorageProvider::new(),
                    NonZeroUsize::new((settings.get_prefetch_bytes() * 2) as usize).unwrap(),
                ),
                settings,
            )
            .await
            .unwrap();

            Ok::<_, Box<dyn Error + Send + Sync>>(Stream {
                reader,
                content_length: 0,
            })
        })
        .unwrap()
}

fn read(stream: &mut Stream, buf: &mut [i8]) -> i64 {
    // SAFETY: `buf` is non-null, the new slice is created with the same length, and it does not
    // outlive the given data pointer
    let buf = unsafe { slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len()) };
    stream.reader.read(buf).unwrap() as i64
}

#[expect(clippy::boxed_local)]
fn close(stream: Box<Stream>) {
    stream.reader.cancel_download();
}

fn seek(stream: &mut Stream, offset: i64) -> i64 {
    stream.reader.seek(SeekFrom::Start(offset as u64)).unwrap() as i64
}

fn size(stream: &mut Stream) -> i64 {
    stream.content_length as i64
}
