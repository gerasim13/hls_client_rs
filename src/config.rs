use std::sync::Arc;

use hls_m3u8::{tags::VariantStream, MasterPlaylist};
use reqwest::IntoUrl;
use url::Url;

use crate::errors::HLSDecoderError;

pub struct Config {
    url: Url,
    base_url: Option<Url>,
    stream_selection_cb:
        Option<Arc<Box<dyn Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync>>>,
}

impl Config {
    pub fn new<T>(
        url: T,
        base_url: Option<Url>,
        stream_selection_cb: Option<
            Arc<Box<dyn Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync>>,
        >,
    ) -> Result<Self, HLSDecoderError>
    where
        T: IntoUrl,
    {
        Ok(Self {
            url: url.into_url()?,
            base_url,
            stream_selection_cb,
        })
    }

    pub(crate) fn get_stream_selection_cb(
        &self,
    ) -> Option<Arc<Box<dyn Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync>>> {
        self.stream_selection_cb.clone()
    }

    pub(crate) fn get_base_url(&self) -> Option<Url> {
        self.base_url.clone()
    }

    pub(crate) fn get_url(&self) -> Url {
        self.url.clone()
    }
}

pub struct ConfigBuilder {
    url: Option<Url>,
    base_url: Option<Url>,
    stream_selection_cb:
        Option<Arc<Box<dyn Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync>>>,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigBuilder {
    /// Creates a new builder with no URL and no callback set.
    pub fn new() -> Self {
        Self {
            url: None,
            base_url: None,
            stream_selection_cb: None,
        }
    }

    /// Sets the URL for the configuration.
    pub fn url<T: TryInto<Url>>(mut self, url: T) -> Result<Self, HLSDecoderError> {
        self.url = Some(
            url.try_into()
                .map_err(|_| HLSDecoderError::MissingURLError)?,
        );
        Ok(self)
    }

    /// Sets base_url for the configuration.
    pub fn base_url<T: TryInto<Url>>(mut self, base_url: T) -> Result<Self, HLSDecoderError> {
        self.base_url = Some(
            base_url
                .try_into()
                .map_err(|_| HLSDecoderError::MissingURLError)?,
        );
        Ok(self)
    }

    /// Sets the stream selection callback.
    pub fn variant_stream_selector(
        mut self,
        cb: impl Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync + 'static,
    ) -> Self {
        self.stream_selection_cb = Some(Arc::new(Box::new(cb)));
        self
    }

    /// Builds the Config.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is not set.
    pub fn build(self) -> Result<Config, HLSDecoderError> {
        let url = self.url.ok_or_else(|| HLSDecoderError::MissingURLError)?;
        Ok(Config {
            url,
            base_url: self.base_url,
            stream_selection_cb: self.stream_selection_cb,
        })
    }
}
