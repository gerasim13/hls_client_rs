use std::sync::Arc;

#[cfg(feature = "aes-encryption")]
use bytes::Bytes;
use hls_m3u8::{tags::VariantStream, MasterPlaylist};
use reqwest::IntoUrl;
use url::Url;

#[cfg(feature = "aes-encryption")]
use crate::aes::{AesConfig, KeyProcessorCallback, KeyQueryParams, KeyRequestHeaders};
use crate::errors::HLSDecoderError;

// Type alias for the variant stream selector callback
pub type VariantStreamSelector = dyn Fn(MasterPlaylist) -> Option<VariantStream> + Send + Sync;

pub struct Config {
    url: Url,
    base_url: Option<Url>,
    stream_selection_cb: Option<Arc<Box<VariantStreamSelector>>>,
    #[cfg(feature = "aes-encryption")]
    pub(crate) aes_config: Option<AesConfig>,
}

impl Config {
    pub fn new<T>(
        url: T,
        base_url: Option<Url>,
        stream_selection_cb: Option<Arc<Box<VariantStreamSelector>>>,
        #[cfg(feature = "aes-encryption")] aes_config: Option<AesConfig>,
    ) -> Result<Self, HLSDecoderError>
    where
        T: IntoUrl,
    {
        Ok(Self {
            url: url.into_url()?,
            base_url,
            stream_selection_cb,
            #[cfg(feature = "aes-encryption")]
            aes_config,
        })
    }

    pub(crate) fn get_base_url(&self) -> Option<Url> {
        self.base_url.clone()
    }

    pub(crate) fn get_url(&self) -> Url {
        self.url.clone()
    }

    pub(crate) fn get_stream_selection_cb(&self) -> Option<Arc<Box<VariantStreamSelector>>> {
        self.stream_selection_cb.clone()
    }

    #[cfg(feature = "aes-encryption")]
    pub(crate) fn get_key_processor_cb(&self) -> Option<Arc<Box<KeyProcessorCallback>>> {
        self.aes_config
            .as_ref()
            .and_then(|config| config.key_processor_cb.clone())
    }

    #[cfg(feature = "aes-encryption")]
    pub(crate) fn get_key_query_params(&self) -> Option<KeyQueryParams> {
        self.aes_config
            .as_ref()
            .and_then(|config| config.key_query_params.clone())
    }

    #[cfg(feature = "aes-encryption")]
    pub(crate) fn get_key_request_headers(&self) -> Option<KeyRequestHeaders> {
        self.aes_config
            .as_ref()
            .and_then(|config| config.key_request_headers.clone())
    }
}

#[derive(Default)]
pub struct ConfigBuilder {
    url: Option<Url>,
    base_url: Option<Url>,
    stream_selection_cb: Option<Arc<Box<VariantStreamSelector>>>,
    #[cfg(feature = "aes-encryption")]
    pub(crate) aes_config: Option<AesConfig>,
}

impl ConfigBuilder {
    /// Creates a new builder with no URL and no callback set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the URL for the configuration.
    pub fn url<T>(mut self, url: T) -> Result<Self, HLSDecoderError>
    where
        T: TryInto<Url>,
        HLSDecoderError: From<T::Error>,
    {
        self.url = Some(url.try_into()?);
        Ok(self)
    }

    /// Sets base_url for the configuration.
    pub fn base_url<T>(mut self, base_url: T) -> Result<Self, HLSDecoderError>
    where
        T: TryInto<Url>,
        HLSDecoderError: From<T::Error>,
    {
        self.base_url = Some(base_url.try_into()?);
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

    /// Gets mutable reference to AES configuration.
    #[cfg(feature = "aes-encryption")]
    fn aes_config_mut(&mut self) -> &mut AesConfig {
        self.aes_config.get_or_insert_with(Default::default)
    }

    /// Sets the decryption callback.
    #[cfg(feature = "aes-encryption")]
    pub fn key_processor_cb(mut self, cb: impl Fn(Bytes) -> Bytes + Send + Sync + 'static) -> Self {
        self.aes_config_mut().key_processor_cb = Some(Arc::new(Box::new(cb)));
        self
    }

    /// Sets the key query parameters.
    #[cfg(feature = "aes-encryption")]
    pub fn key_query_params(mut self, params: KeyQueryParams) -> Self {
        self.aes_config_mut().key_query_params = Some(params);
        self
    }

    /// Sets custom headers for key server requests.
    #[cfg(feature = "aes-encryption")]
    pub fn key_request_headers(mut self, headers: KeyRequestHeaders) -> Self {
        self.aes_config_mut().key_request_headers = Some(headers);
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
            #[cfg(feature = "aes-encryption")]
            aes_config: self.aes_config,
        })
    }
}
