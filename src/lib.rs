#![doc = include_str!("../README.md")]

#[cfg(feature = "aes-encryption")]
pub mod aes;
pub mod config;
#[cfg(feature = "blocking")]
pub mod decoder;
pub mod errors;
pub mod segment;
pub mod stream;
mod utils;

#[cfg(feature = "stream_download")]
pub mod stream_download;
