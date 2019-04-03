use futures::sync::mpsc;
use tokio::codec::{Encoder, Decoder};
use bytes::{BytesMut};

use crate::*;
use crate::paxos::*;
use std::convert::From;
// use failure::Fail;

// #[derive(Debug, Fail)]
// #[fail(display = "An error occurred.")]
#[derive(Debug)]
pub enum  FlowError{
    IOError(std::io::Error),
    SendError(mpsc::SendError<Package>),
}

impl From<()> for FlowError {
    fn from(_e: ()) -> Self {
        unreachable!()
    }
}

impl From<std::io::Error> for FlowError {
    fn from(e: std::io::Error) -> Self {
        FlowError::IOError(e)
    }
}
use network::Package;

impl From<mpsc::SendError<Package>> for FlowError {
    fn from(e: mpsc::SendError<Package>) -> Self {
        FlowError::SendError(e)
    }
}

#[derive(Debug)]
pub struct Codec {
    buf: BytesMut
}

impl Codec {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(32)
        }
    }
}

impl Decoder for Codec {
    type Item = Datagram;
    type Error = FlowError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.buf.unsplit(src.take());
        Ok(Datagram::decode(&mut self.buf))
    }
}

impl Encoder for Codec {
    type Item = Datagram;
    type Error = FlowError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(item.encode(dst))
    }
    
    
}
