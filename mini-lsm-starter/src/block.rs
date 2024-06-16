#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use core::num;
use std::io::Read;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // todo has there a better implement?
        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        for v in self.data.iter() {
            buf.put_u8(*v);
        }

        for v in self.offsets.iter() {
            buf.put_u16(*v);
        }

        buf.put_u16(self.offsets.len() as u16);

        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // get num_of_elements
        let num_of_elements = (&data[data.len() - 2..]).get_u16() as usize;

        // get offset
        let mut offsets = vec![0; num_of_elements];
        for i in 0..num_of_elements {
            offsets[i] = (&data[data.len() - 2 - num_of_elements * 2 + i * 2..]).get_u16();
        }

        // get data
        let data = Vec::from(&data[0..data.len() - 2 - num_of_elements * 2]);

        Self { data, offsets }
    }
}
