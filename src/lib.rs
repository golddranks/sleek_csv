extern crate alloc;
use alloc::fmt;

#[cfg(feature = "serde")]
mod deserialize;

#[cfg(feature = "serde")]
pub mod error;

pub mod byte_arena;
mod printer;
mod raw;
pub mod reader;
pub mod writer;

use crate::raw::{RawRecord, RawRecordIter};
pub use byte_arena::{ByteRecordArena, ByteRecordsIter};
use raw::RawRecordArena;
pub use reader::Reader;
pub use writer::Writer;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub byte: u64,
    pub line: u64,
    pub record: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub struct Headers {
    name_data: Vec<u8>,
    name_ends: Vec<usize>,
}

impl Headers {
    pub fn iter(&self) -> RawRecordIter {
        RawRecord {
            field_data: &self.name_data,
            field_ends: &self.name_ends,
        }
        .iter()
    }

    pub fn get(&self, n: usize) -> &[u8] {
        let field_end = self.name_ends[n];
        let prev_field_end = *self.name_ends.get(n-1).unwrap_or(&0);
        &self.name_data[prev_field_end..field_end]
    }

    pub fn len(&self) -> usize {
        self.name_ends.len()
    }
}

impl fmt::Debug for Headers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        RawRecord {
            field_data: &self.name_data,
            field_ends: &self.name_ends,
        }
        .fmt(f)
    }
}
