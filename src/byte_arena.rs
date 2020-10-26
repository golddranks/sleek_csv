extern crate alloc;

#[cfg(feature = "serde")]
use serde::Deserialize;

use core::fmt;
use core::ops::Not;

use crate::raw::{RawRecord, RawRecordArena, RawRecordsIter};
#[cfg(feature = "serde")]
use crate::{deserialize, error};
use crate::{Headers, Position};

pub struct ByteRecordArena {
    pub(crate) inner: RawRecordArena,
    pub(crate) start_pos: Option<Position>,
    pub(crate) headers_inner: Option<Headers>,
    pub(crate) bytes_init: usize,
}

pub struct ByteRecordsIter<'a>(RawRecordsIter<'a>);

impl ByteRecordArena {
    pub fn new() -> ByteRecordArena {
        ByteRecordArena {
            inner: RawRecordArena::new(),
            start_pos: None,
            headers_inner: None,
            bytes_init: 0,
        }
    }

    pub fn with_headers(headers: Headers) -> ByteRecordArena {
        ByteRecordArena {
            inner: RawRecordArena::new(),
            start_pos: None,
            headers_inner: Some(headers),
            bytes_init: 0,
        }
    }

    /// Returns the amount of full records. A possible partial record isn't included in the count.
    pub fn record_count(&self) -> u64 {
        self.inner.record_ends.len() as u64
    }

    /// Tells if ByteRecordArea contains a partial record.
    /// If either of field_data or field_ends contains items that isn't contained in a full
    /// record indicated by record_ends, the arena is considered to contain a partial record.
    pub fn is_partial(&self) -> bool {
        self.inner.is_partial()
    }

    /// Tells if ByteRecordArena is empty
    /// i.e. it doesn't contain any input information. This includes:
    /// 1) doesn't contain any header information
    /// 2) doesn't contain any records
    /// However, it doesn't mean that the arena is in a "freshly initialized" state;
    /// it might contain a non-zero starting position or headers.
    pub fn is_empty(&self) -> bool {
        self.record_count() == 0 && self.is_partial().not()
    }

    // TODO: do we need this? Maybe delete it.
    /// Tells if ByteRecordArena is in the "freshly initialized" state
    /// i.e. it hasn't got any input information. This includes:
    /// 1) doesn't contain any header information
    /// 2) doesn't contain any records
    /// 3) doesn't contain partial records
    /// 4) doesn't have starting position other than 0.
    /// However, it doesn't take into account some purely internal properties that have only
    /// diminishingly small performance effects. These properties include the internal capacity
    /// of the storage fields and the info whether they have been zeroed or contain undefined bytes.
    pub fn is_pristine(&self) -> bool {
        self.is_empty() && self.headers().is_none() && self.start_pos.is_none()
    }

    /// Migrates the partial data over another arena.
    /// All data, including the partial record, on the `other` arena is deleted.
    /// Returns partial data length and partial field count.
    pub fn migrate_partial(&mut self, other: &mut ByteRecordArena) -> (usize, usize) {
        other.start_pos = None; // TODO: Is it correct to reset this?
        self.inner.migrate_partial(&mut other.inner)
    }

    /// Deletes all records except the partial record.
    /// The user is expected to continue reading the partial record.
    /// Sets the start position anew, to be set again by Reader.
    pub fn flush(&mut self) -> (usize, usize) {
        self.start_pos = None;
        self.inner.flush()
    }

    /// Deletes all records including the partial record.
    /// TODO: decide what to do with headers and start up position
    pub fn clear(&mut self) {
        self.start_pos = None;
        self.inner.clear();
    }

    pub fn headers(&self) -> Option<&Headers> {
        if let Some(headers) = &self.headers_inner {
            Some(headers)
        } else {
            None
        }
    }

    pub fn start_pos(&self) -> Option<&Position> {
        self.start_pos.as_ref()
    }

    pub fn expose_data(&mut self) -> &mut [u8] {
        let cap = self.inner.field_data.capacity();
        let old_len = self.inner.field_data.len();
        if self.bytes_init < cap {
            self.inner.field_data.resize(cap, 0);
            self.bytes_init = cap;
        } else {
            // Optimization: we don't need to initialize data
            // if it's done earlier, as indicated by `self.bytes_init`
            unsafe { self.inner.field_data.set_len(cap) };
        }
        &mut self.inner.field_data[old_len..]
    }

    pub fn terminate_field(&mut self, field_size: usize) {
        let &(last_record_end_field_data, last_record_end_field_end) =
            self.inner.record_ends.last().unwrap_or(&(0, 0));
        let field_ends = &self.inner.field_ends[last_record_end_field_end..];
        let last_field_end = *field_ends.last().unwrap_or(&0);

        // "Original" means that we reconstruct self.inner.field_data.len()
        // as it was before calling `expose_data` that extended it to the full capacity.
        let original_data_len = last_record_end_field_data + last_field_end;
        let new_data_len = original_data_len + field_size;
        assert!(new_data_len <= self.bytes_init);
        self.inner.field_data.truncate(new_data_len);

        let new_field_end = last_field_end + field_size;
        self.inner.field_ends.push(new_field_end);
    }

    pub fn terminate_record(&mut self) {
        self.inner
            .record_ends
            .push((self.inner.field_data.len(), self.inner.field_ends.len()));
    }

    pub fn reserve_space(&mut self, data: usize) {
        let old_len = self.inner.field_data.len();
        // Not only reserve, but also ensure that the buffer is initialized
        self.inner.field_data.resize(data, 0);
        self.bytes_init = self.inner.field_data.len();
        self.inner.field_data.truncate(old_len);
    }

    pub fn iter(&self) -> ByteRecordsIter<'_> {
        ByteRecordsIter(self.inner.iter())
    }

    #[cfg(feature = "serde")]
    pub fn deserialize<'de, D: Deserialize<'de>>(
        &'de self,
        output: &mut Vec<D>,
    ) -> Result<usize, error::Error> {
        deserialize::deserialize_byte_record_arena(&self.inner, None, output)
    }

    pub fn complete_partial(&mut self) {
        self.inner.complete_partial()
    }
}

impl<'a> Iterator for ByteRecordsIter<'a> {
    type Item = RawRecord<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl fmt::Debug for ByteRecordArena {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        fmt::Debug::fmt(&self.inner, f)
    }
}
