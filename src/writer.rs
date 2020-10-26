use std::ops::Not;

use crate::raw::RawRecord;
use crate::ByteRecordArena;

pub struct Writer {
    inner: csv_core::Writer,
    skip_header: bool,
    bytes_written: u64,
    records_written: u64,
}

impl Writer {
    pub fn new(skip_header: bool, delim: u8) -> Writer {
        Self {
            inner: csv_core::WriterBuilder::new().delimiter(delim).build(),
            skip_header,
            bytes_written: 0,
            records_written: 0,
        }
    }

    pub fn from_core(writer: csv_core::Writer, first_row_is_header: bool) -> Writer {
        Self {
            inner: writer,
            skip_header: first_row_is_header,
            bytes_written: 0,
            records_written: 0,
        }
    }

    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    fn write_record(
        record: &RawRecord,
        writer: &mut csv_core::Writer,
        out_buffer: &mut [u8],
    ) -> usize {
        let mut record_bytes_out = 0;
        let field_count = record.field_count() - 1;
        for (i, field) in record.iter().enumerate() {
            let output = &mut out_buffer[record_bytes_out..];
            let (res, bytes_in, bytes_out) = writer.field(field, output);
            // We expect the output buffer to be prepared to have enough space
            debug_assert_eq!(res, csv_core::WriteResult::InputEmpty);
            debug_assert_eq!(bytes_in, field.len());
            record_bytes_out += bytes_out;

            let output = &mut out_buffer[record_bytes_out..];
            let (res, bytes_out) = if i < field_count {
                writer.delimiter(output)
            } else {
                writer.terminator(output)
            };
            // We expect the output buffer to be prepared to have enough space
            debug_assert_eq!(res, csv_core::WriteResult::InputEmpty);
            // 2 bytes if there's a end quote and delimiter,
            // 1 byte in case of delimiter only
            debug_assert!(bytes_out == 1 || bytes_out == 2);
            record_bytes_out += bytes_out;
        }
        record_bytes_out
    }


    pub fn dump_arena(&mut self, out_buffer: &mut Vec<u8>, arena_outer: &ByteRecordArena) {
        let arena = &arena_outer.inner;

        // considering CSV quoting, output size is 2 + (2 * field.len()) at maximum
        let fields_len = 2 + (2 * arena.field_data.len());
        let separators_len = arena.field_ends.len();
        let terminators_len = arena.record_ends.len();
        let max_output_len = fields_len + separators_len + terminators_len;
        out_buffer.clear();
        out_buffer.reserve(max_output_len);
        // This unsafe is okay, because
        // 1) the vec only has allocated memory, guaranteed by `reserve`
        // 2) We don't attempt to read the contents (that might be indeterminate bytes), only write.
        // 3) We set the length of the vector back to area what is certainly written into in the end.
        unsafe { out_buffer.set_len(max_output_len) };
        let mut total_bytes_out = 0;

        if let Some(headers) = &arena_outer.headers_inner {
            if self.skip_header.not() {
                let header_record = RawRecord {
                    field_data: headers.name_data.as_slice(),
                    field_ends: headers.name_ends.as_slice(),
                };
                total_bytes_out += Self::write_record(
                    &header_record,
                    &mut self.inner,
                    &mut out_buffer[total_bytes_out..],
                );
                self.skip_header = true;
            }
        }

        for record in arena.iter() {
            total_bytes_out +=
                Self::write_record(&record, &mut self.inner, &mut out_buffer[total_bytes_out..]);
        }
        self.bytes_written += out_buffer.len() as u64;
        self.records_written += arena_outer.record_count();
        // out_buffer is "safe" again now:
        out_buffer.truncate(total_bytes_out);
    }
}
