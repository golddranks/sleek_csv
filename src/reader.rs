use std::{error, fmt};

use crate::{ByteRecordArena, RawRecordArena, Position};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReadRecordResult {
    NeedsMoreInput,
    NeedsMoreInputOrEof,
    Record(usize),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
/// 0-based rows, not including header.
pub struct WrongColCount {
    pub row_num: usize,
    pub col_count: usize,
    pub expected_col_count: usize,
}

impl error::Error for WrongColCount {}

impl fmt::Display for WrongColCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Wrong column count on row {} (0-based, header not in count). Expected {}, got {}.",
            self.row_num, self.expected_col_count, self.col_count
        )?;
        Ok(())
    }
}

pub struct Reader {
    inner: csv_core::Reader,
    field_data_len: usize, // Temporarily stores arena field_data length while the Vec is overcommitted
    field_ends_len: usize, // Temporarily stores arena field_ends length while the Vec is overcommitted
    skip_header: bool,
    ensure_col_count: bool,
    bytes_read: u64,
    records_read: u64,
}

impl Reader {
    pub fn new(first_row_is_header: bool, delim: u8) -> Reader {
        Self {
            inner: csv_core::ReaderBuilder::new().delimiter(delim).build(),
            field_data_len: 0,
            field_ends_len: 0,
            skip_header: first_row_is_header,
            ensure_col_count: true,
            bytes_read: 0,
            records_read: 0,
        }
    }

    pub fn from_core(reader: csv_core::Reader, first_row_is_header: bool) -> Reader {
        Self {
            inner: reader,
            field_data_len: 0,
            field_ends_len: 0,
            skip_header: first_row_is_header,
            ensure_col_count: true,
            bytes_read: 0,
            records_read: 0,
        }
    }

    fn arena_overcommit(&mut self, arena: &mut RawRecordArena, input_size: usize) {
        debug_assert_eq!(self.field_data_len, 0);
        debug_assert_eq!(self.field_ends_len, 0);
        self.field_data_len = arena.field_data.len();
        self.field_ends_len = arena.field_ends.len();

        arena
            .field_data
            .resize(arena.field_data.len() + input_size, 0);

        // We don't know the exact count of the fields,
        // but let's approximate with each field having 8 bytes at average
        arena
            .field_ends
            .resize(arena.field_ends.len() + input_size / 8 + 1, 0);
    }

    fn arena_shrink_back(&mut self, arena: &mut RawRecordArena) {
        debug_assert!(self.field_data_len <= arena.field_data.len());
        debug_assert!(self.field_ends_len <= arena.field_ends.len());
        arena.field_data.truncate(self.field_data_len);
        arena.field_ends.truncate(self.field_ends_len);
        self.field_data_len = 0;
        self.field_ends_len = 0;
    }

    pub fn arena_extend_field_data(&mut self, arena: &mut RawRecordArena) {
        debug_assert!(self.field_data_len <= arena.field_data.len());
        debug_assert!(self.field_ends_len <= arena.field_ends.len());
        arena.field_data.resize(arena.field_data.len() * 2, 0);
    }

    pub fn arena_extend_field_ends(&mut self, arena: &mut RawRecordArena) {
        debug_assert!(self.field_data_len <= arena.field_data.len());
        debug_assert!(self.field_ends_len <= arena.field_ends.len());
        arena.field_ends.resize(arena.field_ends.len() * 2, 0);
    }

    fn scrape_headers(&mut self, arena: &mut RawRecordArena) -> crate::Headers {
        let (header_data_len, header_ends_len) = arena.record_ends.pop().expect("");
        let headers = crate::Headers {
            name_data: arena.field_data[..header_data_len].to_owned(),
            name_ends: arena.field_ends[..header_ends_len].to_owned(),
        };

        let (prev_field_data_len, prev_field_ends_len) =
            arena.record_ends.last().unwrap_or(&(0, 0));
        self.field_data_len = *prev_field_data_len;
        self.field_ends_len = *prev_field_ends_len;

        headers
    }

    pub fn fill_arena<'a>(
        &mut self,
        mut input: &'a [u8],
        arena_outer: &mut ByteRecordArena,
    ) -> Result<(), WrongColCount> {
        let mut expected_col_count = arena_outer.headers().map(|h| h.len());
        // The empty case must be checked because the CSV core reader
        // considers (erroneously) a record having ended if an empty slice is passed in.
        if input.is_empty() {
            return Ok(());
        }

        if arena_outer.start_pos.is_none() {
            // TODO: determine the exact semantics what counts as "clear" and "empty"
            //       when should we rewrite the start_pos? should we track if it's
            //       initialized or not?
            // For now, restart if fill_arena is called for an arena that contains no records
            let start_pos = Position {
                line: self.inner.line(),
                byte: self.bytes_read,
                record: self.records_read,
            };
            arena_outer.start_pos = Some(start_pos);
        }

        let input_total_bytes = input.len();
        let arena_orig_record_count = arena_outer.record_count();

        let arena = &mut arena_outer.inner;
        self.arena_overcommit(arena, input.len());
        let res = loop {
            let (result, unparsed) = self.read_record(input, arena);
            input = unparsed;
            match result {
                ReadRecordResult::NeedsMoreInput | ReadRecordResult::NeedsMoreInputOrEof => {
                    debug_assert!(input.is_empty());
                    break Ok(());
                }
                ReadRecordResult::Record(col_count) => {
                    if self.ensure_col_count {
                        if let Some(expected_col_count) = expected_col_count {
                            if col_count != expected_col_count {
                                break Err(WrongColCount {
                                    row_num: arena.record_ends.len() - 1,
                                    col_count: col_count,
                                    expected_col_count: expected_col_count,
                                });
                            }
                        } else {
                            expected_col_count = Some(col_count)
                        }
                    }
                    // If the reader must skip header, we remove the newly read record,
                    // save it as a header and roll back
                    // to the field_data and field_ends lengths.
                    if self.skip_header {
                        self.skip_header = false;
                        assert!(arena_outer.headers_inner.is_none());
                        arena_outer.headers_inner = Some(self.scrape_headers(arena));
                    }
                }
            }
        };
        self.arena_shrink_back(arena);
        self.bytes_read += (input_total_bytes - input.len()) as u64;
        self.records_read += arena_outer.record_count() - arena_orig_record_count;
        res
    }

    fn read_record<'a>(
        &mut self,
        mut input: &'a [u8],
        arena: &mut RawRecordArena,
    ) -> (ReadRecordResult, &'a [u8]) {
        let res = loop {
            let (inner_res, bytes_in, bytes_out, ends_out) = self.inner.read_record(
                input,
                &mut arena.field_data[self.field_data_len..],
                &mut arena.field_ends[self.field_ends_len..],
            );

            // Consume input
            input = &input[bytes_in..];

            // Update buffers
            self.field_data_len += bytes_out;
            self.field_ends_len += ends_out;

            match inner_res {
                // Adds capacity and tries again
                csv_core::ReadRecordResult::OutputFull => self.arena_extend_field_data(arena),
                csv_core::ReadRecordResult::OutputEndsFull => self.arena_extend_field_ends(arena),

                // Returns with status
                csv_core::ReadRecordResult::InputEmpty => break ReadRecordResult::NeedsMoreInput,
                csv_core::ReadRecordResult::End => break ReadRecordResult::NeedsMoreInputOrEof,
                csv_core::ReadRecordResult::Record => {
                    let last_record_end_field_end = arena.record_ends.last().unwrap_or(&(0, 0)).1;
                    let col_count = self.field_ends_len - last_record_end_field_end;
                    arena
                        .record_ends
                        .push((self.field_data_len, self.field_ends_len));
                    break ReadRecordResult::Record(col_count);
                }
            };
        };
        (res, input)
    }
}
