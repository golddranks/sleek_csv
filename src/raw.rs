use crate::printer;
use core::fmt;
use core::ops::Range;

#[derive(Clone, Eq, PartialEq)]
pub struct RawRecordArena {
    pub(crate) field_data: Vec<u8>, // The unescaped data from the CSV, all fields/records concatenated
    pub(crate) field_ends: Vec<usize>, // Indices of field ends in field_data, starting from 0 for each record
    pub(crate) record_ends: Vec<(usize, usize)>, // Indices of record ends in field_data and field_ends, respectively
}

pub struct RawRecordsIter<'a> {
    arena: &'a RawRecordArena,
    iter: Range<usize>,
    prev_field_data_end: usize,
    prev_field_ends_end: usize,
}

pub struct RawRecordIter<'a> {
    field_data: &'a [u8],
    field_ends: &'a [usize],
    iter: Range<usize>,
    prev_field_end: usize,
}

pub struct RawRecord<'a> {
    pub(crate) field_data: &'a [u8],
    pub(crate) field_ends: &'a [usize],
}

impl<'a> RawRecord<'a> {
    pub fn field_count(&self) -> usize {
        self.field_ends.len()
    }

    pub fn iter(&self) -> RawRecordIter<'a> {
        RawRecordIter {
            field_data: self.field_data,
            field_ends: self.field_ends,
            iter: 0..self.field_ends.len(),
            prev_field_end: 0,
        }
    }
}

impl<'a> fmt::Debug for RawRecord<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), std::fmt::Error> {
        printer::write_record(f, self.iter())
    }
}

impl<'a> Iterator for RawRecordIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if let Some(i) = self.iter.next() {
            let field_end = self.field_ends[i];
            let field = &self.field_data[self.prev_field_end..field_end];
            self.prev_field_end = field_end;
            Some(field)
        } else {
            None
        }
    }
}

impl<'a> Iterator for RawRecordsIter<'a> {
    type Item = RawRecord<'a>;

    fn next(&mut self) -> Option<RawRecord<'a>> {
        if let Some(i) = self.iter.next() {
            let (field_data_end, field_ends_end) = self.arena.record_ends[i];

            let field_data = &self.arena.field_data[self.prev_field_data_end..field_data_end];
            let field_ends = &self.arena.field_ends[self.prev_field_ends_end..field_ends_end];

            self.prev_field_data_end = field_data_end;
            self.prev_field_ends_end = field_ends_end;

            Some(RawRecord {
                field_data,
                field_ends,
            })
        } else {
            None
        }
    }
}

impl RawRecordArena {
    pub fn new() -> RawRecordArena {
        Self {
            field_data: Vec::new(),
            field_ends: Vec::new(),
            record_ends: Vec::new(),
        }
    }

    /// Tells if RawByteRecordArea contains a partial record.
    /// If either of field_data or field_ends contains items that isn't contained in a full
    /// record indicated by record_ends, the arena is considered to contain a partial record.
    pub fn is_partial(&self) -> bool {
        let (last_record_end_field_data, last_record_end_field_end) =
            *self.record_ends.last().unwrap_or(&(0, 0));
        self.field_data.len() > last_record_end_field_data
            || self.field_ends.len() > last_record_end_field_end
    }

    /// Empties the arena
    pub fn clear(&mut self) {
        self.field_data.clear();
        self.field_ends.clear();
        self.record_ends.clear();
    }

    /// Migrates the partial data over another arena.
    /// Returns partial data length and partial field count.
    pub fn migrate_partial(&mut self, other: &mut RawRecordArena) -> (usize, usize) {
        other.clear();

        let (last_record_end_field_data, last_record_end_field_end) =
            *self.record_ends.last().unwrap_or(&(0, 0));

        // Partial data
        let partial_field_data = &self.field_data[last_record_end_field_data..];
        let partial_field_ends = &self.field_ends[last_record_end_field_end..];

        // Copy values over
        other.field_data.extend_from_slice(partial_field_data);
        other.field_ends.extend_from_slice(partial_field_ends);

        let lengths = (partial_field_data.len(), partial_field_ends.len());

        // Truncate to remove the "moved" partials.
        self.field_data.truncate(last_record_end_field_data);
        self.field_ends.truncate(last_record_end_field_end);

        lengths
    }

    /// Deletes all records except the partial record.
    /// The user is expected to continue reading the partial record.
    /// Returns partial data length and partial field count.
    pub fn flush(&mut self) -> (usize, usize) {
        let (last_record_end_field_data, last_record_end_field_end) =
            if let Some(rec_end) = self.record_ends.last() {
                rec_end
            } else {
                return (self.field_data.len(), self.field_ends.len()); // Already empty of full records.
            };

        // Partial data
        let partial_field_data = last_record_end_field_data..;
        let partial_field_ends = last_record_end_field_end..;

        self.field_data.copy_within(partial_field_data, 0);
        self.field_ends.copy_within(partial_field_ends, 0);

        let partial_field_data_len = self.field_data.len() - *last_record_end_field_data;
        let partial_field_ends_len = self.field_ends.len() - *last_record_end_field_end;

        self.field_data.truncate(partial_field_data_len);
        self.field_ends.truncate(partial_field_ends_len);
        self.record_ends.truncate(0);

        (partial_field_data_len, partial_field_ends_len)
    }

    pub fn iter(&self) -> RawRecordsIter<'_> {
        RawRecordsIter {
            arena: &self,
            iter: 0..self.record_ends.len(),
            prev_field_data_end: 0,
            prev_field_ends_end: 0,
        }
    }

    pub fn iter_partial(&self) -> RawRecordIter<'_> {
        let (last_record_end_field_data, last_record_end_field_ends) =
            *self.record_ends.last().unwrap_or(&(0, 0));
        let field_data = &self.field_data[last_record_end_field_data..];
        let field_ends = &self.field_ends[last_record_end_field_ends..];
        RawRecordIter {
            field_data,
            field_ends,
            iter: 0..field_ends.len(),
            prev_field_end: 0,
        }
    }

    pub fn get_last_partial_field(&self) -> Option<&[u8]> {
        let (last_record_end_field_data, last_record_end_field_ends) =
            *self.record_ends.last().unwrap_or(&(0, 0));
        let field_data = &self.field_data[last_record_end_field_data..];
        let field_ends = &self.field_ends[last_record_end_field_ends..];
        let last_field_end = *field_ends.last().unwrap_or(&0);
        if last_field_end == field_data.len() {
            return None;
        }
        Some(&field_data[last_field_end..])
    }

    pub fn complete_partial(&mut self) {
        if let Some(last_partial_field) = self.get_last_partial_field() {
            let last_field_end = *self.field_ends.last().unwrap_or(&0);
            let final_field_end = last_field_end + last_partial_field.len();
            self.field_ends.push(final_field_end);
        }
        self.record_ends
            .push((self.field_data.len(), self.field_ends.len()));
    }
}

impl std::fmt::Debug for RawRecordArena {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "RawRecordArena. {} records.\n", self.record_ends.len())?;
        write!(f, "field_data: {}\n", self.field_data.len())?;
        write!(f, "field_ends: {}\n", self.field_ends.len())?;

        for mut record in self.iter() {
            fmt::Debug::fmt(&mut record, f)?;
        }
        if self.is_partial() {
            write!(f, " + partial record: ")?;
            printer::write_record(f, self.iter_partial())?;

            if let Some(partial_field) = self.get_last_partial_field() {
                write!(f, " ...and a partial field: ")?;
                printer::write_ascii_escaped(f, partial_field)?;
            }
            write!(f, "\n")?;
        }
        Ok(())
    }
}
