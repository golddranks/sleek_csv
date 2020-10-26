use core::fmt;
use std::error;

use crate::{deserialize::DeserializeError, printer};

#[derive(Debug, Clone)]
pub struct Error {
    kind: ErrorKind,
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self.kind {
            ErrorKind::Deserialize {
                index,
                err,
                field_data,
                field_ends,
            } => {
                write!(
                    f,
                    "ErrorKind::Deserialize at index {:?}. Error: {:?}. Field data: ",
                    index, err
                )?;
                printer::write_ascii_escaped(f, field_data)?;
                write!(f, ". Field ends: {:?}", field_ends)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl Error {
    pub fn new(kind: ErrorKind) -> Self {
        Self { kind }
    }
}

#[derive(Debug, Clone)]
pub enum ErrorKind {
    Deserialize {
        index: usize,
        err: DeserializeError,
        field_data: Vec<u8>,
        field_ends: Vec<usize>,
    },
}
