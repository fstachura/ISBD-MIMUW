use std::string::FromUtf8Error;
use std::io::{BufRead, BufReader, Read, Write};

use zstd::stream::{Decoder, Encoder};

pub use crate::utils::{decode_vle, decode_vle_vec, encode_vle, encode_i64_vle, decode_i64_vle, read_strings, read_numbers};

const ZSTD_COMPRESSION_LEVEL: i32 = 3;
pub const MAGIC: [u8; 4] = [b'I', b'S', b'B', b'D'];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    Int64 = 0,
    Str = 1,
}

impl TryFrom<u32> for ColumnType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ColumnType::Int64),
            1 => Ok(ColumnType::Str),
            _ => Err(()),
        }
    }
}

impl From<ColumnType> for u32 {
    fn from(val: ColumnType) -> u32 {
        match val {
            ColumnType::Int64 => 0,
            ColumnType::Str => 1,
        }
    }
}

// file format:
// magic: u32 (ISBD)
// column type: u32
// number of chunks: u64
// (chunks start)
//
// for int64:
// chunk len in bytes: u64
// chunk len in numbers: u64
// first num: i64
// (data)
//
// for string:
// chunk len in bytes: u64
// chunk len in rows: u64
// zstd compressed data:
// string len: u32
// string: [u8]

#[derive(Debug)]
pub enum DeserializerError {
    InvalidMagic,
    InvalidColumnType,
    InvalidInt64,
    TooShort,
    InvalidString(FromUtf8Error),
    IOError(std::io::Error),
}

pub const HEADER_SIZE: usize = 2 * size_of::<u32>() + size_of::<u64>();

pub fn create_header(writer: &mut impl Write, datatype: ColumnType, number_of_chunks: u64) -> std::io::Result<()> {
    writer.write_all(&MAGIC)?;
    writer.write_all(&(Into::<u32>::into(datatype)).to_be_bytes())?;
    writer.write_all(&number_of_chunks.to_be_bytes())?;
    Ok(())
}

pub fn parse_header(buf: &[u8; HEADER_SIZE]) -> Result<(ColumnType, u64), DeserializerError> {
    if buf[0..4] != MAGIC {
        return Err(DeserializerError::InvalidMagic);
    }

    // check if type matches
    let ctype: ColumnType = u32::from_be_bytes(
            buf[4..8].try_into().or(Err(DeserializerError::TooShort))?
        )
        .try_into().or(Err(DeserializerError::InvalidColumnType))?;

    // read number of chunks
    let num_of_chunks = u64::from_be_bytes(buf[8..16].try_into().unwrap());

    Ok((ctype, num_of_chunks))
}

pub const CHUNK_HEADER_SIZE: usize = 2 * size_of::<u64>();

pub struct ChunkHeader {
    pub bytes: u64,
    pub rows: u64,
}

pub fn parse_chunk_header(buf: &[u8; CHUNK_HEADER_SIZE]) -> Result<ChunkHeader, DeserializerError> {
    let bytes = u64::from_be_bytes(buf[0..8].try_into().or(Err(DeserializerError::TooShort))?);
    let rows = u64::from_be_bytes(buf[8..16].try_into().or(Err(DeserializerError::TooShort))?);

    Ok(ChunkHeader { bytes, rows })
}


// does not take a write because it needs to calculate chunk size to write the whole chunk.
pub fn create_int64_chunk(nums: &[i64]) -> Vec<u8> {
    let mut result = Vec::with_capacity(CHUNK_HEADER_SIZE + nums.len()*4);

    // write chunk len
    result.extend_from_slice(&0_u64.to_be_bytes());

    // write chunk len in numbers
    result.extend_from_slice(&(nums.len() as u64).to_be_bytes());

    let min_num = nums.iter().min().copied().unwrap_or(0);
    result.extend_from_slice(&encode_i64_vle(min_num));
    for num in nums {
        // should not occur as all numbers are larger than min_num
        let num = num.checked_sub(min_num).unwrap();
        result.extend_from_slice(&encode_i64_vle(num));
    }

    let chunk_len = result.len()-CHUNK_HEADER_SIZE;
    result[0..8].copy_from_slice(&(chunk_len as u64).to_be_bytes());

    result
}

// only sync read - regardless of async you are meant to read the whole chunk into memory
pub struct Int64ColumnChunkIterator<'a, T: Read> {
    pub data: &'a mut T,
    pub remaining_items: u64,
    pub leader: i64,
    pub error: bool,
}

impl<'a, T: Read> Iterator for Int64ColumnChunkIterator<'a, T> {
    type Item = Result<i64, DeserializerError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_items != 0 && !self.error {
            self.remaining_items -= 1;
            match decode_i64_vle(&mut self.data).map(|v| v.map(|v| v + self.leader)) {
                Ok(Some(n)) => Some(Ok(n)),
                Ok(None) => {
                    self.error = true;
                    Some(Err(DeserializerError::InvalidInt64))
                },
                Err(err) => {
                    self.error = true;
                    Some(Err(DeserializerError::IOError(err)))
                },
            }
        } else {
            None
        }
    }
}

pub fn parse_int64_chunk<'a, T: Read>(rows: u64, data: &'a mut T) -> Result<Int64ColumnChunkIterator<'a, T>, DeserializerError> {
    let leader = decode_i64_vle(data)
        .map_err(DeserializerError::IOError)?
        .ok_or(DeserializerError::InvalidInt64)?;

    Ok(Int64ColumnChunkIterator {
        data,
        remaining_items: rows,
        leader,
        error: false,
    })
}


pub fn create_str_chunk(strs: &[String]) -> std::io::Result<Vec<u8>> {
    let mut result = Vec::with_capacity(CHUNK_HEADER_SIZE);

    // write chunk len
    result.extend_from_slice(&0_u64.to_be_bytes());

    // write chunk len in rows
    result.extend_from_slice(&(strs.len() as u64).to_be_bytes());

    let mut encoder = Encoder::new(result, ZSTD_COMPRESSION_LEVEL)?;
    // TODO: multithreaded compression? only makes sense for really large chunks

    for s in strs {
        let bytes = s.as_bytes();
        encoder.write_all(&(bytes.len() as u32).to_be_bytes())?;
        encoder.write_all(bytes)?;
    }

    let mut result = encoder.finish()?;
    let chunk_len = result.len() - CHUNK_HEADER_SIZE;

    result[0..8].copy_from_slice(&(chunk_len as u64).to_be_bytes());

    Ok(result)
}

pub struct StringColumnChunkIterator<'a, T> {
    pub decoder: Decoder<'a, T>,
    pub remaining_items: u64,
    pub error: bool,
}

impl<'a, T: BufRead> Iterator for StringColumnChunkIterator<'a, T> {
    type Item = Result<String, DeserializerError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_items != 0 && !self.error {
            let mut len_buf: [u8; 4] = [0; 4];
            if let Err(err) = self.decoder.read(&mut len_buf) {
                self.error = true;
                return Some(Err(DeserializerError::IOError(err)));
            }

            let len = u32::from_be_bytes(len_buf);

            let mut buf: Vec<u8> = vec![0; len as usize];
            if let Err(err) = self.decoder.read(&mut buf) {
                self.error = true;
                return Some(Err(DeserializerError::IOError(err)));
            }

            self.remaining_items -= 1;
            match String::from_utf8(buf) {
                Ok(s) => Some(Ok(s)),
                Err(err) => {
                    self.error = true;
                    Some(Err(DeserializerError::InvalidString(err)))
                }
            }
        } else {
            None
        }
    }
}

pub fn parse_str_chunk(rows: u64, data: &mut impl Read) -> Result<StringColumnChunkIterator<'_, BufReader<&mut impl Read>>, DeserializerError> {
    Ok(StringColumnChunkIterator {
        decoder: Decoder::new(data).unwrap(),
        remaining_items: rows,
        error: false,
    })
}

