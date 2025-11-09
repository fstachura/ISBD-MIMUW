use std::collections::HashMap;
use std::hash::Hash;
use std::iter::Map;
use std::string::FromUtf8Error;
use std::{env::args, fs::File, process::exit};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};

use zstd::stream::{Decoder, Encoder};

mod utils;
pub use utils::{decode_vle, encode_vle, decode_vle_vec, encode_i64_vle, decode_i64_vle, read_strings, read_numbers};

const NUM_PER_CHUNK: usize = 4;
const ZSTD_COMPRESSION_LEVEL: i32 = 3;
const MAGIC: [u8; 4] = ['I' as u8, 'S' as u8, 'B' as u8, 'D' as u8];

#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnType {
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

impl Into<u32> for ColumnType {
    fn into(self) -> u32 {
        match self {
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

// write magic, column type, set number of chunks to zero, remember seek
// write chunk len, write first number, encode next number etc. count bytes
// when numbers or chunk ends, come back to chunk len, write it. come back to num of chunks, write
// it too.

#[derive(Debug)]
enum DeserializerError {
    InvalidMagic,
    InvalidColumnType,
    InvalidInt64,
    InvalidString(FromUtf8Error),
    IOError(std::io::Error),
}

fn read_header(file: &mut File) -> Result<(ColumnType, u64), DeserializerError> {
    let mut buf: [u8; 4] = [0; 4];
    let mut buf8: [u8; 8] = [0; 8];

    // check if magic matches
    file.read_exact(&mut buf).or_else(|e| Err(DeserializerError::IOError(e)))?;
    if buf != MAGIC {
        println!("{buf:?}");
        return Err(DeserializerError::InvalidMagic);
    }

    // check if type matches
    file.read_exact(&mut buf).or_else(|e| Err(DeserializerError::IOError(e)))?;
    let ctype: ColumnType = u32::from_be_bytes(buf).try_into().or(Err(DeserializerError::InvalidColumnType))?;

    // read number of chunks
    file.read_exact(&mut buf8).or_else(|e| Err(DeserializerError::IOError(e)))?;
    let num_of_chunks = u64::from_be_bytes(buf8);

    Ok((ctype, num_of_chunks))
}


fn write_int64_chunk(file: &mut File, nums: &Vec<i64>) -> std::io::Result<()> {
    // write chunk len
    let chunk_len_stream_pos = file.stream_position()?;
    file.write(&(0 as u64).to_be_bytes())?;

    // write chunk len in numbers
    file.write(&(nums.len() as u64).to_be_bytes())?;

    let min_num = nums.iter().min().map(|v| *v).or(Some(0)).unwrap();
    let mut chunk_len: usize = 0;
    chunk_len += file.write(&encode_i64_vle(min_num))?;
    for num in nums {
        // should not occur as all numbers are larger
        let num = num.checked_sub(min_num).unwrap();
        chunk_len += file.write(&encode_i64_vle(num))?;
    }

    file.seek(SeekFrom::Start(chunk_len_stream_pos))?;
    file.write(&(chunk_len as u64).to_be_bytes())?;

    file.seek(SeekFrom::End(0))?;

    Ok(())
}

struct Int64ColumnChunkIterator {
    file: File,
    remaining_items: u64,
    total_bytes: u64,
    leader: i64,
    error: Option<DeserializerError>
}

impl Iterator for Int64ColumnChunkIterator {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_items != 0 && self.error.is_none() {
            self.remaining_items -= 1;
            match decode_i64_vle(&mut self.file).map(|v| v.map(|v| v + self.leader)) {
                Ok(Some(n)) => Some(n),
                Ok(None) => {
                    self.error = Some(DeserializerError::InvalidInt64);
                    None
                }
                Err(err) => {
                    self.error = Some(DeserializerError::IOError(err));
                    None
                }
            }
        } else {
            None
        }
    }
}

fn read_int64_chunk(file: &mut File) -> Result<Int64ColumnChunkIterator, DeserializerError> {
    let mut buf: [u8; 8] = [0; 8];

    file.read_exact(&mut buf).or_else(|e| Err(DeserializerError::IOError(e)))?;
    let bytes = u64::from_be_bytes(buf);

    file.read_exact(&mut buf).or_else(|e| Err(DeserializerError::IOError(e)))?;
    let nums = u64::from_be_bytes(buf);

    let leader = decode_i64_vle(file)
        .or_else(|e| Err(DeserializerError::IOError(e)))?
        .ok_or(DeserializerError::InvalidInt64)?;

    Ok(Int64ColumnChunkIterator {
        file: file.try_clone().or_else(|e| Err(DeserializerError::IOError(e)))?,
        remaining_items: nums,
        total_bytes: bytes,
        leader: leader,
        error: None,
    })
}



fn write_str_chunk(file: &mut File, strs: &Vec<String>) -> std::io::Result<()> {
    // write chunk len
    let chunk_len_stream_pos = file.stream_position()?;
    file.write(&(0 as u64).to_be_bytes())?;

    // write chunk len in rows
    file.write(&(strs.len() as u64).to_be_bytes())?;

    let begin_pos = file.stream_position()?;

    let mut encoder = Encoder::new(file, ZSTD_COMPRESSION_LEVEL)?;
    // TODO: multithreaded compression? only makes sense for really large chunks

    for s in strs {
        encoder.write(&(s.len() as u32).to_be_bytes())?;
        encoder.write(&s.as_bytes())?;
    }

    let file = encoder.finish()?;
    let chunk_len = file.stream_position()? - begin_pos;

    file.seek(SeekFrom::Start(chunk_len_stream_pos))?;
    file.write(&(chunk_len as u64).to_be_bytes())?;

    file.seek(SeekFrom::End(0))?;

    Ok(())
}

struct StringColumnChunkIterator<'a, T> {
    decoder: Decoder<'a, T>,
    remaining_items: u64,
    total_bytes: u64,
    error: Option<DeserializerError>
}

impl<'a, T: BufRead> Iterator for StringColumnChunkIterator<'a, T> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_items != 0 && self.error.is_none() {
            let mut len_buf: [u8; 4] = [0; 4];
            if let Err(err) = self.decoder.read(&mut len_buf) {
                println!("failed num read");
                self.error = Some(DeserializerError::IOError(err));
                return None
            }

            let len = u32::from_be_bytes(len_buf);

            let mut buf: Vec<u8> = Vec::new();
            buf.resize(len as usize, 0);
            if let Err(err) = self.decoder.read(&mut buf) {
                println!("failed str read");
                self.error = Some(DeserializerError::IOError(err));
                return None
            }

            self.remaining_items -= 1;
            match String::from_utf8(buf) {
                Ok(s) => Some(s),
                Err(err) => {
                    self.error = Some(DeserializerError::InvalidString(err));
                    None
                }
            }
        } else {
            None
        }
    }
}

fn read_str_chunk(file: &mut File) -> Result<StringColumnChunkIterator<'_, BufReader<&mut File>>, DeserializerError> {
    let mut buf: [u8; 8] = [0; 8];

    file.read_exact(&mut buf).or_else(|e| Err(DeserializerError::IOError(e)))?;
    let bytes = u64::from_be_bytes(buf);

    file.read_exact(&mut buf).or_else(|e| Err(DeserializerError::IOError(e)))?;
    let nums = u64::from_be_bytes(buf);

    Ok(StringColumnChunkIterator {
        decoder: Decoder::new(file).unwrap(),
        remaining_items: nums,
        total_bytes: bytes,
        error: None,
    })
}


fn main() {
    let mut args = args();
    if args.len() != 3 && args.len() != 4 {
        println!("usage: [encode [int|string]|decode] [filename]");
        exit(-1);
    }

    args.next().unwrap();
    let mode = args.next().unwrap();

    match mode.as_str() {
        "encode" => {
            let datatype = args.next().unwrap();
            let filename = args.next().unwrap();

            let datatype = match datatype.as_str() {
                "int" => ColumnType::Int64,
                "str" => ColumnType::Str,
                _ => {
                    println!("invalid datatype {datatype}. valid types: int, str.");
                    exit(-1);
                }
            };

            let mut file = File::options()
                .create(true)
                .read(true)
                .write(true)
                .append(false)
                .open(filename.clone().trim())
                .unwrap();

            let mut buf: [u8; 4] = [0; 4];
            let first_read = file.read(&mut buf).unwrap();
            let mut num_of_chunks: u64 = 0;

            // file is empty
            if first_read == 0 {
                // magic: u32
                file.write(&MAGIC).unwrap();
                // type: u32
                file.write(&(Into::<u32>::into(datatype)).to_be_bytes()).unwrap();
                // number of chunks: u64
                file.write(&(0 as u64).to_be_bytes()).unwrap();
            } else if first_read == 4 {
                file.seek(SeekFrom::Start(0)).unwrap();

                let (ctype, nc) = read_header(&mut file).unwrap();
                num_of_chunks = nc;
                if ctype != datatype {
                    panic!("file {filename} contains column of different data type {ctype:?}.");
                }

                println!("appending to file with {num_of_chunks} chunks");

                // TODO verify if rest of the file is ok?
                // move to end of the file
                file.seek(SeekFrom::End(0)).unwrap();
            } else {
                panic!("file {filename} exists but is too short to be a valid database file");
            }

            let mut reading = true;

            match datatype {
                ColumnType::Int64 => while reading {
                    let nums = read_numbers(NUM_PER_CHUNK)
                        .expect("failed to read numbers");

                    if nums.is_empty() {
                        break;
                    } else if nums.len() != NUM_PER_CHUNK {
                        reading = false;
                    }

                    write_int64_chunk(&mut file, &nums)
                        .expect("failed to write chunk");
                    num_of_chunks += 1;
                },
                ColumnType::Str => while reading {
                    let strs = read_strings(NUM_PER_CHUNK)
                        .expect("failed to read strings");

                    if strs.is_empty() {
                        break;
                    } else if strs.len() != NUM_PER_CHUNK {
                        reading = false;
                    }

                    write_str_chunk(&mut file, &strs)
                        .expect("failed to write chunk");
                    num_of_chunks += 1;
                }
            }

            println!("total number of chunks: {num_of_chunks}");

            // write number of chunks
            file.seek(SeekFrom::Start(8)).unwrap();
            file.write(&num_of_chunks.to_be_bytes()).unwrap();
        },
        "decode" => {
            let filename = args.next().unwrap();
            let mut file = File::options()
                .create(true)
                .read(true)
                .write(true)
                .append(false)
                .open(filename.clone().trim())
                .unwrap();

            let (ctype, nc) = read_header(&mut file).unwrap();

            match ctype {
                ColumnType::Int64 => {
                    let mut sum: i64 = 0;
                    let mut nums: i64 = 0;
                    println!("opened file of type {ctype:?} with {nc} chunks");
                    for i in 0..nc {
                        println!("chunk {i}");
                        let mut chunk_it = read_int64_chunk(&mut file).unwrap();
                        while let Some(n) = chunk_it.next() {
                            println!("{n}");
                            sum = sum.saturating_add(n);
                            nums += 1;
                        }
                        if let Some(err) = chunk_it.error {
                            panic!("chunk iterator ended with error {err:?}");
                        }
                    }
                    let avg = sum.saturating_div(nums);
                    println!("sum: {sum}, numbers: {nums}, avg: {avg}");
                },
                ColumnType::Str => {
                    let mut chars: HashMap<char, u64> = HashMap::new();
                    let mut chunk_pos = file.stream_position().unwrap();
                    println!("opened file of type {ctype:?} with {nc} chunks");

                    for i in 0..nc {
                        println!("chunk {i} {chunk_pos}");
                        file.seek(SeekFrom::Start(chunk_pos)).unwrap();
                        let mut chunk_it = read_str_chunk(&mut file).unwrap();
                        while let Some(s) = chunk_it.next() {
                            println!("{s}");
                            for c in s.chars() {
                                if c >= 0 as char && c <= 127 as char {
                                    if !chars.contains_key(&c) {
                                        chars.insert(c, 1);
                                    } else {
                                        chars.insert(c, chars[&c]+1);
                                    }
                                }
                            }
                        }
                        if let Some(err) = chunk_it.error {
                            panic!("chunk iterator ended with error {err:?}");
                        }
                        chunk_pos += 16 + chunk_it.total_bytes;
                    }

                    println!("{chars:?}");
                },
            }
        },
        _ => {
            println!("invalid mode {mode}. valid modes: encode, decode.");
            exit(-1);
        },
    }
}
