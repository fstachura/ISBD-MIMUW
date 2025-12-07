use std::collections::HashMap;
use std::{env::args, fs::File, process::exit};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};

const NUM_PER_CHUNK: usize = 8192;
const BUFFER_BYTES: usize = 4*1024*1024;

mod format;
pub use format::*;

mod utils;

struct ChunkIterator<'a, T: Read + Seek> {
    file: &'a mut T,
    next_chunk_pos: u64,
    chunks_left: u64,
    error: Option<DeserializerError>,
}

fn parse_chunks<'a, T: Read + Seek>(file: &'a mut T, chunks: u64) -> ChunkIterator<'a, T> {
    ChunkIterator {
        file,
        next_chunk_pos: HEADER_SIZE as u64,
        chunks_left: chunks,
        error: None
    }
}

impl<'a, T: Read + Seek> ChunkIterator<'a, T> {
    fn get_chunk(&mut self) -> Result<(Vec<u8>, ChunkHeader), DeserializerError> {
        let mut chunk_header_buf = [0; CHUNK_HEADER_SIZE];

        self.file.seek(SeekFrom::Start(self.next_chunk_pos))
            .map_err(DeserializerError::IOError)?;

        self.file.read_exact(&mut chunk_header_buf)
            .map_err(DeserializerError::IOError)?;

        let chunk_header = parse_chunk_header(&chunk_header_buf)?;

        let mut chunk_data = vec![0; chunk_header.bytes as usize];
        self.file.read_exact(&mut chunk_data)
            .map_err(DeserializerError::IOError)?;

        Ok((chunk_data, chunk_header))
    }
}

impl<'a, T: Read + Seek> Iterator for ChunkIterator<'a, T> {
    type Item = (Vec<u8>, ChunkHeader);

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_none() && self.chunks_left > 0 {
            match self.get_chunk() {
                Ok((chunk, header)) => {
                    self.next_chunk_pos += 16 + header.bytes;
                    self.chunks_left -= 1;
                    Some((chunk, header))
                },
                Err(e) => {
                    self.error = Some(e);
                    None
                }
            }
        } else {
            None
        }
    }
}

fn encode_column(file: &mut File, datatype: ColumnType) {
    let mut buf: [u8; 4] = [0; 4];
    let first_read = file.read(&mut buf).unwrap();
    let mut num_of_chunks: u64 = 0;

    // file is empty
    if first_read == 0 {
        let mut header_buf = [0; HEADER_SIZE];
        create_header(&mut header_buf.as_mut_slice(), datatype, 0).unwrap();
        file.write_all(&header_buf).unwrap();
    } else if first_read == 4 {
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut header_buf = [0; HEADER_SIZE];
        file.read_exact(&mut header_buf).unwrap();

        let (ctype, nc) = parse_header(&header_buf).unwrap();
        num_of_chunks = nc;
        if ctype != datatype {
            panic!("file contains column of different data type {ctype:?}.");
        }

        println!("appending to file with {num_of_chunks} chunks");

        // TODO verify if rest of the file is ok?
        // move to end of the file
        file.seek(SeekFrom::End(0)).unwrap();
    } else {
        panic!("file exists but is too short to be a valid database file");
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

            let to_write = create_int64_chunk(&nums);
            file.write_all(&to_write).expect("failed to write chunk");
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

            let to_write = create_str_chunk(&strs).unwrap();
            file.write_all(&to_write).expect("failed to write chunk");
            num_of_chunks += 1;
        }
    }

    println!("total number of chunks: {num_of_chunks}");

    // write number of chunks
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&num_of_chunks.to_be_bytes()).unwrap();
}

fn decode_i64_column(file: &mut File, nc: u64) {
    let mut sum: i64 = 0;
    let mut nums: i64 = 0;
    let mut file = BufReader::with_capacity(BUFFER_BYTES, file);
    let mut chunk_it = parse_chunks(&mut file, nc);

    println!("opened int64 file with {nc} chunks");
    for (chunk, chunk_header) in chunk_it.by_ref() {
        let mut chunk_slice = chunk.as_slice();
        let mut row_it = parse_int64_chunk(chunk_header.rows, &mut chunk_slice).unwrap();
        for n in row_it.by_ref() {
            match n {
                Ok(n) => {
                    // println!("{n}");
                    sum = sum.saturating_add(n);
                    nums += 1;
                },
                Err(err) =>
                    panic!("row iterator ended with error {err:?}"),
            }
        }
    }
    if let Some(err) = chunk_it.error {
        panic!("chunk iterator ended with error {err:?}");
    }
    let avg = sum.saturating_div(nums);
    println!("sum: {sum}, numbers: {nums}, avg: {avg}");
}

fn decode_str_column(file: &mut File, nc: u64) {
    let mut chars: HashMap<char, u64> = HashMap::new();
    let mut file = BufReader::with_capacity(BUFFER_BYTES, file);
    let mut chunk_it = parse_chunks(&mut file, nc);

    println!("opened str file with {nc} chunks");
    for (chunk, chunk_header) in chunk_it.by_ref() {
        let mut chunk_slice = chunk.as_slice();
        let mut row_it = parse_str_chunk(chunk_header.rows, &mut chunk_slice).unwrap();
        for s in row_it.by_ref() {
            match s {
                Ok(s) => for c in s.chars() {
                    if c >= 0 as char && c <= 127 as char {
                        if !chars.contains_key(&c) {
                            chars.insert(c, 1);
                        } else {
                            chars.insert(c, chars[&c]+1);
                        }
                    }
                },
                Err(err) =>
                    panic!("row iterator ended with error {err:?}"),
            }
            // println!("{s}");
        }
    }
    if let Some(err) = chunk_it.error {
        panic!("chunk iterator ended with error {err:?}");
    }

    println!("{chars:?}");
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
                .truncate(false)
                .read(true)
                .write(true)
                .append(false)
                .open(filename.clone().trim())
                .unwrap();

            encode_column(&mut file, datatype);
        },
        "decode" => {
            let mut header_buf = [0; HEADER_SIZE];
            let filename = args.next().unwrap();
            let mut file = File::options()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .append(false)
                .open(filename.clone().trim())
                .unwrap();

            file.read_exact(&mut header_buf).unwrap();
            let (ctype, nc) = parse_header(&header_buf).unwrap();

            match ctype {
                ColumnType::Int64 => decode_i64_column(&mut file, nc),
                ColumnType::Str => decode_str_column(&mut file, nc),
            }
        },
        _ => {
            println!("invalid mode {mode}. valid modes: encode, decode.");
            exit(-1);
        },
    }
}
