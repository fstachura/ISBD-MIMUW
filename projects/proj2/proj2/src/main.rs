use std::collections::HashMap;
use std::{env::args, fs::File, process::exit};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};

const NUM_PER_CHUNK: usize = 8192;
const BUFFER_BYTES: usize = 4*1024*1024;

mod format;
pub use format::*;

mod utils;

fn encode_column(file: &mut File, datatype: ColumnType) {
    let mut buf: [u8; 4] = [0; 4];
    let first_read = file.read(&mut buf).unwrap();
    let mut num_of_chunks: u64 = 0;

    // file is empty
    if first_read == 0 {
        let mut header_buf = [0; HEADER_SIZE];
        create_header(&mut header_buf.as_mut_slice(), datatype, 0).unwrap();
        file.write(&header_buf).unwrap();
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
            file.write(&to_write).expect("failed to write chunk");
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
            file.write(&to_write).expect("failed to write chunk");
            num_of_chunks += 1;
        }
    }

    println!("total number of chunks: {num_of_chunks}");

    // write number of chunks
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write(&num_of_chunks.to_be_bytes()).unwrap();
}

fn decode_i64_column(file: &mut File, nc: u64) {
    let mut sum: i64 = 0;
    let mut nums: i64 = 0;
    let mut chunk_header_buf = [0; CHUNK_HEADER_SIZE];
    let mut file = BufReader::with_capacity(BUFFER_BYTES, file);

    println!("opened int64 file with {nc} chunks");
    for i in 0..nc {
        // println!("chunk {i}");
        file.read_exact(&mut chunk_header_buf).unwrap();
        let (bytes, rows) = parse_chunk_header(&chunk_header_buf).unwrap();

        let mut chunk_data = vec![0; bytes as usize];
        file.read_exact(&mut chunk_data).unwrap();

        let mut chunk_slice = chunk_data.as_slice();
        let mut chunk_it = parse_int64_chunk(rows, &mut chunk_slice).unwrap();
        while let Some(n) = chunk_it.next() {
            // println!("{n}");
            sum = sum.saturating_add(n);
            nums += 1;
        }
        if let Some(err) = chunk_it.error {
            panic!("chunk iterator ended with error {err:?}");
        }
    }
    let avg = sum.saturating_div(nums);
    println!("sum: {sum}, numbers: {nums}, avg: {avg}");
}

fn decode_str_column(file: &mut File, nc: u64) {
    let mut chars: HashMap<char, u64> = HashMap::new();
    let mut chunk_pos = file.stream_position().unwrap();
    let mut chunk_header_buf = [0; CHUNK_HEADER_SIZE];
    let mut file = BufReader::with_capacity(BUFFER_BYTES, file);

    println!("opened str file with {nc} chunks");
    for i in 0..nc {
        // println!("chunk {i} {chunk_pos}");
        file.seek(SeekFrom::Start(chunk_pos)).unwrap();

        file.read_exact(&mut chunk_header_buf).unwrap();
        let (bytes, rows) = parse_chunk_header(&chunk_header_buf).unwrap();

        let mut chunk_data = vec![0; bytes as usize];
        file.read_exact(&mut chunk_data).unwrap();

        let mut chunk_slice = chunk_data.as_slice();
        let mut chunk_it = parse_str_chunk(rows, &mut chunk_slice).unwrap();
        while let Some(s) = chunk_it.next() {
            // println!("{s}");
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
        chunk_pos += 16 + bytes;
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
