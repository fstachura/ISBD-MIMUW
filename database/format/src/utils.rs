use std::error::Error;
use std::io::{Read, stdin};

const U64_MAX_BYTES: u32 = 10;

pub fn encode_vle(val: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut found_nonzero = false;

    if val == 0 {
        return vec![0];
    }

    // println!("{val:032x}");
    for oct in 0..U64_MAX_BYTES {
        let mut cval: u8 = ((val.wrapping_shr(7*(U64_MAX_BYTES-1-oct))) & 0x7f) as u8;
        // println!("{cval:02x}");
        if cval != 0 || found_nonzero {
            found_nonzero = true;
            if oct != 9 {
                cval |= 0b1000_0000;
            }
            result.push(cval);
        }
    }
    // println!("");

    result
}

pub fn decode_vle(bytes: &mut impl Read) -> Result<Option<u64>, std::io::Error> {
    let mut result: u64 = 0;
    let mut ok = false;
    let mut b: [u8; 1] = [0];

    for _ in 0..U64_MAX_BYTES {
        let read = bytes.read(&mut b)?;
        if read == 0 {
            return Ok(None)
        }

        result <<= 7;
        result |= (b[0] & 0x7f) as u64;
        ok = (b[0] & 0x80) == 0;
        if ok {
            break
        }
    }

    Ok(ok.then_some(result))
}

pub fn decode_vle_vec(mut bytes: &[u8]) -> Option<u64> {
    decode_vle(&mut bytes).unwrap()
}

pub fn encode_i64_vle(val: i64) -> Vec<u8> {
    encode_vle(u64::from_ne_bytes(val.to_ne_bytes()))
}

pub fn decode_i64_vle(bytes: &mut impl Read) -> Result<Option<i64>, std::io::Error> {
    decode_vle(bytes).map(|v| v.map(|i| i64::from_ne_bytes(i.to_ne_bytes())))
}

pub fn read_numbers(len: usize) -> Result<Vec<i64>, Box<dyn Error>> {
    let mut nums = Vec::with_capacity(len);

    for _ in 0..len {
        let mut num_str = String::new();
        let bytes_read = stdin().read_line(&mut num_str)?;
        if bytes_read == 0 {
            break;
        }
        let num: i64 = num_str.trim().parse()?;
        nums.push(num);
    }

    Ok(nums)
}

pub fn read_strings(len: usize) -> std::io::Result<Vec<String>> {
    let mut strs = Vec::with_capacity(len);

    for _ in 0..len {
        let mut line = String::new();
        let bytes_read = stdin().read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        // TODO replace \n and \\?
        strs.push(line.trim().into());
    }

    Ok(strs)
}

#[cfg(test)]
mod tests {
    use crate::{encode_vle, decode_vle_vec, decode_i64_vle, encode_i64_vle};

    #[test]
    fn test_encode() {
        assert_eq!(encode_vle(0), vec![0]);
        assert_eq!(encode_vle(1), vec![1]);
        assert_eq!(encode_vle(0x7f), vec![0x7f]);
        assert_eq!(encode_vle(0xff), vec![0x81, 0x7f]);
        assert_eq!(encode_vle(0xffff_ffff), vec![0b1000_1111, 0xff, 0xff, 0xff, 0x7f]);
        assert_eq!(encode_vle(0xffff_ffff_ffff_ffff), vec![0b1000_0001, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
        assert_eq!(encode_vle(0x1234), vec![0xa4, 0x34]);
    }

    #[test]
    fn test_decode() {
        assert_eq!(decode_vle_vec(&vec![0]).unwrap(), 0);
        assert_eq!(decode_vle_vec(&vec![1]).unwrap(), 1);
        assert_eq!(decode_vle_vec(&vec![0x7f]).unwrap(), 0x7f);
        assert_eq!(decode_vle_vec(&vec![0x81, 0x7f]).unwrap(), 0xff);
        assert_eq!(decode_vle_vec(&vec![0b1000_1111, 0xff, 0xff, 0xff, 0x7f]).unwrap(), 0xffff_ffff);
        assert_eq!(decode_vle_vec(&vec![0b1000_0001, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]).unwrap(), 0xffff_ffff_ffff_ffff);
        assert_eq!(decode_vle_vec(&vec![0xa4, 0x34]).unwrap(), 0x1234);

        assert_eq!(decode_vle_vec(&vec![0xff]), None);
        assert_eq!(decode_vle_vec(&vec![0xff, 0xff]), None);
        assert_eq!(decode_vle_vec(&vec![0x7f, 0xff]).unwrap(), 0x7f);
        assert_eq!(decode_vle_vec(&vec![0x81, 0x7f, 0xff]).unwrap(), 0xff);
        assert_eq!(decode_vle_vec(&vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0]), None);
    }

    #[test]
    fn test_i64() {
        assert_eq!(decode_i64_vle(&mut encode_i64_vle(i64::MIN).as_slice()).unwrap().unwrap(), i64::MIN);
        assert_eq!(decode_i64_vle(&mut encode_i64_vle(i64::MAX).as_slice()).unwrap().unwrap(), i64::MAX);
        assert_eq!(decode_i64_vle(&mut encode_i64_vle(0).as_slice()).unwrap().unwrap(), 0);
        assert_eq!(decode_i64_vle(&mut encode_i64_vle(-1).as_slice()).unwrap().unwrap(), -1);
    }
}
