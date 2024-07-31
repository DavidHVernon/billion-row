use std::{
    collections::HashMap,
    fmt::{Formatter, Write},
    fs::File,
    io::{
        Read,
    },
    path::Path,
    str
};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

enum Error {
    StdIoError(std::io::Error),
    Utf8Error(std::str::Utf8Error),
    ParseFloatError(std::num::ParseFloatError)
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::StdIoError(error)
    }
}
impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Error::Utf8Error(error)
    }
}
impl From<std::num::ParseFloatError> for Error {
    fn from(error: std::num::ParseFloatError) -> Self {
        Error::ParseFloatError(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Error::StdIoError(error) => error.fmt(f),
            Error::Utf8Error(error) => error.fmt(f),
            Error::ParseFloatError(error) => error.fmt(f),
        }
    }
}

fn median(vec: &Vec<&[u8]>) -> Result<f32, Error> {
    let l = vec.len();
    if l % 2 == 0 {
        // Even - 1 2 3 4 5 6
        let a = vec[l/2-1];
        let a = str::from_utf8(a)?.parse::<f32>()?;
        let b = vec[l/2];
        let b = str::from_utf8(b)?.parse::<f32>()?;

        Ok((a + b) / 2.0)
    } else {
        // Odd - 1 2 3 4 5
        Ok(str::from_utf8(vec[l/2])?.parse::<f32>()?)
    }
}

fn to_decimal_int(val: &[u8]) -> u32 {
    // eg. val = 10512.4
    let mut accumulator = 0;

    for i in 0..val.len() {
        let v = val[i];
        if v != b'.' {
            let v = v - b'0';
            accumulator *= 10;
            accumulator += v as u32;
        }
    }

    accumulator
}

fn main() {
    if let Err(error) = billion_row_challenge() {
        println!("Error: {}", &error);
    }
}

fn billion_row_challenge() -> Result<(), Error> {

    // Open the data file
    println!("Opening File...");
    //let path = Path::new("../billion-row-data/measurements-1000000000.txt");
    let path = Path::new("../billion-row-data/measurements-1000000000.txt");
    let mut file = File::open(&path)?;
    let mut buf = vec![];
    let _ = file.read_to_end(&mut buf)?;

    // Call scan_data.
    let hash_tab = HashMap::new();
    match scan_data(&mut buf, hash_tab) {
        Ok(hash_tab) => {
            println!("Processing Data...");
            let pb = ProgressBar::new(hash_tab.len() as u64);
            pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .with_key("%", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
                .progress_chars("#>-"));
            pb.set_position(0u64);

            let mut i = 0;
            for (_key, val) in hash_tab {
                let mut min = u32::MAX;
                let mut max = u32::MIN;
                let _med = median(&val);

                for v in val {
                    // Convert [u8] -> str -> f32
                    let f = to_decimal_int(v); // str::from_utf8(v)?.parse::<f32>()?;
                    if f < min {
                        min = f;
                    }
                    if f > max {
                        max = f;
                    }
                }

                i += 1;
                pb.set_position(i);
            }

        },
        Err(error) => {
            println!("Error: {}", error);
        }
    }
    Ok(())
}

fn scan_data<'a>( buf: &'a Vec<u8>,  mut hash_tab: HashMap<&'a[u8], Vec<&'a[u8]>>)-> Result<HashMap<&'a[u8], Vec<&'a[u8]>>, Error> {

    // Process
    println!("Scanning Data...");
    let mut i = 0;
    while i < buf.len() -1 {
        // ex: San Quintín;30.4837
        //     ^                    key_start
        //                 ^        val_start
        //     ^---------^          key
        //                 ^-----^  val

        // Scan key
        let key_start = i;
        let mut c = buf[i];
        // Check for pre-end of file.
        if c == b'\n' {
            break;
        }
        // Record delimiter
        while c != b';' {
            i += 1;
            c = buf[i];
        }
        i += 1;
        c = buf[i];

        // Scan value
        let val_start = i;
        // End of record
        while c != b'\n' {
            i += 1;
            c = buf[i];
        }
        i += 1;

        // Store key/value
        let key = &buf[key_start..val_start-1];
        let val = &buf[val_start..i-1];

        if let Some(vec) = hash_tab.get_mut(&key) {
            vec.push(val);
        } else {
            hash_tab.insert(key,vec![val]);
        }
    }
    Ok(hash_tab)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_to_decimal_int()  {
        assert_eq!(259, to_decimal_int( b"25.9"));
        assert_eq!(207, to_decimal_int(b"20.7"));
    }
}

