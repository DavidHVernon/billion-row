use std::{
    collections::HashMap,
    fs::File,
    io::{
        Read,
        Error
    },
    path::Path
};

fn main() {
    if let Err(error) = billion_row_challenge() {
        println!("Error: {}", error);
    }
}

fn billion_row_challenge() -> Result<(), Error> {
    println!("Begin");

    let path = Path::new("weather_stations.csv");
    let mut file = File::open(&path)?;
    let mut buf = vec![];
    let _ = file.read_to_end(&mut buf)?;

    let hash_tab = HashMap::new();
    match scan_data(&mut buf, hash_tab) {
        Ok(hash_tab) => {
            let mut count = 0;
            for (_,  val) in hash_tab {
                count += val.len();
            }
            println!("File processed successfully.");
            println!("{} locations", count);
        },
        Err(error) => {
            println!("Error: {}", error);
        }
    }

    Ok(())
}

fn scan_data<'a>( buf: &'a Vec<u8>,  mut hash_tab: HashMap<&'a[u8], Vec<&'a[u8]>>)-> Result<HashMap<&'a[u8], Vec<&'a[u8]>>, Error> {

    // Process
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