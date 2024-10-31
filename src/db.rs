use std::{fs::File, io::Read, path::Path};

pub fn print_db() {
    println!("This is the database!");
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata").join("test.db");
    println!("{}", path.to_str().unwrap());
    let mut file = File::open(path).unwrap();
    let mut data: Vec<u8> = vec![0u8; 4096];
    let size = file.read(data.as_mut_slice()).unwrap();
    println!("{}, {:?}",size,  &data[16..20]);
}