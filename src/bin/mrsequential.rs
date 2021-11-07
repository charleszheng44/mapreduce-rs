use libloading::{Library, Symbol};
use std::fs::OpenOptions;
use std::io::Write;

const OUP_FILE: &'static str = "mr-out-0";
type MapFunc = fn(String, String) -> Vec<mapreduce_rs::mr::worker::KeyValue<String, u8>>;
type ReduceFunc = fn(&str, Vec<u8>) -> usize;

fn main() {
    let args = std::env::args();
    if args.len() < 3 {
        eprintln!("Usage: mrsequential dylib inputfiles...");
        std::process::exit(1);
    }

    let library_path = std::env::args()
        .nth(1)
        .expect("failed to get the library path");
    println!("Loading MapFunc and ReduceDunc from {}", library_path);

    unsafe {
        let lib = Library::new(library_path).unwrap();
        let map: Symbol<MapFunc> = lib.get(b"map").unwrap();
        let reduce: Symbol<ReduceFunc> = lib.get(b"reduce").unwrap();

        let inp_files = args.skip(2);
        let mut intermediate = inp_files.fold(vec![], |mut acc, file| {
            let content = std::fs::read_to_string(&file).expect("failed to read file");
            acc.append(&mut map(file.clone(), content));
            acc
        });

        intermediate.sort_by(|a, b| a.key.cmp(&b.key));

        let len = intermediate.len();
        let mut i = 0;
        let mut of = OpenOptions::new()
            .write(true)
            .create(true)
            .open(OUP_FILE)
            .expect("failed to open the output file");

        while i < len {
            let mut count: usize = 0;
            for j in i..len {
                if intermediate[i].key != intermediate[j].key {
                    break;
                }
                count += 1;
            }

            let mut vals = vec![];
            for _ in 0..count {
                vals.push(intermediate[i].val);
            }

            let result = reduce(&intermediate[i].key, vals);
            of.write_all(format!("{} {}\n", &intermediate[i].key, result).as_bytes())
                .expect("failed to write to file");
            i += count;
        }
    }
}
