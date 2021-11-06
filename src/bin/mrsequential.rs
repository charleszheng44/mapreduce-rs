use mapreduce_rs::mrapps::wc::{map, reduce};
use std::fs::OpenOptions;
use std::io::Write;

const OUP_FILE: &'static str = "mr-out-0";

fn main() {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Usage: mrsequential inputfiles...");
        std::process::exit(1);
    }

    let inp_files = args.skip(1);
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
