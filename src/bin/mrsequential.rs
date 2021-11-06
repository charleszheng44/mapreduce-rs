use mapreduce_rs::mrapps::wc::{map, reduce};

fn main() {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Usage: mrsequential inputfiles...");
        std::process::exit(1);
    }

    let inp_files = args.skip(1);
    let mut intermediate = vec![];
    for file in inp_files {
        println!("parsing file {}", file);
        let content = std::fs::read_to_string(&file).expect("failed to read file");
        intermediate.append(&mut map(file.clone(), content));
    }

    intermediate.sort_by(|a, b| a.key.cmp(&b.key));

    let len = intermediate.len();
    for (i, kv) in intermediate.iter().enumerate() {
        let mut count: usize = 0;
        for j in i..len {
            if kv.val != intermediate[j].val {
                break;
            }
            count += 1;
        }

        let mut vals = vec![];
        for _ in 0..count {
            vals.push(kv.val);
        }

        let result = reduce(&kv.key, vals);
        std::fs::write("mr-out-0", format!("{} {}\n", &kv.key, result))
            .expect("failed to write to file");
    }
}
