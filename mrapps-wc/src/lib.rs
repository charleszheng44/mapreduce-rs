use mapreduce_rs::mr::worker;

#[no_mangle]
pub fn map(_: String, contents: String) -> Vec<worker::KeyValue<String, u8>> {
    split_to_words(contents)
        .into_iter()
        .fold(vec![], |mut acc, wrd| {
            acc.push(worker::KeyValue::new(wrd, 1));
            acc
        })
}

#[no_mangle]
pub fn reduce(_: &str, values: Vec<u8>) -> usize {
    values.len()
}

// split_to_words treats punctuations and whitespaces as the delimiter and
// split input string into words.
fn split_to_words(line: String) -> Vec<String> {
    let (mut ret, mut word) = (vec![], vec![]);
    for c in line.chars() {
        if c.is_alphabetic() {
            word.push(c);
            continue;
        }
        if word.len() != 0 {
            ret.push(word.into_iter().collect::<String>());
        }
        word = vec![];
    }
    if word.len() != 0 {
        ret.push(word.into_iter().collect::<String>());
    }
    ret
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_split_to_words() {
        let inp = "almost no restrictions whatsoever.  \
                   You may copy it, give it away or";
        let get = split_to_words(inp.to_owned());
        let expect = vec![
            "almost",
            "no",
            "restrictions",
            "whatsoever",
            "You",
            "may",
            "copy",
            "it",
            "give",
            "it",
            "away",
            "or",
        ];
        assert_eq!(&get, &expect);
    }
}
