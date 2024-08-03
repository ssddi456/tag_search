extern crate linereader;

use std::{fs::File, io::{self, BufRead, BufReader, Read}};
use memmem::{Searcher, TwoWaySearcher};
use linereader::LineReader;
use serde_json::Value;

pub const ALL_POSTS: &str = "./posts/posts.json";
pub const LIST_FILE_NAME: &str = "./posts/files.json";
pub const QUERY_FILE_NAME: &str = "./query.json";
pub const PREFIX: &str = r#"{"id":"#;
pub const PREFIX_LEN: usize = PREFIX.len();
pub const TAG_STRING: &str = r#"tag_string":"#;
pub const TAG_STRING_LEN: usize = TAG_STRING.len();

pub fn get_line_info(line: &String) -> (String, String) {
    let first_comma = line.find(',').unwrap();
    let id = line[PREFIX_LEN..first_comma].to_string();
    let tag_string_start = line.find(TAG_STRING).unwrap();
    let tag_string_end = line[tag_string_start + TAG_STRING_LEN..].find('"').unwrap()
        + tag_string_start
        + TAG_STRING_LEN;

    let tag_strings = line[tag_string_start + TAG_STRING_LEN..tag_string_end].to_string();
    (id, tag_strings)
}

pub fn get_line_id(line: &String) -> String {
    let first_comma = line.find(',').unwrap();
    line[PREFIX_LEN..first_comma].to_string()
}

struct SearchPosition {
    start: usize,
    value: [u8; 3],
}

pub fn match_tag(tag: String, tag_strings: String) -> bool {
    let padding = format!(" {} ", tag);
    let padded_tag_strings = format!(" {} ", tag_strings);
    padded_tag_strings.contains(&padding)
}

pub fn match_tags(tags: &[Vec<u8>], tag_strings: &[u8]) -> bool {
    let tag_string_len = tag_strings.len();
    if tag_string_len < 3 {
        return false;
    }
    let mut space_positions: Vec<SearchPosition> = Vec::new();
    let mut last_visited_index = 2;

    for tag in tags {
        let first_byte = tag[0];
        let second_byte = tag[1];
        let third_byte = tag[2];

        let mut found_tag = false;
        let tag_len = tag.len();
        let mut search_start = last_visited_index;

        for postion in space_positions.iter() {
            if postion.value == [first_byte, second_byte, third_byte] {
                search_start = postion.start;
                // println!("search_start: {} {} {}",
                //     search_start,
                //     std::str::from_utf8(&postion.value).unwrap(),
                //     std::str::from_utf8(&[first_byte, second_byte, third_byte]).unwrap()
                // );
                break;
            }
        }

        // println!("search_start: {} {}", search_start, (tag_string_len - tag_len + 3));

        for i in search_start..(tag_string_len - tag_len + 3) {

            if tag_strings[i] == b' ' && i + 2 < tag_string_len {
                if !(space_positions.iter().any(|x| x.start == i + 2)) {
                    // println!("{} tag_strings[i] {:?}", i, std::str::from_utf8(&tag_strings[(i)..(i + 2)]));
                    space_positions.push(SearchPosition {
                        start: i,
                        value: [
                            tag_strings[i],
                            tag_strings[i + 1],
                            tag_strings[i + 2],
                        ],
                    });
                    last_visited_index = i;
                }
            }
            // println!("check {} {:?} {:?}", i,
            //     std::str::from_utf8(&[
            //         first_byte,
            //         second_byte,
            //         third_byte,
            //     ]),
            //     std::str::from_utf8(&tag_strings[(i-2)..(i + 1)]));
            if (
                tag_strings[i - 2] == first_byte
                && tag_strings[i - 1] == second_byte
                && tag_strings[i] == third_byte
            ) {
                if tag_len == 3 {
                    println!("{} {} {}", i, tag_strings[i], tag[0]);
                    found_tag = true;
                    break;
                }
                let mut j = 3;
                // println!("tag {:?} {:?}", std::str::from_utf8(&[tag[j]]), std::str::from_utf8(&[tag_strings[i + j - 2]]));

                while j < tag_len && i + j - 2 < tag_string_len {
                    // println!("tag {:?} {:?}", std::str::from_utf8(&[tag[j]]), std::str::from_utf8(&[tag_strings[i + j - 2]]));
                    if tag_strings[i + j - 2] != tag[j] {
                        break;
                    }
                    j += 1;
                }
                if j == tag_len {
                    found_tag = true;
                    break;
                }
            }
        }
        if !found_tag {
            // println!("tag not found: {:?}", std::str::from_utf8(&tag));
            return false;
        }
    }
    true
}

pub fn padding_tag(tag: &String) -> Vec<u8> {
    let mut padded_tag = Vec::new();
    padded_tag.push(b' ');
    padded_tag.extend(tag.as_bytes());
    padded_tag.push(b' ');
    padded_tag
}

pub fn chunked_read<R: std::io::Read>(
    mut r: BufReader<R>,
    chunk_lines_size: usize,
) -> impl Iterator<Item = Vec<String>> {
    std::iter::from_fn(move || {
        let mut buf: Vec<String> = Vec::new();
        let mut readlines = 0;

        while readlines < chunk_lines_size {
            let mut line = String::new();
            match r.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    buf.push(line);
                    readlines += 1;
                }
                Err(e) => {
                    eprintln!("Error reading line: {}", e);
                    break;
                }
            }
        }

        Some(buf)
    })
}

pub fn chunked_read_u8<R: io::Read>(
    mut r: LineReader<R>,
    chunk_lines_size: usize,
) -> impl Iterator<Item = Vec<Vec<u8>>> {
    std::iter::from_fn(move || {
        let mut buf: Vec<Vec<u8>> = Vec::new();
        let mut readlines = 0;

        while readlines < chunk_lines_size {

            let line = r.next_line();
            match line {
                Some(Ok(line)) => {
                    buf.push(line.to_vec());
                    readlines += 1;
                }
                Some(Err(e)) => {
                    eprintln!("Error reading line: {}", e);
                    break;
                }
                None => break,
            }
        }

        Some(buf)
    })
}

pub  fn search_with_searchers (
    searchers: &Vec<TwoWaySearcher>,
    tag_strings: &[u8],
) -> bool {
    for searcher in searchers {
        if searcher.search_in(&tag_strings).is_none() {
            return false;
        }
    }
    true
}

pub fn read_json_file(file_name: &str) -> Value {
    let mut tag_file = File::open(file_name).unwrap();
    let mut content = String::new();
    let tag_file_content: Value = {
        tag_file.read_to_string(&mut content).unwrap();
        std::str::from_utf8(&content.as_bytes()).unwrap();
        serde_json::from_str(&content).unwrap()
    };
    tag_file_content
}

pub fn read_json_lines_file(file_name: &str) -> Vec<Value> {
    let mut tag_file = File::open(file_name).unwrap();
    let mut content = String::new();
    let mut tag_file_content: Vec<Value> = Vec::new();
    tag_file.read_to_string(&mut content).unwrap();
    for line in content.lines() {
        tag_file_content.push(serde_json::from_str(line).unwrap());
    }
    tag_file_content
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;
    use std::io::Cursor;

    #[test]
    fn test_chunked_read() {
        let data = "line1\nline2\nline3\nline4\nline5\n";
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);
        let chunk_size = 2;

        let mut chunks = chunked_read(reader, chunk_size);

        assert_eq!(
            chunks.next(),
            Some(vec!["line1\n".to_string(), "line2\n".to_string()])
        );
        assert_eq!(
            chunks.next(),
            Some(vec!["line3\n".to_string(), "line4\n".to_string()])
        );
        assert_eq!(chunks.next(), Some(vec!["line5\n".to_string()]));
        let finished = chunks.next();
        assert_eq!(finished, Some(vec![]));
        let content = finished.unwrap();
        println!("{:?} {}", content.len(), content.is_empty());
        let after_finish = chunks.next();
        assert_eq!(after_finish, Some(vec![]));
    }
}
