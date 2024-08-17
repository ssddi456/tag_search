extern crate walkdir;

use std::io::prelude::*;
use walkdir::WalkDir;

fn search_in_dir(dir_path: &str, keyword: Vec<&str>,) -> bool {
    let walker = WalkDir::new(dir_path).into_iter();
    for entry in walker {
        let entry = entry.unwrap();
        let path = entry.path();
        let path_name = path.to_str().unwrap();

        if entry.path().is_file() {
            // 读取文件内容
            // 判断文件后缀是否是 txt
            if path_name.ends_with(".txt") {
                if search_in_file(path_name, keyword.clone()) {
                    writeln!(std::io::stdout(), "Found: '{}'", path_name).unwrap();
                }
            }
        }
    }
    return true;
}

fn search_in_file(file_path: &str, keyword: Vec<&str>) -> bool {
    let content = match std::fs::read_to_string(file_path) {
        Ok(content) => {
            // writeln!(std::io::stdout(), "Read: {}", file_path).unwrap();
            content
        },
        Err(_) => {
            writeln!(std::io::stdout(), "Error: {}", file_path).unwrap();
            return false;
        },
    };

    let mut not_found_some: bool = false;
    for k in keyword.iter() {
        if !content.contains(k) {
            not_found_some = true;
            break;
        }
    }
    if not_found_some {
        return false;
    }
    return true;
}

#[test]
pub fn main() {

    let start = std::time::Instant::now();
    let root: &str = "some_dir";
    let keyword: Vec<&str> = vec!["some", "keyword"];
    
    search_in_dir(root, keyword);

    writeln!(std::io::stdout(), "Elapsed: {}", start.elapsed().as_secs_f64()).unwrap();

    ()
}
