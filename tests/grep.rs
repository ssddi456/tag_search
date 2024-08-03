extern crate grep;
extern crate termcolor;
extern crate walkdir;

use std::{error::Error, io::IsTerminal, process};

use search::{read_json_file, read_json_lines_file, LIST_FILE_NAME, QUERY_FILE_NAME};

use {
    grep::{
        cli,
        printer::{ColorSpecs, StandardBuilder},
        regex::RegexMatcher,
        searcher::{BinaryDetection, SearcherBuilder},
    },
    termcolor::ColorChoice,
};

#[test]
pub fn main() {
    let start = std::time::Instant::now();
    if let Err(err) = try_main() {
        eprintln!("Error: {} {}", err, start.elapsed().as_secs_f64());
        process::exit(1);
    }
    println!("Elapsed: {}", start.elapsed().as_secs_f64());
}


pub fn try_main() -> Result<(), Box<dyn Error>> {
    let tag_file_content: Vec<String> = read_json_file(QUERY_FILE_NAME).as_array().unwrap().iter().map(|x| x.as_str().unwrap().to_string()).collect();
    let file_list = read_json_lines_file(LIST_FILE_NAME);

    for file_info in file_list {
        let tag_file_path = file_info["tag_file"].as_str().unwrap();
        let detail_file_path = file_info["file"].as_str().unwrap();
        let file = search(&tag_file_content, tag_file_path.to_string(), detail_file_path.to_string()).unwrap();
    }

    Ok(())
}

fn search(pattern: &Vec<String>, tag_file_path: String, detail_file_path: String) -> Result<(), Box<dyn Error>> {
    let matcher = RegexMatcher::new_line_matcher(&pattern[0])?;
    let mut searcher = SearcherBuilder::new()
        .binary_detection(BinaryDetection::quit(b'\x00'))
        .line_number(false)
        .build();
    let mut printer = StandardBuilder::new()
        .color_specs(ColorSpecs::default_with_color())
        .build(cli::stdout(if std::io::stdout().is_terminal() {
            ColorChoice::Auto
        } else {
            ColorChoice::Never
        }));

    let cloned_tag_file_path = tag_file_path.clone();
    let result = searcher.search_path(
        &matcher,
        tag_file_path,
        printer.sink_with_path(&matcher, &cloned_tag_file_path),
    );

    Ok(())
}