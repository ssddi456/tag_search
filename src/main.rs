mod lib;

extern crate core_affinity;
extern crate linereader;
extern crate memmap;
extern crate path_clean;
extern crate serde_json;

use lib::{padding_tag, search_with_searchers};
use memmap::Mmap;
use path_clean::clean;
use serde_json::Value;
use std::collections::VecDeque;
use std::fs::File as SyncFile;
use std::io::{BufRead, BufReader, Read};
use std::sync::{Arc, Mutex};
use std::thread;

use memmem::TwoWaySearcher;
use memchr::memchr;

// This is a comment, and is ignored by the compiler.
// You can test this code by clicking the "Run" button over there ->
// or if you prefer to use your keyboard, you can use the "Ctrl + Enter"
// shortcut.

// This code is editable, feel free to hack it!
// You can always return to the original code by clicking the "Reset" button ->

// This is the main function.
fn main() -> Result<(), std::io::Error> {
    // Statements here are executed when the compiled binary is called.

    // Print text to the console.
   // print current directory
    let current_dir = std::env::current_dir().unwrap();
    println!("current dir: {:?}", current_dir);
    // print all posts file path and if it exists
    let all_posts = lib::LIST_FILE_NAME;
    // relative to current dir, print normarlized path
    let all_posts_path = current_dir.join(all_posts);
    let clean_path = clean(&all_posts_path);
    let query = current_dir.join(lib::QUERY_FILE_NAME);

    println!("all posts path: {:?}", clean_path);
    println!("all posts exists: {}", all_posts_path.exists());
    println!("query path: {:?}", clean(&query));
    println!("query exists: {}", query.exists());

    let mut tag_file = SyncFile::open(query).unwrap();
    let mut content = String::new();
    let tag_file_content = {
        tag_file.read_to_string(&mut content);
        std::str::from_utf8(&content.as_bytes()).unwrap()
    };

    println!("tag file content: {:?}", tag_file_content);

    let tag_query: Vec<String> = serde_json::from_str(tag_file_content).unwrap();

    println!("tag_query: {:?}", tag_query);

    // performance log
    let now = std::time::Instant::now();
    let foundfile = map_reduce_search_tag(&tag_query, 10, None, None);
    let time_elapsed = now.elapsed();
    
    println!("found file: {:?}", foundfile.len());
    // for file in foundfile.iter() {
    //     println!("file: {}", file);
    // }
    println!("time: {:?}", time_elapsed);

    Ok(())
}

fn search_file_with_tag(
    tag: &[Vec<u8>],
    files_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    res_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    thread_id: usize,
) {
    println!("start {}", thread_id);

    let mut line_start: usize = 0;
    let mut line_end: usize = 0;

    let searchers = tag.iter().map(|tag| TwoWaySearcher::new(tag)).collect();
    
    while let Some(file_info_str) = {
        let mut queue = files_queue.lock().unwrap();
        queue.pop_front()
    } {
        let file_info: Value = serde_json::from_slice(&file_info_str).unwrap();
        let tag_file_path: &str = file_info["tag_file"].as_str().unwrap();
        let mut tag_file = SyncFile::open(tag_file_path).unwrap();

        println!("start {}", tag_file_path,);
        let starttime = std::time::Instant::now();
        let mut current_piece_info: Vec<usize> = Vec::new();

        // first read
        let mmap = unsafe { Mmap::map(&tag_file).unwrap() };
        let available: &[u8] = &mmap;

        line_start = 0;
        line_end = 0;
        let file_size = available.len();

        // read 100 lines at a time
        // wrap reader.lines() in a chunk of 100 lines
        
        while true {
            // find the start of the next piece by delimiter
            match memchr(b'\n', &available[line_start..]) {
                Some(v) => line_end = v + line_start,
                None => line_end = file_size,
            }

            // if tag_searcher.search_in( &available[line_start..line_end]).is_some() {
            if search_with_searchers( &searchers,&available[line_start..line_end]) {

                let line_info = match serde_json::from_slice(&available[line_start..line_end]) {
                    // msg.as_str()
                    Ok(v) => v,
                    Err(e) => {
                        println!("error: {}", e);
                        let mut map = serde_json::Map::new();
                        map.insert(
                            "id".to_string(),
                            Value::Number(serde_json::Number::from_f64(-1.0).unwrap()),
                        );
                        Value::Object(map)
                    }
                };
                if line_info.is_object() && line_info["id"].as_f64().unwrap() == -1.0 {
                    let line_content = std::str::from_utf8(&available[line_start..line_end]).unwrap();
                    println!("error: {:?}", line_content);
                } else {
                    let offset = line_info["offset"].as_array().unwrap();
                    let start = offset[0].as_u64().unwrap() as usize;
                    current_piece_info.push(start);
                }
            }

            // remove the first line
            line_start = line_end + 1;
            if line_end == file_size || line_start == file_size {
                break;
            }

        }

        println!("end {} {:?}", tag_file_path, starttime.elapsed());
        let current_piece_info_len: usize = current_piece_info.len();
        if current_piece_info_len == 0 {
            continue;
        }

        println!("current_piece_info: {:?}", current_piece_info_len);

        let post_file_path = file_info["file"].as_str().unwrap();
        let post_file = SyncFile::open(post_file_path).unwrap();
        // first read
        let mmap = unsafe { Mmap::map(&post_file).unwrap() };
        let available: &[u8] = &mmap;

        for i in 0..current_piece_info_len {
            let current_piece_info_item = &current_piece_info[i];
            let start = current_piece_info_item;

            match memchr(b'\n', &available[*start..]) {
                Some(v) => line_end = v + start,
                None => line_end = available.len(),
            }
            let line_content = &available[*start..line_end];
            let mut queue: std::sync::MutexGuard<VecDeque<Vec<u8>>> = res_queue.lock().unwrap();
            queue.push_back(line_content.to_vec());

            // queue.push_back(line_content.to_vec());
        }
        println!("end {} {:?}", post_file_path, starttime.elapsed());
    }
    println!("done all {}", thread_id);
}

fn map_reduce_search_tag(
    tag: &Vec<String>,
    max_count: usize,
    start_id: Option<u64>,
    end_id: Option<u64>,
) -> Vec<Value> {
    let files_queue: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::new()));
    let res_queue: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut threads = Vec::new();

    let list_file_name = lib::LIST_FILE_NAME;
    let list_file = SyncFile::open(list_file_name).unwrap();
    let mut reader = BufReader::new(&list_file);

    while true {
        let mut line = String::new();
        reader.read_line(&mut line);
        
        if line.is_empty() {
            break;
        }
        let file_info: Value = serde_json::from_str(&line).unwrap();
        if let Some(start_id) = start_id {
            if file_info["end_id"].as_u64().unwrap() < start_id {
                continue;
            }
        }
        if let Some(end_id) = end_id {
            if file_info["start_id"].as_u64().unwrap() > end_id {
                continue;
            }
        }
        let mut queue = files_queue.lock().unwrap();
        queue.push_back(line.as_bytes().to_vec());

        // cut file
        // break;
    }

    println!("file added {}", files_queue.lock().unwrap().len());

    // add space to tag for better matching
    let mut updated_tags: Vec<Vec<u8>> = Vec::new();
    for tag in tag.iter() {
        updated_tags.push(padding_tag(tag));
    }
    let core_ids: Vec<core_affinity::CoreId> = core_affinity::get_core_ids().unwrap();

    let max_core_count = 1;
    // max core count is 2 or core count
    let core_count = if core_ids.len() > max_core_count {
        max_core_count
    } else {
        core_ids.len()
    };

    let thread_count: usize = 0;
    println!("max core count: {}", core_count);

    for i in 0..core_count {
        if thread_count >= core_count {
            break;
        }
        let files_queue = Arc::clone(&files_queue);
        let res_queue = Arc::clone(&res_queue);
        let clone_updated_tags = updated_tags.clone();

        let t = thread::spawn(move || {
            let core_ids = core_affinity::get_core_ids().unwrap();
            let core_id = core_ids[i];
            let res = core_affinity::set_for_current(core_id);
            println!("set core: {} {}", i, core_id.id);
            if !res {
                println!("set core failed: {}", i);
            }

            search_file_with_tag(&*clone_updated_tags, files_queue, res_queue, i);
        });
        threads.push(t);
    }

    println!("threads started");
    for t in threads {
        match t.join() {
            Ok(_) => println!("thread done"),
            Err(e) => println!("thread error, {:?}", e),
        }
    }
    println!("threads done");

    let mut ret = Vec::new();
    let mut queue = res_queue.lock().unwrap();
    while let Some(data) = {
        queue.pop_front()
    } {
        if data.is_empty() {
            continue;
        }
        let data_str = std::str::from_utf8(&data).unwrap().trim_ascii_end();
        let json_data: Value = match serde_json::from_str(data_str) {
            Ok(v) => v,
            Err(e) => {
                println!("json error: {}", e);
                println!("error record: {:?}", std::str::from_utf8(&data).unwrap());

                let mut map = serde_json::Map::new();
                map.insert(
                    "id".to_string(),
                    Value::Number(serde_json::Number::from_f64(-1.0).unwrap()),
                );
                Value::Object(map)
            }
        };
        if json_data.is_object() && json_data["id"].as_f64().unwrap() == -1.0 {
            println!("error record: {:?}", data);
            continue;
        }
        if let Some(start_id) = start_id {
            if json_data["id"].as_u64().unwrap() < start_id {
                continue;
            }
        }
        if let Some(end_id) = end_id {
            if json_data["id"].as_u64().unwrap() > end_id {
                continue;
            }
        }
        ret.push(json_data);
    }

    ret
}
