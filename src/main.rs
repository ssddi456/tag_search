mod lib;

extern crate core_affinity;
extern crate linereader;
extern crate path_clean;
extern crate serde_json;
extern crate tokio;

use lib::{read_next_line, read_next_nth_lines};
use lib::{match_tags, padding_tag};
use path_clean::clean;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;

// This is a comment, and is ignored by the compiler.
// You can test this code by clicking the "Run" button over there ->
// or if you prefer to use your keyboard, you can use the "Ctrl + Enter"
// shortcut.

// This code is editable, feel free to hack it!
// You can always return to the original code by clicking the "Reset" button ->

// This is the main function.
#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    // Statements here are executed when the compiled binary is called.

    // Print text to the console.
    println!("Hello World!");
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

    let mut tag_file = File::open(query).await.expect("tag file should exists");
    let mut content = String::new();
    let tag_file_content = {
        tag_file.read_to_string(&mut content).await.unwrap();
        std::str::from_utf8(&content.as_bytes()).unwrap()
    };

    println!("tag file content: {:?}", tag_file_content);

    let tag_query: Vec<String> = serde_json::from_str(tag_file_content).unwrap();

    println!("tag_query: {:?}", tag_query);

    // performance log
    let now = std::time::Instant::now();
    let foundfile = map_reduce_search_tag(&tag_query, 10, None, None).await;
    println!("time: {:?}", now.elapsed());

    println!("found file: {:?}", foundfile.len());
    for file in foundfile.iter() {
        println!("file: {}", file);
    }

    Ok(())
}

async fn search_file_with_tag(
    tag: &[Vec<u8>],
    files_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    res_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    thread_id: usize,
) {
    println!("start {}", thread_id);
    let mut unwrap_tag_line: Vec<u8> = Vec::new();
    let mut current_piece_info: Vec<Value> = Vec::new();

    while let Some(file_info_str) = {
        let mut queue = files_queue.lock().unwrap();
        queue.pop_front()
    } {

        current_piece_info.clear();

        let file_info: Value = serde_json::from_slice(&file_info_str).unwrap();
        let tag_file_path: &str = file_info["tag_file"].as_str().unwrap();
        let tag_file = File::open(tag_file_path).await.unwrap();
        let reader = &mut BufReader::with_capacity(64 * 1024 * 1024, tag_file);
        println!("start {}", tag_file_path,);
        let starttime = std::time::Instant::now();

        // read 100 lines at a time
        // wrap reader.lines() in a chunk of 100 lines
        while true {
            unwrap_tag_line.clear();
            reader.read_until(b'\n', &mut unwrap_tag_line).await.unwrap();

            if unwrap_tag_line.is_empty() {
                break;
            }

            if match_tags(tag, &unwrap_tag_line) {
                let line_info = match serde_json::from_slice(&unwrap_tag_line) {
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
                    let line_content = std::str::from_utf8(&unwrap_tag_line).unwrap();
                    println!("error: {:?}", line_content);
                } else {
                    current_piece_info.push(line_info);
                }
            }
        
        }

        let current_piece_info_len: usize = current_piece_info.len();
        if current_piece_info.is_empty() {
            println!("end {}", tag_file_path,);
            println!("elapsed: {:?}", starttime.elapsed());
            continue;
        }

        println!("current_piece_info: {:?}", current_piece_info_len);

        let post_file_path = file_info["file"].as_str().unwrap();
        let post_file = File::open(post_file_path).await.unwrap();
        let post_reader = &mut BufReader::with_capacity(64 * 1024 * 1024, post_file);

        let mut start = 0;

        let mut line_info_index: usize = 0;
        let mut current_piece_info_item = &current_piece_info[line_info_index];
        let mut offset = current_piece_info_item["offset"].as_array().unwrap();
        let mut current_start = offset[0].as_u64().unwrap() as u64;

        while true {
            unwrap_tag_line.clear();
            post_reader.read_until(b'\n', &mut unwrap_tag_line).await.unwrap();

            if unwrap_tag_line.is_empty() {
                break;
            }

            let post_line_size = unwrap_tag_line.len() as u64;
            if current_start < start {
                // wtf
                break;
            }

            if start == current_start {
                // character length of the line
                let mut queue: std::sync::MutexGuard<VecDeque<Vec<u8>>> =
                    res_queue.lock().unwrap();
                if post_line_size != 0 {
                    queue.push_back(unwrap_tag_line.to_vec());
                }

                start += post_line_size;

                line_info_index += 1;
                if line_info_index >= current_piece_info_len {
                    break;
                }
                current_piece_info_item = &current_piece_info[line_info_index];
                // println!("current_piece_info: {:?} {:?} {:?} {:?}", line_info_index, current_piece_info_len, current_piece_info_item, current_piece_info_item["offset"]);
                offset = current_piece_info_item["offset"].as_array().unwrap();
                current_start = offset[0].as_u64().unwrap() as u64;
            }

            start += post_line_size;
        
        }

        println!("end {} {:?}", tag_file_path, starttime.elapsed());
    }
    println!("done all {}", thread_id);
}

async fn map_reduce_search_tag(
    tag: &Vec<String>,
    max_count: usize,
    start_id: Option<u64>,
    end_id: Option<u64>,
) -> Vec<Value> {
    let files_queue = Arc::new(Mutex::new(VecDeque::new()));
    let res_queue: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut threads = Vec::new();

    let list_file_name = lib::LIST_FILE_NAME;
    let list_file = File::open(list_file_name).await.unwrap();
    let mut reader = BufReader::new(list_file);

    
    while true {
        let mut line: Vec<u8> = Vec::new();
        line.clear();
        reader.read_until(b'\n', &mut line).await.unwrap();
        if line.is_empty() {
            break;
        }
        let file_info: Value = serde_json::from_slice(&line).unwrap();
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
        queue.push_back(line);

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

        let t = tokio::task::spawn(async move {
            let core_ids = core_affinity::get_core_ids().unwrap();
            let core_id = core_ids[i];
            let res = core_affinity::set_for_current(core_id);
            println!("set core: {} {}", i, core_id.id);
            if !res {
                println!("set core failed: {}", i);
            }

            search_file_with_tag(&*clone_updated_tags, files_queue, res_queue, i).await;
        });
        threads.push(t);
    }

    println!("threads started");
    for t in threads {
        match t.await {
            Ok(_) => println!("thread done"),
            Err(e) => println!("thread error, {:?}", e),
        }
    }
    println!("threads done");

    let mut ret = Vec::new();
    while let Some(data) = {
        let mut queue = res_queue.lock().unwrap();
        queue.pop_front()
    } {
        if data.is_empty() {
            continue;
        }

        let json_data: Value = match serde_json::from_slice::<Value>(&data) {
            Ok(v) => v,
            Err(e) => {
                println!("json error: {}", e);
                println!("error record: {:?}", data);
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
