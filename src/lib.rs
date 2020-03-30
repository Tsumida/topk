use std::collections::{HashMap, BinaryHeap};
use std::fs::File;
use std::path::{PathBuf, Path};
use std::io::{Read, BufRead, BufReader, Write, BufWriter};
use std::cmp::{Ordering, Ord, Reverse};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use rayon::prelude::*;
use tempdir::TempDir;
use serde::{Serialize, Deserialize};

//  设每条url2KB(IE最大 2083char, 其他浏览器更长，如果是ascii --> 2KB)
//  100GB url ==> 50 M条, 用u32记录
//  每条记录2KB(url) + 24B(String) + 4B(cnt) = 2076B
//  假定 hashmap load factor 为 50%, 1K条url放入hashmap，需要 2 x 2076 B x 1000 = 4.15MB内存空间
//  1 GB内存分配 --> 一次读入50K条，要215MB数据, hashmap需要两倍也就是一共645MB, 剩下部分给buffer + 其他--> 1000次

pub struct Parameters{
    pub num: usize, 
    pub bf_cap: usize, 
    pub hash_cap: usize,
    pub topk: usize,
    pub input_path: String,
    pub tdir_divider: TempDir,
    pub tdir_reducer: TempDir,
    pub result_path: String,
}


const MB: usize = 1 << 20;

#[derive(Eq, Debug, Serialize, Deserialize)]
pub struct StatEntry{
    cnt: u32,
    url: String,
}

impl Ord for StatEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cnt.cmp(&other.cnt)
    }
}

impl PartialOrd for StatEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for StatEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cnt == other.cnt
    }
}

#[warn(dead_code)]
// Too slow!
pub fn divider_para(para: &Parameters){
    
    let buf_size = para.bf_cap; 
    let num = para.num;
    // 50M --> 4096 --> 12500 items --> 25MB * 2(load factor=0.5) --> 50MB per thread (reducer).
    // 8 thread --> 400MB + buffer.
    // the worst case: the data contains only one url.
    // But it's ok for reducer.
    assert!(num <= 4096, format!("At most 4096 tmp files, got num = {}", num));
    assert!(buf_size >= (1 << 10), format!("Buffer is too small: {}", buf_size)); // at least 1KB buffer.
    
    let (url_send, url_recv) = std::sync::mpsc::sync_channel::<(usize, String)>(para.bf_cap);
    let mut tdir_path = para.tdir_divider.path().to_path_buf();
    // recv thread.
    let recv_handler = std::thread::spawn(move || {
        let mut cnts = vec![0usize; num];
        let mut tmps = Vec::with_capacity(num);
        for i in 0..num{
            tdir_path.push(i.to_string());
            tmps.push(BufWriter::with_capacity(
                buf_size, File::create(&tdir_path).unwrap()
            ));
            tdir_path.pop();
        }
        // hashing: h = hash(url) --> tmp with index = h % num
        // make sure that all the same url will be mapped into only one tmp file.
        while let Ok(hurl) = url_recv.recv(){
            let (h, url) = hurl;
            tmps.get_mut(h).unwrap().write((url + "\n").as_bytes()).unwrap();
            *cnts.get_mut(h).unwrap() += 1;
        }
                
        cnts.iter().enumerate().for_each(
            |t| eprintln!("tmp{}, {} urls.", t.0, t.1)
        );
        eprintln!("divide done.");
    });

    // is dir or file ?
    let url_path = Path::new(&para.input_path);
    let url_files = if url_path.is_dir(){
        url_path.read_dir()
        .expect("Error occurs while reading directory.")
        .filter(|e| e.is_ok())
        .map(|e| e.unwrap().path())
        .collect::<Vec<PathBuf>>()
    }else{
        let mut p = PathBuf::new();
        p.push(&para.input_path);
        vec![p]
    };
    
    // 同时处理两个文件，太多文件cpu会卡爆
    let ck_size = 2;
    for ck in url_files.chunks(ck_size){
        let mut handlers = Vec::with_capacity(ck_size);
        for pa in ck{
            let sender = (&url_send).clone(); // clone.
            let p = pa.clone();
            let handler = std::thread::spawn(move || {
                eprintln!("divider processing: {}", p.display());
                let bfr = BufReader::with_capacity(
                    buf_size, 
                    File::open(&p).unwrap()
                );
                for l in bfr.lines(){
                    if let Ok(row) = l{
                        // 注意, hasher内部状态不会清空，上一次hash的结果会影响下一次。所以每一次都要创建新hasher
                        let mut hasher = DefaultHasher::new();
                        row.hash(&mut hasher); 
                        let h = hasher.finish() as usize % num;
                        if sender.send((h, row)).is_err(){
                            eprintln!("{} done.", p.display());
                            break;
                        }
                    }
                }
            });
            handlers.push(handler);
        }
        for h in handlers{
            h.join().unwrap();
        }
    }
    

}

/// Divide the data into many temparary files so that counting will take less than 1GB mem space.
/// This function need extra disk space as well as the large data.
/// file_path is the path of large data.
/// Assume that there is only one file and sufficient disk space and inodes.
pub fn divider(para: &Parameters){
    let buf_size = 1 * MB;  // 每个小文件给1MB
    let num = para.num;
    assert!(num <= 512, format!("At most 512 tmp files, got num = {}", num));
    assert!(buf_size >= (1 << 10), format!("Buffer is too small: {}", buf_size)); // at least 1KB buffer.
    
    let url_path = Path::new(&para.input_path);
    let url_files = if url_path.is_dir(){
        url_path.read_dir()
        .expect("Error occurs while reading directory.")
        .filter(|e| e.is_ok())
        .map(|e| e.unwrap().path())
        .collect::<Vec<PathBuf>>()
    }else{
        let mut p = PathBuf::new();
        p.push(&para.input_path);
        vec![p]
    };

    // the worst case: the data contains only one url.
    // But it's ok for reducer.
    let mut tmps = Vec::with_capacity(num);
    for i in 0..num{
        let p = para.tdir_divider.path().join(i.to_string());
        tmps.push(BufWriter::with_capacity(
            buf_size, File::create(&p).unwrap()
        ));
    }

    // hashing: h = hash(url) --> tmp with index = h % num
    // make sure that all the same url will be mapped into only one tmp file.
    for pa in url_files{
        eprintln!("Dividing {}", pa.display());
        let bfr = BufReader::with_capacity(
            para.bf_cap, 
            File::open(&pa).unwrap()
        );
        let mut cnts = vec![0usize; num];
        for l in bfr.lines(){
            if let Ok(row) = l{
                // 注意的是，hasher内部状态不会清空，上一次hash的结果会影响下一次。所以每一次都要创建新hasher
                let mut hasher = DefaultHasher::new();
                row.hash(&mut hasher); 
                let h = hasher.finish() as usize % num;
                tmps[h].write((row + "\n").as_bytes()).unwrap();
                cnts[h] += 1;
            }
        }
        /*
        cnts.iter().enumerate().for_each(
            |t| println!("tmp{}: {}", t.0, t.1)
        );*/

    }
}

// Read elements from temparary files produced by divider and find top-k elements sorted by occurences(desc).
// assume that there is only one file.
pub fn reduce(para: &Parameters, path:&Path, target_path:&Path){
    let bf = BufReader::with_capacity(
        para.bf_cap, 
        File::open(path).unwrap()
    );
    let mut counter:HashMap<String, u32> = HashMap::with_capacity(para.hash_cap);

    for line in bf.lines(){
        if let Ok(url) = line {
            *counter.entry(url).or_insert(0u32) += 1;
        }
    }

    // parallel iter

    let mut bheap = BinaryHeap::with_capacity(para.topk);
    for (k, v) in counter.iter(){
        if bheap.len() < para.topk{
            bheap.push(Reverse(StatEntry{
                url: k.clone(),
                cnt: *v,
            }));
        }else if *v > bheap.peek().unwrap().0.cnt{ // 前K个元素最小的一个
            let _ = bheap.pop();
            bheap.push(Reverse(StatEntry{
                url: k.clone(),
                cnt: *v,
            }));
        }
    }

    let mut cnt = 0;
    let mut result = Vec::with_capacity(para.topk);
    while cnt < para.topk && !bheap.is_empty(){
        result.push(bheap.pop().unwrap().0);
        cnt += 1;
    }
    result.reverse();
    // write result.
    let s = serde_json::to_string(&result).unwrap();
    let mut f = File::create(target_path).unwrap();
    f.write(s.as_bytes()).unwrap();
    eprintln!("reduced: p={} ,cnt={}, done", path.display(), cnt);
}

// read direct files in the given directory.
pub fn reducer(para: &Parameters){
    // read file. 
    
    let mut tmp_index = 0;
    for entry in para.tdir_divider.path().read_dir()
    .expect("Error occurs while reading directory.")
    .enumerate(){
        if let (i, Ok(e)) = entry{
            let fp = e.path();
            if fp.is_file(){
                let p = para.tdir_reducer.path().join(tmp_index.to_string());
                eprintln!("{:?}", e.path());
                reduce(&para, &fp, &p);
            }
            tmp_index += 1;
        }
    }
}

/// Processing many file in parallel.
pub fn reducer_parallel(para: &Parameters){
    // read file. 
    let files = para.tdir_divider.path().read_dir()
        .expect("Error occurs while reading directory.")
        .filter(|e| e.is_ok())
        .map(|e| e.unwrap().path())
        .collect::<Vec<PathBuf>>();

    files.par_iter().enumerate().for_each( |t|
        {
            let (tmp_index, fp) = t;
            let p = para.tdir_reducer.path().join(tmp_index.to_string());
            reduce(&para, &fp, &p);
        }
    );
}

pub fn merge(para: &Parameters, fp: &PathBuf, bheap:&mut BinaryHeap<Reverse<StatEntry>>){
    eprintln!("merging {:?}",fp);
    let mut fr = File::open(fp).unwrap(); 
    let mut sbuf = String::with_capacity(100 * MB);
    fr.read_to_string(&mut sbuf).unwrap();
    let s: Vec<StatEntry> = serde_json::from_str(&sbuf).unwrap();
    for ste in s{
        if bheap.len() < para.topk{
        bheap.push(Reverse(ste));
        }else if ste.cnt > bheap.peek().unwrap().0.cnt{ // 前K个元素最小的一个
            let _ = bheap.pop();
            bheap.push(Reverse(ste));
        }
    }
}

// find top-k elements from temp files produced by reducer.
pub fn merger(para: &Parameters){
    let p = para.tdir_reducer.path();
    if !p.is_dir() || !p.exists(){
        panic!("Error");
    }
    let mut bheap = BinaryHeap::with_capacity(para.topk);
    p.read_dir()
    .expect("Error occurs while reading directory.")
    .for_each(|entry|{
        if let Ok(e) = entry{
            let fp = e.path();
            if fp.is_file(){
                merge(&para, &fp, &mut bheap);
            }
        }
    });
    let mut result = Vec::with_capacity(para.topk);
    while let Some(ste) = bheap.pop(){
        result.push(ste.0);
    }
    result.reverse();
    for ste in &result{
        eprintln!("cnt={}, url={}", ste.cnt, ste.url);
    }
    let mut resf = BufWriter::with_capacity(10 * MB, File::create(&para.result_path).unwrap());
    for str in result{
        resf.write(
            serde_json::to_string(&str).unwrap().as_bytes()
        ).unwrap();
    }
}

/// Remember append '\n' to each url.
pub fn gen_case(base: usize){
    let raw_urls = BufReader::new(File::open("./urls/raw.txt").unwrap());
    let topk_urls = raw_urls
        .lines()
        .filter(|url| url.is_ok())
        .map(|url| url.unwrap() + "\n")
        .collect::<Vec<String>>();

    let size = topk_urls.len();
    let mut bfw = BufWriter::with_capacity(100 *  MB, File::create("./urls/input.txt").unwrap());
    let total = base * size;
    let mut bytes_cnt = 0;
    for _ in 0..total{
        bytes_cnt += bfw.write(
            topk_urls.get(rand::random::<usize>() % size).unwrap().as_bytes()
        ).unwrap();
    }
    eprintln!("total: {} Bytes\n", bytes_cnt);
}

#[cfg(test)]
mod test_topk{
    use super::*;

    #[test]
    //#[ignore]
    fn test_gen_case() {
        let s = std::time::SystemTime::now();
        gen_case(30000);
        eprintln!("take {} ms", std::time::SystemTime::now().duration_since(s).unwrap().as_millis());
    }

    #[test]
    #[ignore]
    fn test_divider() {
        let num = 31;
        let p = "./src/urls/input.txt";
        //divider(num, p, 800);
    }

    #[test]
    #[ignore]
    fn test_reducer() {
        let divided_path = "./src/tmps";
        let tmp_dir_path = "./src/reduced";
        //reducer(divided_path, tmp_dir_path);
    }

    #[test]
    #[ignore]
    fn test_merger() {
        let tmp_dir_path = "./src/reduced";
        let res_path = "./src/result";
        //merger(tmp_dir_path, res_path);
    }

    #[test]
    #[ignore]
    fn test_hasher(){
        let mut hasher = DefaultHasher::new();
        "abc".hash(&mut hasher);
        let h1 = hasher.finish(); // hash state has changed.
        "abc".hash(&mut hasher);
        let h2 = hasher.finish();
        assert_ne!(
            h1,
            h2,
        );
    }

}