use std::collections::{HashMap, BinaryHeap};
use std::fs::File;
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::cmp::{Ordering, Ord, Reverse};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;


use serde::{Serialize, Deserialize};

//   Mapper: 
//  设每条url2KB(IE, 2083char, 如果是ascii --> 2KB)
//  100GB url ==> 50 M条, 用u32
//  hasher 承载因子 50%, 每条记录2KB(url) + 24B(String) + 4B(cnt) = 2076B
//  1K条url需要 2 x 2076 B x 1000 = 4.15MB，
//  1 GB内存分配 --> 一次最多读入200K条，要830MB, 剩下部分给buffer + 其他--> 250次
//  这里是否要考虑 HashMap的动态扩容问题？ 

#[derive(Eq, Debug, Serialize, Deserialize)]
struct StatEntry{
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

const bf_cap:usize = 100 * (1 << 20); // 100MB
const hash_cap:usize = 100 * (1 << 20); 
const topk: usize = 4; // top k
const MB: usize = 1 << 20;

impl PartialEq for StatEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cnt == other.cnt
    }
}

// Read elements from temparary files produced by divider and find top-k elements sorted by occurences(desc).
// assume that there is only one file.
fn reduce(path: &str){
    let bf = BufReader::with_capacity(
        bf_cap, 
        File::open(path).unwrap()
    );
    let mut counter:HashMap<String, u32> = HashMap::with_capacity(hash_cap);
    for line in bf.lines(){
        if let Ok(url) = line {
            *counter.entry(url).or_insert(0u32) += 1;
        }
    }

    let mut bheap = BinaryHeap::with_capacity(topk);
    for (k, v) in counter.iter(){
        if bheap.len() < topk{
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
    let mut result = Vec::with_capacity(topk);
    while cnt < topk && !bheap.is_empty(){
        result.push(bheap.pop().unwrap().0);
        cnt += 1;
    }
    result.reverse();
    // write result.
    let s = serde_json::to_string(&result).unwrap();
    let mut f = File::create("./src/urls/result").unwrap();
    f.write(s.as_bytes()).unwrap();
}

// read direct files in the direct.
fn reducer(dir_path: &str){
    // read file. 
    

    
}

/// Divide the large data into num smaller temparary file, 
/// which means this function need extra disk space as well as the large data.
/// file_path is the path of large data.
/// Assume that there is only one file and sufficient disk space and inodes.
fn divider(num: usize, file_path: &str, mem_mb:usize ){
    
    // open file
    let buf_size = mem_mb * MB / num; 
    assert!(num <= 1024, format!("At most 1024 tmp files, got num = {}", num));
    assert!(buf_size >= (1 << 10), format!("Buffer is too small: {}", buf_size)); // at least 1KB buffer.
    
    // the worst case is that: the large data contains only one url.
    // But it's ok for reducer.
    let mut tmps = Vec::with_capacity(num);
    for i in 0..num{
        tmps.push(BufWriter::with_capacity(
            buf_size, File::create(format!("./src/tmps/tmp_{}", i)).unwrap()
        ));
    }

    // hashing: h = hash(url) --> tmp with index = h % num
    // make sure that all the same url will be mapped into only one tmp file.
    let bfr = BufReader::with_capacity(
        100 * MB, 
        File::open(file_path).unwrap()
    );
    let mut cnts = vec![0usize; num];
    for l in bfr.lines(){
        if let Ok(row) = l{
            // 注意的是，hasher内部状态不会清空，上一次hash的结果会影响下一次。
            let mut hasher = DefaultHasher::new();
            row.hash(&mut hasher); 
            let h = hasher.finish() as usize % num;
            tmps[h].write((row + "\n").as_bytes()).unwrap();
            cnts[h] += 1;
        }
    }

    cnts.iter().enumerate().for_each(
        |t| println!("tmp{}: {}", t.0, t.1)
    );
    

}

fn gen_case(){
    //let max_size = 1024; // url长度在1024Byte以内;
    //let k = 20; // top k 个url
    let at_least = 20000;
    let topk_urls = vec![
        "https://rust-random.github.io/book/guide-seq.html\n",
        "https://lib.rs/crates/rand\n",
        "https://github.com/Tsumida/topk/blob/master/src/lib.rs\n",
        "https://lib.rs/crates/failure\n",
        "https://lib.rs/crates/cargo-husky\n",
        "https://lib.rs/crates/rust-embed\n",
        "https://ol.gamersky.com/news/202003/1275197.shtml\n",
        "https://www.gamersky.com/news/202003/1275181.shtml\n",
        "http://i.gamersky.com/u/2945817/\n",
        "https://voice.baidu.com/act/newpneumonia/newpneumonia/?from=osari_pc_1\n",
    ].into_iter().map(|s| s.as_bytes()).collect::<Vec<&[u8]>>();

    let size = topk_urls.len();
    let mut bfw = BufWriter::with_capacity(100 * (1 << 20), File::create("./src/urls/input.txt").unwrap());
    let total = at_least * size + ((size + 1)*size) >> 1;
    let mut bytes_cnt = 0;
    for _ in 0..total{
        bytes_cnt += bfw.write(
            topk_urls[rand::random::<usize>() % size]
        ).unwrap();
    }
    println!("total: {} Bytes\n", bytes_cnt);
}

#[test]
fn test_reducer() {
    reducer("./src/urls");
}

#[test]
#[ignore]
fn test_gen_case() {
    gen_case();
}

#[test]
fn test_divider() {
    let num = 31;
    let p = "./src/urls/input.txt";
    divider(num, p, 800);
}

#[test]
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