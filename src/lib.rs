use std::collections::{HashMap, BinaryHeap};
use std::fs::File;
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::cmp::{Ordering, Ord, Reverse};

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

impl PartialEq for StatEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cnt == other.cnt
    }
}


fn core(){
    // 统计url次数
    let path = "./src/urls/case_2.txt";
    let bf_cap = 100 * (1 << 20); // 100MB
    let hash_cap = 100 * (1 << 20); 
    let k = 4; // top k

    // read file. 
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
    let mut bheap = BinaryHeap::with_capacity(k);
    for (k, v) in counter.iter(){
        if bheap.len() < 10{
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
    let mut result = Vec::with_capacity(k);
    while cnt < k && !bheap.is_empty(){
        result.push(bheap.pop().unwrap().0);
        cnt += 1;
    }
    result.reverse();
    let s = serde_json::to_string(&result).unwrap();
    let mut f = File::create(format!("./src/urls/tmp_{}", 0)).unwrap();
    f.write(s.as_bytes()).unwrap();
    
}

fn gen_case(){
    //let max_size = 1024; // url长度在1024Byte以内;
    //let k = 20; // top k 个url
    let at_least = 10000;
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
    let mut bfw = BufWriter::with_capacity(100 * (1 << 20), File::create("./src/urls/case_2.txt").unwrap());
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
fn test_core() {
    core()
}

#[test]
#[ignore]
fn test_gen_case() {
    gen_case();
}