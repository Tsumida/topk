use std::collections::{HashMap, BinaryHeap};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::cmp::{Ordering, Ord, Reverse};

use serde::{Serialize, Deserialize};

//   Mapper:
//  设每条url2KB(IE, 2083char, 如果是ascii --> 2KB)
//  100GB url ==> 50 M条, 用u32
//  hasher 承载因子 50%, 每条记录2KB(url) + 24B(String) + 4B(cnt) = 2076B
//  1K条url需要 2 x 2076 B x 1000 = 4.15MB，
//  1 GB内存分配 --> 一次最多读入200K条，要830MB, 剩下部分给buffer + 控制 --> 250次
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
    let path = "./src/urls/case_1.txt";
    let bf_cap = 100 * (1 << 20); // 100MB
    let hash_cap = 100 * (1 << 20); 
    let k = 10; // 前10

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
        }else{
            let t = bheap.peek().unwrap();
            let q = Reverse(StatEntry{
                url: k.clone(),
                cnt: *v,
            });
            if &q > t{
                bheap.push(q);
            }
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
    fn random_string(l: usize){
        assert!(l > 0);
    }
}

#[test]
fn test_core() {
    core()
}
