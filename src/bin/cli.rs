use std::time::{SystemTime};

use topk::*;

fn main(){
    let st1 = SystemTime::now();
    gen_case();
    let st2 = SystemTime::now();

    let num = 31;
    let p = "./src/urls/input.txt";
    divider(num, p, 100);
    let st3 = SystemTime::now();

    let divided_path = "./src/tmps";
    let tmp_dir_path = "./src/reduced";
    reducer(divided_path, tmp_dir_path);
    let st4 = SystemTime::now();
    let res_path = "./src/result";
    merger(tmp_dir_path, res_path);
    let st5 = SystemTime::now();

    println!("gen_case takes {:6}ms", st2.duration_since(st1).unwrap().as_millis());
    println!("divider  takes {:6}ms", st3.duration_since(st2).unwrap().as_millis());
    println!("reducer  takes {:6}ms", st4.duration_since(st3).unwrap().as_millis());
    println!("merger   takes {:6}ms", st5.duration_since(st4).unwrap().as_millis());
}