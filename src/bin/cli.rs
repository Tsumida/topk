use std::time::{SystemTime};
use topk::*;

fn main(){
    let num = 31;
    let p = "./src/urls/input.txt";

    let tdir_divider = tempdir::TempDir::new("./divider").unwrap();
    let tdir_reducer = tempdir::TempDir::new("./reduced").unwrap();

    //let st1 = SystemTime::now();
    //gen_case();
    let st2 = SystemTime::now();

    divider(num, p, 100, &tdir_divider);
    let st3 = SystemTime::now();
   
    reducer(tdir_divider.path(), &tdir_reducer);
    let st4 = SystemTime::now();

    merger(tdir_reducer.path(), "./result");
    let st5 = SystemTime::now();

   // println!("gen_case takes {:6}ms", st2.duration_since(st1).unwrap().as_millis());
    println!("divider  takes {:6}ms", st3.duration_since(st2).unwrap().as_millis());
    println!("reducer  takes {:6}ms", st4.duration_since(st3).unwrap().as_millis());
    println!("merger   takes {:6}ms", st5.duration_since(st4).unwrap().as_millis());
}