use std::time::{SystemTime};
use topk::*;

use clap::{App, Arg, SubCommand};

fn main(){

    let matches = App::new("topk")
        .version("0.1")
        .author("Tsuko")
        .about("Find top-k.")
        .subcommand(
            SubCommand::with_name("example")
            .help("run example.")
            .arg(
                Arg::with_name("para")
                .takes_value(false)
                .short("p"))
            .arg(
                Arg::with_name("topk")
                .takes_value(true)
                .short("t")
                .help("find the top-k elements.")
            ).arg(
                Arg::with_name("num")
                .short("n")
                .takes_value(true)
                .help("Divide data into num small files.")
            )
        )
        .subcommand(
            SubCommand::with_name("gen")
            .help("generate test data")
            .arg(
                Arg::with_name("base")
                .takes_value(true)
                .short("b"))
        )
        .get_matches();
    
    if let Some(gen) = matches.subcommand_matches("gen"){
        if let Some(b) = gen.value_of("base"){
            gen_case(b.parse().unwrap());
        }else{
            eprintln!("Error, please input base.");
        }
    }

    if let Some(exam) = matches.subcommand_matches("example"){
        let mut topk = 100;
        if let Some(p) = exam.value_of("topk"){
            topk = p.parse::<usize>().unwrap();
        }
        let mut num = 31;
        if let Some(p) = exam.value_of("num"){
            num = p.parse::<usize>().unwrap();
        }
        const MB: usize = 1 << 20;
        let tdir_divider = tempdir::TempDir::new("./divider").unwrap();
        let tdir_reducer = tempdir::TempDir::new("./reduced").unwrap();
        let para = Parameters{
            topk: topk,
            num: num,
            bf_cap: 50 * MB,
            hash_cap: 50 * MB, 
            input_path: "./urls/input.txt".to_string(),
            tdir_divider: tdir_divider,
            tdir_reducer: tdir_reducer,
            result_path: "./result".to_string(),
        };
        if let Some(_) = exam.values_of("para"){
            proc(true, &para);
        }else{
            proc(false, &para);
        }
    }
}

fn proc(is_parallel: bool, para: &Parameters){
    
    //let st1 = SystemTime::now();
    //gen_case();
    let st2 = SystemTime::now();
    divider(&para);
    let st3 = SystemTime::now();
    
    if is_parallel{
        reducer_parallel(&para);
    }else{
        reducer(&para);
    }
    let st4 = SystemTime::now();
    merger(&para);
    let st5 = SystemTime::now();

   // println!("gen_case takes {:6}ms", st2.duration_since(st1).unwrap().as_millis());
    println!("divider  takes {:6}ms", st3.duration_since(st2).unwrap().as_millis());
    println!("reducer  takes {:6}ms", st4.duration_since(st3).unwrap().as_millis());
    println!("merger   takes {:6}ms", st5.duration_since(st4).unwrap().as_millis());
}