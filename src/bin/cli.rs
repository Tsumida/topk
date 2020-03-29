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
                .short("p")
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
        if let Some(_) = exam.values_of("para"){
            proc(true);
        }else{
            proc(false);
        }
    }

    
}

fn proc(is_parallel: bool){
    let num = 31;
    let p = "./urls/input.txt";

    let tdir_divider = tempdir::TempDir::new("./divider").unwrap();
    let tdir_reducer = tempdir::TempDir::new("./reduced").unwrap();

    //let st1 = SystemTime::now();
    //gen_case();
    let st2 = SystemTime::now();

    divider(num, p, 100, &tdir_divider);
    let st3 = SystemTime::now();
    
    if is_parallel{
        reducer_parallel(tdir_divider.path(), &tdir_reducer);
    }else{
        reducer(tdir_divider.path(), &tdir_reducer);
    }

    let st4 = SystemTime::now();

    merger(tdir_reducer.path(), "./result");
    let st5 = SystemTime::now();

   // println!("gen_case takes {:6}ms", st2.duration_since(st1).unwrap().as_millis());
    println!("divider  takes {:6}ms", st3.duration_since(st2).unwrap().as_millis());
    println!("reducer  takes {:6}ms", st4.duration_since(st3).unwrap().as_millis());
    println!("merger   takes {:6}ms", st5.duration_since(st4).unwrap().as_millis());
}