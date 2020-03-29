# V1
单线程，顺序执行 divide, reduce, merge三个阶段。
```
total: 49753879 Bytes

gen_case takes   3488ms
divider  takes   4316ms
reducer  takes  24456ms
merger   takes      1ms

cnt=50296, url=https://rust-random.github.io/book/guide-seq.html
cnt=50234, url=https://www.gamersky.com/news/202003/1275413.shtml
cnt=50228, url=https://github.com/Tsumida/topk/blob/master/src/lib.rs
cnt=50221, url=https://lib.rs/crates/rand

```

# V2
同时处理多个文件。
```
total: 494 MB
// v1
divider  takes   1489ms
reducer  takes   1016ms
merger   takes      0ms

// v2
divider  takes   1477ms
reducer  takes    953ms
merger   takes      0ms


```