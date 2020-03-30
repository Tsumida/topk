# topk
用法:
```
cargo build --release

// data被划分为63个小文件，使用并行处理，找出按出现次数降序排列前10个url。
./target/release/cli example -n 63 -p -t 10 -s ~/topk/src/urls/input.txt 
```

将大量url按照出现次数降序排序，选出前k个url。
分为三个步骤：
1. divide: 使用`DefaultHasher`把url划分到某一个临时文件，确保的url划分到同一个临时文件中。
1. reduce: 把divide产生的临时文件读入内存中，统计每种url出现次数，降序输出前k个。这里要求上一步划分出的每个小文件中，不同的url数目不能太多，避免HashMap占用过大内存。
1. merger: 把reduce输出的临时文件读入内存，选取出其中前k个。

根据url的分布和参数的变化，这三个阶段的耗时也会不同。
同时处理多个文件。total表示url文件的大小，num表示divide划分成的小文件个数。
```
total: 710 MB

num: 31
                串行reduce  并行reduce
divider  takes   1448ms      1446ms
reducer  takes   3355ms      1048ms
merger   takes      1ms         1ms

num: 63
                串行reduce  并行reduce
divider  takes   1706ms      1667ms
reducer  takes   5651ms      1816ms
merger   takes      1ms         2ms
```
如果文件较大，那么主要耗时的是divide阶段:
```
total: 710 MB * 3
num： 63
                串行reduce  并行reduce
divider  takes   12455ms    12743ms
reducer  takes   7349ms      1706ms
merger   takes      4ms         4ms
```
