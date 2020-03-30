# topk
用法:
```
cargo build --release
./target/release/cli -n 63 -p -t 10 // data被划分为63个小文件，使用并行处理，找出按出现次数降序排列前10个url。
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
// v1
divider  takes   1448ms
reducer  takes   3355ms
merger   takes      1ms

// v2
divider  takes   1446ms
reducer  takes   1048ms
merger   takes      1ms

num: 63
// v1:
divider  takes   1706ms
reducer  takes   5651ms
merger   takes      1ms

// v2
divider  takes   1667ms
reducer  takes   1816ms
merger   takes      2ms

num: 127
// v1
divider  takes   1713ms
reducer  takes  10307ms
merger   takes      2ms

// v2
divider  takes   1725ms
reducer  takes   3388ms
merger   takes      3ms
```