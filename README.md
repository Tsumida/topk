# topk
用法:
```
cargo build --release

// data被划分为63个小文件，使用并行处理，找出按出现次数降序排列前10个url。
// 可以输入文件夹
./target/release/cli example -n 63 -p -t 10 -s ~/topk/src/urls/input.txt 
```
example参数:
- t: top-k，前k个
- n: 划分成多少个文件
- s: url文件的路径，也可以是包含url的文件夹的路径。

# 过程
将大量url按照出现次数降序排序，选出前k个url。
分为三个步骤：
1. divide: 使用`DefaultHasher`把url哈希到某一个临时文件，确保的url划分到同一个临时文件中。这里通过用更大的num参数把大文件划分为更多临时文件，来使得一个临时文件读入内存中，产生的HashMap不会太大。
1. reduce: 把divide产生的临时文件读入内存中，统计每种url出现次数，降序输出前k个。
1. merger: 把reduce输出的临时文件读入内存，选取出其中前k个。

根据url的分布和参数的变化，这三个阶段的耗时也会不同。
同时处理多个文件。total表示url文件的大小，num表示divide划分成的小文件个数。

使用gen_case()对600多条不同的url，按均匀分布生成约710MB url文件，作为cli example的输入，选择k=10，在不同的num下的耗时为：
```
total: 710 MB
k:10

num: 31
                串行reduce  并行reduce
divider  takes   1430ms     1449ms
reducer  takes   2248ms      679ms
merger   takes      1ms        2ms

num: 63
                串行reduce  并行reduce
divider  takes   1406ms      1508ms
reducer  takes   3460ms      1135ms
merger   takes      1ms         4ms
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
