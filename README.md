# topk

将大量url按照出现次数降序排序，选出前k个url。
分为三个步骤：
1. divide: 使用`DefaultHasher`把url划分到某一个临时文件，确保的url划分到同一个临时文件中。
1. reduce: 把divide产生的临时文件读入内存中，统计每种url出现次数，降序输出前k个。这里要求上一步划分出的每个小文件中，不同的url数目不能太多，避免HashMap占用过大内存。
1. merger: 把reduce输出的临时文件读入内存，选取出其中前k个。

根据url的分布不同，这三个阶段的耗时也会不同。

同时处理多个文件。
```
total: 710 MB

// v1, 串行
divider  takes   1448ms
reducer  takes   3355ms
merger   takes      1ms


// v2, 并行处理多个文件
divider  takes   1446ms
reducer  takes   1048ms
merger   takes      1ms


```