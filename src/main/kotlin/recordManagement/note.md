参考 Project Redbase，记录管理模块设计如下：

+ 一个文件分为若干页，第一页为 header 页，后面为 data 页
  + header 页里的元信息以 JSON 格式存储
+ 记录为定长
  + 其中 VARCHAR 长度为【未定】
  + 字节序为小端
+ 一个 RecordHandler，对每个打开的文件维护一个 FileHandler
  + 在 FileHandler 中凭 RID 访问记录
+ FileScan 和 空间释放暂时未定

----

+ 页头用 BitMap 记录各槽是否为空
  + 页头设计：`位图 | 下一个有空槽的页`
  + 每个槽内头部用 BitMap 记录自己各列是否为空