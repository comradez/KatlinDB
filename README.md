# KatlinDB
## 运行方法

该项目依赖 Kotlin 语言 1.5.31 版本，需要安装 IntelliJ IDEA 或手动安装 Kotlin 编译环境。

### IDE 运行

用 IntelliJ IDEA 打开项目路径，在右上角的 “运行-调试配置” 中配置主要类为 `MainKt`，按需配置程序实参：

```
usage: KatlinDB
 -cli                 use the command line interface
 -execute <file>      execute the given sql file
 -help                print the help message
 -workdir <workdir>   use the given directory as the working directory
```

然后点击运行即可。

### 命令行运行

我的机器是 Linux 系统，如要在 Windows 上运行对命令稍加修改即可：

```bash
#!/bin/bash

mkdir -p run/DatabaseDir # 可执行文件放在 run 内，数据库放在 run/DatabaseDir 内
./gradlew distTar # 打包 tar 文件
mv build/distributions/KatlinDB-1.0-SNAPSHOT.tar run # 将打好的包移动到指定位置
cd run
tar -xvf KatlinDB-1.0-SNAPSHOT.tar # 解压缩
cd KatlinDB-1.0-SNAPSHOT/bin
./KatlinDB -cli -workdir DatabaseDir #命令行参数如上所示（对于 Windows 系统，这个路径内也提供了 KatlinDB.bat）
```
