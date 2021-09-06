# vnpy_arctic
vn.py框架的Arctic数据库管理器

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-linux|windows|mac-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
</p>

## 说明

arctic数据库接口，为了正常运行，运行python版本需要低于3.9，运行pandas版本需要低于1.0.3。

## 安装

安装需要基于2.4.0版本以上的[VN Studio](https://www.vnpy.com)。

下载解压后在cmd运行：

```
python setup.py install
```

## 性能

对比sqlite对22755条数据进行连续进行100次读写操作

Arctic

平均存储消耗时间： 1.083201801776886s

平均读取消耗时间 0.8728189110755921s

获取一条overview平均消耗时间 0.007860851287841798s

平均删除消耗时间 0.013419201374053955s

Sqlite

平均存储消耗时间： 1.8237190175056457s

平均读取消耗时间 2.17199054479599s

获取一条overview平均消耗时间 0.0025047659873962402s

平均删除消耗时间 0.07287977933883667s