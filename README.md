
由于arctic不再支持python3.7之后的版本，其官方团队切换至arcticdb项目(目前支持python3.6~3.11），VeighNa官方团队的vnpy_arctic在VNStudio 3.0系列之后不再维护更新。
本项目基于arcticdb文档和vnpy_arctic代码重新开发，供大家参考使用。
=======
# VeighNa框架的Arctic数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.4-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-linux|windows|mac-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.8-blue.svg" />
</p>

## 说明

基于arcticdb开发的数据库接口。

## 使用

在VeighNa中使用Arcticdb连接ArcticDB数据库时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.path|数据库文件路径|是|C:\\users/administrator\\.vntrader\\ 或者 .vntrader|
|database.map_size|数据库文件大小|否|5GB|
|database.name|名称|是|arcticdb|
|database.timezone|时区|否|Asia/Shanghai|
|database.name|名称|是|arcticdb|
|database.password|密码|否||
|database.database|实例|否||

