# VeighNa框架的Arctic数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.4-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-linux|windows|mac-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.11-blue.svg" />
</p>

## 说明

基于arcticdb开发的数据库接口。由于arctic不再支持python3.7之后的版本，其官方团队切换至arcticdb项目(目前支持python3.7~3.11），VeighNa官方团队的vnpy_arctic在VNStudio 3.0系列之后不再维护更新。本项目基于arcticdb文档和vnpy_arctic代码重新开发，供大家参考使用。

## 使用

在VeighNa中使用Arcticdb连接ArcticDB数据库时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.name|名称|是|arctic|
|database.timezone|时区|是|Asia/Shanghai|
|database.database|实例|否|默认值为arctic|
|database.path|数据库文件路径|否|C:\\users\\administrator\\.vntrader\\ 默认值为 .vntrader|
|database.map_size|数据库文件大小|否|默认值为5GB，Windows下会预分配10GB空间|
