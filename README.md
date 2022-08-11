# VeighNa框架的Arctic数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.3-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-linux|windows|mac-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.8-blue.svg" />
</p>

## 说明

基于arctic开发的MongoDB数据库接口。

## 使用

在VeighNa中使用Arctic连接MongoDB数据库时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.name|名称|是|arctic|
|database.host|地址|是|localhost|
|database.port|端口|是|0|
|database.user|用户名|否|admin(若mongodb无认证则不填)|
|database.password|密码|否|123456(若mongodb无认证则不填)|
|database.database|实例|是|vnpy|
