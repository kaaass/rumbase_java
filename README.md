# RumBase Java

| master | dev |
| ------ | --- |
| [![Build Status](https://www.travis-ci.com/kaaass/rumbase_java.svg?token=7d6V7UKwzfD6augATNKx&branch=master)](https://www.travis-ci.com/kaaass/rumbase_java) | [![Build Status](https://www.travis-ci.com/kaaass/rumbase_java.svg?token=7d6V7UKwzfD6augATNKx&branch=dev)](https://www.travis-ci.com/kaaass/rumbase_java) |

Java构建的SQL关系型数据库。

本项目为吉林大学2018级数据库系统课程&系统软件综合实践（荣誉课）课程设计。

## 构建

1. 在 Release 页面下载源码或 clone 项目
2. 在项目目录执行 `./gradlew build`

## 分工

| **模块**                | **内容**             | **负责人** | **包**       |
| ----------------------- | -------------------- | ---------- | ------------ |
| Server Module           | 服务器、会话管理         | @KAAAsS    | server       |
| Query Parse Module      | SQL 语句解析         | @KAAAsS    | parse       |
| Query Execution Module  | 查询执行、优化       | @KveinAxel  | query       |
| Table Management Module | 系统内数据库、表管理 |  @KveinAxel  | table       |
| Indexing Module         | 索引结构，使用 B+ 树 | @DoctorWei1314 | index       |
| Record Module           | 记录管理，实现 MVCC  | @KAAAsS    | record      |
| Transaction Module      | 实现事务的管理与 2PL | @criki    | transaction |
| Data Item Module        | 数据项管理           | @kaito     | dataitem   |
| Recovery Log Module     | 日志与恢复管理       |  @kaito     | recovery    |
| Page Caching Module     | 缓冲与页管理         | @XuanLaoYee    | page        |