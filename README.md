# RumBase Java

Java构建的高性能SQL关系型数据库。

本项目为吉林大学2018级数据库系统课程&系统软件综合实践（荣誉课）课程设计。

## 分工

| **模块**                | **内容**             | **负责人** | **包**       |
| ----------------------- | -------------------- | ---------- | ------------ |
| Query Parse Module      | SQL 语句解析         | @KAAAsS    | parse       |
| Query Execution Module  | 查询执行、优化       |            | query       |
| Table Management Module | 系统内数据库、表管理 |            | table       |
| Indexing Module         | 索引结构，使用 B+ 树 |            | index       |
| Record Module           | 记录管理，实现 MVCC  | @KAAAsS    | record      |
| Transaction Module      | 实现事务的管理与 2PL |            | transaction |
| Data Item Module        | 数据项管理           |            | dataitem   |
| Recovery Log Module     | 日志与恢复管理       |            | recovery    |
| Page Caching Module     | 缓冲与页管理         |            | page        |