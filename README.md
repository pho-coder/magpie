# Introduction to magpie

magpie项目于2013年完成第一版本，最初的目的是服务于最早的一些实时数据库日志接入解析任务的高可用需求，后续逐渐应用于团队其他的一些通过worker高可用的需求。该项目是高可用平台代码，与之配套的有一套magpie-framework作为应用worker的开发框架。该项目整体借鉴于storm早期版本的调度逻辑和实现思路，在调度层面概念和storm基本一致，比如nimbus，supervisor等概念。

magpie分布式实时任务调度系统一共分三部分：
magpie为调度框架
magpie-client为客户端代码
magpie-framework为任务开发的框架代码

# 如何运行

依赖环境：
1. linux环境（ubuntu）
2. jdk8以上
3. lein编译工具https://github.com/technomancy/leiningen
4. maven编译工具

