# Introduction to magpie

magpie项目于2013年完成第一版本，最初的目的是服务于最早的一些实时数据库日志接入解析任务的高可用需求，后续逐渐应用于团队其他的一些通过worker高可用的需求。该项目是高可用平台代码，与之配套的有一套magpie-framework作为应用worker的开发框架。该项目整体借鉴于storm早期版本的调度逻辑和实现思路，在调度层面概念和storm基本一致，比如nimbus，supervisor等概念。

magpie分布式实时任务调度系统一共分三部分：
1. magpie为调度框架
2. magpie-client为客户端代码
3. magpie-framework为任务开发的框架代码

# 如何运行

依赖环境：
1. linux环境（ubuntu）
2. jdk8以上
3. lein编译工具https://github.com/technomancy/leiningen
4. maven编译工具

run-it脚本可以在单机从源代码编译到执行，步骤如下：
1. kill掉机器已有的tomcat, zookeeper, nimbus, superviser, example task的进程
2. 检测本地是否有tomcat9包（该版本需要jdk8），如果没有下载。解压，初始化目录webapps/magpie-jars，该目录为所有task的jars存放目录，supervisor执行时如果本地jars不存在会从这下载
3. 检测本地是否有zookeeper包，如果没有下载。解压，初始化magpie需要的目录/magpie和/magpie/webservice/resource节点值http://127.0.0.1:8080/magpie-jars/ （supervisor通过zk这个节点值的tomcat地址下载task程序包）
4. 清理本地magpie目录
5. 通过lein命令打包magpie
6. 通过maven命令打包magpie-client
7. 通过lein命令那个打包example task程序包
8. 启动nimbus, supervisor
9. 等待20秒，由于supervisor检测自身网络信息时需要一定时间间隔（默认15秒），硬件信息未检测时，nimbus无法选取可用supervisor节点
10. 检测tomcat, zookeeper, nimbus, supervisor进程是否启动
11. magpie-client提交example task，等待启动后tail task日志

# v2.0升级内容

1. jdk7升级为jdk8
2. 升级相关依赖包，主要升级了org.apache.thrift/libthrift "0.10.0"
3. 升级supervisor选取策略从只基于资源选最优，到考虑单节点当前运行任务数（待开发）
