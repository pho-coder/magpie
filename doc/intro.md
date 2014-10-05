# Introduction to jrdw

lein uberjar打包，然后把相关文件集中到这个工程中来，最终工程结构为：

.

├── bin

│   ├── genthrift.sh

│   ├── magpie

│   ├── magpie-client

│   ├── magpie-daemon

│   └── magpie_nimbus.thrift

├── conf

│   ├── magpie.log4j.properties

│   ├── magpie.yaml.example

│   └── worker.log4j.properties

├── doc

│   └── intro.md

├── lib

│   └── native

│       └── Linux-amd64-64

│           └── libsigar-amd64-linux.so

├── magpie-0.1.0-SNAPSHOT-standalone.jar

└── magpie-client-1.0-SNAPSHOT-standalone.jar


架构打包出来的jar包magpie-0.1.0-SNAPSHOT-standalone.jar

--运行nimbus、supervisor，由magpie、magpie-daemon shell调用，magpie与magpie-daemon的区别就是magpie-daemon是非使用nohup启动的，magpie-daemon交由daemontools使用。

  在启动之前，应该先在zookeeper上建立magpie的根目录。同时还需要把下载jar包的webservice地址写入zookeeper的/{zookeeper-root}/webservice/resource节点中

---启动supervisor：magpie start supervisor 

---启动nimbus：magpie start nimbus

---使用daemontools启动supervisor与nimbus：

-----nohup supervise /export/servers/daemontool_service/magpie_supervisor > /dev/null 2>&1 &

------/export/servers/daemontool_service/magpie_supervisor目录下有个run文件，内容为:

#!/usr/bin/env bash

exec 2>&1

exec magpie-daemon restart supervisor

-----nohup supervise /export/servers/daemontool_service/magpie_nimbus > /dev/null 2>&1 &

------/export/servers/daemontool_service/magpie_nimbus目录下有个run文件，内容为:

#!/usr/bin/env bash

exec 2>&1

exec magpie-daemon restart nimbus




命令行客户端打包出来的jar包magpie-client-1.0-SNAPSHOT-standalone.jar

--用于提交命令，由magpie-client shell调用

---提交任务：magpie-client submit -jar jarname(abc.jar) -class classname(com.jd.bdp.ABC) -id 123


daemontools安装脚本：

#!/usr/bin/env bash


Usage(){

echo "

Usage:./daemontools_install.sh target_dir

"

}


target_dir=$1

getSource(){

rm -rf install_tmp

mkdir -p install_tmp

cd install_tmp

wget $1 --no-check-certificate

mv * tmp.tar.gz

tar -zxvf tmp.tar.gz

rm -rf tmp.tar.gz

mv * ../$2

cd ../

rm -rf install_tmp

}



mkdir -p /tmp/daemontools_install_tmp

cd /tmp/daemontools_install_tmp



daemontools_url=http://cr.yp.to/daemontools/daemontools-0.76.tar.gz

patch_url=http://www.qmail.org/moni.csi.hu/pub/glibc-2.3.1/daemontools-0.76.errno.patch

getSource ${daemontools_url} admin

cd admin/daemontools-0.76

cd src

wget ${patch_url} --no-check-certificate

patch < daemontools-0.76.errno.patch

cd ..

cd ../../

mv admin ${target_dir}

cd ${target_dir}/admin/daemontools-0.76

./package/install

cd /tmp

rm -rf /tmp/daemontools_install_tmp





