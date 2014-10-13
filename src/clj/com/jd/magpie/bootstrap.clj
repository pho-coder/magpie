(ns com.jd.magpie.bootstrap)

(use 'clojure.set) 

(def NIMBUS-THRIFT-PORT "nimbus.thrift.port")
(def MAGPIE-ZOOKEEPER-SERVERS "magpie.zookeeper.servers")
(def MAGPIE-ZOOKEEPER-PORT "magpie.zookeeper.port")
(def MAGPIE-ZOOKEEPER-ROOT "magpie.zookeeper.root")
(def MAGPIE-ZOOKEEPER-CONNECTION-TIMEOUT "magpie.zookeeper.connection.timeout.millis")
(def MAGPIE-ZOOKEEPER-SESSION-TIMEOUT "magpie.zookeeper.session.timeout.millis")
(def MAGPIE-ZOOKEEPER-RETRY-INTERVAL "magpie.zookeeper.retry.interval.millis")
(def MAGPIE-ZOOKEEPER-RETRY-TIMES "magpie.zookeeper.retry.times")
(def MAGPIE-ZOOKEEPER-RETRY-INTERVAL-CEILING "magpie.zookeeper.retry.intervalceiling.millis")
(def MAGPIE-ZOOKEEPER-AUTH-SCHEME "magpie.zookeeper.auth.scheme")
(def MAGPIE-ZOOKEEPER-AUTH-PAYLOAD "magpie.zookeeper.auth.payload")
(def MAGPIE-FLOOR-SCORE "magpie.floor.score")
(def MAGPIE-JARS-DIR "magpie.jars.dir")
(def MAGPIE-PIDS-DIR "magpie.pids.dir")
(def MAGPIE-LOGS-DIR "magpie.logs.dir")
(def MAGPIE-HEARTBEAT-INTERVAL "magpie.heartbeat.interval.millis")
(def MAGPIE-SCHEDULE-INTERVAL "magpie.schedule.interval.millis")
(def MAGPIE-SCHEDULE-TIMEOUT "magpie.schedule.timeout.millis")
(def MAGPIE-NET-BANDWIDTH-CALCULATE-INTERVAL "magpie.net-bandwidth-calculate.interval.millis")
(def JAVA-LIBRARY-PATH "java.library.path")
(def MAGPIE-WORKER-CHILDOPTS "magpie.worker.childopts")
(def MAGPIE-SUPERVISOR-GROUP "magpie.supervisor.group")
(def MAGPIE-SUPERVISOR-MAX-NET-BANDWIDTH "magpie.supervisor.max-net-bandwidth")
