(ns com.jd.magpie.util.zookeeper-test
  (:require [clojure.test :refer :all]
            [com.jd.magpie.util.zookeeper :as zookeeper]
            [com.jd.magpie.util.timer :as timer]
            [com.jd.magpie.util.utils :as utils])
  (:use [com.jd.magpie.bootstrap]))

(def conf {MAGPIE-ZOOKEEPER-CONNECTION-TIMEOUT 15000
           MAGPIE-ZOOKEEPER-SESSION-TIMEOUT 20000
           MAGPIE-ZOOKEEPER-RETRY-INTERVAL 1000
           MAGPIE-ZOOKEEPER-RETRY-TIMES 5
           MAGPIE-ZOOKEEPER-RETRY-INTERVAL-CEILING 30000
           MAGPIE-ZOOKEEPER-AUTH-SCHEME nil
           MAGPIE-ZOOKEEPER-AUTH-PAYLOAD nil
           })

(defn test-create-node []
  (let [zk (zookeeper/mk-client conf ["storm1" "storm2" "storm3"] 2181 :root "/magpie-test")]
    (zookeeper/create-node zk "/test-node" (.getBytes "hello world") :ephemeral)
 ;   (zookeeper/delete-node zk "/test-node")
    ))

(defn test-heartbeat []
  (let [timer# (timer/mk-timer)
        zk (zookeeper/mk-client conf ["storm1" "storm2" "storm3"] 2181 :root "/magpie-test")
        afn (fn [] (zookeeper/set-data zk "/test-node" (.getBytes (str "Heartbeat: " (utils/current-time-millis) "\n" (utils/resources-info)) "utf-8")))]
    (timer/schedule-recurring timer# 2 2 afn))

  (loop [flag true] (if flag (recur true))))

(defn test-get-children []
  (let [zk (zookeeper/mk-client conf ["storm1" "storm2" "storm3"] 2181 :root "/")]
    (prn (zookeeper/get-children zk "/" false))
    ))

(defn test-get-data []
  (let [zk (zookeeper/mk-client conf ["storm1" "storm2" "storm3"] 2181 :root "/magpie-test")
        test-path "/test-node"
        test-data (utils/object->bytes (utils/resources-info))]
    (prn (new String test-data))
                                        ;    (zookeeper/create-node zk test-path test-data :ephemeral)
;    (zookeeper/create-node zk test-path test-data)
    (prn (utils/bytes->map (zookeeper/get-data zk test-path false)))))


(deftest a-test
  (testing "Test Zookeeper."
;    (test-create-node)
;    (test-heartbeat)
    (test-get-data)
    ))