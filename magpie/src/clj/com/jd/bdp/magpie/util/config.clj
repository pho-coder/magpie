(ns com.jd.bdp.magpie.util.config
  (:require [com.jd.bdp.magpie.util.utils :as utils]
            [com.jd.bdp.magpie.util.zookeeper :as zookeeper]))

(defn read-magpie-config []
  (utils/find-yaml "magpie.yaml" true))


(defn init-zookeeper [zk-handler]
  (let [nimbus-path "/nimbus"
        assignment-path "/assignments"
        supervisor-path "/supervisors"
        workerbeat-path "/workerbeats"
        status-path "/status"
        command-path "/commands"
        yourtasks-path "/yourtasks"]
    (zookeeper/mkdirs zk-handler nimbus-path)
    (zookeeper/mkdirs zk-handler assignment-path)
    (zookeeper/mkdirs zk-handler supervisor-path)
    (zookeeper/mkdirs zk-handler workerbeat-path)
    (zookeeper/mkdirs zk-handler status-path)
    (zookeeper/mkdirs zk-handler command-path)
    (zookeeper/mkdirs zk-handler yourtasks-path)))
