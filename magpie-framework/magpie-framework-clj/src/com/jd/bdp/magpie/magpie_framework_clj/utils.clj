(ns com.jd.bdp.magpie.magpie-framework-clj.utils
  (:import [org.apache.zookeeper KeeperException$NodeExistsException KeeperException$NoNodeException])
  (:require [clojure.tools.logging :as log]
            
            [clj-zookeeper.zookeeper :as zk]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]))

(defn zk-new-client
  [zk-str]
  (zk/new-client zk-str))

(defn zk-close
  []
  (zk/close))

(defn create-heartbeat-node
  [task-heartbeat-node]
  (try
    (zk/create task-heartbeat-node :mode :ephemeral)
    true
    (catch Exception e
      (if (= (.getClass e) KeeperException$NodeExistsException)
        false
        (do (log/error e)
            (throw e))))))

(defn task-status
  [status]
  (case status
    :reloaded "reloaded"
    :killed "killed"
    :running "running"
    :paused "paused"
    (throw (RuntimeException. (str "task status error! " status)))))

(defn set-task-status
  [task-status-node status]
  (try
    (zk/set-data task-status-node (magpie-utils/string->bytes status))
    (catch Exception e
      (if (= (.getClass e) KeeperException$NoNodeException)
        (do (zk/create task-status-node :mode :persistent)
            (zk/set-data task-status-node (magpie-utils/string->bytes status)))
        (do (log/error e)
            (throw e))))))

(defn task-command
  [command]
  (case command
    :init "init"
    :run "run"
    :reload "reload"
    :pause "pause"
    :wait "wait"
    :kill "kill"
    (throw (RuntimeException. (str "task command error! " command)))))

(defn get-task-command
  [task-command-node]
  (try
    (case ((magpie-utils/bytes->map (zk/get-data task-command-node)) "command")
      "init" :init
      "run" :run
      "reload" :reload
      "pause" :pause
      "wait" :wait
      "kill" :kill
      (throw (RuntimeException. (str "get task command from zk error!" task-command-node))))
    (catch Exception e
      (log/error e)
      (throw e))))

(defn blank-fn
  [job-id])
