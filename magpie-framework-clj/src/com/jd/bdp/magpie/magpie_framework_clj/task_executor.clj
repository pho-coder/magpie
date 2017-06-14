(ns com.jd.bdp.magpie.magpie-framework-clj.task-executor
  (:import [java.io File IOException])
  (:require [clojure.tools.logging :as log]
            
            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [com.jd.bdp.magpie.util.timer :as magpie-timer]
            [com.jd.bdp.magpie.magpie-framework-clj.utils :as utils]))

(defn execute
  "
  run-fn: 这个是系统正常执行的方法，它会被连续无休眠地被调用，即当它执行结束后，这个方法就会被再次调用，直到收到其它命令。
          为了保证任务能收到系统的其它指令，这个方法里最好不要有耗时操作，更不可在此执行死循环。
  prepare-fn: 初始化application，任务里初始化的操作都在这里执行，比如queue的连接、系统的配置等。
              任务在第一次开始执行run-fun前，会先执行这个方法。
  reload-fn: 当对系统任务执行reload命令时，会调用这个方法。
  pause-fn: 当对系统任务执行pause命令时，会调用这个方法。
  close-fn: 当对系统任务执行kill命令时，会调用这个方法。
  "
  [run-fn & {:keys [prepare-fn reload-fn pause-fn close-fn]
             :or {prepare-fn utils/blank-fn
                  reload-fn utils/blank-fn
                  pause-fn utils/blank-fn
                  close-fn utils/blank-fn}}]
  (let [zk-servers (System/getProperty "zookeeper.servers")
        zk-root (System/getProperty "zookeeper.root")
        pids-dir (System/getProperty "pids.dir")
        job-id (System/getProperty "job.id")
        job-node (System/getProperty "job.node")
        heartbeat-path "/workerbeats/"
        status-path "/status/"
        command-path "/commands/"]
    (let [file (File. pids-dir)]
      (if-not (.isDirectory file)
        (try
          (.mkdirs file)
          (catch IOException e
            (log/error (.toString e))
            (System/exit -1))))
      (let [pid-file (File. file (magpie-utils/get-pid))]
        (try
          (.createNewFile pid-file)
          (catch IOException e
            (log/error (.toString e))
            (System/exit -1)))))
    
    (try
      (let [zk-str (str zk-servers zk-root)
            zk-client (utils/zk-new-client zk-str)
            task-heartbeat-node (str heartbeat-path job-node)
            task-status-node (str status-path job-node)
            task-command-node (str command-path job-node)]
        (while (not (utils/create-heartbeat-node task-heartbeat-node))
          (log/warn "zk task heartbeat node exists:" task-heartbeat-node)
          (Thread/sleep 1000))
        (utils/set-task-status task-status-node (utils/task-status :running))
        (let [action (atom (utils/get-task-command task-command-node))
              check-command-timer (magpie-timer/mk-timer)
              has-reset (atom false)]
          (magpie-timer/schedule-recurring check-command-timer 1 3
                                           (fn []
                                             (try
                                               (reset! action (utils/get-task-command task-command-node))
                                               (catch Exception e
                                                 (log/error "error accurs in checking command from zookeeper, maybe connection is lost!")
                                                 (log/error e)
                                                 (System/exit -1)))))
          (loop [act @action]
            (if (= act :kill)
              (do (log/info job-id "command" act "I will exit!")
                  (close-fn job-id)
                  (utils/set-task-status task-status-node (utils/task-status :killed)))
              (case act
                :reload (do (if @has-reset
                              (Thread/sleep 5000)
                              (do (reload-fn job-id)
                                  (utils/set-task-status task-status-node (utils/task-status :reloaded))
                                  (reset! has-reset true)
                                  (log/info job-id "command" act "has reloaded!")))
                            (recur @action))
                :run (do (reset! has-reset false)
                         (try
                           (log/info job-id "command" act "start to prepare")
                           (prepare-fn job-id)
                           (log/info job-id "command" act "start to run")
                           (utils/set-task-status task-status-node (utils/task-status :running))
                           (while (= @action :run)
                             (run-fn job-id))
                           (log/info job-id "command" @action "stop running")
                           (catch Exception e
                             (log/error "error accurs in running process:" e)
                             (throw (RuntimeException. e)))
                           (finally
                             (log/info job-id "end running")
                             (close-fn job-id)))
                         (recur @action))
                :init (recur @action)
                :pause (do (if @has-reset
                             (Thread/sleep 5000)
                             (do (pause-fn job-id)
                                 (utils/set-task-status task-status-node (utils/task-status :paused))
                                 (reset! has-reset true)
                                 (log/info job-id "command" act "has paused!")))
                           (recur @action))
                :wait (do (log/info job-id "command" act "waiting")
                          (Thread/sleep 5000)
                          (recur @action))
                (recur @action)))))
      (if-not (nil? zk-client)
        (utils/zk-close)))
      (log/info job-id "This magpie app will be closed!")
      (catch Throwable e
        (log/error e)
        (System/exit -1)))))
