(ns com.jd.magpie.daemon.nimbus
  (:gen-class)
  (:require [com.jd.magpie.util.zookeeper :as zookeeper]
            [com.jd.magpie.util.config :as config]
            [com.jd.magpie.util.timer :as timer]
            [com.jd.magpie.util.utils :as utils]
            [clojure.tools.logging :as log])
  (:use [com.jd.magpie.bootstrap])
  (:import [com.jd.magpie.generated Nimbus Nimbus$Iface Nimbus$Processor]
           [java.util Arrays]
           [org.apache.thrift.server THsHaServer THsHaServer$Args]
           [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory]
           [org.apache.thrift TException]
           [java.lang.ProcessBuilder$Redirect]
           [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket]))

(defn get-all-supervisors [zk-handler supervisor-path group]
  (let [supervisors (zookeeper/get-children zk-handler supervisor-path false)
        supervisor-infos (map #(utils/bytes->map (zookeeper/get-data zk-handler (str supervisor-path "/" %) false)) supervisors)
        supervisors-of-the-group (filter #(if (nil? %) false true) (map (fn [supervisor] (if (= (get supervisor "group") group)
                                                                                          supervisor
                                                                                          nil)) supervisor-infos))]
    (log/debug "supervisor info:" supervisor-infos)
    (log/debug "supervisors of the group:" supervisors-of-the-group)
    supervisors-of-the-group))

(defn get-best [zk-handler supervisor-path group type floor-score]
  (let [supervisor-infos (get-all-supervisors zk-handler supervisor-path group)
        MAX-SCORE 100]
    (if (empty? supervisor-infos)
      (do (log/error "no supervisor in group" group "is running, this task will not be run, please check...")
          nil)
      (let [supervisors-good (filter (fn [supervisor] (let [memory-score (if-let [score (get supervisor "memory-score")]
                                                                          score
                                                                          0)
                                                           cpu-score (if-let [score (get supervisor "cpu-score")]
                                                                       score
                                                                       0)
                                                           net-bandwidth-score (if-let [score (get supervisor "net-bandwidth-score")]
                                                                                 score
                                                                                 0)]
                                                       (if (and (and (>= memory-score floor-score) (<= memory-score MAX-SCORE))
                                                                (and (>= cpu-score floor-score) (<= cpu-score MAX-SCORE))
                                                                (and (>= net-bandwidth-score floor-score) (<= net-bandwidth-score MAX-SCORE)))
                                                         true
                                                         false))) supervisor-infos)]
        (if (empty? supervisors-good)
          (do (log/warn "no supervisor has enough resource in group" group)
              nil)
          (get (reduce (fn [one two]
                         (let [score-type (case type
                                            "memory" "memory-score"
                                            "cpu" "cpu-score"
                                            "network" "net-bandwidth-score"
                                            "memory-score")]
                           (if (>= (get one score-type) (get two score-type))
                             one
                             two))) supervisors-good) "id"))))))

(defn assign [zk-handler id jar klass group type floor-score & {:keys [last-supervisor]}]
  (let [supervisor-path "/supervisors"
        assignment-path "/assignments"
        command-path "/commands"
        node id
        task-path (str assignment-path "/" node)]
    (if-let [best-supervisor (get-best zk-handler supervisor-path group type floor-score)]
      (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "run" "time" (utils/current-time-millis)}))
          (zookeeper/set-data zk-handler task-path (utils/object->bytes {"start-time" (utils/current-time-millis) "jar" jar "class" klass "id" id "group" group "type" type "supervisor" best-supervisor "last-supervisor" last-supervisor}))
          (log/info "submit task successfully, (topology id='" id "')"))
      (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "wait" "time" (utils/current-time-millis)}))
          (log/warn "resource not enough, this task will be waiting. (topology id='" id "')")))))

(defn clear-topology [zk-handler node]
  (let [assignment-path "/assignments"
        command-path "/commands"
        status-path "/status"]
    (try
      (zookeeper/delete-node zk-handler (str assignment-path "/" node))
      (catch Exception e))
    (try
      (zookeeper/delete-node zk-handler (str command-path "/" node))
      (catch Exception e))
    (try
      (zookeeper/delete-node zk-handler (str status-path "/" node))
      (catch Exception e))))

(defn submit-task [zk-handler id jar klass floor-score group type  assignment-path status-path command-path]
  (let [result (atom "submit failure!")]
          (try
            (let [node id
                  task-path (str assignment-path "/" node)
                  running? (zookeeper/exists-node? zk-handler task-path false)]
              (if running?
                (let [command-info (utils/bytes->map (zookeeper/get-data zk-handler (str command-path "/" node) false))
                      update-time (command-info "time")
                      command (command-info "command")]
                  (if (= command "run")
                    (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "reload" "time" (utils/current-time-millis)}))
                        (reset! result (str "This task has already been running! Will be reloaded! (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")))
                    (reset! result (str "This task has already been running! current command='" command "' (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")))
                  (log/warn @result))
                (do (zookeeper/create-node zk-handler (str assignment-path "/" node) (utils/object->bytes {"start-time" (utils/current-time-millis) "jar" jar "class" klass "id" id "group" group "type" type}))
                    (zookeeper/create-node zk-handler (str command-path "/" node) (utils/object->bytes {"command" "init" "time" (utils/current-time-millis)}))
                    (zookeeper/create-node zk-handler (str status-path "/" node) (utils/object->bytes {"command" "init" "time" (utils/current-time-millis)}))
                    (assign zk-handler id jar klass group type floor-score)
                    (reset! result (str "This task will be submit soon! (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")))))
            (catch Throwable e
              (reset! result (str  e "Task submission exception. (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')"))
              (log/error @result)))
          @result))

(defn operate-task [zk-handler id command assignment-path status-path command-path]
  (let [commands (hash-set "kill" "pause" "active" "reload")]
    (if (not (contains? commands command))
      (str command " command is unsupported!")
      (let [result (atom (str command " failure!"))]
        (try
          (let [node id
                task-path (str assignment-path "/" node)
                command-str (case command
                              "kill" "kill"
                              "pause" "pause"
                              "active" "run"
                              "reload" "reload")
                running? (zookeeper/exists-node? zk-handler task-path false)]
            (if running?
              (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" command-str "time" (utils/current-time-millis)}))
                  (reset! result (str command " successfully!, (task id='" id "')"))
                  (log/info @result))
              (do (reset! result (str "task is not running, (task id='" id "')"))
                  (log/error @result))))
          (catch Throwable e
            (reset! result (str e "Task " command " exception. (task id='" id "')"))
            (log/error @result)))
        @result))))

(defn service-handler [conf zk-handler]
  (let [assignment-path "/assignments"
        status-path "/status"
        command-path "/commands"
        floor-score (conf MAGPIE-FLOOR-SCORE 20)]
    
    (reify Nimbus$Iface
      (^String submitTopology
        [this ^String id ^String jar ^String klass]
        (submit-task zk-handler id jar klass floor-score "default" "memory" assignment-path status-path command-path))
      
      (^String killTopology
        [this ^String id]
        (operate-task zk-handler id "kill" assignment-path status-path command-path))
      
      (^String pauseTopology
        [this ^String id]
        (operate-task zk-handler id "pause" assignment-path status-path command-path))

      (^String activeTopology
        [this ^String id]
        (operate-task zk-handler id "active" assignment-path status-path command-path))
      
      (^String reloadTopology
        [this ^String id]
        (operate-task zk-handler id "reload" assignment-path status-path command-path))
      
      (^String submitTask
        [this ^String id ^String jar ^String klass ^String group ^String type]
        (submit-task zk-handler id jar klass floor-score group type assignment-path status-path command-path))

      (^String operateTask
        [this ^String id ^String command]
        (operate-task zk-handler id command assignment-path status-path command-path)))))

(defn launch-server! [conf]
  (let [zk-handler (zookeeper/mk-client conf (conf MAGPIE-ZOOKEEPER-SERVERS) (conf MAGPIE-ZOOKEEPER-PORT) :root (conf MAGPIE-ZOOKEEPER-ROOT))
        nimbus-path "/nimbus"
        assignment-path "/assignments"
        supervisor-path "/supervisors"
        workerbeat-path "/workerbeats"
        status-path "/status"
        command-path "/commands"
        heartbeat-interval (/ (conf MAGPIE-HEARTBEAT-INTERVAL 2000) 1000)
        schedule-check-interval (/ (conf MAGPIE-SCHEDULE-INTERVAL 5000) 1000)
        _ (config/init-zookeeper zk-handler)
        nimbus-info {"ip" (utils/ip) "hostname" (utils/hostname) "username" (utils/username) "port" (int (conf NIMBUS-THRIFT-PORT))}
        heartbeat-timer (timer/mk-timer)
        workerbeat-timer (timer/mk-timer)
        service-handler# (service-handler conf zk-handler)
        floor-score (conf MAGPIE-FLOOR-SCORE 20)
        options (-> (TNonblockingServerSocket. (int (conf NIMBUS-THRIFT-PORT)))
                    (THsHaServer$Args.)
                    (.workerThreads 64)
                    (.protocolFactory (TBinaryProtocol$Factory.))
                    (.processor (Nimbus$Processor. service-handler#)))
        server (THsHaServer. options)
        nimbus-node (zookeeper/create-node zk-handler (str nimbus-path "/nimbus-") (utils/object->bytes (conj nimbus-info (utils/resources-info))) :ephemeral-sequential)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn []
                                                      (timer/cancel-timer heartbeat-timer)
                                                      (timer/cancel-timer workerbeat-timer)
                                                      (.close zk-handler)
                                                      (.stop server))))
    (log/info "Starting Nimbus server")
    (log/info "nimbus zookeeper node: " nimbus-node)
    (loop [childs# (zookeeper/get-children zk-handler nimbus-path false)]
      (let [childs (.toArray childs#)
            _ (doall (Arrays/sort childs))]
        (when-not (= (str nimbus-path "/" (first childs)) nimbus-node)
          (Thread/sleep 5000)
          (recur (zookeeper/get-children zk-handler nimbus-path false)))))
    (log/info "I am active!")
    (timer/schedule-recurring heartbeat-timer 5 heartbeat-interval
                              (fn []
                                (try
                                  (zookeeper/set-data zk-handler nimbus-node (utils/object->bytes (conj nimbus-info (utils/resources-info))))
                                  (catch Exception e
                                    (log/error e "error accurs in nimbus heartbeat timer")
                                    (System/exit -1)))))
    (timer/schedule-recurring workerbeat-timer 5 schedule-check-interval                              
                              (fn []
                                (try
                                  (let [workers (set (zookeeper/get-children zk-handler workerbeat-path false))
                                        tasks (set (zookeeper/get-children zk-handler assignment-path false))
                                        now (utils/current-time-millis)
                                        nodes (clojure.set/difference tasks workers)]
                                    (doseq [node tasks]
                                      (let [command-info (utils/bytes->map (zookeeper/get-data zk-handler (str command-path "/" node) false))
                                            update-time (command-info "time")
                                            command (command-info "command")
                                            id node]
                                        (case command
                                          "kill" (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                                                   (log/error "worker stop timeout..will be force stop...(topology id='" id "')")
                                                   (zookeeper/set-data zk-handler (str assignment-path "/" node) (utils/object->bytes {"supervisor" ""})))
                                          "reload" (let [status (utils/bytes->string (zookeeper/get-data zk-handler (str status-path "/" node) false))]
                                                     (when (= status "reloaded")
                                                       (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "run" "time" (utils/current-time-millis)}))
                                                       (log/info "reload successfully, (topology id='" id "')")))
                                          "pause" (let [status (utils/bytes->string (zookeeper/get-data zk-handler (str status-path "/" node) false))]
                                                    (when (and (not= status "paused") (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT)))
                                                      (log/error "topology pause timeout......(topology id='" id "', status='" status "')")))
                                          "default")))
                                    (doseq [node nodes]
                                      (let [command-info (utils/bytes->map (zookeeper/get-data zk-handler (str command-path "/" node) false))
                                            update-time (command-info "time")
                                            command (command-info "command")
                                            task-info (utils/bytes->map (zookeeper/get-data zk-handler (str assignment-path "/" node) false))
                                            get-execute-supervisor (fn [] (get task-info "supervisor"))
                                            jar (get task-info "jar")
                                            klass (get task-info "class")
                                            group (get task-info "group" "default")
                                            type (get task-info "type" "memory")
                                            id node]
                                        (case command
                                          "init" (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                                                   (log/error "schedule timeout..will be re-scheduled...(topology id='" id "')")
                                                   (assign zk-handler id jar klass group type floor-score :last-supervisor (get-execute-supervisor)))
                                          "kill" (do (clear-topology zk-handler node)
                                                     (log/info "topology stop successfully...(topology id='" id "', jar='" jar "', class='" klass "')"))
                                          (do (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                                                (log/error "topology heartbeat miss..will be re-scheduled..(topology id='" id "')")
                                                (assign zk-handler id jar klass group type floor-score :last-supervisor (get-execute-supervisor)))))
                                        )))
                                  (catch Exception e
                                    (log/error e "error accurs in nimbus scheduling and checking..")
                                    (System/exit -1)))))
    
    (.serve server)))

(defn -main [ & args ]
  (try
    (launch-server! (config/read-magpie-config))
    (catch Exception e
      (System/exit -1))))
