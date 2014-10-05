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

(defn get-all-supervisors [zk-handler supervisor-path]
  (let [supervisors (zookeeper/get-children zk-handler supervisor-path false)
        supervisor-infos (map #(utils/bytes->map (zookeeper/get-data zk-handler (str supervisor-path "/" %) false)) supervisors)]
    (log/info "supervisor info:" supervisor-infos)
    [supervisors supervisor-infos]))

(defn get-best [zk-handler supervisor-path]
  (let [[supervisors supervisor-infos] (get-all-supervisors zk-handler supervisor-path)]
    (if (empty? supervisors)
      nil
      (if (= (count supervisors) 1)
        [(first supervisors) (get (first supervisor-infos) "free-memory")]
        (reduce (fn [a b] (if (> (second a) (second b))
                           a
                           b))
                (map (fn [s i] [s (get i "free-memory")]) supervisors supervisor-infos))))))

(defn assign [zk-handler id jar klass floor-score & {:keys [last-supervisor]}]
  (let [supervisor-path "/supervisors"
        assignment-path "/assignments"
        command-path "/commands"
        node id
        task-path (str assignment-path "/" node)]
    (if-let [[best-supervisor score] (get-best zk-handler supervisor-path)]
      (if (> score floor-score)
        (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "run" "time" (utils/current-time-millis)}))
            (zookeeper/set-data zk-handler task-path (utils/object->bytes {"start-time" (utils/current-time-millis) "jar" jar "class" klass "id" id "supervisor" best-supervisor "last-supervisor" last-supervisor}))
            (log/info "submit task successfully, (topology id='" id "')"))
        (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "wait" "time" (utils/current-time-millis)}))
            (log/warn "resource not enough, this task will be waiting. (topology id='" id "')")))
      (log/error "no supervisor is running, this task will not be run, please check...(topology id='" id "')"))))

(defn clear-topology [zk-handler node]
  (let [assignment-path "/assignments"
        command-path "/commands"
        status-path "/status"
                                        ;        heartbeat-path "/workerbeats"
        ]
    (try
      (zookeeper/delete-node zk-handler (str assignment-path "/" node))
      (catch Exception e))
                                        ;  (zookeeper/delete-node
                                        ;  zk-handler (str
                                        ;  heartbeat-path "/" node))
    (try
      (zookeeper/delete-node zk-handler (str command-path "/" node))
      (catch Exception e))
    (try
      (zookeeper/delete-node zk-handler (str status-path "/" node))
      (catch Exception e))))

(defn service-handler [conf zk-handler]
  (let [assignment-path "/assignments"
        status-path "/status"
        command-path "/commands"
        floor-score (conf MAGPIE-FLOOR-SCORE 256)]
    (reify Nimbus$Iface
      (^String submitTopology
        [this ^String id ^String jar ^String klass ^String group ^String type]
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
                        (reset! result (str "This task has already been running! Will be reloaded! (topology id='" id "', jar='" jar "', class='" klass "')")))
                    (reset! result (str "This task has already been running! current command='" command "' (topology id='" id "', jar='" jar "', class='" klass "')")))
                  (log/warn @result))
                (do (zookeeper/create-node zk-handler (str assignment-path "/" node) (utils/object->bytes {"start-time" (utils/current-time-millis) "jar" jar "class" klass "id" id}))
                    (zookeeper/create-node zk-handler (str command-path "/" node) (utils/object->bytes {"command" "init" "time" (utils/current-time-millis)}))
                    (zookeeper/create-node zk-handler (str status-path "/" node) (utils/object->bytes {"command" "init" "time" (utils/current-time-millis)}))
                    (assign zk-handler id jar klass floor-score)
                    (reset! result (str "This task will be submit soon! (topology id='" id "', jar='" jar "', class='" klass "')")))))
            (catch Throwable e
              (reset! result (str  e "Topology submission exception. (topology id='" id "', jar='" jar "', class='" klass "')"))
              (log/error @result)))
          @result))
      
      (^String killTopology
        [this ^String id]
        (let [result (atom "killed failure!")]
          (try
            (let [node id
                  task-path (str assignment-path "/" node)
                  running? (zookeeper/exists-node? zk-handler task-path false)]
              (if running?
                (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "kill" "time" (utils/current-time-millis)}))
                    (reset! result (str "killed successfully!, (task id='" id "')"))
                    (log/info @result))
                (do (reset! result (str "topology is not running, (topology id='" id "')"))
                    (log/error @result))))
            (catch Throwable e
              (reset! result (str e "Topology kill exception. (topology id='" id "')"))
              (log/error @result)))
          @result))
      
      (^String pauseTopology
        [this ^String id]
        (let [result (atom "paused failure!")]
          (try
            (let [node id
                  task-path (str assignment-path "/" node)
                  running? (zookeeper/exists-node? zk-handler task-path false)]
              (if running?
                (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "pause" "time" (utils/current-time-millis)}))
                    (reset! result (str "pause successfully, (topology id='" id "')"))
                    (log/info @result))
                (do (reset! result (str "topology is not running, (topology id='" id "')"))
                    (log/error @result))))
            (catch Throwable e
              (reset! result (str e "Topology pause exception. (topology id='" id "')"))
              (log/error @result)))
          @result))

      (^String activeTopology
        [this ^String id]
        (let [result (atom "active failure!")]
          (try
            (let [node id
                  task-path (str assignment-path "/" node)
                  running? (zookeeper/exists-node? zk-handler task-path false)]
              (if running?
                (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "run" "time" (utils/current-time-millis)}))
                    (reset! result (str "active successfully, (topology id='" id "')"))
                    (log/info @result))
                (do (reset! result (str "topology is not running, (topology id='" id "')"))
                    (log/error result))))
            (catch Throwable e
              (reset! result (str e "Topology active exception. (topology id='" id "')"))
              (log/error @result)))
          @result))
      
      (^String reloadTopology
        [this ^String id]
        (let [result (atom "reload failure!")]
          (try
            (let [node id
                  task-path (str assignment-path "/" node)
                  running? (zookeeper/exists-node? zk-handler task-path false)]
              (if running?
                (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "reload" "time" (utils/current-time-millis)}))
                (do (reset! result (str "topology is not running, (topology id='" id "')"))
                    (log/error @result))))
            (catch Throwable e
              (reset! result (str e "Topology reload exception. (topology id='" id "')"))
              (log/error @result)))
          @result))
      )))

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
        floor-score (conf MAGPIE-FLOOR-SCORE 256)
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
                                          "kill" (let [status (utils/bytes->string (zookeeper/get-data zk-handler (str status-path "/" node) false))]
                                                   (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                                                     (log/error "topology stop timeout..will be force stop...(topology id='" id "')")
                                                     (zookeeper/set-data zk-handler (str assignment-path "/" node) (utils/object->bytes {"supervisor" ""}))))
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
                                            id node]
                                        (case command
                                          "init" (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                                                   (log/error "schedule timeout..will be re-scheduled...(topology id='" id "')")
                                                   (assign zk-handler id jar klass floor-score :last-supervisor (get-execute-supervisor)))
                                          "kill" (do (clear-topology zk-handler node)
                                                     (log/info "topology stop successfully...(topology id='" id "', jar='" jar "', class='" klass "')"))
                                          (do (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                                                (log/error "topology heartbeat miss..will be re-scheduled..(topology id='" id "')")
                                                (assign zk-handler id jar klass floor-score :last-supervisor (get-execute-supervisor)))))
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
