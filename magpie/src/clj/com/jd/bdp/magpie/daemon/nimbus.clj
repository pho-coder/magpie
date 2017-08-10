(ns com.jd.bdp.magpie.daemon.nimbus
  (:gen-class)
  (:require [com.jd.bdp.magpie.util.zookeeper :as zookeeper]
            [com.jd.bdp.magpie.util.config :as config]
            [com.jd.bdp.magpie.util.timer :as timer]
            [com.jd.bdp.magpie.util.utils :as utils]
            [com.jd.bdp.magpie.util.mutils :as mutils]
            [clojure.tools.logging :as log]
            [metrics.reporters.jmx :as jmx]
            [mount.core :as mount]
            [clojure.data.json :as json])
  (:use [com.jd.bdp.magpie.bootstrap])
  (:import [com.jd.bdp.magpie.generated Nimbus Nimbus$Iface Nimbus$Processor]
           [java.util Arrays]
           [org.apache.thrift.server THsHaServer THsHaServer$Args]
           [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory]
           [org.apache.thrift TException]
           [java.lang.ProcessBuilder$Redirect]
           [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket]))

(def tasks-health-info {:assigned-num (atom 0)
                        :no-supervisor-num (atom 0)
                        :lost-supervisor-num (atom 0)
                        :error-supervisor-num (atom 0)})

(mount/defstate supervisors-info :start (hash-map))

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

(defn can-got-new-task? [avg-tasks-num supervisor floor-score]
  (let [MAX-SCORE 100
        tasks-num (.size (:tasks supervisor))
        top-tasks-num (if (< tasks-num 5)
                        5
                        (* avg-tasks-num 1.2))
        memory-score (if-let [score (:memory-score supervisor)]
                       score
                       0)
        cpu-score (if-let [score (:cpu-score supervisor)]
                    score
                    0)
        net-bandwidth-score (if-let [score (:net-bandwidth-score supervisor)]
                              score
                              0)]
    (and (and (>= memory-score floor-score) (<= memory-score MAX-SCORE))
         (and (>= cpu-score floor-score) (<= cpu-score MAX-SCORE))
         (and (>= net-bandwidth-score floor-score) (<= net-bandwidth-score MAX-SCORE))
         (< tasks-num top-tasks-num))))

(defn get-best-new [group type floor-score]
  (let [supervisor-infos (filter #(if (nil? %) false true) (map (fn [supervisor] (if (= (:group (second supervisor)) group)
                                                                                   (second supervisor)
                                                                                   nil)) supervisors-info))
        total-tasks-num (reduce #(+ %1 (.size (:tasks %2))) 0 supervisor-infos)
        avg-tasks-num (if (empty? supervisor-infos)
                        0
                        (/ total-tasks-num (.size supervisor-infos)))]
    (if (empty? supervisor-infos)
      (do (log/error "no supervisor in group" group "is running, this task will not be run, please check...")
          nil)
      (let [supervisors-good (filter #(can-got-new-task? avg-tasks-num % floor-score) supervisor-infos)]
        (if (empty? supervisors-good)
          (do (log/warn "no supervisor has enough resource in group" group)
              nil)
          (:id (reduce (fn [one two]
                         (let [score-type (case type
                                            "memory" :memory-score
                                            "cpu" :cpu-score
                                            "network" :net-bandwidth-score
                                            :memory-score)]
                           (if (>= (score-type one) (score-type two))
                             one
                             two))) supervisors-good)))))))

(defn assign [zk-handler id jar klass group type floor-score & {:keys [last-supervisor]}]
  (let [supervisor-path "/supervisors"
        assignment-path "/assignments"
        command-path "/commands"
        yourtasks-path "/yourtasks"
        workerbeats-path "/workerbeats"
        node id
        task-path (str assignment-path "/" node)]
    (if (zookeeper/exists-node? zk-handler (str workerbeats-path "/" node) false)
      (log/warn node "heartbeat exists! NOT assign it!")
      (if-let [best-supervisor (get-best-new group type floor-score) ;(get-best zk-handler supervisor-path group type floor-score)
               ]
        (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "run" "time" (utils/current-time-millis)}))
            (zookeeper/set-data zk-handler task-path (utils/object->bytes {"start-time" (utils/current-time-millis) "jar" jar "class" klass "id" id "group" group "type" type "supervisor" best-supervisor "last-supervisor" last-supervisor}))
            (let [yourtask-path (str yourtasks-path "/" best-supervisor "/" node)]
              (if (zookeeper/exists-node? zk-handler yourtask-path false)
                (zookeeper/set-data zk-handler yourtask-path (utils/object->bytes {"assign-time" (utils/current-time-millis)}))
                (zookeeper/create-node zk-handler yourtask-path (utils/object->bytes {"assign-time" (utils/current-time-millis)}))))
            (mount/start-with {#'supervisors-info (update-in supervisors-info
                                                             [(keyword best-supervisor) :tasks]
                                                             conj node)})
            (log/info "submit task successfully, (topology id='" id "', supervisor id='" best-supervisor "')"))
        (do (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "wait" "time" (utils/current-time-millis)}))
            (log/warn "resource not enough, this task will be waiting. (topology id='" id "')"))))))

(defn clear-topology [zk-handler node]
  (let [assignment-path "/assignments"
        command-path "/commands"
        status-path "/status"
        yourtasks-path "/yourtasks"]
    (try
      (let [zk-data (zookeeper/get-data zk-handler
                                        (str assignment-path "/" node)
                                        false)
            supervisor (get (utils/bytes->map zk-data)
                            "supervisor" nil)]
        (when-not (or (nil? supervisor) (= supervisor ""))
          (zookeeper/delete-node zk-handler
                                 (str yourtasks-path
                                      "/"
                                      supervisor
                                      "/"
                                      node))
          (mount/start-with {#'supervisors-info (update-in supervisors-info
                                                           [(keyword supervisor) :tasks]
                                                           disj node)})))
      (catch Exception e
        (log/error (.toString e))))
    (try
      (zookeeper/delete-node zk-handler (str command-path "/" node))
      (catch Exception e
        (log/error (.toString e))))
    (try
      (zookeeper/delete-node zk-handler (str status-path "/" node))
      (catch Exception e
        (log/error (.toString e))))
    (try
      (zookeeper/delete-node zk-handler (str assignment-path "/" node))
      (catch Exception e
        (log/error (.toString e))))
    (log/info "clear task" node "zk nodes successfully!")))

(defn submit-task
  "returncode -1 : unkonwn error
               1 : task id exists and is running
               2 : task id exists but is not running
               0 : task id not exists submit success"
  [zk-handler id jar klass floor-score group type  assignment-path status-path command-path]
  (let [result (atom (utils/object->jsonstring {"success" false
                                                "info" "submit failure!"
                                                "returncode" -1}))]
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
                        (reset! result (utils/object->jsonstring {"info" (str "This task has already been running! Will be reloaded! (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")
                                                                  "success" true
                                                                  "returncode" 1})))
                    (reset! result (utils/object->jsonstring {"info" (str "This task has already been running! current command='" command "' (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")
                                                              "success" true
                                                              "returncode" 2})))
                  (log/warn @result))
                (do (zookeeper/create-node zk-handler (str assignment-path "/" node) (utils/object->bytes {"start-time" (utils/current-time-millis) "jar" jar "class" klass "id" id "group" group "type" type}))
                    (zookeeper/create-node zk-handler (str command-path "/" node) (utils/object->bytes {"command" "init" "time" (utils/current-time-millis)}))
                    (zookeeper/create-node zk-handler (str status-path "/" node) (utils/string->bytes "initing"))
                    (assign zk-handler id jar klass group type floor-score)
                    (reset! result (utils/object->jsonstring {"info" (str "This task will be submit soon! (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")
                                                              "success" true
                                                              "returncode" 0})))))
            (catch Throwable e
              (reset! result (utils/object->jsonstring {"info" (str e "Task submission exception. (job id='" id "', jar='" jar "', class='" klass "', group='" group "', type=" type "')")
                                                        "success" false
                                                        "returncode" -1}))
              (log/error @result)))
          @result))

(defn operate-task
  "returncode -1 : unknown error
               1 : command is unsupported
               2 : task id not exists
               0 : task id exists and command submit success"
  [zk-handler id command assignment-path status-path command-path]
  (let [commands (hash-set "kill" "pause" "active" "reload")]
    (if (not (contains? commands command))
      (utils/object->jsonstring {"info" (str command " command is unsupported!")
                                 "success" false
                                 "returncode" 1})
      (let [result (atom (utils/object->jsonstring {"info" (str command " failure!")
                                                    "success" false
                                                    "returncode" -1}))]
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
                  (reset! result (utils/object->jsonstring {"info" (str command " command submitted successfully!, (task id='" id "')")
                                                            "success" true
                                                            "returncode" 0}))
                  (log/info @result))
              (do (reset! result (utils/object->jsonstring {"info" (str command "command submitted error! task is not running, (task id='" id "')")
                                                            "success" false
                                                            "returncode" 2}))
                  (log/error @result))))
          (catch Throwable e
            (reset! result (utils/object->jsonstring {"info" (str e "Task " command " exception. (task id='" id "')")
                                                      "success" false
                                                      "returncode" -1}))
            (log/error @result)))
        @result))))

(defn service-handler [conf zk-handler reg]
  (let [assignment-path "/assignments"
        status-path "/status"
        command-path "/commands"
        floor-score (conf MAGPIE-FLOOR-SCORE 20)
        submit-task-timer (.get (.getTimers reg) (clojure.string/join "." MAGPIE-NIMBUS-SUBMIT-TASK-TIMER-METRICS-NAME))
        operate-task-timer (.get (.getTimers reg) (clojure.string/join "." MAGPIE-NIMBUS-OPERATE-TASK-TIMER-METRICS-NAME))]
    
    (reify Nimbus$Iface
      (^String submitTopology
        [this ^String id ^String jar ^String klass]
        (mutils/time-timer submit-task-timer (submit-task zk-handler id jar klass floor-score "default" "memory" assignment-path status-path command-path)))
      
      (^String killTopology
        [this ^String id]
        (mutils/time-timer operate-task-timer (operate-task zk-handler id "kill" assignment-path status-path command-path)))
      
      (^String pauseTopology
        [this ^String id]
        (mutils/time-timer operate-task-timer (operate-task zk-handler id "pause" assignment-path status-path command-path)))

      (^String activeTopology
        [this ^String id]
        (mutils/time-timer operate-task-timer (operate-task zk-handler id "active" assignment-path status-path command-path)))
      
      (^String reloadTopology
        [this ^String id]
        (mutils/time-timer operate-task-timer (operate-task zk-handler id "reload" assignment-path status-path command-path)))
      
      (^String submitTask
        [this ^String id ^String jar ^String klass ^String group ^String type]
        (mutils/time-timer submit-task-timer (submit-task zk-handler id jar klass floor-score group type assignment-path status-path command-path)))

      (^String operateTask
        [this ^String id ^String command]
        (mutils/time-timer operate-task-timer (operate-task zk-handler id command assignment-path status-path command-path))))))

(defn process-all-tasks
  "process all tasks on zookeeper /assignments."
  [conf zk-handler]
  (let [assignment-path "/assignments"
        status-path "/status"
        command-path "/commands"
        yourtasks-path "/yourtasks"
        tasks (set (zookeeper/get-children zk-handler assignment-path false))
        now (utils/current-time-millis)]
    (doseq [node tasks]
      (let [command-info (utils/bytes->map (zookeeper/get-data zk-handler (str command-path "/" node) false))
            update-time (command-info "time")
            command (command-info "command")
            id node]
        (case command
          "kill" (when (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT))
                   (log/error "worker stop timeout..will be force stop...(topology id='" id "')")
                   (try
                     (let [zk-data (zookeeper/get-data zk-handler
                                                       (str assignment-path "/" node)
                                                       false)
                           supervisor (get (utils/bytes->map zk-data)
                                           "supervisor" nil)]
                       (if (or (nil? supervisor) (= supervisor ""))
                         (zookeeper/set-data zk-handler (str assignment-path "/" node) (utils/object->bytes {"supervisor" ""}))
                         (do (zookeeper/delete-node zk-handler
                                                    (str yourtasks-path
                                                         "/"
                                                         supervisor
                                                         "/"
                                                         node))
                             (zookeeper/set-data zk-handler (str assignment-path "/" node) (utils/object->bytes {"supervisor" ""}))
                             (mount/start-with {#'supervisors-info (update-in supervisors-info
                                                                              [(keyword supervisor) :tasks]
                                                                              disj node)}))))
                     (catch Exception e
                       (log/error (.toString e)))))
          "reload" (let [status (utils/bytes->string (zookeeper/get-data zk-handler (str status-path "/" node) false))]
                     (when (= status "reloaded")
                       (zookeeper/set-data zk-handler (str command-path "/" node) (utils/object->bytes {"command" "run" "time" (utils/current-time-millis)}))
                       (log/info "reload successfully, (topology id='" id "')")))
          "pause" (let [status (utils/bytes->string (zookeeper/get-data zk-handler (str status-path "/" node) false))]
                    (when (and (not= status "paused") (> (- now update-time) (conf MAGPIE-SCHEDULE-TIMEOUT)))
                      (log/error "topology pause timeout......(topology id='" id "', status='" status "')")))
          "default")))))

(defn process-dead-tasks
  "process all no heartbeat tasks"
  [conf zk-handler]
  (let [assignment-path "/assignments"
        workerbeat-path "/workerbeats"
        command-path "/commands"
        floor-score (conf MAGPIE-FLOOR-SCORE 20)
        workers (set (zookeeper/get-children zk-handler workerbeat-path false))
        tasks (set (zookeeper/get-children zk-handler assignment-path false))
        now (utils/current-time-millis)
        nodes (clojure.set/difference tasks workers)]
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
                (assign zk-handler id jar klass group type floor-score :last-supervisor (get-execute-supervisor)))))))))

(defn tasks-health-check
  "check whether tasks assignment ok"
  [zk-handler]
  (let [assignment-path "/assignments"
        supervisor-path "/supervisors"
        yourtasks-path "/yourtasks"
        assigned-num (atom 0)
        no-supervisor-num (atom 0)
        lost-supervisor-num (atom 0)
        error-supervisor-num (atom 0)
        tasks (set (zookeeper/get-children zk-handler assignment-path false))
        supervisors (set (zookeeper/get-children zk-handler supervisor-path false))
        yourtasks-supervisors (set (zookeeper/get-children zk-handler yourtasks-path false))
        lost-supervisors (clojure.set/difference supervisors yourtasks-supervisors)
        more-supervisors (clojure.set/difference yourtasks-supervisors supervisors)]
    (doseq [task tasks]
      (let [zk-data (zookeeper/get-data zk-handler
                                        (str assignment-path "/" task)
                                        false)
            task-info (utils/bytes->map zk-data)
            supervisor (get task-info
                            "supervisor" nil)]
        (if (or (nil? supervisor) (= supervisor ""))
          (do (reset! no-supervisor-num (inc @no-supervisor-num))
              (log/warn "the task has no supervisor:" task-info))
          (if-not (contains? supervisors supervisor)
            (do (reset! error-supervisor-num (inc @error-supervisor-num))
                (log/error "the task's supervisor not exists:" task-info))
            (if (zookeeper/exists-node? zk-handler
                                        (str yourtasks-path "/" supervisor "/" task)
                                        false)
              (reset! assigned-num (inc @assigned-num))
              (do (zookeeper/create-node zk-handler (str yourtasks-path "/" supervisor "/" task) (utils/object->bytes {"assign-time" (utils/current-time-millis)}))
                  (reset! lost-supervisor-num (inc @lost-supervisor-num))
                  (log/warn "the task lost supervisor:" task-info)))))))
    (reset! (:assigned-num tasks-health-info) @assigned-num)
    (reset! (:no-supervisor-num tasks-health-info) @no-supervisor-num)
    (reset! (:lost-supervisor-num tasks-health-info) @lost-supervisor-num)
    (reset! (:error-supervisor-num tasks-health-info) @error-supervisor-num)
    (when (or (> @no-supervisor-num 0) (> @lost-supervisor-num 0) (> @error-supervisor-num 0))
      (log/info "assigned num:" @assigned-num)
      (when (> @no-supervisor-num 0)
        (log/warn "no supervisor num:" @no-supervisor-num))
      (when (> @lost-supervisor-num 0)
        (log/warn "lost supervisor num:" @lost-supervisor-num))
      (when (> @error-supervisor-num 0)
        (log/error "error supervisor num:" @error-supervisor-num)))))

(defn supervisors-health-check
  "check supervisors pressure"
  [zk-handler]
  (let [supervisors-path "/supervisors"
        yourtasks-path "/yourtasks"
        supervisors (set (zookeeper/get-children zk-handler supervisors-path false))]
    (doseq [supervisor supervisors]
      (if-not (zookeeper/exists-node? zk-handler
                                      (str yourtasks-path "/" supervisor)
                                      false)
        (log/error "supervisor:" supervisor "hasn't yourtasks node!")
        (if-let [supervisor-info-bytes (zookeeper/get-data zk-handler (str supervisors-path "/" supervisor) false)]
          (let [yourtasks (set (zookeeper/get-children zk-handler (str yourtasks-path "/" supervisor) false))
                supervisor-info (assoc (json/read-str (utils/bytes->string supervisor-info-bytes)
                                                      :key-fn keyword)
                                       :tasks yourtasks)]
            (mount/start-with {#'supervisors-info (assoc supervisors-info
                                                         (keyword supervisor)
                                                         supervisor-info)}))
          (log/warn "NO" supervisor "in" supervisors-path)))))
  (log/info (reduce #(let [one (second %2)
                           group (:group one)
                           id (:id one)
                           tasks-num (.size (:tasks one))]
                       (if-not (contains? %1 group)
                         (assoc %1 group {:max-id id :max-num tasks-num
                                          :min-id id :min-num tasks-num})
                         (let [min-id (:min-id (get %1 group))
                               min-num (:min-num (get %1 group))
                               max-id (:max-id (get %1 group))
                               max-num (:max-num (get %1 group))
                               min-data (if (< tasks-num min-num)
                                          {:min-id id :min-num tasks-num}
                                          {:min-id min-id :min-num min-num})
                               max-data (if (> tasks-num max-num)
                                          {:max-id id :max-num tasks-num}
                                          {:max-id max-id :max-num max-num})]
                           (assoc %1 group (conj min-data max-data)))))
                    {} supervisors-info)))

(defn launch-server! [conf]
  (let [zk-handler (zookeeper/mk-client conf (conf MAGPIE-ZOOKEEPER-SERVERS) (conf MAGPIE-ZOOKEEPER-PORT) :root (conf MAGPIE-ZOOKEEPER-ROOT))
        nimbus-path "/nimbus"
        assignment-path "/assignments"
        supervisor-path "/supervisors"
        workerbeat-path "/workerbeats"
        status-path "/status"
        command-path "/commands"
        yourtasks-path "/yourtasks"
        heartbeat-interval (/ (conf MAGPIE-HEARTBEAT-INTERVAL 2000) 1000)
        healthcheck-interval (/ (conf MAGPIE-HEALTHCHECK-INTERVAL 5000) 1000)
        schedule-interval (/ (conf MAGPIE-SCHEDULE-INTERVAL 5000) 1000)
        _ (config/init-zookeeper zk-handler)
        nimbus-info {"ip" (utils/ip) "hostname" (utils/hostname) "username" (utils/username) "port" (int (conf NIMBUS-THRIFT-PORT))}
        heartbeat-timer (timer/mk-timer)
        healthcheck-timer (timer/mk-timer)
        workerbeat-timer (timer/mk-timer)
        reg (mutils/get-registry)
        heartbeat-counter (mutils/get-counter reg MAGPIE-NIMBUS-HEARTBEAT-COUNTER-METRICS-NAME)
        tasks-health-check-timer (mutils/get-timer reg MAGPIE-NIMBUS-TASKS-HEALTH-CHECK-TIMER-METRICS-NAME)
        supervisors-health-check-timer (mutils/get-timer reg MAGPIE-NIMBUS-SUPERVISORS-HEALTH-CHECK-TIMER-METRICS-NAME)
        submit-task-timer (mutils/get-timer reg MAGPIE-NIMBUS-SUBMIT-TASK-TIMER-METRICS-NAME)
        operate-task-timer (mutils/get-timer reg MAGPIE-NIMBUS-OPERATE-TASK-TIMER-METRICS-NAME)
        process-all-tasks-timer (mutils/get-timer reg MAGPIE-NIMBUS-PROCESS-ALL-TASKS-TIMER-METRICS-NAME)
        process-dead-tasks-timer (mutils/get-timer reg MAGPIE-NIMBUS-PROCESS-DEAD-TASKS-TIMER-METRICS-NAME)
        tasks-assigned-num-gauge (mutils/get-gauge reg MAGPIE-NIMBUS-TASKS-ASSIGNED-NUM-GAUGE-METRICS-NAME (fn []
                                                                                                             @(:assigned-num tasks-health-info)))
        tasks-no-supervisor-num-gauge (mutils/get-gauge reg MAGPIE-NIMBUS-TASKS-NO-SUPERVISOR-NUM-GAUGE-METRICS-NAME (fn []
                                                                                                                       @(:no-supervisor-num tasks-health-info)))
        tasks-lost-supervisor-num-gauge (mutils/get-gauge reg MAGPIE-NIMBUS-TASKS-LOST-SUPERVISOR-NUM-GAUGE-METRICS-NAME (fn []
                                                                                                                           @(:lost-supervisor-num tasks-health-info)))
        tasks-error-supervisor-num-gauge (mutils/get-gauge reg MAGPIE-NIMBUS-TASKS-ERROR-SUPERVISOR-NUM-GAUGE-METRICS-NAME (fn []
                                                                                                                             @(:error-supervisor-num tasks-health-info)))
        jmx-report (jmx/reporter reg {})
        service-handler# (service-handler conf zk-handler reg)
        floor-score (conf MAGPIE-FLOOR-SCORE 20)
        options (-> (TNonblockingServerSocket. (int (conf NIMBUS-THRIFT-PORT)))
                    (THsHaServer$Args.)
                    (.workerThreads 64)
                    (.protocolFactory (TBinaryProtocol$Factory.))
                    (.processor (Nimbus$Processor. service-handler#)))
        server (THsHaServer. options)
        nimbus-node (zookeeper/create-node zk-handler (str nimbus-path "/nimbus-") (utils/object->bytes (conj nimbus-info (utils/resources-info))) :ephemeral-sequential)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn []
                                                      (log/info "cancel heartbeat-timer")
                                                      (timer/cancel-timer heartbeat-timer)
                                                      (log/info "cancel workerbeat-timer")
                                                      (timer/cancel-timer workerbeat-timer)
                                                      (log/info "close zk handler")
                                                      (.close zk-handler)
                                                      (log/info "stop jmx report")
                                                      (jmx/stop jmx-report)
                                                      (log/info "stop thrift server")
                                                      (.stop server)
                                                      (log/info "nimbus exits successfully!"))))
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
                                  (mutils/inc-counter heartbeat-counter)
                                  (when (= (mod (mutils/read-counter heartbeat-counter) 300) 0)
                                    (log/info "heartbeat counts:" (mutils/read-counter heartbeat-counter)))
                                  (catch Exception e
                                    (log/error e "error accurs in nimbus heartbeat timer")
                                    (System/exit -1)))))
    (timer/schedule-recurring healthcheck-timer 0 healthcheck-interval
                              (fn []
                                (try
                                  (mutils/time-timer tasks-health-check-timer (tasks-health-check zk-handler))
                                  (mutils/time-timer supervisors-health-check-timer (supervisors-health-check zk-handler))
                                  (log/debug supervisors-info)
                                  (catch Exception e
                                    (log/error e "error accurs in nimbus health checking.")
                                    (System/exit -1)))))
    (timer/schedule-recurring workerbeat-timer 5 schedule-interval
                              (fn []
                                (try
                                  (mutils/time-timer process-all-tasks-timer (process-all-tasks conf zk-handler))
                                  (mutils/time-timer process-dead-tasks-timer (process-dead-tasks conf zk-handler))
                                  (catch Exception e
                                    (log/error e "error accurs in nimbus scheduling.")
                                    (System/exit -1)))))
    (jmx/start jmx-report)
    (.serve server)))

(defn -main [ & args ]
  (try
    (doseq [component (-> args
                          mount/start-with-args
                          :started)]
      (log/info component "started"))
    (launch-server! (config/read-magpie-config))
    (catch Exception e
      (log/error e)
      (System/exit -1))))
