(ns com.jd.magpie.daemon.supervisor
  (:gen-class)
  (:require [com.jd.magpie.util.zookeeper :as zookeeper]
            [com.jd.magpie.util.config :as config]
            [com.jd.magpie.util.timer :as timer]
            [com.jd.magpie.util.utils :as utils]
            [com.jd.magpie.util.cgutils :as cgutils]
            [clojure.tools.logging :as log])
  (:use [com.jd.magpie.bootstrap]))

(def network-usage (atom {:rx-bytes nil
                          :tx-bytes nil
                          :rx-net-bandwidth nil
                          :tx-net-bandwidth nil
                          :net-bandwidth-score nil
                          :time-millis nil}))

(defn get-my-jobs [zk-handler supervisor-id]
  (log/info "start get my jobs!")
  (let [assignment-path "/assignments"
        supervisor-path "/supervisors"
        yourtasks-path "/yourtasks"
        tasks (zookeeper/get-children zk-handler (str yourtasks-path "/" supervisor-id) false)]
    (if (empty? tasks)
      []
      (filter #(not (nil? %)) (map (fn [task-id]
                                     (let [zk-data (zookeeper/get-data zk-handler (str assignment-path "/" task-id) false)]
                                       (if (nil? zk-data)
                                         (do (try
                                               (zookeeper/delete-node zk-handler (str yourtasks-path "/" supervisor-id "/" task-id))
                                               (log/info "NOT in assignments! delete my task:" task-id)
                                               (catch Exception e
                                                 (log/error (.toString e))))

                                             nil)
                                         (let [zk-data-map (utils/bytes->map zk-data)
                                               assignment-supervisor (get zk-data-map "supervisor")]
                                           (if (= supervisor-id assignment-supervisor)
                                             zk-data-map
                                             (do (log/error task-id (str "found in /yourtasks/" supervisor-id) "but /assignments/" task-id "show in" assignment-supervisor)
                                                 (try
                                                   (zookeeper/delete-node zk-handler (str yourtasks-path "/" supervisor-id "/" task-id))
                                                   (log/info "NOT my task in assignments! delete my task:" task-id)
                                                   (catch Exception e
                                                     (log/error (.toString e))))
                                                 nil)))))) tasks)))))

(defn launch-job [conf job-info get-resources-url-func]
  (let [jars-dir (conf MAGPIE-JARS-DIR)
        pids-dir (conf MAGPIE-PIDS-DIR)
        timeout (conf MAGPIE-SCHEDULE-LAUNCHWORKER-TIMEOUT 10000)
        _ (log/info "supervisor timeout:" timeout)
        _ (log/info (type timeout))
        servers (conf MAGPIE-ZOOKEEPER-SERVERS)
        zk-port (conf MAGPIE-ZOOKEEPER-PORT)
        zk-root (conf MAGPIE-ZOOKEEPER-ROOT)
        zk-servers (clojure.string/join "," (map #(str % ":" zk-port) servers))
        cgroup-enable (conf MAGPIE-CGROUP-ENABLE false)
        _ (log/info "cgroup-enable" cgroup-enable)
        _ (log/info (type cgroup-enable))
        cgname (conf MAGPIE-CGROUP-NAME "magpie")
        _ (log/info "cgname" cgname)
        _ (log/info (type cgname))
        cgcpu-cores (conf MAGPIE-CGROUP-CPU-CORES 1)
        _ (log/info "cgcpu-cores" cgcpu-cores)
        _ (log/info (type cgcpu-cores))
        cgmemory (conf MAGPIE-CGROUP-MEMORY 1024)
        _ (log/info "cgmemory" cgmemory)
        _ (log/info (type cgmemory))
        cgmemsw (conf MAGPIE-CGROUP-MEMSW 512)
        _ (log/info "cgmemsw" cgmemsw)
        _ (log/info (type cgmemsw))]
    (let [jar (job-info "jar")
          klass (job-info "class")
          id (job-info "id")
          cgchild-name id
          _ (log/info "cgchild-name" cgchild-name)
          node id
          get-pid-dir (fn [node] (utils/normalize-path (str pids-dir "/" node)))
          get-pid (fn [node] (first (utils/read-dir-contents (get-pid-dir node))))
          jar-file (utils/normalize-path (utils/find-jar jars-dir jar get-resources-url-func))
          childopts (conf MAGPIE-WORKER-CHILDOPTS "-Xmx512m")
          classpath (utils/add-to-classpath (utils/get-configuration) [jar-file])
          command (clojure.string/join " " ["java"
                                            "-server"
                                            childopts
                                            (str "-Djava.library.path=" (conf JAVA-LIBRARY-PATH))
                                            (str "-Dlog4j.configuration=worker.log4j.properties")
                                            (str "-Dpids.dir=" (get-pid-dir node))
                                            (str "-Dlogs.dir=" (conf MAGPIE-LOGS-DIR))
                                            (str "-Dlogfile.name=worker-" node)
                                            (str "-Djob.node=" node)
                                            (str "-Dzookeeper.servers=" zk-servers)
                                            (str "-Dzookeeper.root=" zk-root)
                                            (str "-Djob.id=" id)
                                            "-cp" classpath klass])
          process (if cgroup-enable
                    (cgutils/cgone cgname cgchild-name cgcpu-cores cgmemory cgmemsw command)
                    (utils/launch-process command))
          _ (log/info (type process))
          time (utils/current-time-millis)]
      (loop [pid (get-pid node)]
        (if (empty? pid)
          (if (> (- (utils/current-time-millis) time) timeout)
            (do (log/error "start job timeout...(topology id='" id "', jar='" jar "', class='" klass "')")
                (if (= Process (type process))
                  (.destroy process))
                (utils/rmr (get-pid-dir node))
                (when cgroup-enable
                  (cgutils/cgdelete cgname cgchild-name)))
            (recur (get-pid node)))
          (log/info "launch job successfully...(topology id='" id "', jar='" jar "', class='" klass "')")))
      (log/info "command: " command))))

(defn process-job [conf zk-handler supervisor-id]
  (log/info "process job starts!")
  (let [my-job-infos (get-my-jobs zk-handler supervisor-id)
        pids-dir (conf MAGPIE-PIDS-DIR)
        cgroup-enable (conf MAGPIE-CGROUP-ENABLE false)
        cgname (conf MAGPIE-CGROUP-NAME "magpie")
        command-path "/commands"
        webservice-path "/webservice"
        get-resources-url-func (fn [] (utils/bytes->string (zookeeper/get-data zk-handler (str webservice-path "/resource") false)))
        my-jobs (set (map #(get % "id") my-job-infos))
        current-jobs (set (utils/read-dir-contents pids-dir))
        waste-jobs (clojure.set/difference current-jobs my-jobs)
        get-pid-dir (fn [node] (utils/normalize-path (str pids-dir "/" node)))
        get-pid (fn [node] (first (utils/read-dir-contents (get-pid-dir node))))]
    (doseq [job waste-jobs]
      (log/info "task:" job "stopped! Clearing the job info..")
      (when-let [pid (get-pid job)]
        (if (utils/process-running? pid)
          (utils/ensure-process-killed! pid)))
      (utils/rmr (get-pid-dir job))
      (when cgroup-enable
        (cgutils/cgdelete cgname job)))
    (doseq [job-info my-job-infos]
      (let [node (job-info "id")
            cgchild-name node
            pid-dir (get-pid-dir node)
            zk-data (zookeeper/get-data zk-handler (str command-path "/" node) false)]
        (if-not (utils/exists-file? pid-dir)
          (if zk-data
            (let [command ((utils/bytes->map zk-data) "command")]
              (when (and command (not= command "kill"))
                (launch-job conf job-info get-resources-url-func))))
          (when-not (utils/process-running? (get-pid node))
            (if zk-data
              (let [command ((utils/bytes->map zk-data) "command")]
                (when (and command (not= command "kill"))
                  (log/error "task" node "process is not running well...."))))
            (utils/ensure-process-killed! (get-pid node))
            (utils/rmr (get-pid-dir node))
            (when cgroup-enable
              (cgutils/cgdelete cgname cgchild-name)))))))
  (log/info "process job ends!"))

(defn get-net-bandwidth [calculate-interval max-net-bandwidth]
  (let [mill (* 1024 1024)
        time-millis-now (utils/current-time-millis)
        net-usage (utils/network-usage)
        rx-bytes (:rx-bytes net-usage)
        tx-bytes (:tx-bytes net-usage)
        network-usage-now @network-usage]
    (if (= (:time-millis network-usage-now) nil)
      (do (reset! network-usage {:time-millis time-millis-now
                                 :rx-bytes rx-bytes
                                 :tx-bytes tx-bytes
                                 :rx-net-bandwidth nil
                                 :tx-net-bandwidth nil
                                 :net-bandwidth-score nil})
          {"rx-net-bandwidth" (:rx-net-bandwidth network-usage-now)
           "tx-net-bandwidth" (:tx-net-bandwidth network-usage-now)
           "net-bandwidth-score" (:net-bandwidth-score network-usage-now)})
      (if (not (> (- time-millis-now
                     (:time-millis network-usage-now))
                  calculate-interval))
        {"rx-net-bandwidth" (:rx-net-bandwidth network-usage-now)
         "tx-net-bandwidth" (:tx-net-bandwidth network-usage-now)
         "net-bandwidth-score" (:net-bandwidth-score network-usage-now)}
        (let [rx-net-bandwidth (quot (/ (- rx-bytes
                                           (:rx-bytes network-usage-now))
                                        mill)
                                     (/ (- time-millis-now
                                           (:time-millis network-usage-now))
                                        1000))
              tx-net-bandwidth (quot (/ (- tx-bytes
                                           (:tx-bytes network-usage-now))
                                        mill)
                                     (/ (- time-millis-now
                                           (:time-millis network-usage-now))
                                        1000))
              net-bandwidth-score (quot (* 100 (- max-net-bandwidth (if (> rx-net-bandwidth tx-net-bandwidth)
                                                                      rx-net-bandwidth
                                                                      tx-net-bandwidth)))
                                        max-net-bandwidth)]
          (reset! network-usage {:time-millis time-millis-now
                                 :rx-bytes rx-bytes
                                 :tx-bytes tx-bytes
                                 :rx-net-bandwidth rx-net-bandwidth
                                 :tx-net-bandwidth tx-net-bandwidth
                                 :net-bandwidth-score net-bandwidth-score})
          {"rx-net-bandwidth" rx-net-bandwidth
           "tx-net-bandwidth" tx-net-bandwidth
           "net-bandwidth-score" net-bandwidth-score})))))

(defn check-env [conf]
  (log/info "check env!")
  (let [jars-dir (conf MAGPIE-JARS-DIR)
        pids-dir (conf MAGPIE-PIDS-DIR)
        logs-dir (conf MAGPIE-LOGS-DIR)]
    (if (or (nil? jars-dir) (nil? pids-dir) (nil? logs-dir))
      (do (log/error "magpie.jars.dir, magpie.pids.dir or magpie.logs.dir is null! check magpie.yaml!")
          (System/exit -1)))
    (if (or (not (.startsWith jars-dir "/")) (not (.startsWith pids-dir "/")) (not (.startsWith logs-dir "/")))
      (do (log/error "magpie.jars.dir, magpie.pids.dir or magpie.logs.dir must be absolute path! check magpie.yaml!")
          (System/exit -1)))
    (try
      (utils/local-mkdirs jars-dir)
      (utils/local-mkdirs pids-dir)
      (utils/local-mkdirs logs-dir)
      (catch Exception e
        (log/error (.toString e))
        (System/exit -1)))
    (log/info "finish checking env!")))

(defn launch-server! [conf]
  (check-env conf)
  (let [zk-handler (zookeeper/mk-client conf (conf MAGPIE-ZOOKEEPER-SERVERS) (conf MAGPIE-ZOOKEEPER-PORT) :root (conf MAGPIE-ZOOKEEPER-ROOT))
        heartbeat-interval (/ (conf MAGPIE-HEARTBEAT-INTERVAL 2000) 1000)
        schedule-check-interval (/ (conf MAGPIE-SCHEDULE-INTERVAL 5000) 1000)
        net-bandwidth-calculate-interval (conf MAGPIE-NET-BANDWIDTH-CALCULATE-INTERVAL 30000)
        supervisor-path "/supervisors"
        yourtasks-path "/yourtasks"
        hostname (utils/hostname)
        pid (utils/process-pid)
        uuid-file (utils/normalize-path (str (conf MAGPIE-LOGS-DIR) "/.uuid"))
        uuid (let [*uuid* (utils/read-file-contents uuid-file)
                   new-uuid (utils/uuid)]
               (if *uuid*
                 *uuid*
                 (do (utils/write-file-contents uuid-file new-uuid)
                     new-uuid)))
        supervisor-id (str hostname "-" uuid)
        supervisor-node (str supervisor-path "/" supervisor-id)
        supervisor-group (conf MAGPIE-SUPERVISOR-GROUP "default")
        supervisor-max-net-bandwidth (conf MAGPIE-SUPERVISOR-MAX-NET-BANDWIDTH 100)
        supervisor-info {"id" supervisor-id "ip" (utils/ip) "hostname" (utils/hostname) "username" (utils/username) "pid" pid "group" supervisor-group "max-net-bandwidth" supervisor-max-net-bandwidth}
        heartbeat-timer (timer/mk-timer)
        schedule-timer (timer/mk-timer)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn []
                                                      (timer/cancel-timer heartbeat-timer)
                                                      (timer/cancel-timer schedule-timer)
                                                      (.close zk-handler))))
    (log/info "Starting supervisor...")
    (config/init-zookeeper zk-handler)
    (zookeeper/create-node zk-handler supervisor-node (utils/object->bytes (conj supervisor-info (utils/resources-info) (get-net-bandwidth net-bandwidth-calculate-interval supervisor-max-net-bandwidth))) :ephemeral)

    (if-not (zookeeper/exists-node? zk-handler (str yourtasks-path "/" supervisor-id) false)
      (do (zookeeper/mkdirs zk-handler (str yourtasks-path "/" supervisor-id))
          (log/info "create my tasks path!")))

    (timer/schedule-recurring heartbeat-timer 10 heartbeat-interval
                              (fn []
                                (try
                                  (zookeeper/set-data zk-handler supervisor-node (utils/object->bytes (conj supervisor-info (utils/resources-info) (get-net-bandwidth net-bandwidth-calculate-interval supervisor-max-net-bandwidth))))
                                  (catch Exception e
                                    (log/error e "error accurs in supervisor heartbeat timer..")
                                    (System/exit -1)))))
    (timer/schedule-recurring schedule-timer 10 schedule-check-interval
                              (fn []
                                (try
                                  (process-job conf zk-handler supervisor-id)
                                  (catch Exception e
                                    (log/error e "error accurs in supervisor processing job")
                                    (System/exit -1)))))))

(defn -main [ & args ]
  (try
    (launch-server! (config/read-magpie-config))
    (loop [flag nil]
      (recur (Thread/sleep 100000)))
    (catch Exception e
      (System/exit -1))))
