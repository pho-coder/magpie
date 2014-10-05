(ns com.jd.magpie.daemon.supervisor
  (:gen-class)
  (:require [com.jd.magpie.util.zookeeper :as zookeeper]
            [com.jd.magpie.util.config :as config]
            [com.jd.magpie.util.timer :as timer]
            [com.jd.magpie.util.utils :as utils]
            [clojure.tools.logging :as log])
  (:use [com.jd.magpie.bootstrap]))

(def NET-BANDWIDTH-INTERVAL-TIME-MILLIS 30000)

(def network-usage (atom {:rx-bytes nil
                          :tx-bytes nil
                          :rx-net-bandwidth nil
                          :tx-net-bandwidth nil
                          :time-millis nil}))

(defn get-my-jobs [zk-handler supervisor-id]
  (let [assignment-path "/assignments"
        assignments (zookeeper/get-children zk-handler assignment-path false)
        assignment-infos (map #(conj (utils/bytes->map (zookeeper/get-data zk-handler (str assignment-path "/" %) false)) {"node" %}) assignments)]
    (if (empty? assignments)
      []
      (filter #(= supervisor-id (get % "supervisor")) assignment-infos))))

(defn launch-job [conf job-info get-resources-url-func]
  (let [jars-dir (conf MAGPIE-JARS-DIR)
        pids-dir (conf MAGPIE-PIDS-DIR)
        timeout (conf MAGPIE-SCHEDULE-TIMEOUT)
        servers (conf MAGPIE-ZOOKEEPER-SERVERS)
        zk-port (conf MAGPIE-ZOOKEEPER-PORT)
        zk-root (conf MAGPIE-ZOOKEEPER-ROOT)
        zk-servers (clojure.string/join "," (map #(str % ":" zk-port) servers))]
    (let [jar (job-info "jar")
          klass (job-info "class")
          id (job-info "id")
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
          process (utils/launch-process command)
          time (utils/current-time-millis)]
      (loop [pid (get-pid node)]
        (if (empty? pid)
          (if (> (- (utils/current-time-millis) time) timeout)
            (do (log/error "start job timeout...(topology id='" id "', jar='" jar "', class='" klass "')")
                (.destroy process)
                (utils/rmr (get-pid-dir node)))
            (recur (get-pid node)))
          (log/info "launch job successfully...(topology id='" id "', jar='" jar "', class='" klass "')")))
      (log/info "command: " command))))

(defn process-job [conf zk-handler supervisor-id]
  (let [my-job-infos (get-my-jobs zk-handler supervisor-id)
        pids-dir (conf MAGPIE-PIDS-DIR)
        command-path "/commands"
        webservice-path "/webservice"
        get-resources-url-func (fn [] (utils/bytes->string (zookeeper/get-data zk-handler (str webservice-path "/resource") false)))
        my-jobs (set (map #(get % "node") my-job-infos))
        current-jobs (set (utils/read-dir-contents pids-dir))
        waste-jobs (clojure.set/difference current-jobs my-jobs)
        get-pid-dir (fn [node] (utils/normalize-path (str pids-dir "/" node)))
        get-pid (fn [node] (first (utils/read-dir-contents (get-pid-dir node))))]
    (doseq [job waste-jobs]
      (log/info "job stopped! Clearing the job info..")
      (when-let [pid (get-pid job)]
        (if (utils/process-running? pid)
          (utils/ensure-process-killed! pid)))
      (utils/rmr (get-pid-dir job)))
    (doseq [job-info my-job-infos]
      (let [node (job-info "node")
            pid-dir (get-pid-dir node)]
        (if-not (utils/exists-file? pid-dir)
          (let [command ((utils/bytes->map (zookeeper/get-data zk-handler (str command-path "/" node) false)) "command")]
            (when (and command (not= command "kill"))
              (launch-job conf job-info get-resources-url-func)))
          (when-not (utils/process-running? (get-pid node))
            (log/error "process is not running well....")
            (utils/ensure-process-killed! (get-pid node))
            (utils/rmr (get-pid-dir node))))))))

(defn get-net-bandwidth []
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
                                 :tx-net-bandwidth nil})
          {"rx-net-bandwidth" (:rx-net-bandwidth network-usage-now)
           "tx-net-bandwidth" (:tx-net-bandwidth network-usage-now)})
      (if (not (> (- time-millis-now
                     (:time-millis network-usage-now))
                  NET-BANDWIDTH-INTERVAL-TIME-MILLIS))
        {"rx-net-bandwidth" (:rx-net-bandwidth network-usage-now)
         "tx-net-bandwidth" (:tx-net-bandwidth network-usage-now)}
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
                                        1000))]
          (reset! network-usage {:time-millis time-millis-now
                                 :rx-bytes rx-bytes
                                 :tx-bytes tx-bytes
                                 :rx-net-bandwidth rx-net-bandwidth
                                 :tx-net-bandwidth tx-net-bandwidth})
          {"rx-net-bandwidth" rx-net-bandwidth
           "tx-net-bandwidth" tx-net-bandwidth})))))
  
(defn launch-server! [conf]
  (let [zk-handler (zookeeper/mk-client conf (conf MAGPIE-ZOOKEEPER-SERVERS) (conf MAGPIE-ZOOKEEPER-PORT) :root (conf MAGPIE-ZOOKEEPER-ROOT))
        heartbeat-interval (/ (conf MAGPIE-HEARTBEAT-INTERVAL 2000) 1000)
        schedule-check-interval (/ (conf MAGPIE-SCHEDULE-INTERVAL 5000) 1000)
        supervisor-path "/supervisors"
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
        supervisor-group (get conf MAGPIE-SUPERVISOR-GROUP "default")
        supervisor-max-net-bandwidth (get conf MAGPIE-SUPERVISOR-MAX-NET-BANDWIDTH 100)
        supervisor-info {"ip" (utils/ip) "hostname" (utils/hostname) "username" (utils/username) "pid" pid "group" supervisor-group "max-net-bandwidth" supervisor-max-net-bandwidth}
        heartbeat-timer (timer/mk-timer)
        schedule-timer (timer/mk-timer)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn []
                                                      (timer/cancel-timer heartbeat-timer)
                                                      (timer/cancel-timer schedule-timer)
                                                      (.close zk-handler))))
    (log/info "Starting supervisor...")
    (config/init-zookeeper zk-handler)
    (zookeeper/create-node zk-handler supervisor-node (utils/object->bytes (conj supervisor-info (utils/resources-info) (get-net-bandwidth))) :ephemeral)
    (timer/schedule-recurring heartbeat-timer 10 heartbeat-interval
                              (fn []
                                (try
                                  (zookeeper/set-data zk-handler supervisor-node (utils/object->bytes (conj supervisor-info (utils/resources-info) (get-net-bandwidth))))
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
