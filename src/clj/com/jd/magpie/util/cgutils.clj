(ns com.jd.magpie.util.cgutils
  (:require [clojure.tools.logging :as log]
            
            [com.jd.magpie.util.utils :as utils])
  (:import [org.apache.commons.exec ExecuteException]))

(defn cgdelete [name child-name]
  (let [subsystems ["cpu" "memory"]]
    (doseq [subsystem subsystems]
      (try
        (utils/rmr (str "/cgroup/" subsystem "/" name "/" child-name))
        (catch Exception e
          (log/error "cgdelete error:" (.toString e))
          {:success false :info (.toString e)})))
    (log/info "cgdelete" name child-name "ok!")
    {:success true}))

(defn cgcreate [name child-name]
  (let [subsystems ["cpu" "memory"]]
    (doseq [subsystem subsystems]
      (try
        (utils/local-mkdirs (str "/cgroup/" subsystem "/" name "/" child-name))
        (catch Exception e
          (log/error "cgcreate error:" (.toString e))
          {:success false :info (.toString e)})))
    (log/info "cgcreate" name child-name "ok!")
    {:success true}))

(defn cgvalues [name child-name cpu-cores memory memsw]
  (let [cpu-file (str "/cgroup/cpu/" name "/" child-name "/cpu.cfs_quota_us")
        memory-file (str "/cgroup/memory/" name "/" child-name "/memory.limit_in_bytes")
        memsw-file (str "/cgroup/memory/" name "/" child-name "/memory.memsw.limit_in_bytes")]
    (try
      (spit cpu-file (str (int (* cpu-cores 100000))))
      (spit memory-file (str (int (* memory 1024 1024))))
      (spit memsw-file (str (int (* memsw 1024 1024))))
      {:success true}
      (catch Exception e
        (log/error "cgvalues write file error:" (.toString e))
        {:success false :info (.toString e)}))))

(defn cgexec [name child-name command]
  (let [exec-str (str "cgexec -g cpu,memory:" name "/" child-name " " command)]
    (try
      {:success true :process (utils/launch-process exec-str)}
      (catch Exception e
        (log/error "cgexec error:" (.toString e))
        {:success false :info (.toString e)}))))

(defn cgone [name child-name cpu-cores memory memsw command]
  (log/info "cgroup exec" command)
  (let [re (cgdelete name child-name)]
    (if (:success re)
      (let [re (cgcreate name child-name)]
        (if (:success re)
          (let [re (cgvalues name child-name cpu-cores memory memsw)]
            (if (:success re)
              (let [re (cgexec name child-name command)]
                (if (:success re)
                  (:process re)
                  {:success false}))))
          {:success false}))
      {:success false})))
