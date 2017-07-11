(ns com.jd.bdp.magpie.util.cgutils
  (:require [clojure.tools.logging :as log]
            
            [com.jd.bdp.magpie.util.utils :as utils])
  (:import [org.apache.commons.exec ExecuteException]))

(defn cgdelete [cgroup-prefix-dir name child-name]
  (let [subsystems ["cpu" "memory"]]
    (try
      (doseq [subsystem subsystems]
        (utils/rmpath (str cgroup-prefix-dir "/" subsystem "/" name "/" child-name)))
      (log/info "cgdelete" name child-name "ok!")
      {:success true}
      (catch Exception e
        (log/error "cgdelete error:" (.toString e))
        {:success false :info (.toString e)}))))

(defn cgcreate [cgroup-prefix-dir name child-name]
  (let [subsystems ["cpu" "memory"]]
    (try
      (doseq [subsystem subsystems]
        (utils/local-mkdirs (str cgroup-prefix-dir "/" subsystem "/" name "/" child-name)))
      (log/info "cgcreate" name child-name "ok!")
      {:success true}
      (catch Exception e
        (log/error "cgcreate error:" (.toString e))
        {:success false :info (.toString e)}))))

(defn cgvalues [cgroup-prefix-dir name child-name cpu-cores memory memsw]
  (let [cpu-file (str cgroup-prefix-dir "/cpu/" name "/" child-name "/cpu.cfs_quota_us")
        memory-file (str cgroup-prefix-dir "/memory/" name "/" child-name "/memory.limit_in_bytes")
        memsw-file (str cgroup-prefix-dir "/memory/" name "/" child-name "/memory.memsw.limit_in_bytes")]
    (try
      (spit cpu-file (str (bigdec (* cpu-cores 100000))))
      (spit memory-file (str (bigdec (* memory 1024 1024))))
      (spit memsw-file (str (bigdec (* memsw 1024 1024))))
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

(defn cgone [cgroup-prefix-dir name child-name cpu-cores memory memsw command]
  (log/info "cgroup exec" command)
  (let [re (cgdelete cgroup-prefix-dir name child-name)]
    (if (:success re)
      (let [re (cgcreate cgroup-prefix-dir name child-name)]
        (if (:success re)
          (let [re (cgvalues cgroup-prefix-dir name child-name cpu-cores memory memsw)]
            (if (:success re)
              (let [re (cgexec name child-name command)]
                (if (:success re)
                  (:process re)
                  {:success false}))))
          {:success false}))
      {:success false})))

(defn get-cgroup-jobs
  [cgroup-prefix-dir name]
  (set (into (utils/read-dir-dirslist (str cgroup-prefix-dir "/cpu/" name))
             (utils/read-dir-dirslist (str cgroup-prefix-dir "/memory/" name)))))
