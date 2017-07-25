(ns com.jd.bdp.example-task-clj.core
  (:gen-class)
  (:require [com.jd.bdp.magpie.magpie-framework-clj.task-executor :refer [execute]]
            [clojure.tools.logging :as log]))

(defn run-fn [job-id]
  (log/info "HI:" job-id)
  (Thread/sleep 3000))

(defn prepare-fn [job-id]
  (log/info "start" job-id))

(defn reload-fn [job-id]
  (log/info "reload" job-id))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "Hello, World!")
  (execute run-fn :prepare-fn prepare-fn))
