(ns com.jd.bdp.example-task-clj.core
  (:gen-class)
  (:require [com.jd.bdp.magpie.magpie-framework-clj.task-executor :refer [execute]]
            [clojure.tools.logging :as log]))

(defn run-fn [job-id]
  (log/info "run-fn" job-id)
  (Thread/sleep 3000))

(defn prepare-fn [job-id]
  (log/info "prefare-fn" job-id)
  (Thread/sleep 1000))

(defn reload-fn [job-id]
  (log/info "reload-fn" job-id)
  (Thread/sleep 1000))

(defn pause-fn [job-id]
  (log/info "pause-fn" job-id)
  (Thread/sleep 1000))

(defn close-fn [job-id]
  (log/info "close-fn" job-id)
  (Thread/sleep 1000))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "Hello, World!")
  (execute run-fn :prepare-fn prepare-fn :reload-fn reload-fn :pause-fn pause-fn :close-fn close-fn))
