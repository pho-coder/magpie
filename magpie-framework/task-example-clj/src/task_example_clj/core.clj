(ns task-example-clj.core
  (:gen-class)
  (:require [com.jd.bdp.magpie.magpie-framework-clj.task-executor :refer [execute]]))

(defn run-fn [job-id]
  (println (str "HI: " job-id))
  (Thread/sleep 3000))

(defn prepare-fn [job-id]
  (println (str "start " job-id)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (execute run-fn :prepare-fn prepare-fn))
