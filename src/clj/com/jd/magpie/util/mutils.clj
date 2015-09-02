(ns com.jd.magpie.util.mutils
  (:require [metrics.core :refer [new-registry]]
            [metrics.counters :as counters]
            [metrics.timers :as timers]))

(defn get-registry []
  (new-registry))

(defn get-counter [reg names]
  (counters/counter reg names))

(defn inc-counter
  ([counter] (counters/inc! counter))
  ([counter num] (counters/inc! counter num)))

(defn dec-counter
  ([counter] (counters/dec! counter))
  ([counter num] (counters/dec! counter num)))

(defn read-counter [counter]
  (counters/value counter))

(defn get-timer [reg names]
  (timers/timer reg names))

(defn time-timer
  "(time-timer a-timer
       (fn-1 ...)
       (fn-2 ...))"
  [timer fns]
  (timers/time! timer fns))

(defn sample-timer
  [timer]
  (timers/sample timer))
