(ns com.jd.magpie.util.timer-test
  (:require [clojure.test :refer :all]
            [com.jd.magpie.util.timer :as timer]))

(defn print-timer []
  (let [timer# (timer/mk-timer)
        afn (fn [] (prn "abc"))]
    (timer/schedule-recurring timer# 2 2 afn)
    (loop [flag true] (if flag (recur true)))))

(deftest a-test
  (testing "FIXME, I fail."
    (print-timer)))

