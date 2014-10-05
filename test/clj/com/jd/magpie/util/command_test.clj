(ns com.jd.magpie.util.command-test
  (:require [clojure.test :refer :all]
            [com.jd.magpie.util.zookeeper :as zookeeper]
            [com.jd.magpie.util.timer :as timer]
            [com.jd.magpie.util.utils :as utils])
  (:use [com.jd.magpie.bootstrap]))

(defn test-find-jar []
  (let [jar-name-prefix "abc123-"
        path "/tmp/magpie-test"
        jar-name-suffix ".jar"]
    (prn (utils/find-best-jar path jar-name-prefix jar-name-suffix))))


(deftest a-test
  (testing "Test ls and find jar"
    (test-find-jar)))