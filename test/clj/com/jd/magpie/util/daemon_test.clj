(ns com.jd.magpie.util.daemon-test
  (:require [clojure.test :refer :all]
            [com.jd.magpie.util.utils :as utils]
            [com.jd.magpie.daemon.nimbus :as nimbus]
            [com.jd.magpie.daemon.supervisor :as supervisor])
  (:import [java.io InputStreamReader FileInputStream]
           [org.yaml.snakeyaml Yaml]))

(defn test-nimbus []
  (let [conf-dir "/home/darwin/projects/magpie/magpie/resources/magpie.yaml"
        conf (apply conj {} (.load (Yaml.) (InputStreamReader. (FileInputStream. conf-dir))))]
    (nimbus/launch-server! conf)
    ))

(defn test-supervisor []
  (let [conf-dir "/home/darwin/projects/jrdw/magpie/resources/magpie.yaml"
        conf (apply conj {} (.load (Yaml.) (InputStreamReader. (FileInputStream. conf-dir))))]
    (supervisor/launch-server! conf)
    (loop [flag nil]
      (recur (Thread/sleep 100000)))
    ))

(defn test-launch-job []
  (let [conf-dir "/home/darwin/projects/jrdw/magpie/resources/magpie.yaml"
        conf (apply conj {} (.load (Yaml.) (InputStreamReader. (FileInputStream. conf-dir))))]
    (supervisor/launch-job conf {"start-time" 1393222085416,"db-type" "mysql","magpie-type" "tracker","id" "1025","supervisor" "0d653343-ed08-45cf-8f5c-2d5c30932738","last-supervisor" "236e1d34-5c2a-44fb-8926-bdf255b20f85"})))

(deftest a-test
  (testing "Fix"
                                        ;    (test-nimbus)
                                        ;  (test-supervisor)
    (test-launch-job)
    (loop [flag true]
      (Thread/sleep 1000)
      (recur flag))
    ))
