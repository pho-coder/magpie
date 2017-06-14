(defproject magpie-client-clj "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.21"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.thrift/libthrift "0.9.1"]
                 [clj-zookeeper "0.2.0-SNAPSHOT"]
                 [com.jd.bdp.magpie/magpie-utils "0.1.3-SNAPSHOT"]]
  :main ^:skip-aot com.jd.bdp.magpie.magpie-client.client
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
