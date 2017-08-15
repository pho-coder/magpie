(defproject magpie "2.2.20170815-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.3.5"]
		 [clj-http "3.6.1"]
                 [commons-io "2.5"]
                 [org.apache.commons/commons-exec "1.3"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.yaml/snakeyaml "1.18"]
                 [org.apache.thrift/libthrift "0.10.0"]
                 [org.apache.curator/curator-framework "2.12.0"]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [org.slf4j/slf4j-log4j12 "1.7.25"]
                 [sigmund/sigmund "0.1.1"]
                 [org.codehaus.jackson/jackson-mapper-asl "1.9.13"]
                 [org.json/json "20170516"]
                 [metrics-clojure "2.9.0"]
                 [mount "0.1.11"]
                 [org.clojure/data.json "0.2.6"]]
  :profiles {:uberjar {:aot [com.jd.bdp.magpie.daemon.nimbus com.jd.bdp.magpie.daemon.supervisor]}}
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test" "test/clj"]
  :resource-paths ["conf"]
  :compile-path "target/classes"
  :target-path "target/"
  :jar-name "magpie.jar"
  :jvm-opts ["-Djava.library.path=native/Linux-amd64-64/"]
)
