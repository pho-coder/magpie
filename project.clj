(defproject magpie "1.1.3.0421-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [
                 ["locals" "http://192.168.137.123/nexus/content/repositories/central/"]
                 ["jd-libs" "http://10.10.224.102:8081/libs-releases"]
                 ["jd-plugin" "http://10.10.224.102:8081/plugins-releases"]
                 ["jd-lib-snapshot" "http://10.10.224.102:8081/libs-snapshots"]
                 ["jd-plugin-snapshot" "http://10.10.224.102:8081/plugins-snapshots"]
                 ["apache.snapshots" "http://repository.apache.org/content/repositories/snapshots/"]
                 ["sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                              :releases {:checksum :fail :update :always}}]
                 ["cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.0"]
		 [clj-http "0.9.2"]
                 [commons-io "2.4"]
                 [org.apache.commons/commons-exec "1.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.yaml/snakeyaml "1.11"]
                 [org.apache.thrift/libthrift "0.9.1"]
;;                 [org.apache.zookeeper/zookeeper "3.4.6"]
                 [org.apache.curator/curator-framework "2.6.0"]
                 [org.slf4j/slf4j-api "1.7.5"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [sigmund/sigmund "0.1.1"]
                 [org.codehaus.jackson/jackson-mapper-asl "1.9.9"]
                 [org.json/json "20131018"]
                 ]
  :profiles {:uberjar {:aot [com.jd.magpie.daemon.nimbus com.jd.magpie.daemon.supervisor]}}
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test" "test/clj"]
  :resource-paths ["conf"]
  :compile-path "target/classes"
  :target-path "target/"
  :jar-name "magpie.jar"
  :jvm-opts ["-Djava.library.path=native/Linux-amd64-64/"]
;  :uberjar-exclusions [#"bin"]
;  :main magpie.core
)
