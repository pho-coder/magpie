(defproject magpie "2.0-SNAPSHOT"
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
                 [metrics-clojure "2.9.0"]]
  :profiles {:uberjar {:aot [com.jd.magpie.daemon.nimbus com.jd.magpie.daemon.supervisor]}}
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test" "test/clj"]
  :resource-paths ["conf"]
  :compile-path "target/classes"
  :target-path "target/"
  :jar-name "magpie.jar"
  :jvm-opts ["-Djava.library.path=native/Linux-amd64-64/"]
)
