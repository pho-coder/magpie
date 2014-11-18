(ns com.jd.magpie.util.utils
  (:require [sigmund.core :as sig]
            [clojure.tools.logging :as log]
            [clj-http.client :as client])
  (:import [java.util UUID]
           [java.io ByteArrayInputStream ByteArrayOutputStream FileOutputStream InputStreamReader InputStream BufferedReader IOException ObjectInputStream ObjectOutputStream DataInputStream File FilenameFilter FileNotFoundException]
           [java.net InetAddress]
           [java.lang.management ManagementFactory]
           [org.apache.commons.exec DefaultExecutor CommandLine]
           [org.apache.commons.io FileUtils]
           [org.apache.commons.exec ExecuteException]
           [org.yaml.snakeyaml Yaml]
           [org.codehaus.jackson.map ObjectMapper]
           [org.json JSONObject]))


(defn exception-cause? [klass ^Throwable t]
  (->> (iterate #(.getCause ^Throwable %) t)
       (take-while identity)
       (some (partial instance? klass))
       boolean))

(defn current-time-millis []
  (System/currentTimeMillis))

(defn uuid []
  (str (UUID/randomUUID)))

(defn normalize-path
  "fix the path to normalized form"
  [path]
  (let [path (if (empty? path) "/" path)
        path (if (and (.endsWith path "/") (> (count path) 1))
               (apply str (drop-last path))
               path)]
    path))

(defn tokenize-path [^String path]
  (let [toks (.split path "/")]
    (vec (filter (complement empty?) toks))
    ))

(defn parent-path [path]
  (let [toks (tokenize-path path)]
    (str "/" (clojure.string/join "/" (butlast toks)))
    ))

(defn toks->path [toks]
  (str "/" (clojure.string/join "/" toks))
  )

(defn full-path [parent name]
  (let [toks (tokenize-path parent)]
    (toks->path (conj toks name))
    ))

(def not-nil? (complement nil?))

(defn barr [& vals]
  (byte-array (map byte vals)))

(defn wrap-in-runtime
  "Wraps an exception in a RuntimeException if needed" 
  [^Exception e]
  (if (instance? RuntimeException e)
    e
    (RuntimeException. e)))

(defn find-yaml [filename & must?]
  (let [^Yaml yaml (Yaml.)
        _resources (.. (Thread/currentThread) getContextClassLoader (getResources filename))]
    (if-not (.hasMoreElements _resources)
      (if must?
        (throw (IOException. "resource magpie.yaml is not exists!"))
        {})
      (let [resources (loop [ret []]
                        (if (.hasMoreElements _resources)
                          (recur (conj ret (.nextElement _resources)))
                          ret))
            _ (if (> (count resources) 1)
                (throw (IOException. "found multiple magpie.yaml!")))
            parameters (.load yaml (InputStreamReader. (.openStream (first resources))))]
        (apply conj {} parameters)))))

(defn ^bytes serialize [obj]
  (try
    (let [^ByteArrayOutputStream bos (ByteArrayOutputStream.)
          ^ObjectOutputStream oos (ObjectOutputStream. bos)]
      (.writeObject oos obj)
      (.close oos)
      (.toByteArray bos))
    (catch IOException ioe
      (throw (wrap-in-runtime ioe)))))

(defn deserialize [^bytes serialized]
  (try
    (let [^ByteArrayInputStream bis (ByteArrayInputStream. serialized)
          ^ObjectInputStream ois (ObjectInputStream. bis)
          ret (.readObject ois)]
      (.close ois)
      ret)
    (catch IOException ioe
      (throw (wrap-in-runtime ioe)))
    (catch ClassNotFoundException e
      (throw (wrap-in-runtime e)))))

(defn object->jsonstring [object]
  (-> (ObjectMapper.) (.writeValueAsString object)))

(defn string->bytes [^String string & {:keys [encode] :or {encode "utf-8"}}]
  (.getBytes string encode))

(defn object->bytes [object]
  (-> object object->jsonstring string->bytes))

(defn ^String bytes->string [bytes]
  (new String bytes "utf-8"))

(defn string->json [^String string]
  (new JSONObject string))

(defn string->map [^String string]
  (let [json (string->json string)
        iterator (.keys json)]
    (loop [flag (.hasNext iterator)
           result {}]
      (if flag
        (let [k (.next iterator)
              v (let [v# (.get json k)]
                  (if (.isNull json k)
                    nil
                    v#))]
          (recur (.hasNext iterator) (conj result {k v})))
        result))))

(defn bytes->json [bytes]
  (-> bytes bytes->string string->json))

(defn bytes->map [bytes]
  (-> bytes bytes->string string->map))

(defn ^String ip []
  (.getHostAddress (InetAddress/getLocalHost)))

(defn ^String hostname []
  (.getHostName (InetAddress/getLocalHost)))

(defn ^String username []
  (System/getProperty "user.name"))

(defn memory []
  (sig/os-memory))

(defn total-memory []
  (quot (:total (memory)) (* 1024 1024)))

(defn actual-free-memory []
  (quot (:actual-free (memory)) (* 1024 1024)))

(defn swap []
  (sig/os-swap))

(defn total-swap []
  (quot (:total (swap)) (* 1024 1024)))

(defn free-swap []
  (quot (:free (swap)) (* 1024 1024)))

(defn cpu-core []
  (count (sig/cpu)))

(defn load-avg []
  (first (sig/os-load-avg)))

(defn network-usage []
  (sig/net-usage))

(defn resources-info []
  (let [mill (* 1024 1024)
        memory (sig/os-memory)
        total-memory (quot (:total memory) mill)
        actual-free-memory (quot (:actual-free memory) mill)
        memory-score (quot (* actual-free-memory 100) total-memory)
        swap (sig/os-swap)
        total-swap (quot (:total swap) mill)
        free-swap (quot (:free swap) mill)
        cpu-core (count (sig/cpu))
        load-avg (first (sig/os-load-avg))
        cpu-score (int (quot (* (- cpu-core load-avg) 100) cpu-core))]
    {"total-memory" total-memory "free-memory" actual-free-memory "memory-score" memory-score "total-swap" total-swap "free-swap" free-swap "load-avg" load-avg "cpu-core" cpu-core "cpu-score" cpu-score}))

(defn system-resources-enough?
  [jvm-mem-size]
  (let [mill (* 1024 1024)
        actual-free-memory (quot (:actual-free (sig/os-memory)) mill)
        swap (sig/os-swap)
        total-swap (quot (:total swap) mill)
        free-swap (quot (:free swap) mill)
        cpu-core (count (sig/cpu))
        load-avg (first (sig/os-load-avg))]
    (log/debug "free-memory(Mb):" actual-free-memory " total-swap(Mb):" total-swap " free-swap(Mb):" free-swap " load-avg:" load-avg " cpu-core:" cpu-core)
                                        ;   (and (> actual-free-memory jvm-mem-size) (> (/ (- free-swap
                                        ;   jvm-mem-size) total-swap) 0.6) (> (* cpu-core 0.8) load-avg))
    (and (> (- actual-free-memory jvm-mem-size) 100) (> (* cpu-core 0.8 2) load-avg))))

(defn process-pid
  "Gets the pid of this JVM. Hacky because Java doesn't provide a real way to do this."
  []
  (let [name (.getName (ManagementFactory/getRuntimeMXBean))
        split (.split name "@")]
    (when-not (= 2 (count split))
      (throw (RuntimeException. (str "Got unexpected process name: " name))))
    (first split)))

(defn exec-command! [command]
  (let [[comm-str & args] (seq (.split command " "))
        command (CommandLine. comm-str)]
    (doseq [a args]
      (.addArgument command a))
    (.execute (DefaultExecutor.) command)))

(defn extract-dir-from-jar [jarpath dir destdir]
  (try
    (exec-command! (str "unzip -qq " jarpath " " dir "/** -d " destdir))
    (catch ExecuteException e
      (log/error "Could not extract " dir " from " jarpath))))

(defn ensure-process-killed! [pid]
  ;; TODO: should probably do a ps ax of some sort to make sure it was killed
  (try
    (exec-command! (str "kill -9 " pid))
    (catch ExecuteException e
      (log/error "Error when trying to kill " pid ". Process is probably already dead."))))

(defn launch-process [command & {:keys [environment redirect?] :or {environment {} redirect? true}}]
  (let [command (->> (seq (.split command " "))
                     (filter (complement empty?)))
        builder (ProcessBuilder. command)
        process-env (.environment builder)]
    (doseq [[k v] environment]
      (.put process-env k v))
    (if redirect?
      (.redirectOutput builder (File. "/dev/null"))
      (.redirectError builder (File. "/dev/null")))
    (.start builder)))

(defn process-running? [pid]
  (try
    (let [^Process p (launch-process (str "ps -p " pid) :redirect? false)
          ^InputStream is (.getInputStream p)
          ^BufferedReader br (BufferedReader. (InputStreamReader. is))
          result (do (.readLine br) (.readLine br))]
      (.waitFor p)
      (.close is)
      (.close br)
      (.destroy p)
      (if result true false))
    (catch Exception e
      (wrap-in-runtime e))))

(defn exists-file? [path]
  (.exists (File. path)))

(defn find-best-jar
  [path prefix suffix & {:keys [version]}]
  (if version
    (str prefix version suffix)
    (let [^File dir (File. path)
          jars (.list dir (reify FilenameFilter
                            (accept [this dir name]
                              (and (.startsWith name prefix) (.endsWith name suffix)))))
          versions (map #(clojure.string/split (subs % (count prefix) (- (count %) (count suffix))) #"\.") jars)]
      (if (and (.exists dir) jars (not (empty? jars)))
        (str prefix (clojure.string/join "." (reduce (fn [a b] (let [v1 (map #(Integer/parseInt %) a)
                                                                    v2 (map #(Integer/parseInt %) b)]
                                                                (loop [t1 v1
                                                                       t2 v2]
                                                                  (if (empty? t1)
                                                                    b
                                                                    (if (empty? t2)
                                                                      a
                                                                      (if (> (first t1) (first t2))
                                                                        a
                                                                        (if (< (first t1) (first t2))
                                                                          b
                                                                          (recur (rest t1) (rest t2))))))))) versions)) suffix)
        (log/error "cannot find jar from " path ", which should has at least one jar name starts with " prefix " and ends with " suffix)))))

(defn rmr [path]
  (log/debug "Rmr path " path)
  (when (exists-file? path)
    (try
      (FileUtils/forceDelete (File. path))
      (catch FileNotFoundException e))))

(defn rmpath
  "Removes file or directory at the path. Not recursive. Throws exception on failure"
  [path]
  (log/debug "Removing path " path)
  (when (exists-file? path)
    (let [deleted? (.delete (File. path))]
      (when-not deleted?
        (throw (RuntimeException. (str "Failed to delete " path))))
      )))

(defn local-mkdirs
  [path]
  (log/debug "Making dirs at " path)
  (FileUtils/forceMkdir (File. path)))

(defn touch [path]
  (log/debug "Touching file at " path)
  (let [success? (.createNewFile (File. path))]
    (when-not success?
      (throw (RuntimeException. (str "Failed to touch " path))))))

(defn read-dir-contents [dir]
  (if (exists-file? dir)
    (let [content-files (.listFiles (File. dir))]
      (map #(.getName ^File %) content-files))
    [] ))

(defn read-file-contents [filepath]
  (if (exists-file? filepath)
    (slurp filepath)))

(defn write-file-contents [filepath contents]
  (let [*parent-path* (parent-path filepath)]
    (if-not (exists-file? *parent-path*)
      (local-mkdirs *parent-path*)))
  (spit filepath contents))

(defn current-classpath []
  (System/getProperty "java.class.path"))

(defn add-to-classpath [classpath paths]
  (clojure.string/join ":" (cons classpath paths)))

(defn get-configuration []
  (System/getProperty "magpie.configuration"))

(defn download [url path]
  (let [contents (client/get url {:as :stream})]
    (if (= (:status contents) 200)
      (let [^DataInputStream dis (DataInputStream. (:body contents))
            ^FileOutputStream fos (FileOutputStream. path)
            buffer (byte-array 1024)]
        (loop [size (.read dis buffer)]
          (when-not (= size -1)
            (.write fos buffer 0 size)
            (recur (.read dis buffer))))
        (.close dis)
        (.close fos))
      (log/error url " file not found!"))
    path))

(defn find-jar [jars-dir jar get-resources-url-func]
  (let [filepath (normalize-path (str jars-dir "/" jar))]
    (if-not (exists-file? filepath)
      (try
        (if-not (exists-file? jars-dir)
          (local-mkdirs jars-dir))
        (download (normalize-path (str (get-resources-url-func) "/" jar)) filepath)
        (catch Exception e
          (log/info "error accurs in downloading jar, exception:" e))))
    filepath))

