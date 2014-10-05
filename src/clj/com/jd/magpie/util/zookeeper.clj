(ns com.jd.magpie.util.zookeeper
  (:require [clojure.tools.logging :as log]
            [com.jd.magpie.util.utils :as utils])
  (:use [com.jd.magpie.bootstrap])
  (:import [org.apache.curator.retry RetryNTimes])
  (:import [org.apache.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  (:import [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [java.net InetSocketAddress BindException])
  (:import [java.io File])
  (:import [com.jd.magpie.utils BoundedExponentialBackoffRetry]))

(defn create-zookeeper-handler [conf]
  (let [zk-servers (conf MAGPIE-ZOOKEEPER-SERVERS)
        zk-port (conf MAGPIE-ZOOKEEPER-PORT)
        zk-root (conf MAGPIE-ZOOKEEPER-ROOT)]
    ))


(def zk-keeper-states
  {Watcher$Event$KeeperState/Disconnected :disconnected
   Watcher$Event$KeeperState/SyncConnected :connected
   Watcher$Event$KeeperState/AuthFailed :auth-failed
   Watcher$Event$KeeperState/Expired :expired
   })

(def zk-event-types
  {Watcher$Event$EventType/None :none
   Watcher$Event$EventType/NodeCreated :node-created
   Watcher$Event$EventType/NodeDeleted :node-deleted
   Watcher$Event$EventType/NodeDataChanged :node-data-changed
   Watcher$Event$EventType/NodeChildrenChanged :node-children-changed
   })

(defn- default-watcher [state type path]
  (log/info "Zookeeper state update: " state type path))

(defn mk-client [conf servers port & {:keys [root watcher auth-conf] :or {root "" watcher default-watcher auth-conf nil}}]
  (let [zkStr (str (clojure.string/join "," (map #(str % ":" port) servers)) root)
        builder (.. (CuratorFrameworkFactory/builder)
                    (connectString zkStr)
                    (connectionTimeoutMs (conf MAGPIE-ZOOKEEPER-CONNECTION-TIMEOUT))
                    (sessionTimeoutMs (conf MAGPIE-ZOOKEEPER-SESSION-TIMEOUT))
                    (retryPolicy
                     (BoundedExponentialBackoffRetry. (conf MAGPIE-ZOOKEEPER-RETRY-INTERVAL)
                                                                  (conf MAGPIE-ZOOKEEPER-RETRY-TIMES)
                                                                  (conf MAGPIE-ZOOKEEPER-RETRY-INTERVAL-CEILING))))
        _ (if auth-conf
            (if-let [scheme (auth-conf MAGPIE-ZOOKEEPER-AUTH-SCHEME)]
              (.authorization builder scheme (if (auth-conf MAGPIE-ZOOKEEPER-AUTH-PAYLOAD) (.getBytes (auth-conf MAGPIE-ZOOKEEPER-AUTH-PAYLOAD) "UTF-8")))))
        fk (.build builder)]
    (.. fk
        (getCuratorListenable)
        (addListener
         (reify CuratorListener
           (^void eventReceived [this ^CuratorFramework _fk ^CuratorEvent e]
             (when (= (.getType e) CuratorEventType/WATCHED)                  
               (let [^WatchedEvent event (.getWatchedEvent e)]
                 (watcher (zk-keeper-states (.getState event))
                          (zk-event-types (.getType event))
                          (.getPath event))))))))
    (.start fk)
    fk))

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT
   :persistent-sequential CreateMode/PERSISTENT_SEQUENTIAL
   :ephemeral-sequential CreateMode/EPHEMERAL_SEQUENTIAL})

(defn create-node
  ([^CuratorFramework zk ^String path ^bytes data mode]
     (try
       (.. zk (create) (withMode (zk-create-modes mode)) (withACL ZooDefs$Ids/OPEN_ACL_UNSAFE) (forPath (utils/normalize-path path) data))
       (catch Exception e (throw (utils/wrap-in-runtime e)))))
  ([^CuratorFramework zk ^String path ^bytes data]
     (create-node zk path data :persistent)))

(defn exists-node? [^CuratorFramework zk ^String path watch?]
  ((complement nil?)
   (try
     (if watch?
       (.. zk (checkExists) (watched) (forPath (utils/normalize-path path))) 
       (.. zk (checkExists) (forPath (utils/normalize-path path))))
     (catch Exception e (throw (utils/wrap-in-runtime e))))))

(defn delete-node [^CuratorFramework zk ^String path & {:keys [force] :or {force false}}]
  (try  (.. zk (delete) (forPath (utils/normalize-path path)))
        (catch KeeperException$NoNodeException e
          (when-not force (throw e)))
        (catch Exception e (throw (utils/wrap-in-runtime e)))))

(defn mkdirs [^CuratorFramework zk ^String path]
  (let [path (utils/normalize-path path)]
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (utils/parent-path path))
      (try
        (create-node zk path (utils/barr 7) :persistent)
        (catch KeeperException$NodeExistsException e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

(defn get-data [^CuratorFramework zk ^String path watch?]
  (let [path (utils/normalize-path path)]
    (try
      (if (exists-node? zk path watch?)
        (if watch?
          (.. zk (getData) (watched) (forPath path))
          (.. zk (getData) (forPath path))))
      (catch KeeperException$NoNodeException e
        ;; this is fine b/c we still have a watch from the successful exists call
        nil)
      (catch Exception e (throw (utils/wrap-in-runtime e))))))

(defn get-children [^CuratorFramework zk ^String path watch?]
  (try
    (if watch?
      (.. zk (getChildren) (watched) (forPath (utils/normalize-path path)))
      (.. zk (getChildren) (forPath (utils/normalize-path path))))
    (catch Exception e (throw (utils/wrap-in-runtime e)))))

(defn set-data [^CuratorFramework zk ^String path ^bytes data]
  (try
    (.. zk (setData) (forPath (utils/normalize-path path) data))
    (catch Exception e (throw (utils/wrap-in-runtime e)))))

(defn exists [^CuratorFramework zk ^String path watch?]
  (exists-node? zk path watch?))

(defn delete-recursive [^CuratorFramework zk ^String path]
  (let [path (utils/normalize-path path)]
    (when (exists-node? zk path false)
      (let [children (try (get-children zk path false)
                          (catch KeeperException$NoNodeException e
                            []
                            ))]
        (doseq [c children]
          (delete-recursive zk (utils/full-path path c)))
        (delete-node zk path :force true)
        ))))
