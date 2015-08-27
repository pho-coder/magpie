(ns com.jd.magpie.util.mutils
    (:require [metrics.core :refer [new-registry]]))

(defn get-registry []
  (new-registry))
