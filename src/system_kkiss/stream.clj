(ns system-kkiss.stream
  (:require [integrant.core :as ig]
            [kkiss.core :as kkiss]
            [kkiss.serde :as serde]
            [taoensso.timbre :refer [debug info warn error fatal report]]))

(defmethod ig/init-key ::engine [_ opts]
  (info {:msg "Initializing Stream Engine"})
  (kkiss/engine opts))


(defmethod ig/init-key ::stream [_ {:keys [name engine
                                           key-serde-id value-serde-id]}]
  (info {:msg "Initializing Stream"
         :name name
         :key-serde-id key-serde-id
         :value-serde-id value-serde-id})
  (kkiss/stream {:engine engine
                 :name name
                 :key.serde (serde/serde key-serde-id)
                 :value.serde (serde/serde value-serde-id)}))


(defmethod ig/init-key ::consumer [_ {:keys [streams handler]}]
  (info {:msg "Initializing Stream Consumer"})
  (let [consumer (kkiss/consumer streams handler)]
    (kkiss/start! consumer)
    consumer))


(defmethod ig/halt-key! ::consumer [_ consumer]
  (info {:msg "Halting Stream Consumer"})
  (kkiss/stop! consumer))
