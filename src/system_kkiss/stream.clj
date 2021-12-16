(ns system-kkiss.stream
  (:require [clojure.instant :as instant]
            [integrant.core :as ig]
            [jsonista.core :as json]
            [jsonista.tagged :as jt]
            [kkiss.core :as kkiss]
            [kkiss.serde :as serde]
            [taoensso.timbre :refer [debug info warn error fatal report]])
  (:import (clojure.lang Keyword
                         Symbol
                         PersistentHashSet)
           (com.fasterxml.jackson.core JsonGenerator)
           (java.util Date
                      UUID)))

(defmethod ig/init-key ::engine [_ opts]
  (info {:msg "Initializing Stream Engine"})
  (kkiss/engine opts))


(defmethod ig/init-key ::lossless-json-mapper [_ {:keys [encode-key-fn
                                                         decode-key-fn]}]
  (json/object-mapper
   {:encode-key-fn (or encode-key-fn (nil? encode-key-fn))
    :decode-key-fn (or decode-key-fn (nil? decode-key-fn))
    :modules [(jt/module
               {:handlers {Keyword {:tag "~key"
                                    :encode jt/encode-keyword
                                    :decode keyword}
                           Symbol {:tag "~sym"
                                   :encode (fn [^Symbol s ^JsonGenerator gen]
                                             (.writeString gen (str s)))
                                   :decode symbol}
                           PersistentHashSet {:tag "~set"
                                              :encode jt/encode-collection
                                              :decode set}
                           Date {:tag "~date"
                                 :encode (fn [^Date d ^JsonGenerator gen]
                                           (.writeString gen (-> d
                                                                 .toInstant
                                                                 .toString)))
                                 :decode (fn [n] (instant/read-instant-date ^String n))}
                           UUID {:tag "~uuid"
                                 :encode (fn [^UUID uuid ^JsonGenerator gen]
                                           (.writeString gen (.toString uuid)))
                                 :decode (fn [n] (UUID/fromString ^String n))}}})]}))


(defmethod ig/init-key ::serde [_ {:keys [serde-id mapper]}]
  (info {:msg "Initializing Serde"
         :serde-id serde-id})
  (serde/serde serde-id mapper))


(defmethod ig/init-key ::stream [_ {:keys [name]
                                    :as opts}]
  (info {:msg "Initializing Stream"
         :name name})
  (kkiss/stream opts))


(defmethod ig/init-key ::consumer [_ {:keys [streams handler opts auto-start?]}]
  (info {:msg "Initializing Stream Consumer"
         :streams streams})
  (let [consumer (kkiss/consumer streams handler opts)]
    (when (or (nil? auto-start?) auto-start?)
      (kkiss/start! consumer))
    consumer))


(defmethod ig/halt-key! ::consumer [_ consumer]
  (info {:msg "Halting Stream Consumer"})
  (kkiss/stop! consumer))
