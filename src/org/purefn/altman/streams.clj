(ns org.purefn.altman.streams
  "Component for creating KafkaStreams app instances."
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.kurosawa.log.api :as log-api]
            [org.purefn.kurosawa.log.protocol :as log-proto]
            [org.purefn.gregor.serdes :as serdes]
            [org.purefn.altman.topology :as topology]
            [org.purefn.altman.state :as state])
  (:import (org.apache.kafka.streams
            KafkaStreams StreamsConfig)
           (org.apache.kafka.streams.state
            QueryableStoreTypes)))


;;------------------------------------------------------------------------------
;; Component.
;;------------------------------------------------------------------------------

(defprotocol StoreAccess
  (default-get [this k]
    "Fetches the value at the given key in the stream app's default state store."))

(def kvstore-type
  "Cached instance of the keyValueStore store type to be used by StoreAccess impls."
  (QueryableStoreTypes/keyValueStore))

(defprotocol Metadata
  (metadata-for-key [this k]
    "Fetches a map of host and port for the given key"))

(defrecord StreamsApp
    [config processor app]

  component/Lifecycle
  (start [this]
    (if app
      (do
        (log/info "StreamsApp already started")
        this)
      (let [_ (println "Initializing StreamsApp with config" config)
            streams-app (KafkaStreams. (topology/topology processor)
                                       (StreamsConfig. config))]
        (log/info "Starting StreamsApp")
        (.start streams-app)
        (assoc this :app streams-app))))

  (stop [this]
    (if app
      (do
        (log/info "Stopping StreamsApp")
        (.close app)
        (assoc this :app nil))
      (do
        (log/info "StreamsApp not started")
        this)))

  ;;----------------------------------------------------------------------------
  StoreAccess
  (default-get [_ k]
    (.. app
        (store state/default-name kvstore-type)
        (get k)))

  Metadata
  (metadata-for-key [_ k]
    (let [m (.metadataForKey app
                             state/default-name
                             k
                             (serdes/nippy-serializer))]
      {:host (.host m)
       :port (.port m)}))

  log-proto/Logging
  (log-namespaces [_]
    ["org.apache.kafka.*"])

  (log-configure [this dir]
    (klog/add-component-appender :kafka (log-api/log-namespaces this)
                                 (str dir "/kafka-streams.log"))))


;;------------------------------------------------------------------------------
;; Creation.
;;------------------------------------------------------------------------------

(defn streams-app
  "Creates a StreamsApp from a given Kafka Streams config and SimpleProcessor.

   Config has:
    * :application.id     Required, unique name of the streams app
    * :bootstrap.servers  Required, comma-separated list of initial
                          Kafka brokers to connect to; the remaining
                          will be discovered afterwards.
    * :state.dir          Where the local state storage goes
    * :ssl.truststore.location  File location of the trust store
    * :ssl.truststore.password  Password for the trust store file
    * :ssl.keystore.location  File location of the key store
    * :ssl.keystore.password  Password for the key store file"
  ([config]
   (streams-app config nil))
  ([config simple-processor]
   (assert (string? (:application.id config)) ":application.id is required config")
   (assert (string? (:bootstrap.servers config)) ":bootstrap.servers is required config")
   (->StreamsApp (zipmap (map name (keys config)) (vals config))
                 simple-processor
                 nil)))
