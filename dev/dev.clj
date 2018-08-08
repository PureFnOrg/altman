(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application.
  Call `(reset)` to reload modified code and (re)start the system.
  The system under development is `system`, referred from
  `com.stuartsierra.component.repl/system`.
  See also https://github.com/stuartsierra/component.repl"
  (:require
   [clojure.java.io :as io]
   [clojure.java.javadoc :refer [javadoc]]
   [clojure.pprint :refer [pprint]]
   [clojure.reflect :refer [reflect]]
   [clojure.repl :refer [apropos dir doc find-doc pst source]]
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :refer [refresh refresh-all clear]]
   [com.stuartsierra.component :as component]
   [com.stuartsierra.component.repl :as repl :refer [set-init reset stop system]]

   [org.purefn.gregor.serdes :as serdes]
   [org.purefn.altman.state :as state]
   [org.purefn.altman.queue :as queue])
  (:import (org.apache.kafka.streams.state
            KeyValueStore KeyValueIterator
            Stores)
           (org.apache.kafka.streams.processor
            Processor ProcessorContext ProcessorSupplier)
           org.apache.kafka.streams.KeyValue))

(def fake-processor-context
  (reify ProcessorContext
    (applicationId [_] "test")
    (taskId [_] "dunno")
    (keySerde [_] (serdes/nippy-serde))
    (valueSerde [_]
      (serdes/nippy-serde))
    (stateDir [_] (io/file "state"))
    (metrics [_] "what")
    (register [_ store logging restore] nil)
    (getStateStore [_ name] nil)
    (appConfigs [_] {})

    ))

(defn open-rocksdb
  "Make a rocksdb state store instance to play with, using optional
   name.
   It will not try to log out to Kafka, but will have a file backend."
  ([]
   (open-rocksdb "test"))
  ([name]
   (let [;; ffs
         rocks-store (.. (Stores/create name)
                         (withKeys (serdes/nippy-serde))
                         (withValues (serdes/nippy-serde))
                         (persistent)
                         (disableLogging)
                         (build)
                         (get)
                         (inner))]
     (doto rocks-store
       (.init fake-processor-context rocks-store)))))

;; Wrapper component to open and close rocksdb instances
(defrecord RocksDB
    [db]
  component/Lifecycle
  (start [this]
    (assoc this :db (open-rocksdb)))
  (stop [this]
    (.close db)
    (assoc this :db nil)))

(defn kv-rocksdb
  []
  (map->RocksDB {}))

(defmacro r
  []
  `(do
     (repl/reset)
     (def kv (-> system :rocksdb :db))
     (def st (state/kv-state-store-backed-map kv))
     (def q0 (queue/kv-backed-queue st :id))))

(clojure.tools.namespace.repl/set-refresh-dirs "dev" "src" "test")

(defn dev-system
  "Constructs a system map suitable for interactive development."
  []
  (component/system-map
   :rocksdb (kv-rocksdb)
   ))

(set-init (fn [_] (dev-system)))
