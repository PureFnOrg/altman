(ns org.purefn.altman.state
  (:require [org.purefn.gregor.serdes :as serdes])
  (:import (org.apache.kafka.streams.state
            KeyValueStore KeyValueIterator
            Stores)
           org.apache.kafka.streams.KeyValue))

;;; Default state store name

(def default-name "default-state")

(defprotocol FlushableStore
  (flush-to-store [_]
    "Flush pending writes to the store, return resulting store."))


(defn- apply-update
  "Takes the updates map, a key and value, and returns the value after
   applying any update for key."
  [updates k v]
  (let [update-val (get updates k ::not-found)]
    (case update-val
        ::delete nil
        ::not-found v
        update-val)))

(deftype KVStateStoreBackedMap
    [^KeyValueStore store updates iters]

  clojure.lang.Associative
  (containsKey [_ k]
    (some? (apply-update updates k (.get store k))))
  (entryAt [_ k]
    (apply-update updates k (.get store k)))

  clojure.lang.IPersistentMap
  (assoc [_ k v]
    (let [new? (nil? (.get store k))]
      (KVStateStoreBackedMap. store (assoc updates k v)
                              iters)))
  (assocEx [_ k v]
    (let [new? (nil? (.get store k))]
      (KVStateStoreBackedMap. store (assoc updates k v)
                              iters)))
  (without [_ k]
    (KVStateStoreBackedMap. store (assoc updates k ::delete)
                            iters))

  clojure.lang.IPersistentCollection
  (empty [_] {})
  (cons [this kv]
    (let [[k v] kv]
      (.assoc this k v)))
  (equiv [_ other] false)

  clojure.lang.Counted
  ;; approximate only!
  (count [_]
    (.approximateNumEntries store))

  clojure.lang.ILookup
  (valAt [_ k]
    (apply-update updates k (.get store k)))
  (valAt [_ k default]
    (let [test-val (apply-update updates k (.get store k))]
      (if (some? test-val)
        test-val
        default)))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))
  (invoke [this k not-found]
    (.valAt this k not-found))

  clojure.lang.Seqable
  (seq [_]
    (let [iter (.all store)
          added (into {} (comp (remove #{::delete})
                               (remove #(.get store (key %))))
                      updates)
          added-keys (set (keys added))]
      (swap! iters conj iter)
      (concat (seq added)
              (sequence (comp
                         (map (fn [^KeyValue kv]
                                (clojure.lang.MapEntry. (.key kv)
                                                        (apply-update updates (.key kv) (.value kv)))))
                         (remove (comp added-keys key)))
                        (iterator-seq iter)))))

  FlushableStore
  (flush-to-store [_]
    (doseq [[k v] updates]
      (if (= ::delete v)
        (.delete store k)
        (.put store k v)))
    (doseq [^KeyValueIterator i @iters]
      (.close i))
    (reset! iters nil)
    (KVStateStoreBackedMap. store {} (atom nil))))

(defn kv-state-store-backed-map
  "Creates a KVStateStoreBackedMap using provided kv-store, which
   should be a Kafka Streams KeyValueStore.

   Allows handlers to treat it as a map, with modification operations
   behaving immutably by appending to a write log which can be flushed
   to the KV store when finished."
  [kv-store]
  (->KVStateStoreBackedMap kv-store {} (atom nil)))


(defn kv-rocksdb-state-store
  "Returns Rocks StateStoreSupplier with given name."
  [name]
  (.. (Stores/create name)
      (withKeys (serdes/nippy-serde))
      (withValues (serdes/nippy-serde))
      (persistent)
      (build)))
