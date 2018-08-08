(ns org.purefn.altman.topology
  (:require [org.purefn.gregor.serdes :as serdes]
            [org.purefn.altman.processor :as processor]
            [org.purefn.altman.state :as state])
  (:import (org.apache.kafka.streams.processor
            ProcessorSupplier
            TopologyBuilder TopologyBuilder$AutoOffsetReset)))


(defn add-source
  "Adds a source to the topology with given name and collection of
   topic names."
  [^TopologyBuilder tb name topics]
  (.addSource tb
              TopologyBuilder$AutoOffsetReset/EARLIEST
              name
              nil ;; use default TimestampExtractor
              (serdes/nippy-deserializer)
              (serdes/nippy-deserializer)
              (into-array String topics)))

(defn add-sink
  "Adds a sink to the topology with given name, output topic name, and
   collection of parent node names."
  [^TopologyBuilder tb name topic parents]
  (.addSink tb
            name
            topic
            (serdes/nippy-serializer)
            (serdes/nippy-serializer)
            nil ;; use default partitioner
            (into-array String parents)))

(defn add-processor
  "Adds a processor to the topology with given name, ProcessorSupplier (ps),
   and collection of parent names."
  [^TopologyBuilder tb name ^ProcessorSupplier ps parents]
  (.addProcessor tb name ps
                 (into-array String parents)))

(defn add-state-store
  "Adds a StateStore to the topology, connected to given collection of
   processor names."
  [^TopologyBuilder tb state-store processors]
  (.addStateStore tb state-store (into-array String processors)))

;; Punting on global for now.
;; TODO: figure this out
;; current thinking is perhaps ignore it and use Jim's "multiplexing" idea

(defn topology
  "Constrained to single-processor topologies, with single state
   store (using smee's RocksDB-backed map). Takes a SimpleProcessor
   and returns a configured Topology."
  [simple-processor]
  (let [tb (TopologyBuilder.)
        input-topics (processor/inputs simple-processor)
        output-topics (processor/outputs simple-processor)
        _ (assert (seq input-topics)
                  "Cannot build topology from SimpleProcessor with no input topics")
        topology (-> tb
                     (add-source "input-source" ;; fixed source name
                                 input-topics)
                     ;; copartitioning here
                     (add-processor "default-processor"
                                    (processor/processor-supplier simple-processor)
                                    ["input-source"])
                     (add-state-store (state/kv-rocksdb-state-store "default-state")
                                      ["default-processor"]))]
    (doseq [t output-topics]
      (add-sink tb t t ["default-processor"]))
    tb))
