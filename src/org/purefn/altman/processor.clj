(ns org.purefn.altman.processor
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [org.purefn.gregor.messaging :as gmsg]
            [org.purefn.altman.state :as state])
  (:import (org.apache.kafka.streams.processor
            Processor ProcessorContext ProcessorSupplier
            StateStore TopologyBuilder)
           (org.apache.kafka.streams
            KafkaStreams StreamsConfig)
           org.purefn.gregor.messaging.Message))

;;;; Componentizing

;; Protocol for extracting processor function from a stateful
;; component, so that we can inject dependencies into processor
;; functions (eg, for Gateways)

(defprotocol SimpleProcessor

  "Defines simple processors that are intended to be included in
   single-processor topologies. Provides a functional algebra for
   Kafka stream Processors, based on functions taking a map-like state
   that wraps StateStore and an input message or timestamp in the case
   of periodic (punctuate) functions, and returning the updated state
   and a collection of output messages.

   This protocol also defines several methods for returning metadata
   about the processor, including its inputs and outputs and the
   interval of its periodic function."

  (process [this state message]
    "Process single message, returning tuple of [new-state [message]].")
  (periodically [this state timestamp]
    "Run periodic function, returning tuple of [new-state [message]]
     or nil if not implemented.")
  (interval [this]
    "Return interval in milliseconds at which the periodic function
    should be called.")
  (inputs [this]
    "Return collection of input topic names that this Processor
     expects to consume.")
  (outputs [this]
    "Return collection of output topic names that this Processor will
    create messages for."))

(extend-protocol SimpleProcessor
  java.lang.Object
  (periodically [_ _ _] nil)
  (interval [_] nil)
  (inputs [_] nil)
  (outputs [_] nil))


(s/def ::state map?)
(s/def ::message (partial instance? Message))
(s/def ::timestamp integer?)
(s/def ::interval integer?)

(s/def ::process
  (s/fspec :args (s/cat :state ::state :message ::message)
           :ret (s/cat :state ::state :messages (s/coll-of ::message))))

(s/def ::periodic-fn
  (s/fspec :args (s/cat :state ::state :timestamp ::timestamp)
           :ret (s/cat :state ::state :messages (s/coll-of :message))))

(s/def ::function ::periodic-fn)

(s/def ::periodic
  (s/keys :req-un [::interval ::function]))

(s/def ::topic-name string?)

(s/def ::inputs (s/coll-of ::topic-name))
(s/def ::outputs (s/coll-of ::topic-name))

(s/def ::simple-processor (partial satisfies? SimpleProcessor))

(defn simple-processor
  {:arglists '([process & {:keys [periodic inputs outputs]}])
   :doc
   "Creates a stateless SimpleProcessor from a processing function
    and optional additional params.

   Takes a processing function, a function of state and message
   to [state [message]], and kwargs:

    * :periodic  Optional periodic function definition, a map
                 of :interval (long milliseconds between invocations)
                 and :function, the periodic function of state and
                 timestamp to [state [message]].
    * :inputs    Collection of input topic names
    * :outputs   Collection of output topics names"}
  [p & {:as opts}]
  ;; Unfortunately some crazy naming here to avoid colliding with the
  ;; protocol method names
  (let [{periodic :periodic in :inputs out :outputs} opts
        ival (:interval periodic)
        pfn (if-let [f (:function periodic)]
              (fn [state ts]
                (f state ts))
              (constantly nil))]
    (reify SimpleProcessor
      (process [_ state message] (p state message))
      (periodically [_ state timestamp] (pfn state timestamp))
      (interval [_] ival)
      (inputs [_] in)
      (outputs [_] out))))

(s/fdef simple-processor
        :args (s/cat :process ::process
                     :opts (s/keys* :opt-un [::inputs ::outputs ::periodic]))
        :ret ::simple-processor)


(defn forward-message
   "In a Kafka Streams Processor, forwards message on to the next
   processor using provided ProcessorContext. The consumer to receive
   the message should be identified by name in the `:topic` field of
   the message.

   Returns nothing."
  [^ProcessorContext pc msg]
  (let [{:keys [key value topic]} msg]
    (.forward pc key value topic)))

(deftype SimpleProcessorHarness
    [simple-processor processor-context state-store]

  Processor
  (init [this context]
    (log/debug "Called init")
    (reset! processor-context context)
    (reset! state-store (.getStateStore context state/default-name))
    (when-let [i (interval simple-processor)]
      (.schedule context i))
    nil)
  (process [_ k v]
    (log/debug "Called process with" k v)
    (try
      (let [^ProcessorContext ctxt @processor-context
            ^StateStore store @state-store
            state (state/kv-state-store-backed-map store)
            msg (gmsg/message (.topic ctxt) k v
                              {:partition (.partition ctxt)
                               :timestamp (.timestamp ctxt)})
            [new-state msgs] (process simple-processor state msg)]
        (doseq [m msgs]
          (forward-message ctxt m))
        (when new-state (state/flush-to-store new-state)))
      ;; TODO: proper logging
      (catch Exception e
        (log/error e "Exception in processing"))))
  (punctuate [_ timestamp]
    (try
      (let [^ProcessorContext ctxt @processor-context
            ^StateStore store @state-store
            state (state/kv-state-store-backed-map store)
            [new-state msgs] (periodically simple-processor state timestamp)]
        (doseq [m msgs]
          (forward-message ctxt m))
        (when new-state (state/flush-to-store new-state)))
      (catch Exception e
        (log/error e "Exception in punctuate"))))
  (close [_] nil))

(defn simple-processor-harness
  "Wrap given SimpleProcessor up in the SimpleProcessorHarness, for
   use in topologies."
  [simple-processor]
  (SimpleProcessorHarness. simple-processor (atom nil) (atom nil)))

(defn processor-supplier
  "Takes a SimpleProcessor, returns an object that implements
   ProcessorSupplier to be called when building toplogies."
  [simple-processor]
  (reify
    ProcessorSupplier
    (get [_]
      (simple-processor-harness simple-processor))))
