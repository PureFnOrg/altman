(ns org.purefn.altman.joins
  "Functions for building higher-level joining processors."
  (:require [org.purefn.irulan.event :as event]
            [taoensso.timbre :as log]))

(defn inner
  "Takes as its arguments a series of event types followed by the joiner function.
   Creates an inner join between events with the given event types on
   the incoming message's partitioning key. Will call `joiner` only once an event
   of each type has been processed. Subsequent events of either type will also trigger
   the joiner function. The joiner function is called with the payloads of both
   events, but not the current state."
  [& args]
  (let [joiner (last args)
        joined-types (butlast args)
        joined? (set joined-types)]
    (fn [state key e]
      (let [events (when (joined? (::event/event-type e))
                     (map (get state key) joined-types))]
        (when (and (seq events) (every? some? events))
          (apply joiner events))))))

(defn handler
  "Takes a collection of joins and returns a handler function that takes a state
   and message. This allows it to be used by anything that expects a simple-processor
   handler function. Note that the joiners are only passed the partitioning key
   and (unversioned) event payloads, not the full message."
  [joins]
  (fn [state {:keys [key] :as msg}]
    (let [e (-> msg
                :value
                ::event/payload
                (update ::event/event-type event/unversioned-event))
          state (assoc-in state [key (::event/event-type e)] e)]
      [state (apply concat ((apply juxt joins) state key e))])))
