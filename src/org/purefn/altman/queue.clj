(ns org.purefn.altman.queue
  "Provides a queue-like wrapper around the internal queue."
  (:refer-clojure :exclude [peek pop])
  (:require [clojure.spec.alpha :as s]
            [org.purefn.altman.proto :as proto]
            [org.purefn.altman.state :as state])
  (:import (org.purefn.altman.state.KVStateStoreBackedMap)))

(defn- first-el
  "Helper function for returning the first element from a q's state store."
  [state]
  (get state (::first state)))

(defn- last-el
  "Helper function for returning the last element from a q's state store."
  [state]
  (get state (::last state)))

(defn- element
  "Creates a queue element structure with the given `val' as the payload,
   `prev` for the previous queue element, and `next' as the key of the next
   element in the queue."
  [val prev next]
  {::payload val
   ::prev prev
   ::next next})

(deftype KVStateStoreBackedQueue
    [state key-fn]

  clojure.lang.Associative
  (containsKey [_ k]
    (.containsKey state k))
  (entryAt [_ k]
    (.entryAt state k))

  clojure.lang.IPersistentMap
  (assoc [_ k v]
    (.assoc state k v))
  (assocEx [_ k v]
    (.assocEx state k v))
  (without [_ k]
    (.without state k))

  clojure.lang.IPersistentCollection
  (empty [_]
    (.empty state))

  clojure.lang.Counted
  (count [_]
    (.count state))

  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt state k))
  (valAt [_ k default]
    (.valAt state k default))

  clojure.lang.IFn
  (invoke [_ k]
    (.invoke state k))
  (invoke [_ k not-found]
    (.invoke state k not-found))

  clojure.lang.Seqable
  (seq [_]
    (.seq state))

  state/FlushableStore
  (flush-to-store [_]
    (.flush_to_store state))

  proto/Queue
  (peek [this]
    (::payload (first-el state)))

  (pop [this]
    (let [next (::next (first-el state))]
      (KVStateStoreBackedQueue.
       (-> state
           (dissoc (::first state))
           (assoc ::first next)
           (cond->
               (nil? next) (assoc ::last nil)
               (some? next) (assoc-in [next ::prev] nil)))
       key-fn)))

  (push [this val]
    (let [k (key-fn val)]
      (KVStateStoreBackedQueue.
       (-> state
           (assoc k (element val (::last state) nil))
           (assoc ::last k)
           (cond->
               (nil? (::first state)) (assoc ::first k)

               (some? (::last state))
               (assoc-in [(::last state) ::next] k)))
       key-fn)))

  (delete [this k]
    (when (nil? (get state k))
      (throw (ex-info "Cannot delete: Element with given key does not exist in state."
                      {:key k})))

    (let [deleted (get state k)
          prev    (::prev deleted)
          next    (::next deleted)]
      (KVStateStoreBackedQueue.
       (-> state
           (dissoc k)
           (cond->
               (= (::first state) k) (assoc ::first next)
               (= (::last state) k) (assoc ::last prev)
               (some? prev) (assoc-in [prev ::next] next)
               (some? next) (assoc-in [next ::prev] prev)))
       key-fn))))

(defn peek
  "Returns the first value in the queue."
  [q]
  (proto/peek q))

(defn pop
  "Takes a queue and returns another queue with the first value removed."
  [q]
  (proto/pop q))

(defn push
  "Pushes a value `v' onto the queue."
  [q v]
  (proto/push q v))

(defn delete
  "Deletes the element in the queue with the given key.
   Alters the elements before and after the deleted element to now point to each
   other."
  [q k]
  (proto/delete q k))


(defn kv-backed-queue
  "Takes a KV-based Kafka Streams state store and returns a data structure that
   implements a queue on top of the store. Re-uses any existing Altman queue if
   one exists.

   Under the hood this implements a doubly linked list on top of the KV store,
   effectively using it as an address space. `key-fn' is used to derive the key
   that a given element is stored at in the KV store. , Pointers are maintained
   to the head and to the last element of this list. Each element in the queue
   maintains a pointer to the `next' element and the `prev` element.

   Pushing to the queue means adding an element after the current last element
   and then resetting the last pointer. Popping off the queue involves looking
   at whatever the current first element has as its `next' and setting the
   `first' pointer to that one. Peeking simply means returning whatever element
   is the current `first'.

   In the edge cases of pushing onto an empty list or popping and getting an
   empty list both pointers need to be set to the new element or to nil respectively."
  [state key-fn]
  (->KVStateStoreBackedQueue (cond-> state
                               (or (nil? (::first state)) (nil? (::last state)))
                               (-> (assoc ::first nil)
                                   (assoc ::last nil)))
                             key-fn))

(s/def ::state map?)

;; The key by which an element is stored in the raw kv-state
(s/def ::key     some?)
(s/def ::first   (s/nilable ::key))
(s/def ::last    (s/nilable ::key))
(s/def ::next    (s/nilable ::key))
(s/def ::prev    (s/nilable ::key))
(s/def ::payload any?)

(s/def ::element (s/keys :req [::payload ::next]))
(s/def ::queue (s/and ::state
                      (s/keys :req [::first ::last])))
