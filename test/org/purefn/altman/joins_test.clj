(ns org.purefn.altman.joins-test
  (:require [clojure.test :as test :refer [is deftest testing]]
            [clojure.spec.alpha :as s]
            [org.purefn.altman.joins :as joins]
            [org.purefn.irulan.event :as event]
            [org.purefn.gregor.messaging :as msg]))

(defn unversion
  "Helper function to unversion an event payload."
  [payload]
  (update payload ::event/event-type event/unversioned-event))

(def e1 {::event/payload {::event/event-type :test.event.v1/foo :key 0 :val "hello"}})
(def e2 {::event/payload {::event/event-type :test.event.v1/bar :key 0 :val "world"}})
(def e3 {::event/payload {::event/event-type :test.event.v1/other :key 0 :val "universe"}})
(def e1-payload (unversion (::event/payload e1)))
(def e2-payload (unversion (::event/payload e2)))
(def e3-payload (unversion (::event/payload e3)))

(def other-key {::event/payload {::event/event-type :test.event.v1/foo :key 1 :val "other"}})
(def other-key-payload (unversion (::event/payload (unversion other-key))))
(def other-type {::event/payload {::event/event-type :test.event.v1/other :key 0 :val "other"}})
(def other-type-payload (unversion (::event/payload (unversion other-type))))

(def msg1 (msg/message "testing-topic" 0 e1))
(def msg2 (msg/message "testing-topic" 0 e2))
(def other-key-msg (msg/message "testing-topic"
                                (:key other-key)
                                other-key))
(def other-type-msg (msg/message "testing-topic"
                                 (:key other-type)
                                 other-type))

(def e1-state {0 {:test/foo e1-payload}})
(def e2-state {0 {:test/bar e2-payload}})
(def both-state {0 {:test/foo e1-payload :test/bar e2-payload}})
(def triple-state {0 {:test/foo e1-payload :test/bar e2-payload :test/other e3-payload}})
(def e1-other-type-state {0 {:test/foo e1-payload :test/other other-type-payload}})
(def e2-other-type-state {0 {:test/bar e2-payload :test/other other-type-payload}})
(def e1-other-key-state {0 {:test/foo e2-payload} 1 {:test/foo other-key-payload}})
(def e2-other-key-state {0 {:test/bar e2-payload} 1 {:test/foo other-key-payload}})

(def tuple-join
  "Returns a tuple of the :val's of the :foo and :bar events."
  (joins/inner :test/foo :test/bar (fn [l r] [[(:val l) (:val r)]])))
(def e1e2-tuple [[(:val e1-payload) (:val e2-payload)]])

(def map-join
  "Returns a map of :foo and :bar events' :val's."
  (joins/inner :test/foo :test/bar
               (fn [l r] [{:foo (:val l)
                           :bar (:val r)}])))
(def e1e2-map
  [{:foo (:val e1-payload)
    :bar (:val e2-payload)}])

(def other-join
  "Like tuple-join, but for :foo and :other."
  (joins/inner :test/foo :test/other (fn [l r] [[(:val l) (:val r)]])))
(def other-tuple [[(:val e1-payload) (:val other-type-payload)]])

(def triple-join
  "Returns a string concat of three events' vals."
  (joins/inner :test/foo :test/bar :test/other
               (fn [& events]
                 [(apply str (map :val events))])))
(def triple-join-result [(str (:val e1-payload)
                              (:val e2-payload)
                              (:val e3-payload))])

(def handler (joins/handler [tuple-join map-join other-join]))
(def empty-handler (joins/handler []))

(deftest inner-join-tests
  (let [e1 e1-payload
        e2 e2-payload
        e3 e3-payload
        other-key other-key-payload
        other-type other-type-payload]

    (testing "Inner join works when both events exist in the state with either event being passed to the joiner fn."
     (is (= e1e2-tuple (tuple-join both-state 0 e1)))
     (is (= e1e2-tuple (tuple-join both-state 0 e2))))

    (testing "Inner join of 3 events works correctly."
      (is (= triple-join-result (triple-join triple-state 0 e1)))
      (is (= triple-join-result (triple-join triple-state 0 e2)))
      (is (= triple-join-result (triple-join triple-state 0 e3))))

   (testing "Inner join returns nil when only one of the needed events is in the state."
     (is (nil? (tuple-join e1-state 0 e1)))
     (is (nil? (tuple-join e1-state 0 e2)))

     (is (nil? (tuple-join e2-state 0 e1)))
     (is (nil? (tuple-join e2-state 0 e2))))

   (testing "Inner join returns nil when called with an expected event type but another key."
     (is (nil? (tuple-join e1-other-key-state 0 other-key)))
     (is (nil? (tuple-join e2-other-key-state 0 other-key))))

   (testing "Inner join returns nil when called with an unrecognized event."
     (is (nil? (tuple-join e1-other-type-state 0 other-type)))
     (is (nil? (tuple-join e2-other-type-state 0 other-type))))))

(deftest handler-test
  (testing "Handler works when passed msg2 and e1 is already in."
    (is (= [both-state (concat e1e2-tuple e1e2-map)]
           (handler e1-state msg2))))
  (testing "Handler works when passed msg1 and e2 is already in the state."
    (is (= [both-state (concat e1e2-tuple e1e2-map)]
           (handler e2-state msg1)))))
