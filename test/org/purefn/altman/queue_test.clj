(ns org.purefn.altman.queue-test
  (:require [clojure.test :as test :refer [is deftest testing]]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [org.purefn.altman.queue :as queue]))

(def key-fn
  "All tests below will use :id as the key for each queue message."
  :id)

(def populated-state
  "The raw underlying state of a queue with two elements.
   First element: {:id :foo}
   Second element: {:id :bar}
   Useful for any tests that need existing values in a queue. "
  {::queue/first :foo,
   ::queue/last :bar,
   :foo #::queue{:payload {:id :foo}, :prev nil, :next :bar},
   :bar #::queue{:payload {:id :bar}, :prev :foo, :next nil}})

(deftest utility-fn-tests
  (let [q (queue/kv-backed-queue populated-state key-fn)]
   (testing "Calling `element' with value, prev, and next parameters returns a correct element map."
     (let [value :foo
           nxt   :bar
           prv   nil]
       (is (= #::queue{:payload value
                       :next nxt
                       :prev prv}
              (#'queue/element value prv nxt)))))
   (testing "`first-el` returns the first element in a queue."
     (is (= #::queue{:payload {:id :foo} :prev nil, :next :bar} (#'queue/first-el q))))
   (testing "`last-el` returns the last element in a queue."
     (is (= #::queue{:payload {:id :bar} :prev :foo, :next nil} (#'queue/last-el q))))))

(deftest constructor-tests
  (let [empty-state {}]
    (testing "Creating a queue from an empty state results in an empty queue"
      (let [empty-q (queue/kv-backed-queue empty-state key-fn)]
        (is (nil? (::queue/first empty-q)))
        (is (nil? (::queue/last empty-q)))
        (is (nil? (queue/peek empty-q)))))
    (testing "Creating a queue from a populated state uses the existing values."
      (let [populated-q (queue/kv-backed-queue populated-state key-fn)]
        (is (= :foo (::queue/first populated-q)))
        (is (= :bar (::queue/last  populated-q)))
        (is (= {:id :foo} (queue/peek populated-q)))
        (is (= {:id :bar} (queue/peek (queue/pop populated-q))))))))

(deftest push-tests
  (let [empty-q (queue/kv-backed-queue {} key-fn)
        v1 {:id :foo}
        v2 {:id :bar}
        one-el (queue/push empty-q v1)
        two-el (queue/push one-el v2)]
    (testing "Pushing a value onto an empty queue sets both first and last pointers to the only element."
      (is (= :foo (::queue/first one-el)))
      (is (= :foo (::queue/last one-el)))
      (is (= #::queue{:payload v1 :prev nil, :next nil}
             (get one-el (::queue/first one-el))))
      (is (= #::queue{:payload v1 :prev nil, :next nil}
             (get one-el (::queue/last one-el)))))
    (testing "Pushing a value onto a queue with one element sets the last pointer and the next pointer of the existing element."
      (is (= :foo (::queue/first two-el)))
      (is (= :bar (::queue/last two-el)))
      (is (= #::queue{:payload v1 :prev nil :next :bar}
             (get two-el (::queue/first two-el))))
      (is (= #::queue{:payload v2 :prev :foo :next nil}
             (get two-el (::queue/last two-el)))))))

(deftest pop-tests
  (testing "Popping an empty queue returns an empty queue."
    (let [popped (queue/pop (queue/kv-backed-queue {} :key-fn))]
      (is (nil? (::queue/first popped)))
      (is (nil? (::queue/last popped)))))
  (let [q (queue/kv-backed-queue populated-state key-fn)
        popped (queue/pop q)
        empty-q (queue/pop popped)]
    (testing "Popping off a queue with two elements returns a queue with just one."
      (is (= :bar (::queue/first popped)))
      (is (= :bar (::queue/last popped)))
      (is (= #::queue{:payload {:id :bar} :prev nil :next nil}
             (get popped (::queue/first popped))))
      (is (= #::queue{:payload {:id :bar} :prev nil :next nil}
             (get popped (::queue/last popped)))))
    (testing "Popping off a queue with one element returns an empty queue."
      (is (nil? (::queue/first empty-q)))
      (is (nil? (::queue/last empty-q))))))

(deftest peek-tests
  (testing "Peeking on an empty queue returns nil"
    (is (nil? (queue/peek (queue/kv-backed-queue {} key-fn)))))
  (let [q (queue/kv-backed-queue populated-state key-fn)]
    (testing "Peeking on a queue created from an existing raw state returns the original head."
      (is (= {:id :foo} (queue/peek q))))
    (testing "Pushing a value onto a queue and then peeking should still return the original head."
      (is (= {:id :foo} (queue/peek (queue/push q {:id :baz})))))
    (testing "Popping a queue and then peeking should return the next element, no more original head."
      (is (= {:id :bar} (queue/peek (queue/pop q)))))))

(def three-element-state
  "A raw state like `populated-state`, but has three elements instead of two."
  {::queue/first :foo,
   ::queue/last :baz,
   :foo #::queue{:payload {:id :foo}, :prev nil, :next :bar},
   :bar #::queue{:payload {:id :bar}, :prev :foo, :next :baz}
   :baz #::queue{:payload {:id :baz}, :prev :bar, :next nil}})

(def after-first-deletion-state
  "What the state from `three-element-state` should look like after the middle
   element has been deleted."
  {::queue/first :bar,
   ::queue/last :baz,
   :bar #::queue{:payload {:id :bar}, :prev nil, :next :baz}
   :baz #::queue{:payload {:id :baz}, :prev :bar, :next nil}})

(def after-middle-deletion-state
  "What the state from `three-element-state` should look like after the middle
   element has been deleted."
  {::queue/first :foo,
   ::queue/last :baz,
   :foo #::queue{:payload {:id :foo}, :prev nil, :next :baz},
   :baz #::queue{:payload {:id :baz}, :prev :foo, :next nil}})

(def after-last-deletion-state
  "What the state from `three-element-state` should look like after the last
   element has been deleted."
  {::queue/first :foo,
   ::queue/last :bar,
   :foo #::queue{:payload {:id :foo}, :prev nil, :next :bar},
   :bar #::queue{:payload {:id :bar}, :prev :foo, :next nil}})

(def after-first-two-deleted-state
  "What the state from `three-element-state` should look like after the first
   two elements have been deleted."
  {::queue/first :baz,
   ::queue/last :baz,
   :baz #::queue{:payload {:id :baz}, :prev nil, :next nil}})

(def empty-queue
  "What a queue should look like after everything has been deleted from it."
  {::queue/first nil ::queue/last nil})

(deftest delete-tests
  (let [q (queue/kv-backed-queue three-element-state key-fn)
        first-deleted (queue/delete q :foo)
        middle-deleted (queue/delete q :bar)
        last-deleted (queue/delete q :baz)
        first-two-deleted (-> q
                              (queue/delete :foo)
                              (queue/delete :bar))
        all-deleted (-> q
                        (queue/delete :baz)
                        (queue/delete :foo)
                        (queue/delete :bar))]
    (testing "Deleting the first element of a 3-element queue."
      (is (= after-first-deletion-state (.state first-deleted))))
    (testing "Deleting the middle element of a 3-element queue."
      (is (= after-middle-deletion-state (.state middle-deleted))))
    (testing "Deleting the last element of a 3-element queue."
      (is (= after-last-deletion-state (.state last-deleted))))
    (testing "Deleting the first two elements of a 3-element queue."
      (is (= after-first-two-deleted-state (.state first-two-deleted))))
    (testing "Deleting all elements in a queue."
      (is (= empty-queue (.state all-deleted))))))
