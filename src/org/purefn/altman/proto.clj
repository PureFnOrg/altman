(ns org.purefn.altman.proto
  (:refer-clojure :exclude [peek pop]))

(defprotocol Queue
  (peek [this])
  (pop [this])
  (push [this el])
  (delete [this k]))
