(ns org.purefn.altman.version
  (:gen-class))

(defn -main
  []
  (println (System/getProperty "altman.version")))
