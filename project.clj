(defproject org.purefn/altman "0.4.2-SNAPSHOT"
  :description "Kafka Streams Event Sourcing framework"
  :url "https://github.com/PureFnOrg/altman"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.7.1"
  ;; :global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [com.stuartsierra/component "0.3.2"]
                 [org.purefn/gregor "2.1.0"]
                 [org.purefn/irulan "0.2.2"]
                 [org.purefn/kurosawa.web "0.1.0"]
                 [com.taoensso/timbre "4.10.0"]
                 [bidi "2.1.2"]
                 [ring/ring-json "0.4.0"]
                 [fogus/ring-edn "0.4.0"]
                 [clj-http "3.7.0"]
                 [cheshire "5.8.0"]
                 [org.apache.kafka/kafka-streams "1.0.0"]]

  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [com.stuartsierra/component.repl "0.2.0"]]
                   :jvm-opts ["-Xmx2g"]
                   :source-paths ["dev"]
                   :codeina {:sources ["src"]
                             :exclude [org.purefn.altman.version]
                             :reader :clojure
                             :target "doc/dist/latest/api"
                             :src-uri "http://github.com/PureFnOrg/altman/blob/master/"
                             :src-uri-prefix "#L"}
                   :plugins [[funcool/codeina "0.4.0"
                              :exclusions [org.clojure/clojure]]
                             [lein-ancient "0.6.10"]]}}
  :aliases {"project-version" ["run" "-m" "org.purefn.altman.version"]})
