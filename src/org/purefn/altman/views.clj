(ns org.purefn.altman.views
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]
            [bidi.ring :refer (make-handler)]
            [ring.middleware.json :refer (wrap-json-response)]
            [ring.middleware.edn :refer (wrap-edn-params)]
            [ring.util.response :as response]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [org.purefn.kurosawa.web.server :as server]
            [org.purefn.kurosawa.web.app :as app]
            [org.purefn.irulan.common :as icommon]
            [org.purefn.altman.streams :as streams]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]))

(defn lookup
  "Looks up the given key in the local instance's state store."
  [streams-apps {:keys [params] :as req}]
  (let [k (:key params)
        view-name (:view-name params)]
    (-> (get streams-apps view-name)
        (streams/default-get k)
        (json/generate-string)
        (response/response))))

(defn discover-lookup
  "Uses kafka streams metadata to look up which instance owns a particular
   key in its state store, then uses /lookup/:key endpoint to talk to actually
   get the value at that key."
  [streams-apps {:keys [params] :as req}]
  (try 
    (let [k (:key params)
          view-name (:view-name params)
          {:keys [host port]} (-> (get streams-apps view-name)
                                  (streams/metadata-for-key k))]
      (-> (http/post (str "http://" host ":" port "/lookup")
                     {:form-params {:key k
                                    :view-name view-name}
                      :content-type :edn})
          :body
          response/response))
    (catch Exception ex
      (log/error ex)
      (response/response {:status 500
                          :body (.getMessage ex)}))))

(def not-found (constantly (response/response "Not found")))

(defn view-routes
  "Creates a ring handler for the two view endpoints and wraps the streams app."
  [streams-apps]
  (-> (make-handler
       ["/" {["lookup"] (partial lookup streams-apps)
             ["state"] (partial discover-lookup streams-apps)
             true not-found}])
      (wrap-json-response)
      (wrap-edn-params)))

(defn http-app
  "Sets up a default kurosawa web app that passes the streams app instance
   through to the ring handler."
  [streams-apps]
  (app/app (app/default-config)
           (view-routes streams-apps)))

(defrecord ImmutantServerViewApp
    [config server]

  component/Lifecycle
  (start [this]
    (let [streams-apps (into {} (map (juxt ::name identity))
                             (vals (dissoc this :config :server)))]
      (log/info "Starting views app for" (keys streams-apps))
      (assoc this
             :streams-apps streams-apps
             :server (-> (merge (server/default-config)
                                {::server/port (::port config)})
                         (server/immutant-server (http-app streams-apps))
                         component/start))))
  (stop [this]
    (component/stop server)
    (assoc this
           :server nil
           :streams-apps nil)))

(def default-port 8000)

(defn default-config
  []
  {::port default-port
   ::host (or (System/getenv "POD_IP_ADDR") "0.0.0.0")})

(defn server
  "Creates a web server using the given Kafka Streams apps passed in as dependencies.
   The StreamsApps should be marked as `queryable` or they will not provide views.

   The web server listens on two endpoints:
   - /state/:key --> Called by the gatekeeper to look up the value at a key
   - /lookup/:key --> Used between the instances once they figure out which instance
                      owns a particular key."
  ([] (server (default-config)))
  ([config]
   (map->ImmutantServerViewApp
    {:config config})))

(defn queryable
  "Marks a StreamsApp as 'queryable' with optional parameters:
   - `name` the name of the view to use for service discovery.  This should correlate 
            with a usage of `defview`.
   - `config` a map of:
      `::port` the port which the view server routing to this app is listening on
      `::host` the IP address this streams app is reachable at
  
   Sets the `application.server` configuration parameter of the app. In Kubernetes, 
   this expects the Pod's IP address to be set up in an environment variable named 
   POD_IP_ADDR."
  ([app]
   (queryable (default-config) :app app))
  ([name app]
   (queryable (default-config) name app))
  ([config name app]
   (let [{:keys [::host ::port]} config]
     (-> (assoc app ::name name)
         (assoc-in [:config "application.server"] (str host ":" port))))))

(s/def ::name keyword?)
(s/def ::host icommon/ip4-addr?)
(s/def ::port icommon/port?)
(s/def ::config (s/keys :req [::host ::port]))
(def streams-app? (partial instance? org.purefn.altman.streams.StreamsApp))

(s/fdef queryable
        :args (s/alt :nameless (s/cat :app streams-app?)
                     :default-config (s/cat :name ::name
                                            :app streams-app?)
                     :with-config (s/cat :config ::config
                                         :name ::name
                                         :app streams-app?)))

(stest/instrument `queryable)
