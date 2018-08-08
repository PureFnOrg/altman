# altman

Kafka Streams framework for Clojure and (soon) Event Sourcing.

>**Hotlips O'Houlihan:** I wonder how a degenerated person like that could have reached a position of responsibility in the Army Medical Corps!  
>**Father Mulcahy:** He was drafted.

>**Lola Johnson:** What if you die some day?  
>**Garrison Keillor:** I will die.  
>**Lola Johnson:** Don't you want people to remember you?  
>**Garrison Keillor:** I don't want them to be told to remember me.

## Views
The `org.purefn.altman.views` namespace provides an RPC layer for Kafka Streams
[Interactive Queries](https://docs.confluent.io/current/streams/developer-guide/interactive-queries.html#streams-developer-guide-interactive-queries-discovery).

The component constructed by the `server` function in this namespace will start an
Immutant web server (on port `8000` by default) which accepts `edn` web requests at
the `/state` endpoint.  This component should receive `queryable` streams apps as
dependencies in this system map like so:

```clojure
(require '[ladders-domains.recruiter.save-candidates :as candidate])

(component/system-map
   :views
   (component/using (views/server)
                    [:projects-view-app
                     :project-candidates-view-app]) 
   :projects-view-app
   (->> (processor/simple-processor event-processor/projects-processor
                                    :inputs ["recruiter.save.candidates.events"])
        (streams/streams-app (:projects-view config))
        (views/queryable ::candidate/project-view))
   :project-candidates-view-app
   (->> (processor/simple-processor event-processor/project-candidates-processor
                                    :inputs ["recruiter.save.candidates.events"])
        (streams/streams-app (:project-candidates-view config))
        (views/queryable ::candidate/project-candidates-view))
```		

Any number of streams apps are supported.  The namespaced keyword passed to the
`views/queryable` function should correlate with a usage of `defview`, e.g.

```clojure
(ns ladders-domains.recruiter.save-candidates
  (:require [org.purefn.irulan.view :as view])))

(view/defview ::project-view "ryan.platform")
```

This keyword is used in service discovery in the gatekeeper
([Rashomon](https://github.com/theladders/rashomon)) and to access the correct streams
app locally.

Once started the web server can be queried like

```clojure
(require '[ladders-domains.recruiter.save-candidates :as candidate])
(require '[clj-http.client :as http])

(defn fetch-key
  [view-name k]
  (http/post "http://localhost:8000/state"
             {:form-params {:key k
                            :view-name view-name}
              :content-type :edn}))

(fetch-key ::candidate/project-view
           #uuid "00000002-0012-8002-c000-000000000000")
```

This is the same mechanism Rashomon uses to retrieve state for individual processors.

## License

Copyright © 2017 Ladders

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
