(defproject statekeeper "0.1.0-SNAPSHOT"

  :description "statekeeper"
  :url "http://github.com/infonova/statekeeper"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.9.0"]]

  :profiles {:dev {:dependencies [[riemann "0.3.1"]]}}

  :resource-paths ["resources" "target/resources"])
