(defproject automatic-stress "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jmx "0.3.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [clj-yaml "0.4.0"]
                 [com.datastax.cassandra/cassandra-driver-core "2.1.6"]
                ]
  :main ^:skip-aot automatic-stress.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
