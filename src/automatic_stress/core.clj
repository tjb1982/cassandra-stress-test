(ns automatic-stress.core
  (:require [clojure.java.shell :refer [sh]]
            [clojure.java.jmx :as jmx :refer [mbean-names with-connection]])
            ;[clojure.core.async :as async :refer :all])
  (:import (com.datastax.driver.core Cluster)
           (com.datastax.driver.core.exceptions NoHostAvailableException))
  (:gen-class))

(defn cassandra-is-running?
  [node-address]
  (try
    (let [cluster (-> (Cluster/builder) (.addContactPoint node-address) .build)]
      (-> cluster .getMetadata)
      (-> cluster .close)
      (println "Cassandra server found...")
      true)
    (catch NoHostAvailableException e
      (println (.getMessage e))
      false)))

(defn record-attribute
  ([host port mb frequency finished]
    (record-attribute host port mb frequency finished 0))
  ([host port mb frequency finished idx]
    (println ;; TODO: insert into a different cluster
      (with-connection {:host host
                        :port (or port 7199)}
        (jmx/read (first mb) (keyword (second mb)))))
    (Thread/sleep frequency)
    (if (nil? @finished)
      (recur host port mb frequency finished (inc idx))
      {:mbean mb :iterations idx})))

(defn -main
  [& [node-address jmx-port]]

  (let [node-address (or node-address "localhost")]
    (if (cassandra-is-running? node-address)
      (let [host (first (clojure.string/split node-address #":"))
            finished (atom nil)
            mbeans {"org.apache.cassandra.metrics:type=ColumnFamily,name=LiveSSTableCount" "Value"
                    "org.apache.cassandra.metrics:type=ColumnFamily,name=AllMemtablesDataSize" "Value"
                    "org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency" "95thPercentile"}
            record-promises (doall (map #(future (record-attribute host jmx-port % 100 finished)) mbeans))
            stress-promise (future (sh "/home/tjb1982/nclk/them/datastax/apache-cassandra-2.0.15/tools/bin/cassandra-stress"))];(sh "ls" "-la"))] ; ;
        (swap! finished (fn [_] @stress-promise))
        (println @finished)
        (doseq [p record-promises] (println @p))
        ;(assertion-checks)
        )
      (println "Cassandra couldn't be found. Test aborted.")))

  (shutdown-agents))

