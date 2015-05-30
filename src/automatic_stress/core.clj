(ns automatic-stress.core
  (:require [clojure.java.shell :refer [sh]]
            [clojure.java.jmx :as jmx :refer [mbean-names with-connection]]
            [clj-yaml.core :as yaml]
            [clojure.pprint :as pprint]
            ;[clojure.core.async :as async :refer :all])
            )
  (:import (com.datastax.driver.core Cluster
                                     BoundStatement)
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

(defn serialize-value
  [value]
  (let [baos (java.io.ByteArrayOutputStream.)
        oos (java.io.ObjectOutputStream. baos)]
    (-> oos (.writeObject value))
    (java.nio.ByteBuffer/wrap (-> baos .toByteArray))))

(defn deserialize-value
  [value]
  (let [bais (java.io.ByteArrayInputStream. value)
        ois (java.io.ObjectInputStream. bais)]
    (-> ois .readObject)))

(defn record-attribute
  ([session test-keyspace iteration jmx-host jmx-port attribute frequency finished]
    (record-attribute session test-keyspace iteration jmx-host jmx-port attribute frequency finished 0))
  ([session test-keyspace iteration jmx-host jmx-port attribute frequency finished idx]
    (let [statement (BoundStatement. (-> session (.prepare (str
                                                             "insert into " test-keyspace ".attributes ( "
                                                             "  iteration,"
                                                             "  object_name,"
                                                             "  attribute,"
                                                             "  type,"
                                                             "  value,"
                                                             "  level,"
                                                             "  received"
                                                             ") values (?, ?, ?, ?, ?, ?, ?);"))))]
      (with-connection
        {:host jmx-host :port jmx-port}
        (-> session (.execute (-> statement (.bind
          (into-array Object
            [iteration
             (name (:object-name attribute))
             (:attribute attribute)
             "bigint"
             (serialize-value (jmx/read (name (:object-name attribute)) (keyword (:attribute attribute))))
             (int 0)
             (.getTime (java.util.Date.))]))))))
      (print ".") (flush)
      (Thread/sleep (or (:frequency attribute) frequency))
      (if (nil? @finished)
        (recur session test-keyspace iteration jmx-host jmx-port attribute frequency finished (inc idx))
        {:attribute attribute :iterations idx}))))

(defn maybe-create-test-schema
  [session test-keyspace]
  (-> session (.execute (str
                          "create keyspace if not exists " test-keyspace
                          " with replication = "
                          "{ 'class':'SimpleStrategy', 'replication_factor':3};")))
  (-> session (.execute (str
                          "create table if not exists " test-keyspace ".attributes ("
                          "  iteration uuid,"
                          "  object_name text,"
                          "  attribute text,"
                          "  type text,"
                          "  value blob,"
                          "  level int," ;; in the case where you want to be able to zoom in and out
                          "  received bigint," ;; i.e., timestamp
                          "  primary key (iteration, object_name, attribute, level, received)"
                          ");"))))

(defn -main
  [properties-file & iteration]

  (let [properties (yaml/parse-string (slurp properties-file))
       
        node-address (or (-> properties :tester-contact-point) "localhost")]
    (if (cassandra-is-running? node-address)
      (let [cluster (-> (Cluster/builder) (.addContactPoint (-> properties :recorder-contact-point)) .build)
            test-keyspace (str (-> properties :test-name))
            session (-> cluster .connect)]

        (maybe-create-test-schema session test-keyspace)

        (let [host (first (clojure.string/split node-address #":"))
              finished (atom nil)
              iteration (java.util.UUID/fromString (or iteration (str (java.util.UUID/randomUUID))))
              attributes (-> properties :attributes)
              record-promises (doall
                                (pmap
                                  #(future
                                     (record-attribute session
                                                       test-keyspace
                                                       iteration
                                                       host
                                                       (-> properties :jmx-port)
                                                       %
                                                       (-> properties :frequency)
                                                       finished))
                                  attributes))
              stress-promise (future (sh "/home/tjb1982/nclk/them/datastax/apache-cassandra-2.0.15/tools/bin/cassandra-stress"))
                             ;(future (sh "ls" "-la"))
              ]
          (swap! finished (fn [_] @stress-promise))
          (println "\n" (:out @finished))
          (doseq [p record-promises] (pprint/pprint @p))
          (println "UUID for this iteration: " iteration)
          (-> session .close)
          (-> cluster .close)
          ;(assertion-checks)
          ))
      (println "Cassandra couldn't be found. Test aborted.")))

  (shutdown-agents))

