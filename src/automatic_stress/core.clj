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

(defn non-negative
  [value state assertion & _]
  (let [state (if (nil? (:non-negative state))
                (assoc state :non-negative {:count 0})
                state)]
    (if ((if (true? (first (val (first assertion)))) neg? pos?) value)
      (assoc state :non-negative {:count (inc (-> state :non-negative :count))})
      state)))

(defn directional
  [value state assertion & _]
  (let [state (if (nil? (:directional state))
                (assoc state :directional {:count 0 :prev nil})
                state)
        op (if (= "asc" (first (val (first assertion)))) > <)]
    (assoc-in
      (if (and (not (nil? (-> state :directional :prev)))
               (op (-> state :directional :prev) value))
        (update-in state [:directional :count] inc)
        state)
      [:directional :prev]
      value)))

(defn volatile
  [value state assertion idx]
  (let [state (if (nil? (:volatile state))
                (assoc state :volatile {:count 0, :prev nil})
                state)
        freq (first (val (first assertion)))]
    (if (= (mod idx freq) 0)
      (assoc-in
        (if (= (-> state :volatile :prev) value)
          (update-in state [:volatile :count] inc)
          state)
        [:volatile :prev]
        value)
      state)))

(defn record-attribute
  ([session recording-keyspace iteration jmx-host jmx-port attribute frequency finished]
    (record-attribute session recording-keyspace iteration jmx-host jmx-port attribute frequency finished 0 {}))
  ([session recording-keyspace iteration jmx-host jmx-port attribute frequency finished idx state]
    (let [value (with-connection {:host jmx-host :port jmx-port}
                  (jmx/read (name (:object-name attribute)) (keyword (:attribute attribute))))
          assertion-data (reduce
                           (fn [state assertion]
                             (if-let [fun (resolve (symbol (str "automatic-stress.core/" (name (ffirst assertion)))))]
                               (fun value state assertion idx)
                               state))
                           state
                           (-> attribute :assertions))
          statement (BoundStatement. (-> session (.prepare (str
                                                             "insert into " recording-keyspace ".attributes ( "
                                                             "  iteration,"
                                                             "  object_name,"
                                                             "  attribute,"
                                                             "  type,"
                                                             "  value,"
                                                             "  level,"
                                                             "  received"
                                                             ") values (?, ?, ?, ?, ?, ?, ?);"))))]
      (-> session (.execute (-> statement (.bind
        (into-array Object
          [iteration
           (name (:object-name attribute))
           (:attribute attribute)
           "bigint"
           (serialize-value value)
           (int 0)
           (.getTime (java.util.Date.))])))))
      (print ".") (flush)
      (Thread/sleep (or (:frequency attribute) frequency))
      (if (nil? @finished)
        (recur session recording-keyspace iteration jmx-host jmx-port attribute frequency finished (inc idx) assertion-data)
        {:attribute attribute :records (inc idx) :assertion-data assertion-data}))))

(defn maybe-create-test-schema
  [session recording-keyspace]
  (-> session (.execute (str
                          "create keyspace if not exists " recording-keyspace
                          " with replication = "
                          "{ 'class':'SimpleStrategy', 'replication_factor':3};")))
  (-> session (.execute (str
                          "create table if not exists " recording-keyspace ".iterations ("
                          "  iteration uuid primary key,"
                          "  attributes set<text>"
                          ");")))
  (-> session (.execute (str
                          "create table if not exists " recording-keyspace ".attributes ("
                          "  iteration uuid,"
                          "  object_name text,"
                          "  attribute text,"
                          "  type text,"
                          "  value blob,"
                          "  level int," ;; in the case where you want to be able to zoom in and out
                          "  received bigint," ;; i.e., timestamp
                          "  primary key (iteration, object_name, attribute, level, received)"
                          ");"))))

(defn record-iteration
  [session keyspace iteration attributes]
  (let [statement (BoundStatement.
                    (-> session
                      (.prepare
                        (str "insert into " keyspace ".iterations ("
                             "  iteration,"
                             "  attributes"
                             ") values (?,?);"))))]
    (-> session
      (.execute
        (-> statement
          (.bind
            (into-array Object
              [iteration
               (let [hs (java.util.HashSet.)]
                 (doseq [attr attributes]
                   (.add hs (str (:object-name attr) " " (:attribute attr))))
                 hs)])))))))

(defn -main
  [properties-file & iteration]

  (let [properties (yaml/parse-string (slurp properties-file))
        tester-address (or (-> properties :tester-contact-point) "localhost")]
    (if (cassandra-is-running? tester-address)
      (let [cluster (-> (Cluster/builder)
                      (.addContactPoint (-> properties :recorder-contact-point))
                      .build)
            recording-keyspace (str (-> properties :recording-keyspace))
            iteration (java.util.UUID/fromString
                        (or iteration (str (java.util.UUID/randomUUID))))
            attributes (-> properties :attributes)
            session (-> cluster .connect)]

        (maybe-create-test-schema session recording-keyspace)
        (record-iteration session recording-keyspace iteration attributes)

        (let [host (first (clojure.string/split tester-address #":"))
              finished (atom nil)
              record-promises (doall
                                (pmap
                                  #(future
                                     (record-attribute session
                                                       recording-keyspace
                                                       iteration
                                                       host
                                                       (-> properties :jmx-port)
                                                       %
                                                       (or (-> properties :frequency) 1000)
                                                       finished))
                                  attributes))
              stress-promise (future (sh "ls"))] ;(apply sh (clojure.string/split (-> properties :test-invocation) #" ")))]
          (Thread/sleep 1000)
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

