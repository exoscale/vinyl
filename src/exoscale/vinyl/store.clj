(ns exoscale.vinyl.store
  "A component and helper functions to expose access
   to a specific schema accessible by FDB's record layer.

   This is agnostic to the schema which will need to
   be supplied to the component using the rough DDL
   exposed in `exoscale.vinyl.schema`"
  (:refer-clojure :exclude [contains?])
  (:require [clojure.tools.logging      :as log]
            [com.stuartsierra.component :as component]
            [exoscale.vinyl.schema      :as schema]
            [exoscale.vinyl.query       :as query]
            [exoscale.vinyl.tuple       :as tuple]
            [exoscale.vinyl.cursor      :as cursor]
            [exoscale.vinyl.fn          :as fn])
  (:import
   (com.apple.foundationdb.record.provider.foundationdb.keyspace
    DirectoryLayerDirectory
    KeySpaceDirectory
    KeySpaceDirectory$KeyType
    KeySpace)
   com.apple.foundationdb.record.query.RecordQuery
   com.apple.foundationdb.record.provider.foundationdb.FDBDatabase
   com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory
   com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore$Builder
   com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext
   com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore
   com.apple.foundationdb.record.provider.foundationdb.FDBRecord
   com.apple.foundationdb.record.TupleRange
   com.apple.foundationdb.record.ScanProperties
   com.apple.foundationdb.record.query.plan.plans.QueryPlan
   com.apple.foundationdb.record.RecordMetaDataProvider
   com.apple.foundationdb.tuple.Tuple
   java.util.concurrent.CompletableFuture
   java.util.concurrent.TimeUnit
   java.util.function.Function))

(defprotocol DatabaseContext
  (run-in-context [this f]
    "Run a function against an FDBRecordStore.
    Protocolized so it can be called against the database or
    the store")
  (run-async [this f]
    "Run an asynchronous function against an FDBRecordStore.
    Protocolized so it can be called against the database or
    the store")
  (get-metadata [this]
    "Return this context's record metadata"))

(def ^KeySpace top-level-keyspace
  "This builds a directory structure of /$environment/$schema"
  (let [kt KeySpaceDirectory$KeyType/STRING
        ds (doto (DirectoryLayerDirectory. "environment")
             (.addSubdirectory (KeySpaceDirectory. "schema" kt)))]
    (KeySpace. (into-array KeySpaceDirectory [ds]))))

(defn ^FDBDatabase db-from-instance
  "Build a valid FDB database from configuration. Use the standard
   cluster-file location or a specific one if instructed to do so."
  ([]
   (db-from-instance nil))
  ([^String cluster-file]
   (let [^FDBDatabaseFactory factory (FDBDatabaseFactory/instance)]
     (if (some? cluster-file)
       (.getDatabase factory cluster-file)
       (.getDatabase factory)))))

(defn ^FDBRecordStore$Builder record-store-builder
  "Yield a new record store builder"
  []
  (FDBRecordStore/newBuilder))

(defn key-for*
  [db record-type items]
  (let [^RecordMetaDataProvider md
        (if (instance? FDBRecordStore db)
          db
          (get-metadata db))]
    (tuple/from-seq
     (concat
      [(-> md
           .getRecordMetaData
           (.getRecordType (name record-type))
           (.getRecordTypeKey))]
      items))))

(defn key-for
  [db record-type & args]
  (key-for* db record-type args))

(defn ^Tuple record-primary-key
  [^FDBRecord r]
  (.getPrimaryKey r))

(defn ^CompletableFuture async-store-from-builder
  [^FDBRecordStore$Builder builder ^FDBRecordContext context]
  (-> (.copyBuilder builder)
      (.setContext context)
      (.createOrOpenAsync)))

(defn ^FDBRecordStore store-from-builder
  [^FDBRecordStore$Builder builder ^FDBRecordContext context]
  (-> (.copyBuilder builder)
      (.setContext context)
      (.createOrOpen)))

(defrecord RecordStore [cluster-file schema-name schema descriptor env]
  component/Lifecycle
  (start [this]
    (let [env      (name (or env (gensym "testing")))
          kspath   (-> top-level-keyspace
                       (.path "environment" env)
                       (.add "schema" (name schema-name)))
          metadata (schema/create-record-meta descriptor schema)
          db       (db-from-instance cluster-file)
          builder  (doto (record-store-builder)
                     (.setMetaDataProvider ^RecordMetaDataProvider metadata)
                     (.setKeySpacePath kspath))]
      (-> (.performNoOpAsync db)
          (.get 2 TimeUnit/SECONDS))
      (log/info "started store" schema-name "/" env)
      (assoc this ::db db ::builder builder ::metadata metadata)))
  (stop [this]
    (dissoc this ::db ::builder ::metadata))
  DatabaseContext
  (get-metadata [this]
    (::metadata this))
  (run-async [this f]
    (.runAsync
     ^FDBDatabase (::db this)
     (reify Function
       (apply [_ context]
         (.thenCompose (async-store-from-builder (::builder this) context)
                       (fn/make-fun f))))))
  (run-in-context [this f]
    (.run ^FDBDatabase (::db this)
          (reify Function
            (apply [_ context]
              (f (store-from-builder (::builder this) context)))))))

(def start component/start)
(def stop component/stop)

(defn initialize
  ([schema-name descriptor schema]
   (map->RecordStore {:schema-name schema-name
                      :descriptor  descriptor
                      :schema      schema}))
  ([schema-name descriptor schema opts]
   (map->RecordStore (merge opts {:schema-name schema-name
                                  :descriptor  descriptor
                                  :schema      schema}))))

(extend-protocol DatabaseContext
  FDBRecordStore
  (run-in-context [this f] (f this))
  (run-async [this f]      (f this)))

(defn ^RecordQuery as-query
  [q]
  (if (instance? RecordQuery q)
    q
    (apply query/build-query q)))

(defn store-query-fn
  [^RecordQuery query {::keys [values intercept-plan-fn log-plan?] :as opts}]
  (fn [^FDBRecordStore store]
    (let [plan ^QueryPlan (.planQuery store query)
          ctx  (if (some? values) (query/bindings values) query/empty-context)]
      (when (true? log-plan?)
        (log/info "planned query:" (str plan)))
      (when (ifn? intercept-plan-fn)
        (intercept-plan-fn plan))
      (-> (.execute plan store ctx)
          (cursor/apply-transforms opts)))))

(defn save-record
  [txn-context record]
  (run-in-context txn-context
                  (fn [^FDBRecordStore store]
                    (.saveRecord store record))))

(defn insert-record
  [txn-context record]
  (run-in-context txn-context
                  (fn [^FDBRecordStore store]
                    (.insertRecord store record))))

(defn save-record-batch
  [txn-context batch]
  (run-in-context txn-context
                  (fn [^FDBRecordStore store]
                    (run! #(.saveRecord store %) batch))))

(defn delete-record
  ([txn-context ^Tuple k]
   (run-in-context txn-context
                   (fn [^FDBRecordStore store]
                     (.deleteRecord store k))))
  ([txn-context record-type items]
   (delete-record txn-context (key-for* txn-context record-type items))))

(defn load-record
  ([txn-context ^Tuple k]
   (run-in-context txn-context
                   (fn [^FDBRecordStore store]
                     (.loadRecord store k))))
  ([txn-context record-type items]
   (load-record txn-context (key-for* txn-context record-type items))))

(defn exists?
  ([txn-context ^Tuple k]
   (run-in-context txn-context
                   (fn [^FDBRecordStore store]
                     (.loadRecord store k))))
  ([txn-context record-type items]
   (load-record txn-context (key-for* txn-context record-type items))))

(defn execute-query
  ([txn-context query]
   (execute-query txn-context query {}))
  ([txn-context query opts]
   (run-async txn-context (store-query-fn (as-query query) opts)))
  ([txn-context query opts values]
   (execute-query txn-context query (assoc opts ::values values))))

(defn list-query
  ([txn-context query]
   (list-query txn-context query {}))
  ([txn-context query opts]
   (execute-query txn-context query (assoc opts ::list? true)))
  ([txn-context query opts values]
   (execute-query txn-context query (assoc opts ::list? true ::values values))))

(defn iterator-query
  ([txn-context query]
   (list-query txn-context query {}))
  ([txn-context query opts]
   (execute-query txn-context query (assoc opts ::iterator? true))))

(defn delete-all-records
  [txn-context]
  (run-in-context
   txn-context
   (fn [^FDBRecordStore store] (.deleteAllRecords store))))

(defn delete-by-query
  "Delete all records surfaced by a query"
  [txn-context query]
  (run-async
   txn-context
   (fn [^FDBRecordStore store]
     (let [opts {::foreach  #(delete-record store (record-primary-key %))}]
       (run-async store (store-query-fn (as-query query) opts))))))

(defn ^TupleRange prefix-range
  [txn-context record-type items]
  (let [fixed  (butlast items)
        prefix (last items)
        range   (TupleRange/prefixedBy (str prefix))]
    (.prepend range (key-for* txn-context record-type fixed))))

(defn ^TupleRange all-of-range
  [txn-context record-type items]
  (TupleRange/allOf (key-for* txn-context record-type items)))

(defn ^TupleRange greater-than-range
  [txn-context record-type items]
  (TupleRange/between (key-for* txn-context record-type items)
                      nil))

(defn ^TupleRange between
  [txn-context record-type items start end]
  (TupleRange/between
   (key-for* txn-context record-type (conj (vec items) start))
   (key-for* txn-context record-type (conj (vec items) end))))

(defn scan-range
  [txn-context ^TupleRange range opts]
  (run-async
   txn-context
   (fn [^FDBRecordStore store]
     (-> (.scanRecords store range nil ScanProperties/FORWARD_SCAN)
         (cursor/apply-transforms opts)))))

(defn scan-prefix
  [txn-context record-type items opts]
  (scan-range txn-context (prefix-range txn-context record-type items) opts))

(defn delete-by-range
  [txn-context ^TupleRange range]
  (let [callback (fn [store] #(delete-record store (record-primary-key %)))]
    (run-async txn-context #(scan-range % range {::foreach (callback %)}))))

(defn delete-by-prefix-scan
  "Delete all records surfaced by a prefix scan"
  [txn-context record-type items]
  (delete-by-range txn-context (prefix-range txn-context record-type items)))

(defn delete-by-key-component
  "In cases where composite keys are used, this can be used to clear
   all records for a specific composite key prefix"
  [txn-context record-type items]
  (delete-by-range txn-context (all-of-range txn-context record-type items)))
