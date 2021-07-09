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
   com.apple.foundationdb.record.IndexScanType
   com.apple.foundationdb.record.metadata.Index
   com.apple.foundationdb.record.TupleRange
   com.apple.foundationdb.record.IsolationLevel
   com.apple.foundationdb.record.ExecuteProperties
   com.apple.foundationdb.record.ScanProperties
   com.apple.foundationdb.record.query.plan.plans.QueryPlan
   com.apple.foundationdb.record.RecordMetaDataProvider
   com.apple.foundationdb.record.RecordMetaData
   com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner
   com.apple.foundationdb.tuple.Tuple
   java.lang.AutoCloseable
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
    "Return this context's record metadata")
  (new-runner [this]
    "Return a runner to handle retryable logic"))

(defn ^FDBDatabaseRunner runner-opts
  [^FDBDatabaseRunner runner
   {::keys [max-attempts
            initial-delay
            max-delay
            transaction-timeout]}]
  (when (some? max-attempts)
    (.setMaxAttempts runner (int max-attempts)))
  (when (some? initial-delay)
    (.setInitialDelayMillis runner (long initial-delay)))
  (when (some? max-delay)
    (.setMaxDelayMillis runner (long max-delay)))
  (when (some? transaction-timeout)
    (.setTransactionTimeoutMillis runner (long transaction-timeout)))
  runner)

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
  (new-runner [this]
    (.newRunner ^FDBDatabase (::db this)))
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

(defn wrapped-runner
  [db opts]
  (let [runner (-> (new-runner db) (runner-opts opts))]
    (reify
      DatabaseContext
      (get-metadata [_] (get-metadata db))
      (new-runner [_] runner)
      (run-async [_ f]
        (.runAsync
         runner
         (reify Function
           (apply [_ context]
             (.thenCompose (async-store-from-builder (::builder db) context)
                           (fn/make-fun f))))))
      (run-in-context [_ f]
        (.run
         runner
         (reify Function
           (apply [_ context]
             (f (store-from-builder (::builder db) context))))))
      AutoCloseable
      (close [_] (.close runner)))))

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

(def ^:no-doc  ^:private
  known-scan-types
  {::by-value       IndexScanType/BY_VALUE
   ::by-group       IndexScanType/BY_GROUP
   ::by-rank        IndexScanType/BY_RANK
   ::by-time-window IndexScanType/BY_TIME_WINDOW
   ::by-text-token  IndexScanType/BY_TEXT_TOKEN})

(defn ^IndexScanType as-scan-type
  [t]
  (if (instance? IndexScanType t)
    t
    (get known-scan-types t IndexScanType/BY_VALUE)))

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

(defn insert-record-batch
  [txn-context batch]
  (run-in-context txn-context
                  (fn [^FDBRecordStore store]
                    (run! #(.insertRecord store %) batch))))

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

(defn ^TupleRange all-of-range
  [txn-context record-type items]
  (if (= ::raw record-type)
    (TupleRange/allOf (tuple/from-seq items))
    (TupleRange/allOf (key-for* txn-context record-type items))))

(defn ^TupleRange prefix-range
  [txn-context record-type items]
  (let [fixed  (butlast items)
        prefix (last items)
        range   (TupleRange/prefixedBy (str prefix))]
    (if (seq prefix)
      (.prepend range (if (= record-type ::raw)
                        (tuple/from-seq fixed)
                        (key-for* txn-context record-type fixed)))
      ;; An empty prefix would result in a bad range, we want an
      ;; all-of range on the leading parts of the tuple in this
      ;; case.
      (all-of-range txn-context record-type fixed))))

(defn inc-prefix
  "Given an object path, yield the next semantic one."
  [^String p]
  (when (seq p)
    (let [[c & s]  (reverse p)
          reversed (conj s (-> c int inc char))]
      (reduce str "" (reverse reversed)))))

(defn ^TupleRange marker-range
  [txn-context record-type items marker]
  (if marker
    (TupleRange/between
     (key-for* txn-context record-type (conj (vec (butlast items)) marker))
     (let [prefix (last items)]
       (if (seq prefix)
         (key-for* txn-context record-type (conj (vec (butlast items)) (inc-prefix prefix)))
         (key-for* txn-context record-type (conj (vec (drop-last 2 items)) (inc-prefix (nth items (- (count items) 2))))))))
    (prefix-range txn-context record-type items)))

(defn ^TupleRange greater-than-range
  [txn-context record-type items]
  (TupleRange/between (if (= ::raw record-type)
                        (tuple/from-seq items)
                        (key-for* txn-context record-type items))
                      nil))

(defn ^TupleRange between
  [txn-context record-type items start end]
  (if (= ::raw record-type)
    (TupleRange/between
     (tuple/from-seq (conj (vec items) start))
     (tuple/from-seq (conj (vec items) end)))
    (TupleRange/between
     (key-for* txn-context record-type (conj (vec items) start))
     (key-for* txn-context record-type (conj (vec items) end)))))

(defn ^IsolationLevel as-isolation-level
  [level]
  (if (= ::snapshot level)
    IsolationLevel/SNAPSHOT
    IsolationLevel/SERIALIZABLE))

(defn ^ExecuteProperties execute-properties
  [{::keys [fail-on-scan-limit-reached?
            isolation-level
            skip
            limit]
    :as    props}]
  (if (nil? props)
    ExecuteProperties/SERIAL_EXECUTE
    (-> (ExecuteProperties/newBuilder)
        (.setFailOnScanLimitReached (boolean fail-on-scan-limit-reached?))
        (.setIsolationLevel (as-isolation-level isolation-level))
        (cond-> (some? skip) (.setSkip (int skip)))
        (cond-> (some? limit) (.setReturnedRowLimit (int limit)))
        (.build))))

(defn ^ScanProperties scan-properties
  [{::keys [reverse?] :as props}]
  (if (nil? props)
    ScanProperties/FORWARD_SCAN
    (.asScanProperties (execute-properties props) (boolean reverse?))))

(defn scan-range
  [txn-context ^TupleRange range opts]
  (let [props               (scan-properties opts)
        ^bytes continuation (::continuation opts)]
    (run-async
     txn-context
     (fn [^FDBRecordStore store]
       (-> (.scanRecords store range continuation props)
           (cursor/apply-transforms opts))))))

(defn scan-prefix
  [txn-context record-type items opts]
  (scan-range txn-context (prefix-range txn-context record-type items) opts))

(defn ^Index metadata-index
  [^RecordMetaData metadata ^String index-name]
  (.getIndex metadata index-name))

(defn scan-index
  [txn-context index-name scan-type ^TupleRange range ^bytes continuation opts]
  (let [props     (scan-properties opts)
        scan-type (as-scan-type scan-type)
        index     (-> txn-context get-metadata (metadata-index index-name))]
    (run-async
     txn-context
     (fn [^FDBRecordStore store]
       (-> (.scanIndex store index scan-type range continuation props)
           (cursor/apply-transforms opts))))))

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

(def ^:private ^:no-doc runner-params
  {::max-attempts        Integer/MAX_VALUE
   ::max-delay           2
   ::initial-delay       2})

(defn long-range-reducer
  "A reducer over large ranges.
   Results are reduced into an accumulator with the help of the reducing
   function `f`.
   The accumulator is initiated to `init`. `clojure.core.reduced` is honored.

   Obviously, this approach does away with any consistency guarantees usually
   offered by FDB.

   Results being accumulated in memory, this also means that care must be
   taken with the accumulator."
  ;; Some cliff notes to read the code below.
  ;; The basic idea is that scanning is done while it works, up until
  ;; the point where an exception will be raised.
  ;;
  ;; By default, `run-async` uses a simple exponential back-off algorithm
  ;; between transaction function retries. In this case we want to avoid that.
  ;; To that effect, a runner is created with specific parameters (a large
  ;; `max-attempts` value, as well as minimum viable delays.
  ;;
  ;; We then call `apply-reduce` on the returned cursor from our query with a
  ;; twist: every visited element will get its continuation stored in an atom.
  ;; When interrupted, the function will be retried, which pops the last seen
  ;; continuation.
  ;;
  ([db f init record-type items]
   (long-range-reducer db f init record-type items {}))
  ([db f init record-type items {::keys [marker] :as opts}]
   (let [cont    (atom nil)
         result  (atom init)
         props   (execute-properties (dissoc opts ::limit))
         runner  (wrapped-runner db runner-params)
         range   (marker-range runner record-type items marker)]
     (-> (run-async
          runner
          (fn [^FDBRecordStore store]
            (-> (.scanRecords store range @cont (scan-properties props))
                (cursor/apply-reduce f result #(reset! cont %)))))
         (fn/close-on-complete runner)))))

(defn long-query-reducer
  "A reducer over large queries. Accepts queries as per `execute-query`. Results
   are reduced into an accumulator with the help of the reducing function `f`.
   The accumulator is initiated to `init`. `clojure.core.reduced` is honored.

   Obviously, this approach does away with any consistency guarantees usually
   offered by FDB.

   Results being accumulated in memory, this also means that care must be
   taken with the accumulator."
  ;; Some cliff notes to read the code below.
  ;; The basic idea is that scanning is done while it works, up until
  ;; the point where an exception will be raised.
  ;;
  ;; By default, `run-async` uses a simple exponential back-off algorithm
  ;; between transaction function retries. In this case we want to avoid that.
  ;; To that effect, a runner is created with specific parameters (a large
  ;; `max-attempts` value, as well as minimum viable delays.
  ;;
  ;; We then call `apply-reduce` on the returned cursor from our query with a
  ;; twist: every visited element will get its continuation stored in an atom.
  ;; When interrupted, the function will be retried, which pops the last seen
  ;; continuation.
  ;;
  ([db f init query]
   (long-query-reducer db f init query {}))
  ([db f init query {::keys [values] :as opts}]
   (let [cont    (atom nil)
         result  (atom init)
         props   (execute-properties (dissoc opts ::limit))
         ctx     (if (some? values) (query/bindings values) query/empty-context)
         q       (as-query query)
         runner  (wrapped-runner db runner-params)]
     (-> (run-async
          runner
          (fn [^FDBRecordStore store]
            (-> (.execute (.planQuery store q)
                          store
                          ctx
                          ^bytes @cont
                          props)
                (cursor/apply-reduce f result #(reset! cont %)))))
         (fn/close-on-complete runner))))
  ([db f init query opts values]
   (long-query-reducer db f init query (assoc opts ::values values))))
