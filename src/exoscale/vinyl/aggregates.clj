(ns exoscale.vinyl.aggregates
  (:require [exoscale.vinyl.fn    :as fn]
            [exoscale.vinyl.store :as store]
            [exoscale.vinyl.tuple :as tuple])
  (:import
   com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore
   com.apple.foundationdb.record.metadata.IndexAggregateFunction
   com.apple.foundationdb.record.IsolationLevel
   com.apple.foundationdb.record.FunctionNames
   com.apple.foundationdb.record.metadata.Key$Evaluated
   com.apple.foundationdb.record.metadata.Index
   java.util.concurrent.CompletableFuture
   java.util.List))

(def function-names
  {:count-not-null FunctionNames/COUNT_NOT_NULL
   :sum            FunctionNames/SUM})

;; If this gets bigger let's move to auspex
(defn then-apply
  ^CompletableFuture [^CompletableFuture ftr f]
  (.thenApply ftr (fn/make-fun f)))

(defn get-index
  ^Index [^FDBRecordStore store index-name]
  (let [record-meta (.getRecordMetaData store)]
    (.getIndex record-meta (name index-name))))

(defn evaluate-aggregate-function
  [^FDBRecordStore store ^String record-type ^IndexAggregateFunction agg param]
  (.evaluateAggregateFunction store
                              (List/of record-type)
                              agg
                              (Key$Evaluated/scalar param)
                              IsolationLevel/SERIALIZABLE))

(defn index-aggregate-function
  ^IndexAggregateFunction  [store function-name index-name]
  (let [index (get-index store index-name)]
    (IndexAggregateFunction. (str (if (string? function-name)
                                    function-name
                                    (get function-names function-name)))
                             (.getRootExpression index)
                             (.getName index))))

(defn compute
  [txn function-name record-type index-name param]
  (store/run-in-context
   txn
   (fn [^FDBRecordStore store]
     (let [aggfn (index-aggregate-function store function-name index-name)]
       (-> (evaluate-aggregate-function store (name record-type) aggfn param)
           (then-apply tuple/get-long)
           (.get))))))
