(ns exoscale.vinyl.cursor
  "Utilities to work with `RecordCursor`"
  (:require [exoscale.vinyl.fn          :as fn])
  (:import com.apple.foundationdb.async.AsyncUtil
           com.apple.foundationdb.async.AsyncIterable
           com.apple.foundationdb.record.RecordCursor
           com.apple.foundationdb.record.RecordCursorResult
           com.apple.foundationdb.KeyValue
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier))

(set! *warn-on-reflection* true)

(defprotocol CursorHolder
  (as-list     [this] "Transform a cursor or cursor future to a list")
  (as-iterator [this] "Transform a cursor or cursor future to an iterator"))

(defn apply-transduce
  "A variant of `RecordCursor::reduce` that honors `reduced?` and supports
   transducers.
   Hopefully https://github.com/FoundationDB/fdb-record-layer/pull/1272
   gets in which will provide a way to do this directly from record layer.

   When `cont-fn` is given, it will be called on the last seen continuation
   byte array for every new element."
  ([^RecordCursor cursor xform f init cont-fn]
   (let [reducer (if (some? xform) (xform f) f)
         acc     (if (instance? clojure.lang.Atom init)
                   init
                   (atom (or init (f))))]
     (.thenApply
      (AsyncUtil/whileTrue
       (reify Supplier
         (get [_]
           (-> cursor
               .onNext
               (.thenApply
                (fn/make-fun
                 (fn [^RecordCursorResult result]
                   (when (ifn? cont-fn)
                     (-> result .getContinuation .toBytes cont-fn))
                   (let [next?   (.hasNext result)
                         new-acc (when next? (swap! acc reducer (.get result)))]
                     (and (not (reduced? new-acc)) next?))))))))
       (.getExecutor cursor))
      (fn/make-fun (fn [_]
                     (unreduced
                      (cond-> @acc
                        (some? xform)
                        reducer)))))))
  ([cursor f init cont-fn]
   (apply-transduce cursor nil f init cont-fn))
  ([cursor f init]
   (apply-transduce cursor nil f init nil)))

(defn apply-iterable-transduce
  ([^AsyncIterable async-iterable xform f init cont-fn]
   (let [reducer (if (some? xform) (xform f) f)
         acc     (if (instance? clojure.lang.Atom init)
                   init
                   (atom (or init (f))))
         iterator (.iterator async-iterable)]

     ;; Using the blocking API is actually faster than using the non-blocking
     ;; one (2 times faster according to naive benchmarks)
     (while (and (not (reduced? @acc)) (.hasNext iterator))
       (let [key-value ^KeyValue (.next iterator)]
         (when (ifn? cont-fn)
           (-> key-value .getKey cont-fn))
         (swap! acc reducer key-value)))

     (CompletableFuture/completedFuture
      (unreduced (cond-> @acc
                   (some? xform)
                   reducer)))))
  ([async-iterable f init cont-fn]
   (apply-iterable-transduce async-iterable nil f init cont-fn))
  ([async-iterable f init]
   (apply-iterable-transduce async-iterable nil f init nil)))

(defn apply-transforms
  "Apply transformations to a record cursor."
  [^RecordCursor cursor
   {:exoscale.vinyl.store/keys [list? skip limit transform reduce-init
                                transducer reducer filter foreach iterator?]}]
  (let [list?     (if reducer false list?)
        iterator? (if reducer false iterator?)
        foreach!  (fn [^RecordCursor c] (.forEach c (fn/make-fun foreach)))]
    (cond-> cursor
      (some? filter)
      (.filter (fn/make-fun filter))
      (some? skip)
      (.skip (int skip))
      (some? limit)
      (.limitRowsTo (int limit))
      (some? transform)
      (.map (fn/make-fun transform))
      (some? reducer)
      (apply-transduce transducer reducer reduce-init nil)
      (some? foreach)
      (foreach!)
      (true? list?)
      (as-list)
      (true? iterator?)
      (as-iterator))))

(deftype ReducibleCursor [cursor]
  clojure.lang.IReduceInit
  (reduce [_ f init]
    @(apply-transduce cursor f init)))

(defn reducible
  [cursor]
  (ReducibleCursor. cursor))

(extend-protocol CursorHolder
  RecordCursor
  (as-list [this] (.asList this))
  (as-iterator [this] (.asIterator this))
  CompletableFuture
  (as-list [this] (.thenCompose this (fn/make-fun as-list)))
  (as-iterator [this] (.thenCompose this (fn/make-fun as-iterator))))
