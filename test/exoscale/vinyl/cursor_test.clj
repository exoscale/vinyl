(ns exoscale.vinyl.cursor-test
  (:require [clojure.test :refer [deftest are is]]
            [exoscale.vinyl.cursor :refer [apply-transforms apply-transduce-with-reducer]]
            [exoscale.vinyl.demostore :as ds :refer [*db*]]
            [exoscale.vinyl.store :as store])
  (:import [com.apple.foundationdb.record RecordCursor]
           [com.apple.foundationdb FDBException]
           [java.util Iterator]))

(defn from-list
  "Transform a list of items to a `RecordCursor` instance"
  [items]
  (RecordCursor/fromList (seq items)))

(defn from-iterator
  [iterator]
  (RecordCursor/fromIterator iterator))

(defrecord FaultyIterator [items pos error-index]
  Iterator
  (forEachRemaining [_ _action])
  (hasNext [_]
    (< @pos (count items)))
  (next [_]
    (if (= @pos @error-index)
      (do
        (vreset! error-index -1)
        (throw (FDBException. "Retryable error" 1007)))
      (let [result (nth items @pos)]
        (vswap! pos inc)
        result)))
  (remove [_]))

(defn make-faulty-iterator [items error-index]
  (->FaultyIterator items (volatile! 0) (volatile! error-index)))

(defn reduce-plus
  [x y]
  (let [acc (+ x y)]
    (cond-> acc (> acc 10) reduced)))

(deftest reduce-test
  (are [items reducer init result]
       (= result @(apply-transforms (from-list items)
                                    {::store/reducer     reducer
                                     ::store/reduce-init init}))
    [0 1 2 3 4 5 6] +           0 21
    [0 1 2 3 4 5 6] reduce-plus 0 15))

(deftest transduce-test
  (are [items reducer init result]
       (= result @(apply-transforms (from-list items)
                                    {::store/reducer     reducer
                                     ::store/transducer  (map inc)
                                     ::store/reduce-init init}))
    [0 1 2 3 4 5 6] +           0 28
    [0 1 2 3 4 5 6] (completing reduce-plus) 0 15))

(deftest stateful-transducer-test
  (is (= [[1 2 3] [4 5 6] [7 8 9] [10]]
         (ds/with-build-fdb
           (fn [] (let [cursor (from-iterator (make-faulty-iterator [1 2 3 4 5 6 7 8 9 10] 5))
                        transducer (partition-all 3)
                        reducer-fn (completing (fn [acc items] (conj acc items)))
                        reducer (transducer reducer-fn)
                        acc (atom [])]
                    @(store/run-async *db* (fn [_store] (apply-transduce-with-reducer
                                                         cursor
                                                         transducer
                                                         nil
                                                         reducer
                                                         acc)))))))))
