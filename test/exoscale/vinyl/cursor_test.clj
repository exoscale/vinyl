(ns exoscale.vinyl.cursor-test
  (:require [clojure.test          :refer [deftest are]]
            [exoscale.vinyl.cursor :refer [apply-transforms]]
            [exoscale.vinyl.store  :as    store])
  (:import com.apple.foundationdb.record.RecordCursor))

(defn from-list
  "Transform a list of items to a `RecordCursor` instance"
  [items]
  (RecordCursor/fromList (seq items)))

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
