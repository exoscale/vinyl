(ns exoscale.vinyl.scan-test
  (:require [clojure.test              :as test :refer [deftest testing is are]]
            [exoscale.vinyl.aggregates :as agg]
            [exoscale.vinyl.payload    :as p :refer [object->record]]
            [exoscale.vinyl.store      :as store]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]]))


(test/use-fixtures :once ds/with-open-fdb)

(defn install-records
  [db paths]
  (run! (partial store/insert-record-batch db)
        (partition-all 5000 (map object->record paths))))

(defn record-generator
  "lazy sequence of fake entries for a bucket"
  [bucket-name size]
  (for [i (range size)]
    {:bucket bucket-name :path (format "files/%08d.txt" i) :size i}))

(defn max-incrementor
  [ceiling]
  (fn [x _]
    (let [incremented (inc x)]
      (cond-> incremented (>= incremented ceiling) reduced))))

(defn incrementor
  [x _]
  (inc x))

(defn inc-prefix
  "Given an object path, yield the next semantic one."
  [^String p]
  (when (seq p)
    (let [[c & s]  (reverse p)
          reversed (conj s (-> c int inc char))]
      (reduce str "" (reverse reversed)))))

;; For the sake of test run times we keep this small, bump at will
;; in the REPL!
(def small-test-data
  (record-generator "small-bucket" 100))

(deftest large-range-scan-test-on-small-data

  (install-records *db* small-test-data)

  (testing "we get back expected values"
    (is (= 100
           (agg/compute *db* :count-not-null :Object :path_count "small-bucket")))

    (is (= 100
           @(store/long-query-reducer *db* incrementor 0
                                      [:Object [:= :bucket "small-bucket"]])))

    (is (= 90
           @(store/long-query-reducer *db* incrementor 0
                                      [:Object [:and
                                                [:= :bucket "small-bucket"]
                                                [:>= :path "files/00000010.txt"]]])))
    (is (= 90
           @(store/long-query-reducer *db* incrementor 0
                                      [:Object [:and
                                                [:= :bucket "small-bucket"]
                                                [:>= :path "files/00000010.txt"]
                                                [:< :path (inc-prefix "files/")]]])))
    (is (= 1
           @(store/long-query-reducer *db* incrementor 0
                                      [:Object [:and
                                                [:= :bucket "small-bucket"]
                                                [:starts-with? :path "files/00000010.txt"]]]))))
    (is (= 100
           @(store/long-range-reducer *db* incrementor 0 :Object ["small-bucket"])))

    (is (= 100
           @(store/long-range-reducer *db* incrementor 0 :Object ["small-bucket" ""])))

    (is (= 100
           @(store/long-range-reducer *db* incrementor 0 :Object ["small-bucket" "files/"])))

    (is (= 90
           @(store/long-range-reducer *db* incrementor 0 :Object ["small-bucket" "files/"] {::store/marker "files/00000010.txt"})))

    (is (= 90
           @(store/long-range-reducer *db* incrementor 0 :Object ["small-bucket" ""] {::store/marker "files/00000010.txt"})))

  (testing "returning a `reduced` value stops iteration"
    (is (= 10
           @(store/long-query-reducer *db* (max-incrementor 10) 0
                                      [:Object [:= :bucket "small-bucket"]]))))

  (testing "maintained indices are consistent"
    (is (= (agg/compute *db* :count-not-null :Object :path_count "small-bucket")
           @(store/long-query-reducer *db* incrementor 0
                                      [:Object [:= :bucket "small-bucket"]]))))

  (testing "incompatible operators"
    (is (thrown? java.util.concurrent.ExecutionException
                 @(store/long-query-reducer *db* incrementor 0
                                            [:Object [:and
                                                      [:= :bucket "small-bucket"]
                                                      [:starts-with? :path "files/00000010.txt"]
                                                      [:>= :path "files/"]]])))

    (is (thrown? java.util.concurrent.ExecutionException
                 @(store/long-query-reducer *db* incrementor 0
                                            [:Object [:and
                                                      [:= :bucket "small-bucket"]
                                                      [:starts-with? :path "files/00000010.txt"]
                                                      [:< :path (inc-prefix "files/")]]])))))

(comment

  ;; Here we do the same thing but with a larger amount of records
  (def db (store/start (assoc ds/demostore :env "dev")))

  ;; Installing a few records
  (install-records db (record-generator "mini-bucket" 100))
  (install-records db (record-generator "small-bucket" 100000))

  ;; Run this if you want 4M records
  #_(install-records db (record-generator "big-test-bucket" 4000000))

  (agg/compute db :count-not-null :Object :path_count "big-test-bucket")

  ;; This should yield 4000000
  (time
   @(store/long-query-reducer db incrementor 0
                              [:Object [:= :bucket "big-test-bucket"]]))

  ;; This should yield 100 and return quick
  @(store/long-query-reducer db (max-incrementor 100) 0
                             [:Object [:= :bucket "big-test-bucket"]]))
