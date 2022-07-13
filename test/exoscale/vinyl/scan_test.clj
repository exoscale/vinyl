(ns exoscale.vinyl.scan-test
  (:require [clojure.test              :as test :refer [deftest testing is]]
            [exoscale.vinyl.aggregates :as agg]
            [exoscale.vinyl.payload    :as p :refer [object->record]]
            [exoscale.vinyl.store      :as store]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]]
            [exoscale.vinyl.tuple :as tuple]))

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

(deftest large-range-scan-static-data
  (testing "we get back expected values"
    (is (= [{:id 1, :location {:name "Lausanne", :zip-code 1000}}
            {:id 2, :location {:name "Lausanne", :zip-code 1001}}
            {:id 3, :location {:name "Lausanne", :zip-code 1002}}
            {:id 4, :location {:name "Lausanne", :zip-code 1003}}
            {:id 5, :location {:name "Lausanne", :zip-code 1004}}
            {:id 6, :location {:name "Neuchatel", :zip-code 2000}}]
           @(store/long-range-transduce *db* (map p/parse-record) (completing conj) [] :City [""] {})))

    (is (= [{:id 1, :location {:name "Lausanne", :zip-code 1000}}
            {:id 2, :location {:name "Lausanne", :zip-code 1001}}
            {:id 3, :location {:name "Lausanne", :zip-code 1002}}
            {:id 4, :location {:name "Lausanne", :zip-code 1003}}
            {:id 5, :location {:name "Lausanne", :zip-code 1004}}]
           @(store/long-range-transduce *db* (map p/parse-record) (completing conj) [] :City ["Lausanne" nil] {})))

    (is (= [{:id 3, :location {:name "Lausanne", :zip-code 1002}}
            {:id 4, :location {:name "Lausanne", :zip-code 1003}}
            {:id 5, :location {:name "Lausanne", :zip-code 1004}}]
           @(store/long-range-transduce *db* (map p/parse-record) (completing conj) [] :City ["Lausanne" nil] {::store/continuation [1002]})))

    (is (= [{:id 3, :location {:name "Lausanne", :zip-code 1002}}
            {:id 4, :location {:name "Lausanne", :zip-code 1003}}
            {:id 5, :location {:name "Lausanne", :zip-code 1004}}
            {:id 6, :location {:name "Neuchatel", :zip-code 2000}}]
           @(store/long-range-transduce *db* (map p/parse-record) (completing conj) [] :City [""] {::store/continuation ["Lausanne" 1002]})))))

(deftest large-range-scan-over-raw-key-values
  (let [city-key-xf              (comp (map #(.getKey %))
                                       (map tuple/from-bytes)
                                       (map (juxt #(tuple/get-string % 4) #(tuple/get-long % 5))))
        city-key-reducer         (fn [acc [name zip-code]]
                                   (conj acc {:name name :zip-code zip-code}))
        city-key-reduced-reducer (fn [acc [name zip-code]]
                                   (cond-> (conj acc {:name name :zip-code zip-code})
                                     (= 1003 zip-code) (reduced)))]

    (testing "we get back expected values"
      (is (= [{:name "Lausanne", :zip-code 1000}
              {:name "Lausanne", :zip-code 1001}
              {:name "Lausanne", :zip-code 1002}
              {:name "Lausanne", :zip-code 1003}
              {:name "Lausanne", :zip-code 1004}
              {:name "Neuchatel", :zip-code 2000}]
             @(store/long-range-transduce *db* city-key-xf (completing city-key-reducer) [] :City [""] {::store/raw? true}))))
    (testing "we get back expected values with a limit"
      (is (= [{:name "Lausanne", :zip-code 1000}
              {:name "Lausanne", :zip-code 1001}]
             @(store/long-range-transduce *db* city-key-xf (completing city-key-reducer) [] :City [""] {::store/raw? true
                                                                                                        ::store/limit 2}))))
    (testing "we get back expected values with a marker"
      (is (= [{:name "Neuchatel", :zip-code 2000}]
             @(store/long-range-transduce *db* city-key-xf (completing city-key-reducer) [] :City [""] {::store/raw? true
                                                                                                        ::store/limit 2
                                                                                                        ::store/continuation ["Lausannf"]}))))
    (testing "we get back expected values with a early reduced reducer"
     (is (= [{:name "Lausanne", :zip-code 1000}
             {:name "Lausanne", :zip-code 1001}
             {:name "Lausanne", :zip-code 1002}
             {:name "Lausanne", :zip-code 1003}]
            @(store/long-range-transduce *db* city-key-xf (completing city-key-reduced-reducer) [] :City [""] {::store/raw? true}))))))

(deftest large-range-scan-test-on-small-data

  (install-records *db* (record-generator "small-bucket" 100))
  (install-records *db* (record-generator "small-bucket-a" 100))

  (testing "we get back expected values"
    (is (= 100
           (agg/compute *db* :count-not-null :Object :path_count "small-bucket")))

    (is (= 100
           @(store/long-query-reduce *db* incrementor 0
                                     [:Object [:= :bucket "small-bucket"]])))

    (is (= 90
           @(store/long-query-reduce *db* incrementor 0
                                     [:Object [:and
                                               [:= :bucket "small-bucket"]
                                               [:>= :path "files/00000010.txt"]]])))
    (is (= 90
           @(store/long-query-reduce *db* incrementor 0
                                     [:Object [:and
                                               [:= :bucket "small-bucket"]
                                               [:>= :path "files/00000010.txt"]
                                               [:< :path (inc-prefix "files/")]]])))
    (is (= 1
           @(store/long-query-reduce *db* incrementor 0
                                     [:Object [:and
                                               [:= :bucket "small-bucket"]
                                               [:starts-with? :path "files/00000010.txt"]]])))
    ;; As small-bucket is considered as a prefix of small-bucket-a, we get both
    ;; the objects from small-bucket and small-bucket-a (and it's expected)
    (is (= 200
           @(store/long-range-reduce *db* incrementor 0 :Object ["small-bucket"])))

    (is (= 100
           @(store/long-range-reduce *db* incrementor 0 :Object ["small-bucket" ""])))

    (is (= 100
           @(store/long-range-reduce *db* incrementor 0 :Object ["small-bucket" "files/"])))

    (is (= 90
           @(store/long-range-reduce *db* incrementor 0 :Object ["small-bucket" "files/"] {::store/continuation ["files/00000010.txt"]})))

    (is (= 90
           @(store/long-range-reduce *db* incrementor 0 :Object ["small-bucket" ""] {::store/continuation ["files/00000010.txt"]})))

    (testing "returning a `reduced` value stops iteration"
      (is (= 10
             @(store/long-query-reduce *db* (max-incrementor 10) 0
                                       [:Object [:= :bucket "small-bucket"]]))))

    (testing "maintained indices are consistent"
      (is (= (agg/compute *db* :count-not-null :Object :path_count "small-bucket")
             @(store/long-query-reduce *db* incrementor 0
                                       [:Object [:= :bucket "small-bucket"]]))))

    (testing "incompatible operators"
      (is (thrown? java.util.concurrent.ExecutionException
                   @(store/long-query-reduce *db* incrementor 0
                                             [:Object [:and
                                                       [:= :bucket "small-bucket"]
                                                       [:starts-with? :path "files/00000010.txt"]
                                                       [:>= :path "files/"]]])))

      (is (thrown? java.util.concurrent.ExecutionException
                   @(store/long-query-reduce *db* incrementor 0
                                             [:Object [:and
                                                       [:= :bucket "small-bucket"]
                                                       [:starts-with? :path "files/00000010.txt"]
                                                       [:< :path (inc-prefix "files/")]]]))))))

(deftest large-range-scan-test-on-overlapping-bucket-names
  (install-records *db* (record-generator "bucket-66"  20))
  (install-records *db* (record-generator "bucket-666" 20))

  (testing "we get back expected values"

    ;; This is an example of what to NOT do, listing an object without a final
    ;; marker would include all elements with the same prefix
    (is (= 40
           @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-66"])))
    (is (= 20
           @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-666"])))

    ;; Using the multi-elements continuation, we can safely iterate over a
    ;; "table" when the primary-key is a concatenation of multiple fields
    (is (= 40
           @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-"])))
    (is (= 30
           @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-"] {::store/continuation ["bucket-66" "files/00000010.txt"]})))

    (doseq [bucket ["bucket-66" "bucket-666"]]
      (is (= 20
             (agg/compute *db* :count-not-null :Object :path_count bucket)))

      (is (= 20
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket nil])))

      (is (= 15
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket nil] {::store/continuation ["files/00000005.txt"]})))

      (is (= 0
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket nil] {::store/continuation ["files/66666666.txt"]})))

      (is (= 10
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket "files/0000000"])))

      (is (= 10
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket "files/0000001"])))

      (is (= 5
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket "files/0000000"] {::store/continuation ["files/00000005.txt"]})))

      (is (= 5
             @(store/long-range-reduce *db* incrementor 0 :Object [bucket "files/0000001"] {::store/continuation ["files/00000015.txt"]})))

      (is (thrown? java.util.concurrent.ExecutionException
                   @(store/long-range-reduce *db* incrementor 0 :Object [bucket "files/0000000"] {::store/continuation ["files/00000015.txt"]})))

      (is (thrown? java.util.concurrent.ExecutionException
                   @(store/long-range-reduce *db* incrementor 0 :Object [bucket "files/0000001"] {::store/continuation ["files/00000005.txt"]}))))))

(deftest delete-by-prefix-scan-test
  @(store/delete-by-prefix-scan *db* :Object ["bucket-66"])
  (install-records *db* (record-generator "bucket-66"  20))
  (install-records *db* (record-generator "bucket-666" 20))

  (testing "we delete all the objects on all buckets starting with a prefix"
    (is (= 20 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-66" nil])))
    (is (= 20 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-666" nil])))
    @(store/delete-by-prefix-scan *db* :Object ["bucket-66"])
    (is (= 0 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-66" nil])))
    (is (= 0 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-666" nil])))))

(deftest delete-by-tuple-prefix-scan-test
  @(store/delete-by-prefix-scan *db* :Object ["bucket-66"])
  (install-records *db* (record-generator "bucket-66"  20))
  (install-records *db* (record-generator "bucket-666" 20))

  (testing "we delete only the objects on starting with a specific tuple prefix"
    (is (= 20 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-66" nil])))
    (is (= 20 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-666" nil])))
    @(store/delete-by-tuple-prefix-scan *db* :Object ["bucket-66"])
    (is (= 0 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-66" nil])))
    (is (= 20 @(store/long-range-reduce *db* incrementor 0 :Object ["bucket-666" nil])))
    @(store/delete-by-tuple-prefix-scan *db* :Object ["bucket-666"])))

(comment

  ;; Here we do the same thing but with a larger amount of records
  (def db (store/start (assoc ds/demostore :env "dev")))

  ;; Installing a few records
  (install-records db (record-generator "mini-bucket" 100))
  (install-records db (record-generator "small-bucket" 100000))

  ;; Run this if you want 4M records
  (install-records db (record-generator "big-test-bucket" 4000000))

  (time @(store/long-range-transduce db identity (completing incrementor) 0
                                     :Object ["big-test-bucket"]))

  (time @(store/long-range-transduce db identity (completing incrementor) 0
                                     :Object ["big-test-bucket"]
                                     {::store/raw? true}))

  (agg/compute db :count-not-null :Object :path_count "big-test-bucket")

  ;; This should yield 4000000
  (time
   @(store/long-query-reduce db incrementor 0
                             [:Object [:= :bucket "big-test-bucket"]]))

  ;; This should yield 100 and return quick
  @(store/long-query-reduce db (max-incrementor 100) 0
                            [:Object [:= :bucket "big-test-bucket"]]))
