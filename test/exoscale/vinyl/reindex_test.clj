(ns exoscale.vinyl.reindex-test
  (:require  [clojure.test :refer [is deftest] :as test]

             [exoscale.vinyl.demostore  :as ds :refer [*db*]]
             [exoscale.vinyl.store      :as store]
             [exoscale.vinyl.payload    :as p]
             [exoscale.vinyl.tuple      :as tuple]))

(test/use-fixtures :each ds/with-create-fdb)

(defn- refcounting-frequencies
  ([] (refcounting-frequencies #{}))
  ([removed]
   (let [fixtures (remove (fn [{:keys [id]}] (removed id)) (:Invoice ds/fixtures))]
     (frequencies (mapcat #(map (fn [line] (:product line)) (:lines %)) fixtures)))))

(defn- scan-refcounting-index
  ([] (scan-refcounting-index tuple/all))
  ([range]
   (let [entries @(store/scan-index *db* "refcount_index" ::store/by-group
                                    range nil {::store/list? true})]
     (into {}
           (map (juxt #(second (.getKey %)) #(tuple/get-long (.getValue %))) entries)))))

(defn- long-scan-refcounting-index
  ([]
   (long-scan-refcounting-index tuple/all))
  ([range]
   (let [entries @(store/long-index-transduce *db* identity conj [] "refcount_index" ::store/by-group range {})]
    (into {}
          (map (juxt #(second (.getKey %)) #(tuple/get-long (.getValue %))) entries)))))

(deftest reindex-test
  (is (= (refcounting-frequencies)
         (scan-refcounting-index)
         (long-scan-refcounting-index)))

  (store/reindex *db* "refcount_index" {::store/progress-log-interval 1})
  (store/reindex *db* "refcount_index" {::store/progress-log-interval 1})

  (is (= (refcounting-frequencies)
         (scan-refcounting-index)
         (long-scan-refcounting-index))))

(deftest scan-index-test
  (store/insert-record *db* (p/object->record {:bucket "bucket" :path "path" :size 2}))

  (is (= (assoc (refcounting-frequencies) "bucket" 1)
         (scan-refcounting-index)
         (long-scan-refcounting-index)))
  (store/delete-record *db* :Invoice [1 1])
  (is (= (assoc (refcounting-frequencies #{1}) "bucket" 1)
         (scan-refcounting-index)
         (long-scan-refcounting-index)))
  (store/delete-record *db* :Invoice [1 2])
  (is (= (assoc (refcounting-frequencies #{1 2}) "bucket" 1)
         (scan-refcounting-index)
         (long-scan-refcounting-index)))
  (store/delete-record *db* :Invoice [3 3])
  (is (= (assoc (refcounting-frequencies #{1 2 3}) "bucket" 1)
         (scan-refcounting-index)
         (long-scan-refcounting-index)))
  (store/delete-record *db* :Invoice [4 4])
  (is (= (assoc (refcounting-frequencies #{1 2 3 4}) "bucket" 1)
         (scan-refcounting-index (tuple/all-of ["refcount"]))
         (long-scan-refcounting-index (tuple/all-of ["refcount"]))))
  (is (= #{"p1" "p2"}
         (into #{} (map #(second (.getKey %))
                        @(store/scan-index *db* "refcount_index" ::store/by-group
                                           (tuple/all-of ["zero"]) nil {::store/list? true})))))
  (store/delete-record *db* :Invoice [4 5])
  (is (= #{"p1" "p2" "p4"}
         (into #{} (map #(second (.getKey %))
                        @(store/scan-index *db* "refcount_index" ::store/by-group
                                           (tuple/all-of ["zero"]) nil {::store/list? true})))))
  (is (= {"bucket" 1}
         (scan-refcounting-index (tuple/all-of ["refcount"]))
         (long-scan-refcounting-index (tuple/all-of ["refcount"])))))
