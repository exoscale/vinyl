(ns exoscale.vinyl.delete-test
  (:require [clojure.test :as test]
            [clojure.test :refer [deftest testing is]]
            [exoscale.vinyl.demostore :as ds]
            [exoscale.vinyl.demostore :as ds :refer [*db*]]
            [exoscale.vinyl.payload :as p]
            [exoscale.vinyl.store :as store]))

(test/use-fixtures :each ds/with-build-fdb)

(deftest delete
  (testing "delete-all-records"
    @(store/delete-all-records *db*)
    (let [opts {::store/transform p/parse-record}]
      (is (= []
             @(store/list-query *db* [:User [:starts-with? :name "a1"]] opts))))))
