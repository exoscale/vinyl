(ns exoscale.vinyl.query-test
  (:require [clojure.test              :as test :refer [deftest testing is are]]
            [exoscale.vinyl.payload    :as p]
            [exoscale.vinyl.store      :as store]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]]))

(test/use-fixtures :each ds/with-create-fdb)

(deftest query-test
  (let [opts     {::store/transform p/parse-record
                  ;; indexes on account_id|id, username
                  ::store/required-results [:name]}]

    (testing "required-results: PK + selective index"
      (is
       (= [{:id 1 :account-id 1 :name "a1u1" :email "a1u1@hello.com"}
           {:id 2 :account-id 1 :name "a1u2" :email "a1u2@hello.com"}]
          @(store/list-query *db* [:User [:starts-with? :name "a1"]] opts))))))
