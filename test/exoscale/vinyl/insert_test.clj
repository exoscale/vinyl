(ns exoscale.vinyl.insert-test
  (:require [clojure.test              :as test :refer [deftest testing is are]]
            [exoscale.vinyl.payload    :as p]
            [exoscale.vinyl.store      :as store]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]])
  (:import [com.apple.foundationdb.record RecordCoreException]))

(test/use-fixtures :each ds/with-create-fdb)

(deftest insert-test
  (let [db2 (store/start (ds/make-demostore {:open-mode :create-or-open
                                             :split-long-records true}))]

    (testing "Save more than 10K key / 100K value"
      (let [opts {::store/transform p/parse-record
                  ::store/required-results [:name]}
            long-email (str (apply str (repeat 100000 "x")) "a1u3@hello.com")
            large-val {:id    3 :account-id 3 :name "a1u3"
                       :email long-email}]

        (testing "record too large"
          (is (thrown-with-msg? RecordCoreException #"Record is too long"
                                (store/insert-record *db* (p/map->record :User large-val)))))

        (testing "db with split_record option"
          (store/insert-record db2 (p/map->record :User large-val)))

        (is
         (= [{:id 3 :account-id 3 :name "a1u3" :email long-email}]
            @(store/list-query db2 [:User [:starts-with? :name "a1"]] opts)))))))
