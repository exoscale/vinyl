(ns exoscale.vinyl.sql-test
  (:require [clojure.test              :as test :refer [deftest are]]
            [exoscale.vinyl.payload    :as p]
            [exoscale.vinyl.sql        :refer [list-query]]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]]))

(test/use-fixtures :once ds/with-open-fdb)

(deftest query-test
  (let [opts {:exoscale.vinyl.store/transform p/parse-record}]
    (are [query results] (= results @(list-query *db* query opts))
      "SELECT * FROM User WHERE name STARTS WITH 'a1'"
      [{:id 1 :account-id 1 :name "a1u1" :email "a1u1@hello.com"}
       {:id 2 :account-id 1 :name "a1u2" :email "a1u2@hello.com"}]

      "SELECT * FROM Invoice where id >= 3"
      [{:id 3 :account-id 3 :total 80 :lines [{:product "p4" :quantity 1}]}
       {:id 4 :account-id 4 :total 10 :lines [{:product "p1" :quantity 2}
                                              {:product "p2" :quantity 4}]}
       {:id 5 :account-id 4 :total 80 :lines [{:product "p4" :quantity 1}]}]

      "SELECT * FROM Account where state != 'terminated'"
      [{:id 1 :name "a1" :state :active :payment :wired}
       {:id 2 :name "a2" :state :suspended :payment :prepaid}
       {:id 3 :name "a3" :state :suspended :payment :wired}]

      "SELECT * FROM Account where !(state = 'terminated')"
      [{:id 1 :name "a1" :state :active :payment :wired}
       {:id 2 :name "a2" :state :suspended :payment :prepaid}
       {:id 3 :name "a3" :state :suspended :payment :wired}])))
