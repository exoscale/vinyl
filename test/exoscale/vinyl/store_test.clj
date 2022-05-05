(ns exoscale.vinyl.store-test
  (:require [clojure.test              :as test :refer [deftest testing is are]]
            [exoscale.vinyl.payload    :as p]
            [exoscale.vinyl.store      :as store]
            [exoscale.vinyl.aggregates :as agg]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]]))

(test/use-fixtures :once ds/with-open-fdb)

(deftest liveness-test
  (testing "Things are set up correctly"
    (is (= {:id 1 :name "a1" :state :active}
           (p/parse-record (store/load-record *db* :Account [1]))))))

(deftest query-test
  (let [all-data ds/fixtures
        opts     {::store/transform p/parse-record}]
    (testing "Select * queries"
      (are [record-type] (= (get all-data record-type)
                            @(store/list-query *db* [record-type] opts))
        :Account
        :User
        :Invoice
        :City))
    (testing "starts-with"
      (is
       (= [{:id 1 :account-id 1 :name "a1u1" :email "a1u1@hello.com"}
           {:id 2 :account-id 1 :name "a1u2" :email "a1u2@hello.com"}]
          @(store/list-query *db* [:User [:starts-with? :name "a1"]] opts))))
    (testing "int comparisons"
      (is
       (= [{:id 3 :account-id 3 :total 80 :lines [{:product "p4" :quantity 1}]}
           {:id 4 :account-id 4 :total 10 :lines [{:product "p1" :quantity 2}
                                                  {:product "p2" :quantity 4}]}
           {:id 5 :account-id 4 :total 80 :lines [{:product "p4" :quantity 1}]}]
          @(store/list-query *db* [:Invoice [:>= :id 3]] opts)))
      (is
       (= [{:account-id 1 :id 1 :total 10 :lines [{:product "p1" :quantity 2}
                                                  {:product "p2" :quantity 4}]}
           {:account-id 1 :id 2 :total 30 :lines [{:product "p1" :quantity 8}]}
           {:account-id 3 :id 3 :total 80 :lines [{:product "p4" :quantity 1}]}]
          @(store/list-query *db* [:Invoice [:<= :id 3]] opts))))
    (testing "equality comparisons"
      (is
       (= [{:id 1 :name "a1" :state :active}
           {:id 2 :name "a2" :state :suspended}
           {:id 3 :name "a3" :state :suspended}]
          @(store/list-query *db* [:Account [:not [:= :state "terminated"]]]
                             opts)))
      (is
       (= [{:id 1 :name "a1" :state :active}
           {:id 2 :name "a2" :state :suspended}
           {:id 3 :name "a3" :state :suspended}]
          @(store/list-query *db* [:Account [:not= :state "terminated"]] opts)))
      (is
       (= [{:id 2 :name "a2" :state :suspended}
           {:id 3 :name "a3" :state :suspended}]
          @(store/list-query *db* [:Account [:= :state "suspended"]] opts))))
    (testing "list-query"
      (is
       (= [{:id 1 :location {:name "Lausanne"  :zip-code 1000}}
           {:id 2 :location {:name "Lausanne"  :zip-code 1001}}
           {:id 3 :location {:name "Lausanne"  :zip-code 1002}}
           {:id 4 :location {:name "Lausanne"  :zip-code 1003}}
           {:id 5 :location {:name "Lausanne"  :zip-code 1004}}]
          @(store/list-query *db* [:City [:matches :location [:= :name "Lausanne"]]] opts))
       (= [{:id 6 :location {:name "Neuchatel" :zip-code 2000}}]
          @(store/list-query *db* [:City [:matches :location [:= :name "Neuchatel"]]] opts))))))

(defn- ensure-plan [query plan-str]
  (let [plan (atom nil)]
    @(store/list-query *db* query
                       {::store/intercept-plan-fn
                        (fn [p] (reset! plan p))})
    (is (= plan-str (str @plan)))))

(deftest query-plan-test
  (testing "Planned queries"
    (ensure-plan [:City] "Scan([IS City])")
    (ensure-plan [:Account [:= :state "terminated"]] "Index(account_state [[terminated],[terminated]])")
    (ensure-plan [:Account [:not= :state "terminated"]] "Scan(<,>) | [Account] | state NOT_EQUALS terminated")
    (ensure-plan [:City [:nest :location [:= :name "Lausanne"]]] "Scan(<,>) | [City] | location/{name EQUALS Lausanne}")))
    
(deftest aggregation-test
  (testing "Aggregation queries"
    (testing "Count not null aggregation"
      (are [account-id total]
          (= total (agg/compute *db* :count-not-null :User :usercnt account-id))
        1 2
        2 1
        3 3
        4 0))
    (testing "Sum aggregation"
      (are [account-id total]
          (= total (agg/compute *db* :sum :Invoice :total_invoiced account-id))
        1 40
        2 0
        3 80
        4 90))))
