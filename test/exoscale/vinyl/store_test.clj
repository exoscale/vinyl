(ns exoscale.vinyl.store-test
  (:require [clojure.test              :as test :refer [deftest testing is are]]
            [exoscale.vinyl.payload    :as p]
            [exoscale.vinyl.store      :as store]
            [exoscale.vinyl.aggregates :as agg]
            [exoscale.vinyl.demostore  :as ds :refer [*db*]])
  (:import [java.util.concurrent ExecutionException]))

(test/use-fixtures :once ds/with-build-fdb)

(deftest liveness-test
  (testing "Things are set up correctly"
    (is (= {:id 1 :name "a1" :state :active :payment :wired}
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
       (= [{:id 1 :name "a1" :state :active    :payment :wired}
           {:id 2 :name "a2" :state :suspended :payment :prepaid}
           {:id 3 :name "a3" :state :suspended :payment :wired}]
          @(store/list-query *db* [:Account [:not [:= :state "terminated"]]]
                             opts)))
      (is
       (= [{:id 1 :name "a1" :state :active    :payment :wired}
           {:id 2 :name "a2" :state :suspended :payment :prepaid}
           {:id 3 :name "a3" :state :suspended :payment :wired}]
          @(store/list-query *db* [:Account [:not= :state "terminated"]] opts)))
      (is
       (= [{:id 2 :name "a2" :state :suspended :payment :prepaid}
           {:id 3 :name "a3" :state :suspended :payment :wired}]
          @(store/list-query *db* [:Account [:= :state "suspended"]] opts))))
    (testing "list-query"
      (is
       (= [{:id 1 :location {:name "Lausanne"  :zip-code 1000}}
           {:id 2 :location {:name "Lausanne"  :zip-code 1001}}
           {:id 3 :location {:name "Lausanne"  :zip-code 1002}}
           {:id 4 :location {:name "Lausanne"  :zip-code 1003}}
           {:id 5 :location {:name "Lausanne"  :zip-code 1004}}]
          @(store/list-query *db* [:City [:nested :location [:= :name "Lausanne"]]] opts)))
      (is
       (= [{:id 6 :location {:name "Neuchatel" :zip-code 2000}}]
          @(store/list-query *db* [:City [:nested :location [:= :name "Neuchatel"]]] opts))))
    (testing "list-query with :one-of-them"
      (is
       (= [{:id 2 :account-id 1 :total 30 :lines [{:product "p1" :quantity 8}]}]
          @(store/list-query *db* [:Invoice [:one-of-them :lines [:= :quantity 8]]] opts))))))

(defn- ensure-plan [query plan-str]
  (let [plan (atom nil)]
    @(store/list-query *db* query
                       {::store/intercept-plan-fn
                        (fn [p] (reset! plan p))})
    (is (= plan-str (str @plan)))))

(deftest query-plan-test
  (testing "Planned queries"
    (ensure-plan [:User [:starts-with? :name "a1"]] "Index(username {[a1],[a1]})")
    (ensure-plan [:User [:= :name "a1"]] "Index(username [[a1],[a1]])")
    (ensure-plan [:User [:= :email "a1@exoscale.ch"]] "Scan([IS User]) | email EQUALS a1@exoscale.ch")
    (ensure-plan [:Account [:= :state "terminated"]] "Index(account_state [[terminated],[terminated]])")
    (ensure-plan [:Account [:not= :state "terminated"]] "Scan(<,>) | [Account] | state NOT_EQUALS terminated")
    (ensure-plan [:Invoice [:= :id 3]] "Scan([IS Invoice]) | id EQUALS 3")
    (ensure-plan [:Invoice [:>= :id 3]] "Scan([IS Invoice]) | id GREATER_THAN_OR_EQUALS 3")
    (ensure-plan [:Invoice [:<= :id 3]] "Scan([IS Invoice]) | id LESS_THAN_OR_EQUALS 3")
    (ensure-plan [:City] "Scan([IS City])")
    (ensure-plan [:City [:nested :location [:= :name "Lausanne"]]] "Scan(<,>) | [City] | location/{name EQUALS Lausanne}")))

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
        4 90))
    (testing "Enum aggregation"
      (are [payment cnt]
           (= cnt (agg/compute *db* :count-not-null :Account :account_payment_count payment))
        ;; enum value 0 cannot be indexed
        p/invalid 0
        p/prepaid 1
        p/postpaid 1
        p/wired 2))))

(deftest open-mode-test
  (testing "create-or-open : writing to new schema and reading from the old one"
    (let [env           (gensym "create-or-open")
          old-ds-reader (ds/create+start ds/schema {:env env :open-mode :create-or-open})
          new-ds-writer (ds/create+start ds/next-schema {:env env :open-mode :create-or-open})]
      (store/save-record-batch new-ds-writer (ds/all-records))
      (is (thrown-with-msg? ExecutionException #"Local meta-data has stale version"
                            @(store/list-query old-ds-reader [:User [:starts-with? :name "a1"]])))))

  (testing "open : writing to new schema and reading from the old one"
    (let [env           (gensym "open")
          ds-creator    (ds/create+start ds/schema {:env env :open-mode :create-or-open})
          old-ds-reader (ds/create+start ds/schema {:env env :open-mode :open})
          new-ds-writer (ds/create+start ds/next-schema {:env env :open-mode :open})]
      (store/save-record-batch ds-creator (ds/all-records))
      (store/save-record-batch new-ds-writer (ds/all-records))
      (is (thrown-with-msg? ExecutionException #"Local meta-data has stale version"
                            @(store/list-query old-ds-reader [:User [:starts-with? :name "a1"]])))))

  (testing "unchecked-open : writing to new schema and reading from the old one"
    (let [env           (gensym "unchecked-open")
          ds-creator    (ds/create+start ds/schema {:env env :open-mode :create-or-open})
          old-ds-reader (ds/create+start ds/schema {:env env :open-mode :unchecked-open})
          new-ds-writer (ds/create+start ds/next-schema {:env env :open-mode :unchecked-open})]
      (store/save-record-batch ds-creator (ds/all-records))
      (store/save-record-batch new-ds-writer (ds/all-records))
      (is @(store/list-query old-ds-reader [:User [:starts-with? :name "a1"]]))))

  (testing "build : writing to new schema and reading from the old one"
    (let [env           (gensym "build")
          ds-creator    (ds/create+start ds/schema {:env env :open-mode :create-or-open})
          old-ds-reader (ds/create+start ds/schema {:env env :open-mode :build})
          new-ds-writer (ds/create+start ds/next-schema {:env env :open-mode :build})]
      (store/save-record-batch ds-creator (ds/all-records))
      (store/save-record-batch new-ds-writer (ds/all-records))
      (is @(store/list-query old-ds-reader [:User [:starts-with? :name "a1"]])))))
