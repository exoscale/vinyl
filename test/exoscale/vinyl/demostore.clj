(ns exoscale.vinyl.demostore
  (:require [clojure.tools.logging    :as log]
            [exoscale.vinyl.payload   :as p]
            [exoscale.vinyl.store     :as store]
            [exoscale.vinyl.demostore :as ds])
  (:import exoscale.vinyl.Demostore))

(def fixtures
  {:Account
   [{:id 1 :name "a1" :state :active}
    {:id 2 :name "a2" :state :suspended}
    {:id 3 :name "a3" :state :suspended}
    {:id 4 :name "a4" :state :terminated}]
   :Invoice
   [{:id 1 :account-id 1 :total 10 :lines [{:product "p1" :quantity 2}
                                           {:product "p2" :quantity 4}]}
    {:id 2 :account-id 1 :total 30 :lines [{:product "p1" :quantity 8}]}
    {:id 3 :account-id 3 :total 80 :lines [{:product "p4" :quantity 1}]}
    {:id 4 :account-id 4 :total 10 :lines [{:product "p1" :quantity 2}
                                           {:product "p2" :quantity 4}]}
    {:id 5 :account-id 4 :total 80 :lines [{:product "p4" :quantity 1}]}]
   :User
   [{:id 1 :account-id 1 :name "a1u1" :email "a1u1@hello.com"}
    {:id 2 :account-id 1 :name "a1u2" :email "a1u2@hello.com"}
    {:id 3 :account-id 2 :name "a2u3" :email "a2u3@hello.com"}
    {:id 4 :account-id 3 :name "a3u4" :email "a3u5@hello.com"}
    {:id 5 :account-id 3 :name "a3u5" :email "a3u5@hello.com"}
    {:id 6 :account-id 3 :name "a3u6" :email "a3u6@hello.com"}]
   :City
   [{:id 1 :location {:name "Lausanne"  :zip-code 1000}}
    {:id 2 :location {:name "Lausanne"  :zip-code 1001}}
    {:id 3 :location {:name "Lausanne"  :zip-code 1002}}
    {:id 4 :location {:name "Lausanne"  :zip-code 1003}}
    {:id 5 :location {:name "Lausanne"  :zip-code 1004}}
    {:id 6 :location {:name "Neuchatel" :zip-code 2000}}]})
   

(def schema
  {:Account {:primary-key [:concat :type-key "id"]
             :indices     [{:name "account_state" :on "state"}]}
   :User    {:primary-key [:concat :type-key "account_id" "id"]
             :indices     [{:name "username" :on "name"}
                           {:name "usercnt"
                            :on   [:group-by "name" "account_id"]
                            :type :count-not-null}]}
   :Invoice {:primary-key [:concat :type-key "account_id" "id"]
             :indices     [{:name "total_invoiced"
                            :on   [:group-by "total" "account_id"]
                            :type :sum}]}
   :Object  {:primary-key [:concat :type-key "bucket" "path"]
             :indices     [{:name "path_count"
                            :on [:group-by "path" "bucket"]
                            :type :count-not-null}
                           {:name "bucket_paths"
                            :on [:concat "bucket" "path"]}]}
   :City   {:primary-key [:concat :type-key [:nest "location" "name"]
                                            [:nest "location" "zip_code"]]
            :indices     [{:name "city_name_zip_code"
                           :on [:nest "location" "name"]
                           #_
                           [:concat :type-key
                            [:nest "location" "name"]
                            [:nest "location" "zip_code"]]}]}})



(def demostore
  (store/initialize :demostore (Demostore/getDescriptor) schema))

(defn all-records
  []
  (->> fixtures
       (mapcat (fn [[type batch]] (map #(p/map->record type %) batch)))
       (doall)))

(def ^:dynamic *db*)

(defn with-open-fdb
  [f]
  (let [db      (store/start demostore)
        records (all-records)]
    (log/info "installing test data:" (count records) "records")
    (store/run-in-context
     db
     (fn [store]
       (store/delete-all-records store)
       (store/save-record-batch store records)))

    (Thread/sleep 200)
    (log/info "ready to serve fdb queries")
    (try
      (binding [*db* db] (f))
      (finally
        (Thread/sleep 200)
        (log/info "cleaning fdb store")
        (store/delete-all-records db)))))

(defn with-paths
  [f]
  (let [db      (store/start demostore)
        records (all-records)]
    (log/info "installing test data:" (count records) "records")
    (store/run-in-context
     db
     (fn [store]
       (store/delete-all-records store)
       (store/save-record-batch store records)))

    (Thread/sleep 200)
    (log/info "ready to serve fdb queries")
    (try
      (binding [*db* db] (f))
      (finally
        (Thread/sleep 200)
        (log/info "cleaning fdb store")
        (store/delete-all-records db)))))
