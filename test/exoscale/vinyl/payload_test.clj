(ns exoscale.vinyl.payload-test
  (:require [clojure.spec.alpha              :as s]
            [clojure.test.check.properties   :as prop]
            [exoscale.vinyl.payload          :as p]
            [clojure.test.check.clojure-test :refer [defspec]]))

(defspec account-serialization-test 100
  (prop/for-all
   [account (s/gen ::p/account)]
   (= account (-> account p/account->record p/parse-record))))

(defspec user-serialization-test 100
  (prop/for-all
   [user (s/gen ::p/user)]
   (= user (-> user p/user->record p/parse-record))))

(defspec invoice-serialization-test 100
  (prop/for-all
   [invoice (s/gen ::p/invoice)]
   (= invoice (-> invoice p/invoice->record p/parse-record))))
