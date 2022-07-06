(ns exoscale.vinyl.query-filter-test
  (:require [clojure.test :refer :all]
            [exoscale.vinyl.query :as query]
            [clojure.spec.alpha :as s]))

(deftest filter-types-test
  (testing "Filter spec"
    (is (s/valid? ::query/filter [:matches :a [:= :b 2]]))
    (is (s/valid? ::query/filter [:nested :a [:= :b 2]]))
    (is (s/valid? ::query/filter [:one-of-them :a [:= :b 2]]))
    (is (s/valid? ::query/filter [:not= :a 1]))
    (is (s/valid? ::query/filter [:= :a 1]))
    (is (s/valid? ::query/filter [:in :a [1]]))
    (is (s/valid? ::query/filter [:nil? :a]))
    (is (s/valid? ::query/filter [:some? :a]))
    (is (s/valid? ::query/filter [:and [:= :a 1]
                                       [:= :b 2]]))
    (is (s/valid? ::query/filter [:or [:= :a 1]
                                      [:= :b 2]]))
    (is (s/valid? ::query/filter [:not [:= :a 1]]))
    (is (s/valid? ::query/filter [:starts-with? :a "1"]))
    (is (s/valid? ::query/filter [:> :a 1]))
    (is (s/valid? ::query/filter [:>= :a 1]))
    (is (s/valid? ::query/filter [:< :a 1]))
    (is (s/valid? ::query/filter [:<= :a 1]))))
