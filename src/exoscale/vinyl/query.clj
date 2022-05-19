(ns exoscale.vinyl.query
  "A miniature DSL for building queries and prepared
   queries for the FoundationDB record layer"
  (:require [clojure.spec.alpha        :as s]
            [exoscale.ex               :as ex])
  (:import com.apple.foundationdb.record.query.RecordQuery
           com.apple.foundationdb.record.query.expressions.Query
           com.apple.foundationdb.record.query.expressions.Field
           com.apple.foundationdb.record.query.expressions.NestedField
           com.apple.foundationdb.record.query.expressions.QueryComponent
           com.apple.foundationdb.record.EvaluationContextBuilder
           com.apple.foundationdb.record.EvaluationContext
           java.util.List))

(defn ^Field build-field
  [k]
  (Query/field (name k)))

(s/def ::field keyword?)

(defmulti vec-filter-type first)

(defmethod vec-filter-type :matches
  [_]
  (s/cat :type #{:matches} :field keyword? :filter ::filter))

(defmethod vec-filter-type :nested
  [_]
  (s/cat :type #{:nested} :field keyword? :filter ::filter))

(defmethod vec-filter-type :one-of-them
  [_]
  (s/cat :type #{:one-of-them} :field keyword? :filter ::filter))

(defmethod vec-filter-type :not=
  [_]
  (s/cat :type #{:not=} :field ::field :value any?))

(defmethod vec-filter-type :=
  [_]
  (s/cat :type #{:=} :field ::field :value any?))

(defmethod vec-filter-type :in
  [_]
  (s/cat :type #{:in} :field ::field :param any? :tail (s/* any?)))

(defmethod vec-filter-type :nil?
  [_]
  (s/cat :type #{:=} :field ::field))

(defmethod vec-filter-type :some?
  [_]
  (s/cat :type #{:=} :field ::field))

(defmethod vec-filter-type :and
  [_]
  (s/cat :type #{:and} :components (s/* ::filter)))

(defmethod vec-filter-type :or
  [_]
  (s/cat :type #{:and} :components (s/* ::filter)))

(defmethod vec-filter-type :not
  [_]
  (s/cat :type #{:not} :operand ::filter))

(defmethod vec-filter-type :starts-with?
  [_]
  (s/cat :type #{:starts-with?} :field ::field :comparand string?))

(defmethod vec-filter-type :>
  [_]
  (s/cat :type #{:>} :field ::field :comparand any?))

(defmethod vec-filter-type :>=
  [_]
  (s/cat :type #{:>=} :field ::field :comparand any?))

(defmethod vec-filter-type :<
  [_]
  (s/cat :type #{:<} :field ::field :comparand any?))

(defmethod vec-filter-type :<=
  [_]
  (s/cat :type #{:<=} :field ::field :comparand any?))

(s/def ::filter (s/and coll?
                       #(keyword? (first %))
                       (s/multi-spec vec-filter-type :type)))

(defmulti ^QueryComponent multi-build-filter :type)

(defn ^QueryComponent build-filter
  [filter]
  (ex/assert-spec-valid ::filter filter)
  (let [data (s/conform ::filter filter)]
    (multi-build-filter data)))

(defmethod multi-build-filter :=
  [{:keys [field value]}]
  (if (keyword? value)
    (.equalsParameter (build-field field) (name value))
    (.equalsValue (build-field field) value)))

(defmethod multi-build-filter :in
  [{:keys [field param tail]}]
  (if (keyword? param)
    (.in (build-field field) (name param))
    (.in (build-field field) ^List (into [] (concat [param] tail)))))

(defmethod multi-build-filter :not=
  [{:keys [field value]}]
  (.notEquals (build-field field) value))

(defmethod multi-build-filter :nil?
  [{:keys [field]}]
  (-> (build-field field)
      (.isNull)))

(defmethod multi-build-filter :some?
  [{:keys [field]}]
  (-> (build-field field)
      (.notNull)))

(defmethod multi-build-filter :and
  [{:keys [components]}]
  (Query/and (mapv multi-build-filter components)))

(defmethod multi-build-filter :or
  [{:keys [components]}]
  (Query/or (mapv multi-build-filter components)))

(defmethod multi-build-filter :not
  [{:keys [operand]}]
  (Query/not (multi-build-filter operand)))

(defmethod multi-build-filter :starts-with?
  [{:keys [field comparand]}]
  (-> (build-field field)
      (.startsWith (str comparand))))

(defmethod multi-build-filter :matches
  [{:keys [field filter]}]
  (-> (build-field field)
      (.matches (multi-build-filter filter))))

(defmethod multi-build-filter :nested
  [{:keys [field filter]}]
  (NestedField. (name field) (multi-build-filter filter)))

(defmethod multi-build-filter :>
  [{:keys [field comparand]}]
  (-> (build-field field)
      (.greaterThan comparand)))

(defmethod multi-build-filter :>=
  [{:keys [field comparand]}]
  (-> (build-field field)
      (.greaterThanOrEquals comparand)))

(defmethod multi-build-filter :<
  [{:keys [field comparand]}]
  (-> (build-field field)
      (.lessThan comparand)))

(defmethod multi-build-filter :<=
  [{:keys [field comparand]}]
  (-> (build-field field)
      (.lessThanOrEquals comparand)))

(defn ^RecordQuery build-query
  ([record-type]
   (-> (RecordQuery/newBuilder)
       (.setRecordType (name record-type))
       (.build)))
  ([record-type filter]
   (-> (RecordQuery/newBuilder)
       (.setRecordType (name record-type))
       (.setFilter (build-filter filter))
       (.build))))

(defn ^EvaluationContext bindings
  ([m]
   (let [^EvaluationContextBuilder builder (EvaluationContext/newBuilder)]
     (doseq [[k v] m]
       (.setBinding builder (name k) v))
     (.build builder))))

(def ^EvaluationContext empty-context
  EvaluationContext/EMPTY)
