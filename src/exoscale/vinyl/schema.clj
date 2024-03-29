(ns exoscale.vinyl.schema
  (:require [clojure.spec.alpha :as s]
            [exoscale.ex        :as ex])
  (:import com.apple.foundationdb.record.RecordMetaData
           com.apple.foundationdb.record.metadata.Key$Expressions
           com.apple.foundationdb.record.metadata.Index
           com.apple.foundationdb.record.metadata.IndexTypes
           com.apple.foundationdb.record.metadata.RecordTypeBuilder
           (com.apple.foundationdb.record.metadata.expressions
            KeyExpression
            FieldKeyExpression
            NestingKeyExpression)
           com.google.protobuf.Descriptors$FileDescriptor))

(defmulti ^KeyExpression multi-build-field :type)

(defprotocol Nestable
  (nest [parent child]))

(defprotocol Groupable
  (group [field expr]))

(extend-protocol Nestable
  FieldKeyExpression
  (nest [parent child]
    (.nest parent ^KeyExpression child)))

(extend-protocol Groupable
  FieldKeyExpression
  (group [field expr]
    (.groupBy field ^KeyExpression expr (into-array KeyExpression [])))
  NestingKeyExpression
  (group [field expr]
    (.groupBy field ^KeyExpression expr (into-array KeyExpression []))))

(defn build-field
  ^KeyExpression [field-def]
  (ex/assert-spec-valid ::field field-def)
  (let [[type field] (s/conform ::field field-def)]
    (case type
      :str      (Key$Expressions/field (str field))
      :type-key (Key$Expressions/recordType)
      :kw       (Key$Expressions/field (name field))
      :cat      (multi-build-field field)
      :field    field)))

(defmethod multi-build-field :concat
  [{:keys [args]}]
  (Key$Expressions/concat (mapv build-field args)))

(defmethod multi-build-field :nested
  [{:keys [args]}]
  (nest (build-field (first args))
        (build-field (last args))))

(defmethod multi-build-field :group-by
  [{:keys [args]}]
  (group (build-field (first args))
         (build-field (last args))))

(defn set-primary-key
  [^RecordTypeBuilder builder field-name]
  (.setPrimaryKey builder (build-field field-name)))

(defn set-record-type-key
  [^RecordTypeBuilder builder ^String rtk]
  (.setRecordTypeKey builder rtk))

(def index-types
  {:count-not-null IndexTypes/COUNT_NOT_NULL
   :sum            IndexTypes/SUM})

(defn- make-index-type [type]
  (cond
    (keyword? type) (get index-types type)
    (string? type)  type
    :else           IndexTypes/VALUE))

(defn make-index
  ^Index [index-name ^KeyExpression kx type]
  (Index. (str index-name) kx (make-index-type type)))

(defn create-record-meta
  ^RecordMetaData [^Descriptors$FileDescriptor descriptor schema]
  (ex/assert-spec-valid ::schema schema)
  (let [builder (doto (RecordMetaData/newBuilder)
                  (.setRecords descriptor)
                  (.setSplitLongRecords false))]
    (doseq [[record-type {:keys [type-key primary-key indices]}] schema]
      (when primary-key
        (let [rt   (.getRecordType builder (name record-type))]
          (set-primary-key rt (build-field primary-key))
          (when (some? type-key)
            (set-record-type-key rt type-key))
          (doseq [{:keys [name on type]} indices]
            (.addIndex builder rt (make-index name (build-field on) type))))))
    (doseq [{:keys [name on type]} (:indices schema)]
      (.addMultiTypeIndex builder (map #(.getRecordType builder %) on) (Index. (str name) (build-field :type-key) (make-index-type type))))
    (.build builder)))

;; Schema spec
(s/def ::field       (s/or :type-key #{:type-key}
                           :str string?
                           :kw keyword?
                           :cat (s/cat :type keyword? :args (s/* any?))
                           :field (partial instance? KeyExpression)))

(s/def ::record-types (s/or :str (s/coll-of string?)
                            :kw  (s/coll-of keyword?)))

(s/def ::record-type string?)
(s/def ::type-key    string?)
(s/def ::primary-key ::field)
(s/def ::name         string?)
(s/def ::on          (s/or :field        ::field
                           :record-types ::record-types))
(s/def ::type        (s/or :str string? :kw keyword?))
(s/def ::index       (s/keys :req-un [::name ::on] :opt-un [::type]))
(s/def ::indices     (s/coll-of ::index))
(s/def ::entity      (s/keys :req-un [::primary-key]
                             :opt-un [::indices ::type-key]))
(s/def ::schema      (s/map-of keyword? (s/or :entity ::entity
                                              :indices ::indices)))
