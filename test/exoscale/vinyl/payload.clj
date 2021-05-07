(ns exoscale.vinyl.payload
  (:require [clojure.spec.alpha :as s]
            [exoscale.ex        :as ex])
  (:import com.apple.foundationdb.record.provider.foundationdb.FDBRecord
           com.google.protobuf.Message
           exoscale.vinyl.Demostore$Account
           exoscale.vinyl.Demostore$AccountOrBuilder
           exoscale.vinyl.Demostore$Account$Builder
           exoscale.vinyl.Demostore$User
           exoscale.vinyl.Demostore$UserOrBuilder
           exoscale.vinyl.Demostore$User$Builder
           exoscale.vinyl.Demostore$Invoice
           exoscale.vinyl.Demostore$InvoiceOrBuilder
           exoscale.vinyl.Demostore$Invoice$Builder
           exoscale.vinyl.Demostore$InvoiceLine
           exoscale.vinyl.Demostore$InvoiceLineOrBuilder))

(defprotocol RecordParser (parse-record [this]))
(defprotocol RecordMerger (merge-from [this other]))

(defonce counter (atom 0))

(defn merge-in  [x y]                (-> (merge-from x y) (parse-record)))
(defn next-id   []                   (swap! counter inc))
(defn ensure-id [{:keys [id] :as m}] (cond-> m (nil? id) (assoc :id (next-id))))

(defn ^Demostore$Account account->record
  [account]
  (let [account (ex/assert-spec-valid ::account (ensure-id account))
        b       (Demostore$Account/newBuilder)]
    (-> b
        (.setId (long (or (:id account) (next-id))))
        (.setName (str (:name account)))
        (.setState (name (:state account)))
        (.build))))

(defn ^Demostore$User user->record
  [user]
  (let [user (ex/assert-spec-valid ::user (ensure-id user))
        b    (Demostore$User/newBuilder)]
    (-> b
        (.setAccountId (long (:account-id user)))
        (.setId (long (or (:id user) (next-id))))
        (.setName (str (:name user)))
        (.setEmail (str (:email user)))
        (.build))))

(defn ^Demostore$InvoiceLine invoice-line->record
  [line]
  (ex/assert-spec-valid ::line line)
  (let [b (Demostore$InvoiceLine/newBuilder)]
    (-> b
        (.setProduct (str (:product line)))
        (.setQuantity (long (:quantity line)))
        (.build))))

(defn ^Demostore$Invoice invoice->record
  [invoice]
  (let [invoice (ex/assert-spec-valid ::invoice (ensure-id invoice))
        b       (Demostore$Invoice/newBuilder)]
    (-> b
        (.setAccountId (long (:account-id invoice)))
        (.setId (long (or (:id invoice) (next-id))))
        (.setTotal (long (:total invoice)))
        (.addAllLines (mapv invoice-line->record (:lines invoice)))
        (.build))))

(defn ^Message map->record
  ([m]
   (map->record (::record-type m) m))
  ([record-type m]
   (case record-type
     :Account     (account->record m)
     :User        (user->record m)
     :InvoiceLine (invoice-line->record m)
     :Invoice     (invoice->record m))))

(defrecord RecordInfo [type record]
  RecordParser
  (parse-record [_this]
    (case type
      "Account" (merge-in (Demostore$Account/newBuilder) record)
      "User"    (merge-in (Demostore$User/newBuilder)    record)
      "Invoice" (merge-in (Demostore$Invoice/newBuilder) record))))

(extend-protocol RecordParser
  nil
  (parse-record [r]
    nil)
  FDBRecord
  (parse-record [r]
    (parse-record
     (RecordInfo. (-> r .getRecordType .getName) (.getRecord r))))
  Demostore$AccountOrBuilder
  (parse-record [r]
    (with-meta
      {:id    (.getId r)
       :name  (.getName r)
       :state (some-> (.getState r) keyword)}
      {::record-type :Account}))
  Demostore$UserOrBuilder
  (parse-record [r]
    (with-meta
      {:account-id (.getAccountId r)
       :id         (.getId r)
       :name       (.getName r)
       :email      (.getEmail r)}
      {::record-type :User}))
  Demostore$InvoiceLineOrBuilder
  (parse-record [r]
    (with-meta
      {:product  (.getProduct r)
       :quantity (.getQuantity r)}
      {::record-type :InvoiceLine}))
  Demostore$InvoiceOrBuilder
  (parse-record [r]
    (with-meta
      {:account-id (.getAccountId r)
       :id         (.getId r)
       :total      (.getTotal r)
       :lines      (mapv parse-record (.getLinesList r))}
      {::record-type :Invoice})))

(extend-protocol RecordMerger
  Demostore$Account$Builder
  (merge-from [x y] (.mergeFrom x ^Message y))
  Demostore$User$Builder
  (merge-from [x y] (.mergeFrom x ^Message y))
  Demostore$Invoice$Builder
  (merge-from [x y] (.mergeFrom x ^Message y)))

(s/def ::id         nat-int?)
(s/def ::name       string?)
(s/def ::email      string?)
(s/def ::product    string?)
(s/def ::quantity   pos-int?)
(s/def ::total      pos-int?)
(s/def ::state      #{:active :suspended :terminated})
(s/def ::account-id ::id)
(s/def ::account    (s/keys :req-un [::id ::name ::state]))
(s/def ::user       (s/keys :req-un [::id ::account-id ::name ::email]))
(s/def ::line       (s/keys :req-un [::product ::quantity]))
(s/def ::lines      (s/coll-of ::line))
(s/def ::invoice    (s/keys :req-un [::id ::account-id ::total ::lines]))
