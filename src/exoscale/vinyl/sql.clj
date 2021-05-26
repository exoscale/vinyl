(ns exoscale.vinyl.sql
  "Small facade for vinyl to perform simple SQL queries. See `list-query`
   for syntax details."
  (:require [exoscale.vinyl.store :as store]
            [instaparse.core      :as insta]))

(def ^:private ^:no-doc vinyl-statement
  "BNF SQL syntax for vinyl"
  (insta/parser
   "
<statement> = select
<select> = <selectkw> <ws> <target> <ws> <fromkw> <ws> record <ws> (where)?
           options
options =  (<ws> option)*
<option> = limit | skip
limit = <'limit'> <ws> number
skip = <'skip'> <ws> number
<target> = <'*'>
where = <wherekw> <ws> filterexpr
<record> = word
selectkw = 'select'
wherekw = 'where'
fromkw =  'from'
<filterexpr> = groupedexpr | ungroupedexpr
negation = <'!'>
groupedexpr = negation? <'('> ungroupedexpr <')'>
             (<ws> logicoperator <ws> filterexpr)?
logicoperator = 'and' | 'or'
<ungroupedexpr> = fieldfilter (<ws> logicoperator <ws> filterexpr)?
<fieldfilter> = fieldeqfilter | fieldnilfilter | fieldsomefilter |
                fieldltfilter | fieldltefilter | fieldneqfilter |
                fieldgtfilter | fieldgtefilter |
                fieldswfilter | fieldinfilter
fieldeqfilter = field <optws> <'='> <optws> fieldvalue
fieldneqfilter = field <optws> <'!='> <optws> fieldvalue
fieldnilfilter = field <ws> <'is'> <ws> <'null'>
fieldsomefilter = field <ws> <'is'> <ws> <'not'> <ws> < 'null'>
fieldgtfilter = field <ws> <'>'> <optws> fieldvalue
fieldgtefilter = field <ws> <'>='> <optws> fieldvalue
fieldltfilter = field <ws> <'<'> <optws> fieldvalue
fieldltefilter = field <ws> <'<='> <optws> fieldvalue
fieldswfilter = field <ws> <'starts'> <ws> <'with'> <ws> fieldvalue
fieldinfilter = field <ws> <'in'> <optws> <'('> <optws> fieldvalue <optws>
                ( <','> <optws> fieldvalue <optws>)* <')'>
<field> = directfield|nestedfield
<directfield> = word
nestedfield = word <'.'> word
word = #'[a-zA-Z][a-zA-Z0-9_/:-]*'
number = #'[0-9]+'
quoted = <'\\''> (#'[^\\'\\\\]+' | escaped-char)* <'\\''>
<escaped-char> = #'\\\\.'
<fieldvalue> = word | number | quoted
<ws> = #'\\s+'
<optws> = <ws>?"
   :string-ci true))

(defmulti ^:private ^:no-doc parse-filter
  "Dispatcher for filter types as parsed in by the instaparse syntax"
  first)

(defn- ^:no-doc translate-filters
  "Expand logical operations to vinyl logical operations.
   This is essentially an infix to prefix swap, where

       x OR y

   gets rewritten to

      [:or x y]"
  [[head operator & tail]]
  (let [expr (parse-filter head)]
    (if (some? operator)
      [(-> operator last keyword) expr (translate-filters tail)]
      expr)))

(defn- ^:no-doc transform-value
  "Rewrite value based on its supplied type (for now, converts numbers to
   numbers, assumes the rest to be strings)."
  [[t v]]
  (if (= :number t) (Long/parseLong v) (str v)))

(defn- ^:no-doc make-target
  "A helper for `transform-for-target`, expands a field to a direct invocation
   or a `:matches` stanza for nested fields."
  [target]
  (if (= (first target) :nestedfield)
    [(keyword (last (last target)))
     [:matches (keyword (last (second target)))]]
    [(keyword (last target))]))

(defn- ^:no-doc transform-for-target
  "The proposed SQL parser allows expressing nested fields with `parent.child`.
   The vinyl query language follows the record layer's notation more closely
   and requires using `matches` to access nested fields.

   To avoid having `parse-filter` handle this in all of its defmethods, this
   function can be called with the extracted `target`: and expression. Within
   the expression, the `::target` keyword can be used as a placeholder, letting
   `transform-for-target` do the rewriting.

   For plain expressions this happens:

       [[:word \"id\"] [:= ::target 1]]
       ;;=> [:= :id 1]

  For nested expressions this happens:

      [[:nestedfield [:word \"p\"] [:word \"id\"]] [:= ::target 1]
      ;;=> [:matches :p [:= :id 1]]
  "
  [target expr]
  (let [[field wrapping] (make-target target)
        parsed           (replace {::target field} expr)]
    (if (some? wrapping)
      (conj wrapping parsed)
      parsed)))

(defmethod parse-filter :fieldinfilter
  [[_ target & vals]]
  (transform-for-target
   target
   [:in ::target (mapv transform-value vals)]))

(defmethod parse-filter :fieldeqfilter
  [[_ target val]]
  (transform-for-target
   target
   [:= ::target (transform-value val)]))

(defmethod parse-filter :fieldneqfilter
  [[_ target val]]
  (transform-for-target
   target
   [:not= ::target (transform-value val)]))

(defmethod parse-filter :fieldltfilter
  [[_ target val]]
  (transform-for-target
   target
   [:< ::target (transform-value val)]))

(defmethod parse-filter :fieldltefilter
  [[_ target val]]
  (transform-for-target
   target
   [:<= ::target (transform-value val)]))

(defmethod parse-filter :fieldgtfilter
  [[_ target val]]
  (transform-for-target
   target
   [:> ::target (transform-value val)]))

(defmethod parse-filter :fieldgtefilter
  [[_ target val]]
  (transform-for-target
   target
   [:>= ::target (transform-value val)]))

(defmethod parse-filter :fieldswfilter
  [[_ target val]]
  (transform-for-target
   target
   [:starts-with? ::target (transform-value val)]))

(defmethod parse-filter :groupedexpr
  [[_ & filters]]
  (if (= (first filters) [:negation])
    [:not (translate-filters (rest filters))]
    (translate-filters filters)))

(defmulti ^:private ^:no-doc translate-option
  "Translate a provided parsed option into a valid
   vinyl cursor option"
  first)

(defmethod translate-option :limit
  [[_ val]]
  {::store/limit (transform-value val)})

(defmethod translate-option :skip
  [[_ val]]
  {::store/skip (transform-value val)})

(defn- ^:no-doc as-query
  "Transform an SQL string into a query"
  [statement]
  (let [ast (vinyl-statement statement)]
    (when (insta/failure? ast)
      (throw (ex-info (str (insta/get-failure ast)) {})))
    (let [[record filters options] ast]
      {:query (cond-> [(-> record last keyword)]
                (seq filters)
                (conj (translate-filters (rest filters))))
       :opts (reduce merge {} (map translate-option (rest options)))})))


(defn list-query
  "Acts as `exoscale.vinyl.store/list-query` but accepts strings queries
  written in a subset of SQL.

  Queries take the following form:

       SELECT * FROM <record-type> <filters> <options>

  No field selection can be done, queries **must** start with 'SELECT *'.
  Record types are not checked at query parsing time.

  Supported filters:

  - field comparisons: field = value, field != value, field starts with 'prefix'
  - field presence test: field is null, field is not null
  - field set membership: field in ( 1, 2, 3)
  - integer comparisons: field > val, field < val, field >= val, field <= val
  - logical operations: field = v1 or vield = v2 and state = 'disabled'
  - grouping: (field = v1 or field = v2)
  - group negation: !(field = v1 or field = v2)

  All value filters work on nested fields:

    parent.child = 'foo'

  Trailing options can be any of:

  - LIMIT <number>
  - SKIP <number>
  "
  ([store q]
   (list-query store q {}))
  ([store q opts]
   (let [parsed (as-query q)]
     (store/list-query store (:query parsed) (merge (:opts parsed) opts)))))
