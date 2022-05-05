vinyl: a record store for Clojure
=================================

Vinyl provides a facade for [FoundationDB](https://www.foundationdb.org/)'s
[record-layer](https://foundationdb.github.io/fdb-record-layer/).

The intent of the record layer is to provide a protobuf based storage
engine for indexed records stored in FoundationDB.

## Search queries

Queries in vinyl can be supplied using the following functions:

- `exoscale.vinyl.store/list-query`
- `exoscale.vinyl.store/execute-query`

A typical query will be executed this way:

``` clojure
@(store/list-query vinyl-store [:RecordType [:= :field "value"]])
```

Both `list-query` and `execute-query` accept different arities:

``` clojure
(list-query store query)
(list-query store query opts)
(list-query store query opts values)

(execute-query store query)
(execute-query store query opts)
(execute-query store query opts values)
```

The `query` argument can either be an instance of `RecordQuery` or a vector,
in which case a `RecordQuery` will be built from the vector. See
[Query language](#query-language) for a reference.

The additional `opts` argument allows supplying options to perform modifications
on the results *inside* the transaction in which the query is executed. See
[Record cursors](#record-cursors) for details on what can be supplied there.

The additional `values` argument is a map of bindings to be applied to a
prepared query where applicable.

## Query language

The data representation for queries takes the following shape:

``` clojure
[:RecordType optional-filter]
```

#### Select all queries

To select all fields you can provide a single member vector containing
the record type:

``` clojure
[:RecordType] ;; for instance: @(store/list-query store [:RecordType])
```

#### Field equality

Field equality is supported in two ways, if the comparand is anything but
a vector it will be treated as a fixed value:

``` clojure
@(store/list-query store [:RecordType [:= :id 1234]])
@(store/list-query store [:RecordType [:= :name "jane"]])
```

If the comparand is provided as a keyword, a prepared query is built and the
corresponding keyword is expected to be found in the query's evaluation context:

``` clojure
(def my-query (query/build-query :RecordType [:= :user-name :name]))
@(store/list-query store my-query {} {:name "jane"})
@(store/list-query store my-query {} {:name "unknown"})
```

#### Field set membership

A field can be checked for membership in a set:

``` clojure
@(store/list-query store [:RecordType [:in :id [10 20 30]]])
```

Prepared queries are also supported for membership tests:

``` clojure
(def my-query (query/build-query :RecordType [:= :in :user-name :names]))
@(store/list-query store my-query {} {:names ["jane" "unknown"]})
```

#### Field inequality, null value check, and non null value checks

Fields can be checked for values that do not match a fixed comparand. Prepared
queries are currently unsupported for the comparand of inequality tests:

``` clojure
@(store/list-query store [:RecordType [:not= :id 1234]])
@(store/list-query store [:RecordType [:some? :email]])
@(store/list-query store [:RecordType [:nil? :purge_date]])
```

#### Range comparisons

For values supporting comparisons, range operations are supported:

``` clojure
@(store/list-query store [:RecordType [:> :id 100]])
@(store/list-query store [:RecordType [:< :id 100]])
@(store/list-query store [:RecordType [:>= :id 100]])
@(store/list-query store [:RecordType [:<= :id 100]])
```

For string fields, prefix searches are supported:

``` clojure
@(store/list-query store [:RecordType [:starts-with? :path "/usr/local/"]])
```

#### Boolean operations

Filters can be composed with boolean operations `:not`, `:or`, `:and`:

``` clojure
@(store/list-query store [:Rec [:and [:not [:= :id 1]]
                                     [:or [:> :id 100] [:< :id 50]]]])
```

#### Nested field matches

Since the FDB record layer supports storing records which contain nested values,
There needs to be support for matching those in queries.

Suppose you have the following protobuf definition:

``` protocol-buffer
Message Info {
  string path = 1;
}

Message Top {
   int64 id = 1;
   Info info = 2;
}
```

You can apply any field query to fields in info by using `:nest`:

``` clojure
@(store/list-query store [:Top [:nest :info [:starts-with? :path "/"]]])
```
