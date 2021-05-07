(defproject exoscale/vinyl "0.1.0-SNAPSHOT"
  :description "Clojure facade for the FoundationDB record-layer"
  :url "https://github.com/exoscale/vinyl"
  :license {:name "MIT/ISC"}

  :dependencies [[org.clojure/clojure                        "1.10.3"]
                 [org.clojure/tools.logging                  "1.1.0"]
                 [org.foundationdb/fdb-record-layer-core-pb3 "2.8.110.0"]
                 [com.google.protobuf/protobuf-java          "3.15.8"]
                 [com.stuartsierra/component                 "1.0.0"]
                 [exoscale/ex                                "0.3.17"]]

  :deploy-repositories [["snapshots" :clojars]
                        ["releases"  :clojars]]

  :global-vars {*warn-on-reflection* true}
  :java-source-paths ["protobuf"]
  :pedantic? :warn
  :profiles
  {:dev  {:dependencies [[org.slf4j/slf4j-api    "1.7.30"]
                         [org.slf4j/slf4j-simple "1.7.30"]
                         [org.clojure/test.check "1.1.0"]]}
   :lint {:pedantic? :ranges
          :plugins   [[lein-cljfmt "0.7.0"]]}})
