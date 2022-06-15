(ns build
  (:require [clojure.tools.build.api :as b]))

(defn compile-java
  [_]
  (b/javac {:src-dirs ["protobuf"]
            :class-dir "target/classes"
            :basis (b/create-basis {})}))
