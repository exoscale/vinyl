(ns exoscale.vinyl.tuple
  (:import com.apple.foundationdb.tuple.Tuple))

(defprotocol Tuplable
  :extend-via-metadata true
  (as-tuple [this]))

(defn ^Tuple from-seq
  [objs]
  (Tuple/from (into-array Object objs)))

(defn ^Tuple from
  [& objs]
  (from-seq objs))

(defn ^bytes pack
  [^Tuple t]
  (.pack t))

(defn ^String get-string
  [^Tuple t index]
  (.getString t (int index)))

(defn expand
  [^Tuple t]
  (.getItems t))

(defn get-long
  ([t]
   (get-long t 0))
  ([^Tuple t index]
   (.getLong t (long index))))

(defn ^Tuple decode
  [^bytes b]
  (Tuple/fromBytes b))

(def decode-and-expand
  (comp expand decode))
