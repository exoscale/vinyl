(ns exoscale.vinyl.tuple
  (:import com.apple.foundationdb.tuple.Tuple))

(defprotocol Tuplable
  :extend-via-metadata true
  (as-tuple [this]))

(defn from-seq
  ^Tuple [objs]
  (Tuple/from (into-array Object objs)))

(defn from-bytes
  ^Tuple [^bytes bytes]
  (Tuple/fromBytes bytes))

(defn from
  ^Tuple [& objs]
  (from-seq objs))

(defn pack
  ^bytes [^Tuple t]
  (.pack t))

(defn get-string
  ^String [^Tuple t index]
  (.getString t (int index)))

(defn expand
  [^Tuple t]
  (.getItems t))

(defn get-long
  ([t]
   (get-long t 0))
  ([^Tuple t index]
   (.getLong t (long index))))

(defn decode
  ^Tuple [^bytes b]
  (Tuple/fromBytes b))

(def decode-and-expand
  (comp expand decode))
