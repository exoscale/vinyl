(ns exoscale.vinyl.fn
  "Utility functions to build java.util.function implementations"
  (:import java.util.function.Function
           java.util.function.BiFunction
           java.util.function.Consumer
           java.util.function.BiConsumer
           java.util.function.Supplier
           java.util.concurrent.CompletableFuture
           java.lang.AutoCloseable))

(defn ^Function make-fun
  "Turn a function of one argument into a java Function"
  [f]
  (if (instance? Function f)
    f
    (reify
      Function   (apply  [_ arg]       (f arg))
      BiFunction (apply  [_ arg1 arg2] (f arg1 arg2))
      Consumer   (accept [_ arg]       (f arg))
      BiConsumer (accept [_ arg1 arg2] (f arg1 arg2)))))

(defn ^Supplier make-supplier
  [f]
  (reify
    Supplier (get [_] (f))))

(defmacro supplier
  [& fntail]
  `(make-supplier (fn [] ~@fntail)))

(defn when-complete
  [^CompletableFuture x f]
  (.whenComplete x (reify BiConsumer (accept [_ v t] (f v t)))))

(defn close-on-complete
  [^CompletableFuture x opened]
  (.whenComplete
   x
   (reify BiConsumer (accept [_ _ _] (.close ^AutoCloseable opened)))))
