(ns exoscale.vinyl.fn
  "Utility functions to build java.util.function implementations"
  (:import java.util.function.Function
           java.util.function.BiConsumer
           java.util.function.Supplier
           java.util.concurrent.CompletableFuture
           java.lang.AutoCloseable))

;; Add an interface that extends everything reifed
;; by `make-fun`
(gen-interface
 :name exoscale.vinyl.fn.Functional
 :extends [java.util.function.Function
           java.util.function.BiFunction
           java.util.function.Consumer
           java.util.function.BiConsumer])

(defn ^exoscale.vinyl.fn.Functional make-fun
  "Turn a function of one argument into an implementation
   of `Function`, `BiFunction`, `Consumer`, and `BiConsumer`.

   To allow type hinting on the way out, the extending interface
   `exoscale.vinyl.fn.Functional` is used which extends all
    of the above."
  [f]
  (if (instance? Function f)
    f
    (reify
      exoscale.vinyl.fn.Functional
      (apply  [_ arg]       (f arg))
      (apply  [_ arg1 arg2] (f arg1 arg2))
      (accept [_ arg]       (f arg))
      (accept [_ arg1 arg2] (f arg1 arg2)))))

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
