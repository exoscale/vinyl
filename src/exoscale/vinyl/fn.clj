(ns exoscale.vinyl.fn
  "Utility functions to build java.util.function implementations"
  (:import java.util.function.Function
           java.util.function.BiFunction
           java.util.function.Consumer))

(defn ^Function make-fun
  "Turn a function of one argument into a java Function"
  [f]
  (reify
    Function   (apply  [_ arg]       (f arg))
    BiFunction (apply  [_ arg1 arg2] (f arg1 arg2))
    Consumer   (accept [_ arg]       (f arg))))
