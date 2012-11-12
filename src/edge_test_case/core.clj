(ns edge-test-case.core
  (:import (com.thinkaurelius.titan.core TitanFactory)
           (com.tinkerpop.blueprints TransactionalGraph$Conclusion)
           (org.apache.commons.configuration BaseConfiguration)))

(def conf (BaseConfiguration.))
(.setProperty conf "storage.backend" "hbase")
(.setProperty conf "storage.hostname" "127.0.0.1")

(def ^:dynamic *graph* (TitanFactory/open conf))

(def success-flag TransactionalGraph$Conclusion/SUCCESS)

(def failure-flag TransactionalGraph$Conclusion/FAILURE)

(defmacro transact! [& forms]
  `(try
     (let [tx#      (.startTransaction *graph*)
           results# (binding [*graph* tx#] ~@forms)]
       (.commit tx#)
       (.stopTransaction *graph* success-flag)
       results#)
     (catch Exception e#
       (println e#)
       (.stopTransaction *graph* failure-flag)
       (println "Stopped transaction")
       (throw e#))))

(defn -main [& args]
  (let [u  (transact! (.addVertex *graph*))
        v  (transact! (.addVertex *graph*))
        w  (transact! (.addVertex *graph*))
        
        e1 (transact! (.addEdge *graph*
                                (.getVertex *graph* u)
                                (.getVertex *graph* v)
                                "string"))
        
        e2 (transact! (.addEdge *graph*
                                (.getVertex *graph* u)
                                (.getVertex *graph* w)
                                "string"))]
    [u v w e1 e2]))
