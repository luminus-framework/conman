(ns luminus-db.core
  (:require [yesql.core :as yesql]
            [clj-dbcp.core :as dbcp]
            [clojure.java.jdbc :as jdbc]))

(defn- queries-ns-name []
  (str (name (ns-name *ns*)) ".connectionless-queries"))

#_(defn init! [filename]
  (let [conn* (atom nil)
        base-namespace *ns*
        queries-ns (symbol (queries-ns-name))]
    (in-ns queries-ns)
    (require '[yesql.core :as yesql])
    (defonce ^:dynamic conn conn*)
    (let [yesql-queries (yesql/defqueries filename)]
      (doall
       (for [yesql-query yesql-queries]
         (intern base-namespace
                 (with-meta (:name (meta yesql-query)) (meta yesql-queries))
                 (fn
                   ([] (yesql-query {} {:connection @conn}))
                   ([args] (yesql-query args {:connection @conn}))
                   ([args conn] (yesql-query args conn)))))))
      (in-ns (ns-name base-namespace))
      conn*))

(defn init! [& filenames]
  (let [conn* (atom nil)
        base-namespace *ns*
        queries-ns (symbol (queries-ns-name))]
    (in-ns queries-ns)
    (require '[yesql.core :as yesql])
    (defonce ^:dynamic conn conn*)
    (doseq [filename filenames]
      (let [yesql-queries (yesql/defqueries filename)]
        (doall
         (for [yesql-query yesql-queries]
           (intern base-namespace
                   (with-meta (:name (meta yesql-query)) (meta yesql-queries))
                   (fn
                     ([] (yesql-query {} {:connection @conn}))
                     ([args] (yesql-query args {:connection @conn}))
                     ([args conn] (yesql-query args conn))))))))
    (in-ns (ns-name base-namespace))
    conn*))

(defmacro with-transaction [t-conn & body]
  `(jdbc/with-db-transaction
     [~t-conn @conn]
     (binding [luminus-db.core/conn (atom ~t-conn)]
       ~@body)))

(defn connect! [conn pool-spec]
  (try
    (reset!
     conn
     {:datasource (dbcp/make-datasource pool-spec)})
    (catch Throwable t
      (throw (Exception. "Error occured while connecting to the database!" t)))))

(defn disconnect! [conn]
  (when-let [ds (:datasource @conn)]
    (when-not (.isClosed ds)
      (.close ds)
      (reset! conn nil))))
