(ns luminus-db.core
  (:require [yesql.core :as yesql]
            [clj-dbcp.core :as dbcp]
            [clojure.java.jdbc :as jdbc]))

(defn- queries-ns-name []
  (str (name (ns-name *ns*)) ".connectionless-queries"))

(defn init! [conn & filenames]
  (let [base-namespace *ns*
        queries-ns (symbol (queries-ns-name))]
    (in-ns queries-ns)
    (require '[yesql.core :as yesql])
    (doseq [filename filenames]
      (let [yesql-queries (yesql/defqueries filename)]
        (doall
         (for [yesql-query yesql-queries]
           (intern base-namespace
                   (with-meta (:name (meta yesql-query)) (meta yesql-queries))
                   (fn
                     ([] (yesql-query {} {:connection @conn}))
                     ([args] (yesql-query args {:connection @conn}))
                     ([args conn] (yesql-query args {:connection conn}))))))))
    (in-ns (ns-name base-namespace))))

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
