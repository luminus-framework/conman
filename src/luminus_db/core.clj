(ns luminus-db.core
  (:require [yesql.core :as yesql]
            [clj-dbcp.core :as dbcp]
            [clojure.java.jdbc :as jdbc]
            [to-jdbc-uri.core :refer [to-jdbc-uri]]))

(defonce ^:dynamic conn (atom nil))

(def pool-spec
  {:adapter    :postgresql
   :init-size  1
   :min-idle   1
   :max-idle   4
   :max-active 32})

(defn connect! [url]
  (try
    (reset!
      conn
      {:datasource
       (dbcp/make-datasource
         (assoc pool-spec
           :jdbc-url (to-jdbc-uri url)))})
    (catch Throwable t
      (throw (Exception. "Error occured while connecting to the database!" t)))))

(defn disconnect! []
  (when-let [ds (:datasource @conn)]
    (when-not (.isClosed ds)
      (.close ds)
      (reset! conn nil))))

(defmacro with-transaction [t-conn & body]
  `(jdbc/with-db-transaction
     [~t-conn @conn]
     (binding [luminus-db.core/conn (atom ~t-conn)]
       ~@body)))

(defn defqueries [filename]
  (let [base-namespace *ns*
        queries-ns (symbol (str (name (ns-name *ns*)) ".connectionless-queries"))]
    (create-ns queries-ns)
    (in-ns queries-ns)
    (require '[yesql.core :as yesql])
    (let [yesql-queries (yesql/defqueries filename)
          queries (doall
                   (for [yesql-query yesql-queries]
                     (intern base-namespace
                             (with-meta (:name (meta yesql-query)) (meta yesql-queries))
                             (fn
                               ([] (yesql-query {} {:connection @conn}))
                               ([args] (yesql-query args {:connection @conn}))
                               ([args conn] (yesql-query args conn))))))]
      (in-ns (ns-name base-namespace))
      queries)))
