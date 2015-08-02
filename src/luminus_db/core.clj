(ns luminus-db.core
  (:require [yesql.core :as yesql]
            [clj-dbcp.core :as dbcp]
            [clojure.java.jdbc :as jdbc]))

(defmacro bind-connection
  [conn & filenames]
  `(let [base-namespace# *ns*
         queries-ns# (-> *ns* ns-name name (str ".connectionless-queries") symbol)]
     (create-ns queries-ns#)
     (in-ns queries-ns#)
     (require 'yesql.core)
     (doseq [filename# ~(vec filenames)]
       (let [yesql-queries# (yesql.core/defqueries filename#)]
         (doseq [yesql-query# yesql-queries#]
           (intern base-namespace#
                   (with-meta (:name (meta yesql-query#)) (meta yesql-queries#))
                   (fn
                     ([] (yesql-query# {} {:connection (deref ~conn)}))
                     ([args#] (yesql-query# args# {:connection (deref ~conn)}))
                     ([args# conn#] (yesql-query# args# {:connection conn#})))))))
     (in-ns (ns-name base-namespace#))))

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
      (.close ds)))
  (reset! conn nil))

(defmacro with-transaction
  "runs the body in a transaction where t-conn is the name of the transaction connection
   the body will be evaluated within a binding where conn is set to the transactional
   connection"
  [args & body]
  `(jdbc/with-db-transaction [~(first args) (deref ~(second args))]
     (binding [~(second args) (atom ~(first args))]
       ~@body)))
