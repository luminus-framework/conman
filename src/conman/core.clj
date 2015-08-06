(ns conman.core
  (:require [clj-dbcp.core :as dbcp]
            [to-jdbc-uri.core :refer [to-jdbc-uri]]
            yesql.core
            clojure.java.jdbc))

(defmacro bind-connection
  "binds yesql queries to the connection atom specified by conn"
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

(defn connect!
  "attempts to create a new connection and set it as the value of the conn atom,
   does nothing if conn atom is already populated"
  [conn pool-spec]
  (when-not @conn
    (try
      (reset!
       conn
       {:datasource
        (-> pool-spec
            (update-in [:jdbc-url] #(when % (to-jdbc-uri %)))
            dbcp/make-datasource)})
      (catch Throwable t
        (throw (Exception. "Error occured while connecting to the database!" t))))))

(defn disconnect!
  "checks if there's a connection and closes it
   resets the conn to nil"
  [conn]
  (when-let [ds (:datasource @conn)]
    (when-not (.isClosed ds)
      (.close ds)))
  (reset! conn nil))

(defn reconnect!
  "calls disconnect! to ensure the connection is closed
   then calls connect! to establish a new connection"
  [conn pool-spec]
  (disconnect! conn)
  (connect! conn pool-spec))

(defmacro with-transaction
  "runs the body in a transaction where t-conn is the name of the transaction connection
   the body will be evaluated within a binding where conn is set to the transactional
   connection"
  [args & body]
  `(clojure.java.jdbc/with-db-transaction [~(first args) (deref ~(second args))]
     (binding [~(second args) (atom ~(first args))]
       ~@body)))
