(ns conman.core
  (:require [to-jdbc-uri.core :refer [to-jdbc-uri]]
            yesql.core
            clojure.java.jdbc)
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defmacro bind-connection
  "binds yesql queries to the connection atom specified by conn"
  [conn & filenames]
  `(let [base-namespace# *ns*
         queries-ns#     (-> *ns* ns-name name (str ".connectionless-queries") symbol)]
     (create-ns queries-ns#)
     (in-ns queries-ns#)
     (require 'yesql.core)
     (let [yesql-connected-queries#
           (doall
             (flatten
               (for [filename# ~(vec filenames)]
                 (let [yesql-queries# (yesql.core/defqueries filename#)]
                   (for [yesql-query# yesql-queries#]
                     (intern base-namespace#
                             (:name (meta yesql-query#))
                             (fn
                               ([] (yesql-query# {} {:connection (deref ~conn)}))
                               ([args#] (yesql-query# args# {:connection (deref ~conn)}))
                               ([args# conn#] (yesql-query# args# {:connection conn#})))))))))]
       (in-ns (ns-name base-namespace#))
       yesql-connected-queries#)))

(defn- make-config
  [{:keys [jdbc-url username password auto-commit? conn-timeout idle-timeout
           max-lifetime min-idle max-pool-size pool-name]}]
  (let [cfg (HikariConfig.)
        uri (when jdbc-url (to-jdbc-uri jdbc-url))]
    (when uri                  (.setJdbcUrl cfg uri))
    (when username             (.setUsername cfg username))
    (when password             (.setPassword cfg password))
    (when (some? auto-commit?) (.setAutoCommit cfg auto-commit?))
    (when conn-timeout         (.setConnectionTimeout cfg conn-timeout))
    (when idle-timeout         (.setIdleTimeout cfg conn-timeout))
    (when max-lifetime         (.setMaxLifetime cfg max-lifetime))
    (when max-pool-size        (.setMaximumPoolSize cfg max-pool-size))
    (when min-idle             (.setMinimumIdle cfg min-idle))
    (when pool-name            (.setPoolName cfg pool-name))
    cfg))

(defn connect!
  "attempts to create a new connection and set it as the value of the conn atom,
   does nothing if conn atom is already populated"
  [conn pool-spec]
  (when-not @conn
    (try
      (reset!
        conn
        {:datasource (HikariDataSource. (make-config pool-spec))})
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
