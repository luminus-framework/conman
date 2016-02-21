(ns conman.core
  (:require [to-jdbc-uri.core :refer [to-jdbc-uri]]
            [hugsql.core :as hugsql]
    ;yesql.core
            clojure.java.jdbc)
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn load-queries [filenames]
  (reduce
    (fn [queries file]
      (let [{snips true
             fns   false}
            (group-by
              #(-> % second :snip? boolean)
              (hugsql/map-of-db-fns file))]
        (-> queries
            (update :snips into snips)
            (update :fns into fns))))
    {}
    filenames))

(defmacro bind-connection [conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (conman.core/load-queries '~filenames)]
     (doseq [[id# {fn# :fn}] snips#]
       (intern *ns* (symbol (name id#)) fn#))
     (doseq [[id# {fn# :fn}] fns#]
       (intern *ns* (symbol (name id#))
               (fn
                 ([] (fn# (deref ~conn) {}))
                 ([params#] (fn# (deref ~conn) params#))
                 ([conn# params#] (fn# conn# params#))
                 ([conn# params# opts# & command-opts#]
                  (apply fn# conn# params# opts# command-opts#)))))
     queries#))

(defn- make-config
  [{:keys [jdbc-url datasource datasource-classname username
           password auto-commit? conn-timeout idle-timeout
           max-lifetime min-idle max-pool-size pool-name]}]
  (let [cfg (HikariConfig.)
        uri (when jdbc-url (to-jdbc-uri jdbc-url))]
    (when uri                  (.setJdbcUrl cfg uri))
    (when datasource           (.setDataSource cfg datasource))
    (when datasource-classname (.setDataSourceClassName cfg datasource-classname))
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
  "Runs the body in a transaction where t-conn is the name of the transaction connection.
   The body will be evaluated within a binding where conn is set to the transactional
   connection. The isolation level and readonly status of the transaction may also be specified.
   (with-db-transaction [t-conn conn :isolation level :read-only? true]
     ... t-conn ...)
   See clojure.java.jdbc/db-transaction* for more details."
  [args & body]
  `(clojure.java.jdbc/with-db-transaction [~(first args) (deref ~(second args)) ~@(rest (rest args))]
                                          (binding [~(second args) (atom ~(first args))]
                                            ~@body)))
