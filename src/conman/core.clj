(ns conman.core
  (:require [to-jdbc-uri.core :refer [to-jdbc-uri]]
            [hugsql.core :as hugsql]
            [clojure.java.io :as io]
            clojure.java.jdbc)
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn validate-files [filenames]
  (doseq [file filenames]
    (when-not (io/resource file)
      (throw (Exception. (str "conman could not find the query file:" file))))))

(defn load-queries [filenames]
  (validate-files filenames)
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
       (doseq [[id# {fn# :fn {doc# :doc} :meta}] snips#]
         (intern *ns* (with-meta (symbol (name id#)) {:doc doc#}) fn#))
       (doseq [[id# {fn# :fn {doc# :doc} :meta}] fns#]
         (intern *ns* (with-meta (symbol (name id#)) {:doc doc#})
                 (fn
                   ([] (fn# ~conn {}))
                   ([params#] (fn# ~conn params#))
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
    (when (not (or uri datasource datasource-classname ))
      (throw (Exception. ":jdbc-url, :datasource, or :datasource-classname is required to initialize the connection!")))
    (when uri (.setJdbcUrl cfg uri))
    (when datasource (.setDataSource cfg datasource))
    (when datasource-classname (.setDataSourceClassName cfg datasource-classname))
    (when username (.setUsername cfg username))
    (when password (.setPassword cfg password))
    (when (some? auto-commit?) (.setAutoCommit cfg auto-commit?))
    (when conn-timeout (.setConnectionTimeout cfg conn-timeout))
    (when idle-timeout (.setIdleTimeout cfg conn-timeout))
    (when max-lifetime (.setMaxLifetime cfg max-lifetime))
    (when max-pool-size (.setMaximumPoolSize cfg max-pool-size))
    (when min-idle (.setMinimumIdle cfg min-idle))
    (when pool-name (.setPoolName cfg pool-name))
    cfg))

(defn connect!
  "attempts to create a new connection and set it as the value of the conn atom,
   does nothing if conn atom is already populated"
  [pool-spec]
  {:datasource (HikariDataSource. (make-config pool-spec))})

(defn disconnect!
  "checks if there's a connection and closes it
   resets the conn to nil"
  [conn]
  (when-let [ds (:datasource conn)]
    (when-not (.isClosed ds)
      (.close ds))))

(defn reconnect!
  "calls disconnect! to ensure the connection is closed
   then calls connect! to establish a new connection"
  [conn pool-spec]
  (disconnect! conn)
  (connect! pool-spec))

(defmacro with-transaction
  "Runs the body in a transaction where t-conn is the name of the transaction connection.
   The body will be evaluated within a binding where conn is set to the transactional
   connection. The isolation level and readonly status of the transaction may also be specified.
   (with-db-transaction [t-conn conn :isolation level :read-only? true]
     ... t-conn ...)
   See clojure.java.jdbc/db-transaction* for more details."
  [args & body]
  `(clojure.java.jdbc/with-db-transaction [t-conn# ~(first args) ~@(rest args)]
                                          (binding [~(first args) t-conn#]
                                            ~@body)))
