(ns conman.core
  (:require [to-jdbc-uri.core :refer [to-jdbc-uri]]
            [hugsql.core :as hugsql]
            [clojure.java.io :as io]
            [org.tobereplaced.lettercase :refer [mixed-name]]
            [hikari-cp.core :refer [make-datasource datasource-config BaseConfigurationOptions]]
            [clojure.set :refer [rename-keys]]
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

(defn- add-datasource-property
  ""
  [config property value]
  (if value (.addDataSourceProperty config (mixed-name property) value)))

(defn- direct-datasource-config
  ""
  [options]
  (let [config (HikariConfig.)
        not-core-options (apply dissoc options
                                :username :password :pool-name
                                :connection-test-query
                                :configure :leak-detection-threshold
                                (keys BaseConfigurationOptions))
        {:keys [jdbc-url
                auto-commit
                configure
                datasource
                datasource-classname
                connection-test-query
                connection-timeout
                validation-timeout
                idle-timeout
                max-lifetime
                maximum-pool-size
                minimum-idle
                password
                pool-name
                read-only
                username
                leak-detection-threshold
                register-mbeans
                connection-init-sql]} options]
    (if jdbc-url (.setJdbcUrl config (to-jdbc-uri jdbc-url)))
    ;; Set pool-specific properties
    (if auto-commit (.setAutoCommit config auto-commit))
    (if read-only (.setReadOnly config read-only))
    (if connection-timeout (.setConnectionTimeout config connection-timeout))
    (if validation-timeout (.setValidationTimeout config validation-timeout))
    (if idle-timeout (.setIdleTimeout config idle-timeout))
    (if max-lifetime (.setMaxLifetime config max-lifetime))
    (if minimum-idle (.setMinimumIdle config minimum-idle))
    (if maximum-pool-size (.setMaximumPoolSize config maximum-pool-size))
    (if datasource (.setDataSource config datasource))
    (if datasource-classname (.setDataSourceClassName config datasource-classname))
    ;; Set optional properties
    (if username (.setUsername config username))
    (if password (.setPassword config password))
    (if pool-name (.setPoolName config pool-name))
    (if connection-test-query (.setConnectionTestQuery config connection-test-query))
    (when leak-detection-threshold
        (.setLeakDetectionThreshold config ^Long leak-detection-threshold))
    (when configure
        (configure config))
    (when connection-init-sql
        (.setConnectionInitSql config connection-init-sql))
    (when register-mbeans
        (.setRegisterMbeans config register-mbeans))
    ;; Set datasource-specific properties
    (doseq [[k v] not-core-options]
        (add-datasource-property config k v))
    config))

(defn make-config [{:keys [jdbc-url driver-class-name adapter datasource datasource-classname] :as pool-spec}]
  (when (not (or jdbc-url adapter datasource datasource-classname))
    (throw (Exception. "one of :jdbc-url, :adapter, :datasource, or :datasource-classname is required to initialize the connection!")))
  (if (or (and jdbc-url (nil? driver-class-name)) datasource datasource-classname)
    (direct-datasource-config pool-spec)
    (datasource-config
      (rename-keys
        pool-spec
        {:auto-commit?  :auto-commit
         :conn-timeout  :connection-timeout
         :min-idle      :minimum-idle
         :max-pool-size :maximum-pool-size}))))

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
