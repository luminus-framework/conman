(ns conman.core
  (:require [to-jdbc-uri.core :refer [to-jdbc-uri]]
            [hugsql.core :as hugsql]
            [clojure.java.io :as io]
            [org.tobereplaced.lettercase :refer [mixed-name]]
            [hikari-cp.core :refer [make-datasource datasource-config BaseConfigurationOptions]]
            [clojure.set :refer [rename-keys]]
            clojure.java.jdbc)
  (:import [com.zaxxer.hikari HikariDataSource]))

(defn validate-files [filenames]
  (doseq [file filenames]
    (when-not (io/resource file)
      (throw (Exception. (str "conman could not find the query file:" file))))))

(defn load-queries
  ([filenames] (load-queries filenames {}))
  ([filenames options]
   (validate-files filenames)
   (reduce
     (fn [queries file]
       (let [{snips true
              fns   false}
             (group-by
               #(-> % second :meta :snip? boolean)
               (hugsql/map-of-db-fns file options))]
         (-> queries
             (update :snips into snips)
             (update :fns into fns))))
     {}
     filenames)))

(defmacro bind-connection [conn & filenames]
  (let [options? (map? (first filenames))
        options (if options? (first filenames) {})
        filenames (if options? (rest filenames) filenames)]
    `(let [{snips# :snips fns# :fns :as queries#} (conman.core/load-queries '~filenames ~options)]
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
       queries#)))

(defn- format-url [pool-spec]
  (if (:jdbc-url pool-spec)
    (update pool-spec :jdbc-url to-jdbc-uri)
    pool-spec))

(defn make-config [{:keys [jdbc-url adapter datasource datasource-classname] :as pool-spec}]
  (when (not (or jdbc-url adapter datasource datasource-classname))
    (throw (Exception. "one of :jdbc-url, :adapter, :datasource, or :datasource-classname is required to initialize the connection!")))
  (datasource-config
    (-> pool-spec
        (format-url)
        (rename-keys
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
   (with-transaction [conn {:isolation level :read-only? true}]
     ... t-conn ...)
   See clojure.java.jdbc/db-transaction* for more details on the semantics of the :isolation and
   :read-only? options."
  [args & body]
  `(clojure.java.jdbc/with-db-transaction [t-conn# ~(first args) ~@(rest args)]
                                          (binding [~(first args) t-conn#]
                                            ~@body)))
