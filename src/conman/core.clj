(ns conman.core
  (:require [clojure.java.io :as io]
            clojure.java.jdbc
            [clojure.set :refer [rename-keys]]
            [hikari-cp.core :refer [make-datasource]]
            [hugsql.core :as hugsql]
            [to-jdbc-uri.core :refer [to-jdbc-uri]]))

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
             (update :snips (fnil into {}) snips)
             (update :fns (fnil into {}) fns))))
     {}
     filenames)))

(defn query
  "runs a database query and returns the result
  conn    - database connection
  queries - a map of queries
  id      - keyword indicating the name of the query
  args    - arguments that will be passed to the query"
  [conn queries id & args]
  (if-let [query (-> queries :fns id :fn)]
    (apply query conn args)
    (throw (Exception. (str "no query found for the key '" id
                            "', available queries: " (keys (:fns queries)))))))
(defn snip
  "runs a SQL query snippet
  queries - a map of queries
  id      - keyword indicating the name of the query
  args    - arguments that will be passed to the query"
  [queries id & args]
  (if-let [snip (-> queries :snips id :fn)]
    (apply snip args)
    (throw (Exception. (str "no snippet found for the key '" id
                            "', available queries: " (keys (:snpis queries)))))))

(defmacro bind-connection [conn & filenames]
  (let [options?  (map? (first filenames))
        options   (if options? (first filenames) {})
        filenames (if options? (rest filenames) filenames)]
    `(let [{snips# :snips fns# :fns :as queries#} (conman.core/load-queries '~filenames ~options)]
       (doseq [[id# {fn# :fn {doc# :doc} :meta}] snips#]
         (intern *ns* (with-meta (symbol (name id#)) {:doc doc#})
                 (fn [& args#]
                   (try (apply fn# args#)
                        (catch Exception e#
                          (throw (Exception. (format "Exception in %s" id#) e#)))))))
       (doseq [[id# {fn# :fn {doc# :doc} :meta}] fns#]
         (intern *ns* (with-meta (symbol (name id#)) {:doc doc#})
                 (fn f#
                   ([] (f# (if (fn? ~conn) (~conn) ~conn) {}))
                   ([params#] (f# (if (fn? ~conn) (~conn) ~conn) params#))
                   ([conn# params#]
                    (try (fn# (if (fn? conn#) (conn#) conn#) params#)
                         (catch Exception e#
                           (throw (Exception. (format "Exception in %s" id#) e#)))))
                   ([conn# params# opts# & command-opts#]
                    (try (apply fn# (if (fn? conn#) (conn#) conn#) params# opts# command-opts#)
                         (catch Exception e#
                           (throw (Exception. (format "Exception in %s" id#) e#))))))))
       queries#)))

(defn- format-url [pool-spec]
  (if (:jdbc-url pool-spec)
    (update pool-spec :jdbc-url to-jdbc-uri)
    pool-spec))

(defn make-config [{:keys [jdbc-url adapter datasource datasource-classname] :as pool-spec}]
  (when (not (or jdbc-url adapter datasource datasource-classname))
    (throw (Exception. "one of :jdbc-url, :adapter, :datasource, or :datasource-classname is required to initialize the connection!")))
  (-> pool-spec
      (format-url)
      (rename-keys
        {:auto-commit?  :auto-commit
         :conn-timeout  :connection-timeout
         :min-idle      :minimum-idle
         :max-pool-size :maximum-pool-size})))

(defn connect!
  "attempts to create a new connection and set it as the value of the conn atom,
   does nothing if conn atom is already populated"
  [pool-spec]
  {:datasource (make-datasource (make-config pool-spec))})

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
