(ns conman.core
  (:require [clojure.java.io :as io]
            [clojure.set :refer [rename-keys]]
            [hikari-cp.core :refer [make-datasource]]
            [hugsql.core :as hugsql]
            [hugsql.adapter.next-jdbc :as next-adapter]
            [to-jdbc-uri.core :refer [to-jdbc-uri]])
  (:import [clojure.lang IDeref]))

(hugsql/set-adapter! (next-adapter/hugsql-adapter-next-jdbc))

(defn validate-files [filenames]
  (doseq [file filenames]
    (when-not (or (instance? java.io.File file) (io/resource file))
      (throw (Exception. (str "conman could not find the query file:" file))))))

(defn try-snip [[id snip]]
  [id
   (update snip :fn
           (fn [snip]
             (fn [& args]
               (try (apply snip args)
                    (catch Exception e
                      (throw (Exception. (str "Exception in " id) e)))))))])

(defn try-query [[id query]]
  [id
   (update query :fn
           (fn [query]
             (fn
               ([conn params]
                (try (query conn params)
                     (catch Exception e
                       (throw (Exception. (str "Exception in " id) e)))))
               ([conn params opts & command-opts]
                (try (apply query conn params opts command-opts)
                     (catch Exception e
                       (throw (Exception. (str "Exception in " id) e))))))))])

(defn load-queries [& args]
  (let [options?  (map? (first args))
        options   (if options? (first args) {})
        filenames (if options? (rest args) args)]
    (validate-files filenames)
    (reduce
      (fn [queries file]
        (let [{snips true
               fns   false}
              (group-by
                #(-> % second :meta :snip? boolean)
                (hugsql/map-of-db-fns file options))]
          (-> queries
              (update :snips (fnil into {}) (mapv try-snip snips))
              (update :fns (fnil into {}) (mapv try-query fns)))))
      {}
      filenames)))

(defn intern-fn [ns id meta f]
  (intern ns (with-meta (symbol (name id)) meta) f))

(defmacro bind-connection [conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (conman.core/load-queries ~@filenames)]
     (doseq [[id# {fn# :fn meta# :meta}] snips#]
       (conman.core/intern-fn *ns* id# meta# fn#))
     (doseq [[id# {query# :fn meta# :meta}] fns#]
       (conman.core/intern-fn *ns* id# meta#
                              (fn f#
                                ([] (query# ~conn {}))
                                ([params#] (query# ~conn params#))
                                ([conn# params# & args#] (apply query# conn# params# args#)))))
     queries#))

(defmacro bind-connection-deref [conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (conman.core/load-queries ~@filenames)]
     (doseq [[id# {fn# :fn meta# :meta}] snips#]
       (conman.core/intern-fn *ns* id# meta# fn#))
     (doseq [[id# {query# :fn meta# :meta}] fns#]
       (conman.core/intern-fn *ns* id# meta#
                              (fn f#
                                ([] (query# (deref ~conn) {}))
                                ([params#] (query# (deref ~conn) params#))
                                ([conn# params# & args#] (apply query# conn# params# args#)))))
     queries#))

(defn bind-connection-map [conn & args]
  (-> (apply load-queries args)
      (update :snips
              (fn [snips]
                (reduce (fn [acc [id snip]] (assoc acc id snip)) {} snips)))
      (update :fns
              (fn [queries]
                (reduce
                  (fn [acc [id query]]
                    (assoc acc id
                               (update query
                                       :fn
                                       (fn [query]
                                         (fn fn#
                                           ([] (query conn {}))
                                           ([params]
                                            (query conn params))
                                           ([conn params & args] (apply query conn params args)))))))
                  {}
                  queries)))))

(defn find-fn [connection-map query-type k]
  (or (get-in connection-map [query-type k :fn])
      (throw (IllegalArgumentException.
               (str (if (= query-type :snips) "no snippet" "no query")
                    " found for the key: " k
                    "', available queries: " (keys (get connection-map query-type)))))))

(defn snip [connection-map snip-key & args]
  "runs a SQL query snippet
  queries - a map of queries
  id      - keyword indicating the name of the query
  args    - arguments that will be passed to the query"
  (apply (find-fn connection-map :snips snip-key) args))

(defn query
  "runs a database query and returns the result
  conn    - database connection
  queries - a map of queries
  id      - keyword indicating the name of the query
  args    - arguments that will be passed to the query"
  ([connection-map query-key]
   ((find-fn connection-map :fns query-key)))
  ([connection-map query-key params]
   ((find-fn connection-map :fns query-key) params))
  ([conn connection-map query-key params & opts]
   (apply (find-fn connection-map :fns query-key) conn params opts)))

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
  (make-datasource (make-config pool-spec)))

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

(extend-protocol next.jdbc.protocols/Sourceable
  IDeref
  (get-datasource [this]
    (next.jdbc.protocols/get-datasource (deref this))))

(defmacro with-transaction
  "Runs the body in a transaction where t-conn is the name of the transaction connection.
   The body will be evaluated within a binding where conn is set to the transactional
   connection. The isolation level and readonly status of the transaction may also be specified.
   (with-transaction [conn {:isolation level :read-only? true}]
     ... t-conn ...)
   See next.jdbc/transact for more details on the semantics of the :isolation and
   :read-only options."
  [[dbsym & opts] & body]
  `(if (instance? IDeref ~dbsym)
     (next.jdbc/with-transaction [t-conn# (deref ~dbsym) ~@opts]
                                 (binding [~dbsym (delay t-conn#)]
                                   ~@body))
     (next.jdbc/with-transaction [t-conn# ~dbsym ~@opts]
                                 (binding [~dbsym t-conn#]
                                   ~@body))))
