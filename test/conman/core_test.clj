(ns conman.core-test
  (:require [clojure.test :refer :all]
            [conman.core :refer :all]
            [clojure.java.jdbc :as sql]
            [clojure.repl :refer [doc]]
            [clojure.java.io :as io]))

(defonce ^:dynamic conn
         {:connection-uri "jdbc:h2:./test.db"
          :make-pool?     true
          :naming         {:keys   clojure.string/lower-case
                           :fields clojure.string/upper-case}})

(bind-connection conn "queries.sql")

(defn delete-test-db []
  (io/delete-file "test.db.mv.db" true)
  (io/delete-file "test.db.trace.db" true))

(defn create-test-table []
  (sql/db-do-commands
    conn
    "DROP TABLE fruits IF EXISTS;"
    (sql/create-table-ddl
      :fruits
      [[:id :int "DEFAULT 0"]
       [:name "VARCHAR(32)" "PRIMARY KEY"]
       [:appearance "VARCHAR(32)"]
       [:cost :int]
       [:grade :int]]
      {:table-spec ""})))

(use-fixtures
  :once
  (fn [f]
    (delete-test-db)
    (create-test-table)
    (f)))

(deftest doc-test
  (is (= "-------------------------\nconman.core-test/get-fruit\n  gets fruit by name\n"
         (with-out-str (doc get-fruit)))))

(deftest datasource
  (is
    (instance?
      clojure.lang.PersistentArrayMap
      (make-config
        {:jdbc-url             "jdbc:h2:./test.db"
         :datasource-classname "org.h2.Driver"}))))

(deftest datasource-classname
  (is
    (instance?
      clojure.lang.PersistentArrayMap
      (make-config
        {:datasource-classname "org.h2.Driver"
         :jdbc-url             "jdbc:h2:./test.db"}))))

(deftest jdbc-url
  (is
    (instance?
      clojure.lang.PersistentArrayMap
      (make-config
        {:jdbc-url "jdbc:h2:./test.db"}))))

(deftest transaction
  (with-transaction
    [conn]
    (sql/db-set-rollback-only! conn)
    (is
      (= 1
         (add-fruit!
           {:name       "apple"
            :appearance "red"
            :cost       1
            :grade      1})))
    (is
      (= [{:appearance "red" :cost 1 :grade 1 :id 0 :name "apple"}]
         (get-fruit {:name "apple"}))))
  (is
    (= []
       (get-fruit {:name "apple"}))))

(deftest transaction-options
  (with-transaction
    [conn {:isolation :serializable}]
    (is (= java.sql.Connection/TRANSACTION_SERIALIZABLE
           (.getTransactionIsolation (sql/db-connection conn)))))
  (with-transaction
    [conn {:isolation :read-uncommitted}]
    (is (= java.sql.Connection/TRANSACTION_READ_UNCOMMITTED
           (.getTransactionIsolation (sql/db-connection conn))))))

(deftest hugsql-snippets
  (is (= 1
         (add-fruit!
           {:name       "orange"
            :appearance "orange"
            :cost       1
            :grade      1})))
  (is (= "orange"
         (:name
           (get-fruit-by {:by-appearance
                          (by-appearance {:appearance "orange"})})))))



(deftest explicit-queries
  (let [queries (load-queries ["queries.sql"])]
    (is (= 1
           (query conn
                  queries
                  :add-fruit!
                  {:name       "banana"
                   :appearance "banana"
                   :cost       1
                   :grade      1})))
    (is (= "banana"
           (-> (query conn
                      queries
                      :get-fruit
                      {:name "banana"})
               first
               :name)))
    (is (= "banana"
           (:name (query conn
                         queries
                         :get-fruit-by
                         {:by-appearance
                          (snip queries :by-appearance {:appearance "banana"})}))))
    (query
      conn
      queries
      :add-fruit!
      {:name       "foo"
       :appearance "foo"
       :cost       1
       :grade      1})
    (try
      (with-transaction [conn]
        (query
          conn
          queries
          :add-fruit!
          {:name       "baz"
           :appearance "baz"
           :cost       1
           :grade      1})
        (query
          conn
          queries
          :add-fruit!
          {:name       "foo"
           :appearance "foo"
           :cost       1
           :grade      1}))
      (catch Exception _))
    (is (= [] (query conn queries :get-fruit {:name "baz"})))))
