(ns conman.core-test
  (:require [clojure.test :refer :all]
            [conman.core :refer :all]
            [clojure.java.jdbc :as sql]))

(defonce ^:dynamic conn
         {:classname   "org.h2.Driver"
          :connection-uri "jdbc:h2:./test.db"
          :make-pool?     true
          :naming         {:keys   clojure.string/lower-case
                           :fields clojure.string/upper-case}})

(bind-connection conn "queries.sql")

(defn create-test-table []
  (sql/db-do-commands
   conn
   "DROP TABLE fruits IF EXISTS;"
   (sql/create-table-ddl
    :fruits
    [:id :int "DEFAULT 0"]
    [:name "VARCHAR(32)" "PRIMARY KEY"]
    [:appearance "VARCHAR(32)"]
    [:cost :int]
    [:grade :int]
    :table-spec "")))

(use-fixtures
 :once
 (fn [f]
   (create-test-table)
   (f)))

(deftest transaction
  (with-transaction
    [t-conn conn]
    (sql/db-set-rollback-only! t-conn)
    (is
     (= 1
        (add-fruit!
         {:name "apple"
          :appearance "red"
          :cost 1
          :grade 1})))
    (is
     (= [{:appearance "red" :cost 1 :grade 1 :id 0 :name "apple"}]
        (get-fruit {:name "apple"}))))
  (is
     (= []
        (get-fruit {:name "apple"}))))

(deftest transaction-options
  (with-transaction
    [t-conn conn :isolation :serializable]
    (is (= java.sql.Connection/TRANSACTION_SERIALIZABLE
           (.getTransactionIsolation (sql/db-connection t-conn)))))
  (with-transaction
    [t-conn conn :isolation :read-uncommitted]
    (is (= java.sql.Connection/TRANSACTION_READ_UNCOMMITTED
           (.getTransactionIsolation (sql/db-connection t-conn))))))
