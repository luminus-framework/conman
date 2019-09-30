;; A subset of conman.core-test to test the case when `conn` is an IDeref
(ns conman.core2-test
  (:require [clojure.test :refer :all]
            [conman.core :refer :all]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [mount.core :as m]))

(m/defstate ^:dynamic conn2
  :start {:jdbcUrl       "jdbc:h2:./test.db"
          :make-pool?     true
          :naming         {:keys   clojure.string/lower-case
                           :fields clojure.string/upper-case}})

(bind-connection-deref conn2 "queries.sql")

(defn delete-test-db []
  (io/delete-file "test.db.mv.db" true)
  (io/delete-file "test.db.trace.db" true))

(defn create-test-table []
  (jdbc/execute!
    conn2
    ["DROP TABLE fruits IF EXISTS;
    CREATE TABLE fruits (
     id int default 0,
     name varchar(32) primary key,
     appearance varchar(32),
     cost int,
     grade int
     );"]))

(use-fixtures
  :once
  (fn [f]
    (m/in-cljc-mode)
    (m/start #'conn2)
    (delete-test-db)
    (create-test-table)
    (f)
    (m/stop)))

(deftest transaction
  (with-transaction
    [conn2 {:rollback-only true}]
    (is
      (= 1
         (add-fruit!
           {:name       "apple"
            :appearance "red"
            :cost       1
            :grade      1}))))
  (is
    (= []
       (get-fruit {:name "apple"}))))
