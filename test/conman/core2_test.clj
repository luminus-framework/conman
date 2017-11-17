;; A subset of conman.core-test to test the case when `conn` is an IDeref
(ns conman.core2-test
  (:require [clojure.test :refer :all]
            [conman.core :refer :all]
            [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [mount.core :as m]))

(m/defstate ^:dynamic conn2
  :start {:connection-uri "jdbc:h2:./test.db"
          :make-pool?     true
          :naming         {:keys   clojure.string/lower-case
                           :fields clojure.string/upper-case}})

(bind-connection conn2 "queries.sql")

(defn delete-test-db []
  (io/delete-file "test.db.mv.db" true)
  (io/delete-file "test.db.trace.db" true))

(defn create-test-table []
  (sql/db-do-commands
    @conn2
    ["DROP TABLE fruits IF EXISTS;"
     (sql/create-table-ddl
       :fruits
       [[:id :int "DEFAULT 0"]
        [:name "VARCHAR(32)" "PRIMARY KEY"]
        [:appearance "VARCHAR(32)"]
        [:cost :int]
        [:grade :int]]
       {:table-spec ""})]))

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
    [conn2]
    (sql/db-set-rollback-only! @conn2)
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
