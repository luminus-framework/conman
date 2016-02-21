(defproject conman "0.4.2"
  :description "a database connection management library"
  :url "https://github.com/luminus-framework/conman"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.zaxxer/HikariCP "2.4.3"]
                 [com.layerware/hugsql "0.4.1"]
                 [com.carouselapps/to-jdbc-uri "0.5.0"]
                 [org.clojure/java.jdbc "0.4.2"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.4.191"]]}})
