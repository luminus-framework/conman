(defproject conman "0.7.9"
  :description "a database connection management library"
  :url "https://github.com/luminus-framework/conman"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.layerware/hugsql "0.4.9"]
                 [com.carouselapps/to-jdbc-uri "0.5.0"]
                 [org.clojure/java.jdbc "0.7.6"]
                 [hikari-cp "2.4.0"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.4.196"]
                   [mount "0.1.12"]]}})
