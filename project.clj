(defproject conman "0.1.8"
  :description "a database connection management library"
  :url "https://github.com/luminus-framework/conman"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clj-dbcp "0.8.1"]
                 [yesql "0.5.0-rc3"]
                 [instaparse "1.4.1"]
                 [to-jdbc-uri "0.3.0"]
                 [org.clojure/java.jdbc "0.4.1"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.4.187"]]}})
