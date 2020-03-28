(defproject conman "0.8.5"
  :description "a database connection management library"
  :url "https://github.com/luminus-framework/conman"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.layerware/hugsql "0.5.1"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.1"]
                 [com.carouselapps/to-jdbc-uri "0.5.0"]
                 [seancorfield/next.jdbc "1.0.409"]
                 [hikari-cp "2.11.0"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.4.199"]
                   [mount "0.1.16"]]}})
