(defproject conman "0.9.4"
  :description "a database connection management library"
  :url "https://github.com/luminus-framework/conman"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.layerware/hugsql-core "0.5.3"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.3"]
                 [com.carouselapps/to-jdbc-uri "0.5.0"]
                 [seancorfield/next.jdbc "1.1.613"]
                 [hikari-cp "2.13.0"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.4.200"]
                   [mount "0.1.16"]]}})
