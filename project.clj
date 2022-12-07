(defproject conman "0.9.6"
  :description "a database connection management library"
  :url "https://github.com/luminus-framework/conman"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.layerware/hugsql-core "0.5.3"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.3"]
                 [com.carouselapps/to-jdbc-uri "0.5.0"]
                 [com.github.seancorfield/next.jdbc "1.2.796"]
                 [hikari-cp "3.0.1"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "2.1.214"]
                   [mount "0.1.16"]]}})
