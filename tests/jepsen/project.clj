(defproject mydb-jepsen "0.1.0-SNAPSHOT"
  :description "MySQL-protocol READ COMMITTED tests for mydb"
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.13"]
                 [elle "0.2.7"]
                 [com.mysql/mysql-connector-j "9.4.0"]]
  :repositories [["central" {:url "https://repo.maven.apache.org/maven2/"}]]
  :target-path "/tmp/mydb/jepsen/target/%s"
  :main mydb.core
  :jvm-opts ["-Xmx2g" "-Djava.awt.headless=true"])
