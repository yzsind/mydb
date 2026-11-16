(ns mydb.jdbc
  (:require [clojure.string :as str])
  (:import (java.sql Connection DriverManager SQLException)
           (java.util Properties)))

(defn execute!
  [^Connection conn sql & params]
  (with-open [stmt (.prepareStatement conn sql)]
    (.setQueryTimeout stmt 10)
    (doseq [[i value] (map-indexed vector params)]
      (.setObject stmt (inc i) value))
    (.executeUpdate stmt)))

(defn query
  [^Connection conn sql & params]
  (with-open [stmt (.prepareStatement conn sql)]
    (.setQueryTimeout stmt 10)
    (doseq [[i value] (map-indexed vector params)]
      (.setObject stmt (inc i) value))
    (with-open [rs (.executeQuery stmt)]
      (let [n (.getColumnCount (.getMetaData rs))]
        (loop [rows []]
          (if (.next rs)
            (recur (conj rows (mapv #(.getObject rs (int %)) (range 1 (inc n)))))
            rows))))))

(defn open!
  ([test] (open! test (:database test)))
  ([test database]
   (let [props (doto (Properties.)
                 (.setProperty "user" (:db-user test))
                 (.setProperty "password" (or (System/getenv "JEPSEN_MYDB_PASSWORD") "root"))
                 (.setProperty "sslMode" (:jdbc-ssl-mode test))
                 (.setProperty "allowPublicKeyRetrieval" "true")
                 (.setProperty "autoReconnect" "false")
                 (.setProperty "useServerPrepStmts" (str (boolean (:server-prepared? test))))
                 (.setProperty "emulateUnsupportedPstmts" "false")
                 (.setProperty "cachePrepStmts" "false")
                 (.setProperty "useCursorFetch" (str (boolean (:server-prepared? test))))
                 ;; Keep this above Connector/J's minimum packet/chunk setting.
                 (.setProperty "blobSendChunkSize" "16384")
                 (.setProperty "connectTimeout" "5000")
                 (.setProperty "socketTimeout" "15000"))
         conn (DriverManager/getConnection
                (str "jdbc:mysql://" (:host test) ":" (:port test) "/" database)
                props)]
     (try
       (.setTransactionIsolation conn Connection/TRANSACTION_READ_COMMITTED)
       (let [actual (ffirst (query conn "SELECT @@session.transaction_isolation"))]
         (when-not (= "READ-COMMITTED" (str/upper-case (str actual)))
           (throw (ex-info "Server did not enable READ COMMITTED" {:actual actual}))))
       conn
       (catch Throwable e
         (.close conn)
         (throw e))))))

(defn rollback!
  "True only when the server acknowledged the rollback."
  [^Connection conn]
  (try (.rollback conn) true (catch SQLException _ false)))

(defn sql-error
  "Never infer COMMIT failure from a timeout or subsequent rollback."
  [^SQLException e commit-attempted? rollback-confirmed?]
  {:type (if (and (not commit-attempted?) rollback-confirmed?) :fail :info)
   :error {:sql-state (.getSQLState e)
           :code (.getErrorCode e)
           :message (.getMessage e)}
   ;; Only concurrency/connection failures are expected. Syntax errors and
   ;; unsupported SQL must fail the harness, even if rollback succeeded.
   :unexpected-error? (not (or (#{1205 1213} (.getErrorCode e))
                              (str/starts-with? (or (.getSQLState e) "") "08")
                              (instance? java.sql.SQLTimeoutException e)))})
