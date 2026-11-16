(ns mydb.recovery
  "Bounded fault scenarios with real JDBC histories and an owned local controller."
  (:require [clojure.java.io :as io]
            [elle.list-append :as elle]
            [jepsen.history :as history]
            [mydb.checker :as check]
            [mydb.jdbc :as jdbc]
            [mydb.workload.append :as append])
  (:import (java.sql Connection SQLException)
           (java.util UUID)))

(defn control! [action]
  (println (str "CONTROL " action))
  (flush)
  (when-not (= "OK" (read-line))
    (throw (ex-info "Local fault controller failed" {:action action}))))

(defn record! [events op]
  (swap! events #(conj % (assoc op :index (long (count %)) :time (System/nanoTime)))))

(defn close-after-fault! [^Connection conn]
  ;; Connector/J attempts ROLLBACK during close. A dead connection can throw
  ;; here; cleanup must not replace the recorded COMMIT outcome or prevent reads.
  (try (.close conn)
       (catch SQLException e
         (binding [*out* *err*] (println "Fault connection cleanup:" (.getMessage e))))))

(defn transaction! [events conn process mops abort?]
  (let [op {:process process :f :txn :value mops}
        committing? (volatile! false)]
    (record! events (assoc op :type :invoke))
    (try
      (.setAutoCommit ^Connection conn false)
      (let [observed (mapv #(append/micro-op! conn %) mops)]
        (if abort?
          (.rollback ^Connection conn)
          (do (vreset! committing? true) (.commit ^Connection conn)))
        (record! events (cond-> (assoc op :type (if abort? :fail :ok) :value observed)
                         abort? (assoc :error :intentional-rollback))))
      (catch SQLException e
        (record! events (merge op (jdbc/sql-error e @committing? (jdbc/rollback! conn))))))
    (last @events)))

(defn recovery-result
  "Unknown transactions resolve atomically and cannot change after readback."
  [events expected-unknown]
  (let [keys (set (for [op events [_ k] (:value op)] k))
        snapshots (vec (for [[i op] (map-indexed vector events) :when (and (:final-read? op) (not= :invoke (:type op)))]
                         (let [actual (set (for [[f k vs] (:value op) :when (= f :r) v vs] [k v]))
                               unknown (filter #(and (= :info (:type %)) (= :txn (:f %))) (take i events))
                               outcomes (mapv (fn [tx]
                                                (let [writes (vec (for [[f k v] (:value tx) :when (= f :append)] [k v]))
                                                      present (count (filter actual writes))]
                                                  {:process (:process tx) :tokens writes
                                                   :writes (count writes) :present present
                                                   :valid? (case expected-unknown
                                                             :absent (zero? present)
                                                             :present (= present (count writes))
                                                             :either (or (zero? present) (= present (count writes))))})) unknown)]
                           {:valid? (boolean (and (:valid? (check/readback-result op keys))
                                                  (every? #(true? (:valid? %)) outcomes)))
                            :unknown-outcomes outcomes})))
        by-tokens (group-by :tokens (mapcat :unknown-outcomes snapshots))
        stable? (every? #(apply = (map :present %)) (vals by-tokens))]
    {:valid? (boolean (and (seq snapshots) stable? (every? #(true? (:valid? %)) snapshots)))
     :unknown-outcomes (:unknown-outcomes (last snapshots))
     :unknown-outcome-stable? stable? :snapshots snapshots}))

(defn read-state! [events test process]
  (with-open [conn (jdbc/open! test)]
    (let [result (transaction! events conn process (mapv #(vector :r % nil) (range 4)) false)]
      (when-not (= :ok (:type result))
        (throw (ex-info "Recovery read failed" result))))
    (swap! events update (dec (count @events)) assoc :final-read? true)
    (:value (last @events))))

(defn scenario! [port directory mode checkpoint?]
  (let [test {:host "127.0.0.1" :port port :db-user "root" :jdbc-ssl-mode "DISABLED"
              :database (str "jepsen_recovery_" (.replace (str (UUID/randomUUID)) "-" ""))}
        events (atom [])
        expected (case mode "before" :absent "after-ok" :present :either)]
    (.mkdirs (io/file directory))
    (spit (io/file directory "test.edn") (pr-str (assoc test :mode mode :checkpoint? checkpoint?)))
    (try
      (with-open [conn (jdbc/open! test "")]
        (jdbc/execute! conn (str "CREATE DATABASE " (:database test))))
      (with-open [conn (jdbc/open! test)]
        (jdbc/execute! conn "CREATE TABLE rc_append (id BIGINT PRIMARY KEY, v LONGTEXT NOT NULL)")
        (doseq [k (range 4)] (jdbc/execute! conn "INSERT INTO rc_append VALUES (?, '')" k))
        (when checkpoint? (jdbc/execute! conn "ALTER SYSTEM CHECKPOINT")))
      (let [winner (jdbc/open! test)
            loser (try (jdbc/open! test)
                       (catch Throwable e (close-after-fault! winner) (throw e)))]
        (try
          ;; Hold an uncommitted two-row transaction across the crash. The winner's
          ;; later strict commit also flushes the loser's earlier WAL records.
          (.setAutoCommit ^Connection loser false)
          (record! events {:process 1 :type :invoke :f :txn :value [[:append 2 90] [:append 3 91]]})
          (doseq [mop [[:append 2 90] [:append 3 91]]] (append/micro-op! loser mop))
          (when-not (= :ok (:type (transaction! events winner 0 [[:append 0 1] [:append 1 2]] false)))
            (throw (ex-info "Baseline commit failed" {})))
          (when-not (= :ok (:type (transaction! events winner 0 [[:r 0 nil] [:r 1 nil]] false)))
            (throw (ex-info "Baseline read failed" {})))
          (when-not (= :intentional-rollback (:error (transaction! events winner 0 [[:append 0 3] [:append 1 4]] true)))
            (throw (ex-info "Baseline rollback failed" {})))
          (when-not (= "crash" mode)
            (control! (str "arm-" mode))
            (let [op (transaction! events winner 0 [[:append 0 5] [:append 1 6]] false)]
              (when-not (and (= :info (:type op)) (not (:unexpected-error? op)))
                (throw (ex-info "COMMIT fault did not yield expected uncertainty" op)))))
          (control! "crash-restart")
          ;; No COMMIT was ever sent on loser. Process death ends this transaction.
          (record! events {:process 1 :type :fail :f :txn :value [[:append 2 90] [:append 3 91]]})
          (finally (close-after-fault! loser) (close-after-fault! winner))))
      (let [first-state (read-state! events test 2)
            first-check (check/final-state-result @events)
            first-atomicity (recovery-result @events expected)]
        ;; Crash recovered state again before adding new writes, checking replay
        ;; idempotence independently of post-recovery traffic.
        (control! "crash-restart")
        (let [second-state (read-state! events test 2)]
          (with-open [conn (jdbc/open! test)]
            (when-not (= :ok (:type (transaction! events conn 3 [[:append 0 7] [:append 1 8]] false)))
              (throw (ex-info "Post-recovery commit failed" {})))
            (when-not (= :intentional-rollback (:error (transaction! events conn 3 [[:append 0 9] [:append 1 10]] true)))
              (throw (ex-info "Post-recovery rollback failed" {}))))
          (control! "crash-restart")
          (read-state! events test 2)
          (let [final-check (check/final-state-result @events)
                atomicity (recovery-result @events expected)
                isolation (elle/check {:consistency-models [:read-committed]
                                       :max-plot-bytes 0 :directory (str directory "/elle")}
                                      (history/history @events))
                stable? (= (:unknown-outcomes first-atomicity) (:unknown-outcomes atomicity))
                result {:valid? (and (= first-state second-state) stable?
                                     (every? #(true? (:valid? %))
                                             [first-check first-atomicity final-check atomicity isolation]))
                        :replay-idempotent? (= first-state second-state)
                        :unknown-outcome-stable? stable?
                        :first-state first-check :first-atomicity first-atomicity
                        :final-state final-check :atomicity atomicity :isolation isolation}]
            (spit (io/file directory "results.edn") (pr-str result))
            result)))
      (finally (spit (io/file directory "history.edn") (pr-str @events))))))

(defn -main [port output]
  (try
    (let [results (doall
                    (for [checkpoint? [false true] mode ["crash" "before" "after-ok" "inflight"]]
                      (let [name (str mode (if checkpoint? "-checkpoint" "-wal"))
                            result (scenario! (parse-long port) (str output "/" name) mode checkpoint?)]
                        (println "SCENARIO" name (:valid? result))
                        (flush)
                        [name result])))]
      (spit (io/file output "results.edn") (pr-str (into {} results)))
      (System/exit (if (every? #(true? (:valid? (second %))) results) 0 1)))
    (catch Throwable e
      (spit (io/file output "error.edn") (pr-str (Throwable->map e)))
      (.printStackTrace e)
      (System/exit 1))))
