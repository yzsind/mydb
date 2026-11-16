(ns mydb.checker-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [elle.list-append :as elle]
            [jepsen.history :as history]
            [jepsen.checker :as checker]
            [mydb.checker :as check]
            [mydb.jdbc :as jdbc]
            [mydb.recovery :as recovery]
            [mydb.workload.append :as append])
  (:import (java.sql SQLException)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(defn transaction-history [transactions]
  (history/history
    (vec (mapcat (fn [i [type value]]
                   [{:process i :type :invoke :f :txn :value (mapv (fn [[f k v]] [f k (when (= :append f) v)]) value)}
                    {:process i :type type :f :txn :value value}])
                 (range) transactions))))

(deftest elle-rejects-aborted-and-intermediate-reads
  (doseq [transactions [[[:fail [[:append 0 1]]] [:ok [[:r 0 [1]]]]]
                        [[:ok [[:append 0 1] [:append 0 2]]] [:ok [[:r 0 [1]]]]]]]
    (is (false? (:valid? (elle/check {:consistency-models [:read-committed]}
                                    (transaction-history transactions)))))))

(deftest rc-allows-nonrepeatable-reads
  ;; T0 commits between two reads in T1. This is legal READ COMMITTED.
  (let [h (history/history
            [{:process 1 :type :invoke :f :txn :value [[:r 0 nil] [:r 0 nil]]}
             {:process 0 :type :invoke :f :txn :value [[:append 0 1]]}
             {:process 0 :type :ok :f :txn :value [[:append 0 1]]}
             {:process 1 :type :ok :f :txn :value [[:r 0 []] [:r 0 [1]]]}])]
    (is (true? (:valid? (elle/check {:consistency-models [:read-committed]} h))))))

(deftest elle-rejects-cyclic-information-flow
  (let [h (transaction-history
            [[:ok [[:append 0 1] [:r 1 [2]]]]
             [:ok [[:append 1 2] [:r 0 [1]]]]])
        result (elle/check {:consistency-models [:read-committed]} h)]
    (is (false? (:valid? result)))
    (is (some #{:G1c} (:anomaly-types result)))))

(deftest anomaly-analysis-does-not-require-graphviz
  (let [directory (.toFile (Files/createTempDirectory "mydb-elle-test-" (make-array FileAttribute 0)))
        h (transaction-history [[:ok [[:append 0 1] [:r 1 [2]]]]
                                [:ok [[:append 1 2] [:r 0 [1]]]]])]
    (try
      (with-redefs [shell/sh (fn [& _] (throw (Exception. "External renderer must not run")))]
        (is (false? (:valid? (elle/check {:consistency-models [:read-committed]
                                          :directory (str directory) :max-plot-bytes 0} h)))))
      (is (.isFile (io/file directory "G1c.txt")))
      (finally (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))

(deftest commit-uncertainty-is-never-a-definite-abort
  (let [timeout (SQLException. "connection lost" "08S01" 0)
        deadlock (SQLException. "deadlock" "40001" 1213)
        syntax (SQLException. "syntax" "42000" 1064)]
    (is (= :info (:type (jdbc/sql-error timeout true true))))
    (is (= :info (:type (jdbc/sql-error timeout false false))))
    (is (= :fail (:type (jdbc/sql-error deadlock false true))))
    (is (false? (:unexpected-error? (jdbc/sql-error deadlock false true))))
    (is (:unexpected-error? (jdbc/sql-error syntax false true)))))

(deftest final-state-detects-lost-or-aborted-writes
  (let [committed {:type :ok :f :txn :value [[:append 0 1] [:r 0 [1]]]}
        aborted {:type :fail :f :txn :value [[:append 0 2]]}
        final (fn [vs] {:type :ok :f :txn :final-read? true :value [[:r 0 vs]]})]
    (is (:valid? (check/final-state-result [committed aborted (final [1])])))
    (is (= [[0 1]] (:missing (check/final-state-result [committed (final [])]))))
    (is (= [[0 2]] (:unexpected (check/final-state-result [committed aborted (final [1 2])]))))))

(deftest empty-or-broken-workload-cannot-pass
  (is (false? (:valid? (check/health-result [] false))))
  (is (false? (:valid? (check/probe-result []))))
  (is (false? (:valid? (check/health-result [{:type :ok} {:type :info}] false))))
  (is (false? (:valid? (check/probe-result
                        [{:f :probe :type :ok :value {:checks [{:case :dirty-read :valid? false}]}}])))))

(deftest seeded-plans-and-unique-tokens
  (let [opts {:seed 7 :key-count 4 :max-txn-length 5 :abort-percent 20}
        plans (vec (take 100 (append/transactions opts)))
        tokens (for [op plans [f _ v] (:value op) :when (= :append f)] v)]
    (is (= plans (vec (take 100 (append/transactions opts)))))
    (is (= (count tokens) (count (set tokens))))
    (is (= [1 2 3] (append/decode-list "1,2,3,")))))

(deftest baseline-probe-requires-complete-individual-iterations
  (let [labels [:dirty-update :dirty-delete :dirty-insert :secondary-index-dirty-read
                :own-update :own-delete :own-null-insert :rollback
                :before-concurrent-commit :fresh-statement-snapshot :intermediate-write
                :own-latest-write :before-final-commit :committed-final-write
                :partial-transaction-invisible :uncommitted-transaction-invisible :atomic-commit-visible]
        op {:f :probe :type :ok :value {:checks (mapv #(hash-map :case % :valid? true) labels)}}
        valid? #(:valid? (checker/check (check/probe-checker) {} % {}))]
    (is (true? (valid? [op])))
    (doseq [i (range (count labels))]
      (is (false? (valid? [(update-in op [:value :checks]
                                    #(vec (concat (take i %) (drop (inc i) %))))]))))
    (doseq [bad [(assoc-in op [:value :checks] [])
                 (update-in op [:value :checks] conj (first (get-in op [:value :checks])))
                 (assoc-in op [:value :checks 0 :case] :unknown)
                 (assoc-in op [:value :checks 0 :valid?] false)
                 (assoc-in op [:value :checks 0 :valid?] :truthy)]]
      (is (false? (valid? [op bad]))))))

(deftest malformed-data-is-not-an-empty-list
  (doseq [value [nil "1,x," "1" "9223372036854775808,"]]
    (is (thrown? clojure.lang.ExceptionInfo (append/decode-list value)))))

(deftest recovery-unknown-transaction-is-atomic
  (let [unknown {:type :info :f :txn :process 0 :value [[:append 0 1] [:append 1 2]]}
        check (fn [a b expected]
                (recovery/recovery-result
                  [unknown {:type :ok :f :txn :final-read? true
                            :value [[:r 0 a] [:r 1 b]]}] expected))]
    (is (:valid? (check [] [] :either)))
    (is (:valid? (check [1] [2] :either)))
    (is (false? (:valid? (check [1] [] :either))))
    (is (false? (:valid? (check [] [2] :either))))
    (is (false? (:valid? (check [] [] :present))))
    (is (false? (:valid? (check [1] [2] :absent))))
    (is (false? (:valid? (check [1 1] [2] :either))))))

(deftest recorded-recovery-history-is-readable-by-jepsen
  (let [events (atom [])]
    (doseq [op [{:process 0 :type :invoke :f :txn :value [[:append 0 1]]}
                {:process 0 :type :ok :f :txn :value [[:append 0 1]]}
                {:process 1 :type :invoke :f :txn :value [[:r 0 nil]]}
                {:process 1 :type :ok :f :txn :value [[:r 0 [1]]]}]]
      (recovery/record! events op))
    (is (true? (:valid? (elle/check {:consistency-models [:read-committed]}
                                    (history/history @events)))))))
