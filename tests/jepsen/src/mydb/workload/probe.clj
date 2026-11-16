(ns mydb.workload.probe
  "Deterministic transaction overlaps: each worker owns disjoint keys and two
  connections, so assertions don't depend on thread timing or sleeps."
  (:require [jepsen.client :as client]
            [mydb.jdbc :as jdbc])
  (:import (java.sql Connection)))

(defn run-probe!
  [^Connection writer ^Connection reader iteration]
  (let [base (* 4 iteration)
        a base b (inc base) inserted (+ base 2)
        checks (atom [])
        check! (fn [label expected actual]
                 (swap! checks conj {:case label :expected expected :actual actual
                                     :valid? (= expected actual)}))
        row (fn [conn id]
              (jdbc/query conn "SELECT v FROM rc_probe WHERE id=?" id))
        pair (fn [] (jdbc/query reader
                      "SELECT id,v FROM rc_probe WHERE grp=? ORDER BY id" base))]
    (.setAutoCommit writer true)
    (jdbc/execute! writer "INSERT INTO rc_probe (id,grp,v) VALUES (?,?,10),(?,?,10)"
                   a base b base)
    (.setAutoCommit writer false)
    (.setAutoCommit reader false)
    (try
      ;; Uncommitted INSERT, UPDATE and DELETE must remain invisible both via
      ;; primary lookup and a secondary index, even in a long-lived RC txn.
      (jdbc/execute! writer "UPDATE rc_probe SET v=99 WHERE id=?" a)
      (jdbc/execute! writer "DELETE FROM rc_probe WHERE id=?" b)
      (jdbc/execute! writer "INSERT INTO rc_probe (id,grp,v) VALUES (?,?,NULL)" inserted base)
      (check! :dirty-update [[10]] (row reader a))
      (check! :dirty-delete [[10]] (row reader b))
      (check! :dirty-insert [] (row reader inserted))
      (check! :secondary-index-dirty-read [[a 10] [b 10]]
              (jdbc/query reader
                "SELECT id,v FROM rc_probe FORCE INDEX(by_group) WHERE grp=? ORDER BY id" base))
      (check! :own-update [[99]] (row writer a))
      (check! :own-delete [] (row writer b))
      (check! :own-null-insert [[nil]] (row writer inserted))
      (.rollback writer)
      (check! :rollback [[a 10] [b 10]] (pair))

      ;; RC must refresh its snapshot between statements, without reader COMMIT.
      (check! :before-concurrent-commit [[10]] (row reader a))
      (jdbc/execute! writer "UPDATE rc_probe SET v=20 WHERE id=?" a)
      (.commit writer)
      (check! :fresh-statement-snapshot [[20]] (row reader a))

      ;; A txn's intermediate write must never escape to another transaction.
      (jdbc/execute! writer "UPDATE rc_probe SET v=30 WHERE id=?" a)
      (check! :intermediate-write [[20]] (row reader a))
      (jdbc/execute! writer "UPDATE rc_probe SET v=40 WHERE id=?" a)
      (check! :own-latest-write [[40]] (row writer a))
      (check! :before-final-commit [[20]] (row reader a))
      (.commit writer)
      (check! :committed-final-write [[40]] (row reader a))

      ;; Observe both rows in ONE statement. Two RC SELECTs may legitimately
      ;; straddle a commit and return different versions.
      (jdbc/execute! writer "UPDATE rc_probe SET v=100 WHERE grp=?" base)
      (.commit writer)
      (jdbc/execute! writer "UPDATE rc_probe SET v=200 WHERE id=?" a)
      (check! :partial-transaction-invisible [[a 100] [b 100]] (pair))
      (jdbc/execute! writer "UPDATE rc_probe SET v=200 WHERE id=?" b)
      (check! :uncommitted-transaction-invisible [[a 100] [b 100]] (pair))
      (.commit writer)
      (check! :atomic-commit-visible [[a 200] [b 200]] (pair))
      (.commit reader)
      {:iteration iteration :checks @checks}
      (finally
        (jdbc/rollback! writer)
        (jdbc/rollback! reader)))))

(defrecord ProbeClient [writer reader]
  client/Client
  (open! [this test _]
    (let [writer (jdbc/open! test)]
      (try
        (assoc this :writer writer :reader (jdbc/open! test))
        (catch Throwable e (.close ^Connection writer) (throw e)))))
  (setup! [this _] this)
  (invoke! [_ _ op]
    ;; Infrastructure/SQL exceptions propagate to Jepsen as crashes. The probe
    ;; checker rejects them; a broken scenario must never be counted as a pass.
    (assoc op :type :ok :value (run-probe! writer reader (:value op))))
  (teardown! [_ _])
  (close! [_ _]
    (try (.close ^Connection writer) (finally (.close ^Connection reader)))))
