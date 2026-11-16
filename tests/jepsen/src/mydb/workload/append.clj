(ns mydb.workload.append
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [mydb.jdbc :as jdbc])
  (:import (java.sql Connection SQLException)
           (java.util Random)))

(defn decode-list [value]
  (when-not (and (string? value) (re-matches #"(?:[0-9]+,)*" value))
    (throw (ex-info "Invalid append list encoding" {:value value})))
  (if (= "" value)
    []
    (mapv (fn [s] (or (parse-long s)
                       (throw (ex-info "Invalid append token" {:token s}))))
          (str/split value #","))))

(defn micro-op! [conn [f k v]]
  (case f
    :r (let [rows (jdbc/query conn "SELECT v FROM rc_append WHERE id=?" k)]
         (when-not (= 1 (count rows))
           (throw (ex-info "Expected exactly one append row" {:key k :rows rows})))
         [:r k (decode-list (ffirst rows))])
    :append (do
              ;; A single SQL update, never client-side read/modify/write.
              (when-not (= 1 (jdbc/execute! conn
                              "UPDATE rc_append SET v=CONCAT(v, ?) WHERE id=?"
                              (str v ",") k))
                (throw (ex-info "Missing append row" {:key k})))
              [:append k v])))

(defrecord AppendClient [conn]
  client/Client
  (open! [this test _] (assoc this :conn (jdbc/open! test)))
  (setup! [this _] this)
  (invoke! [_ _ op]
    (let [committing? (volatile! false)
          observed (volatile! [])]
      (try
        (.setAutoCommit ^Connection conn false)
        (doseq [mop (:value op)]
          (vswap! observed conj (micro-op! conn mop)))
        (if (:abort? op)
          (do (.rollback ^Connection conn)
              (assoc op :type :fail :value @observed :error :intentional-rollback))
          (do (vreset! committing? true)
              (.commit ^Connection conn)
              (assoc op :type :ok :value @observed)))
        (catch SQLException e
          ;; Keep the original planned writes on indeterminate outcomes: an
          ;; append may have reached the server even when its response was lost.
          (merge op (jdbc/sql-error e @committing? (jdbc/rollback! conn)))))))
  (teardown! [_ _])
  (close! [_ _] (.close ^Connection conn)))

(defn transactions
  "Seeded operation plans; the concurrent execution schedule is not replayable
  from a seed alone. Tokens are unique across the complete history."
  [{:keys [seed key-count max-txn-length abort-percent]}]
  (let [rng (Random. seed)
        token (atom 0)]
    (repeatedly
      (fn []
        {:type :invoke :f :txn
         :abort? (< (.nextInt rng 100) abort-percent)
         :value (vec
                  (repeatedly (inc (.nextInt rng max-txn-length))
                    (fn []
                      (let [k (.nextInt rng key-count)]
                        (if (< (.nextInt rng 100) 50)
                          [:r k nil]
                          [:append k (swap! token inc)])))))}))))

(defn final-read [test]
  {:type :invoke :f :txn :final-read? true
   :value (mapv (fn [k] [:r k nil]) (range (:key-count test)))})
