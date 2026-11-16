(ns mydb.fault-history-test
  (:require [clojure.test :refer :all]
            [mydb.checker :as check]
            [mydb.fault-history :as fault]
            [mydb.recovery :as recovery]))

(def committed {:process 0 :type :ok :f :txn :value [[:append 0 1] [:r 0 [1]]]})
(def unknown {:process 1 :type :info :f :txn :value [[:append 1 2] [:append 2 3]]})
(defn final-read [values]
  {:process 2 :type :ok :f :txn :final-read? true
   :value (mapv (fn [[k vs]] [:r k vs]) values)})

(deftest incomplete-and-duplicate-readback-cannot-pass
  (is (false? (:valid? (recovery/recovery-result [unknown] :either))))
  (is (false? (:valid? (check/final-state-result [committed unknown (final-read {0 [1]})]))))
  (is (false? (:valid? (fault/final-result [committed (final-read {0 [1 1]})] []))))
  (is (false? (:valid? (fault/final-result [committed (final-read {0 [1]})
                                         (assoc committed :process 3 :value [[:append 0 4]])] [])))))

(deftest unknown-resolution-cannot-flip-between-recoveries
  (is (false? (:valid? (recovery/recovery-result
                        [unknown (final-read {1 [2] 2 [3]})
                         (final-read {1 [] 2 []})] :either)))))

(deftest loss-exemptions-must-cover-complete-confirmed-transactions
  (let [tx (assoc committed :value [[:append 0 1] [:append 1 2] [:r 0 [1]]])]
    (is (false? (:valid? (fault/final-result [tx (final-read {0 [1] 1 []})] [[1 2]]))))
    (is (false? (:valid? (fault/final-result [committed unknown (final-read {0 [1] 1 [] 2 []})]
                                          [[1 2] [2 3]]))))))

(defn pair [process mops & [extra]]
  [(merge {:process process :type :invoke :f :txn
           :value (mapv (fn [[f k v]] [f k (when (= :append f) v)]) mops)} extra)
   (merge {:process process :type :ok :f :txn :value mops} extra)])

(defn indexed [events]
  (mapv (fn [i op] (assoc op :index i :time (* 100 i))) (range) events))

(def base-events
  (indexed (concat (pair 0 [[:append 0 1] [:append 1 2] [:r 0 [1]]])
                   (pair 1 [[:r 0 [1]] [:r 1 [2]]] {:final-read? true}))))

(deftest complete-history-and-checker-unknown
  (is (:valid? (fault/analyze-result base-events {:expected-keys [0 1]} "/tmp/mydb/elle-p0")))
  (with-redefs [elle.list-append/check (fn [& _] {:valid? :unknown})]
    (is (false? (:valid? (fault/analyze-result base-events {:expected-keys [0 1]} "/tmp/mydb/elle-p0")))))
  (doseq [events [(vec (butlast base-events))
                  (vec (rest base-events))
                  (assoc-in base-events [1 :value 0 2] 99)
                  (assoc-in base-events [1 :index] 99)
                  (assoc-in base-events [1 :time] -1)
                  (assoc-in base-events [3 :unexpected-error?] true)
                  (assoc-in base-events [3 :value] [[:r 0 [1]]])
                  (assoc-in base-events [3 :value] [[:r 0 [1]] [:r 0 [1]] [:r 1 [2]]])]]
    (is (false? (:valid? (fault/analyze-result events {:expected-keys [0 1]} "/tmp/mydb/elle-p0")))))
  (is (false? (:valid? (fault/analyze-result base-events {} "/tmp/mydb/elle-p0")))))

(def epoch-events
  (indexed
    (concat base-events
            [{:process :controller :type :info :f :loss-window :candidate-lsn 10}]
            (pair 0 [[:append 0 3] [:append 1 4] [:r 0 [1 3]]])
            (pair 1 [[:r 0 [1 3]] [:r 1 [2 4]]] {:final-read? true})
            [{:process :controller :type :info :f :epoch :epoch 1
              :allowed-loss [[0 3] [1 4]] :value [[:r 0 [1]] [:r 1 [2]]]
              :evidence {:fenced-pid 123 :fenced-exit-code -9 :candidate-lsn 10 :partition-lsn 10
                         :source-lsn 20 :loss-start-index 4 :timeline-before "old" :timeline-after "new"}}]
            (pair 0 [[:append 0 5] [:r 0 [1 5]]] {:epoch 1})
            (pair 1 [[:r 0 [1 5]] [:r 1 [2]]] {:epoch 1 :final-read? true}))))

(deftest explicit-epoch-preserves-all-real-operations
  (let [opts {:expected-keys [0 1] :allowed-loss [[0 3] [1 4]]}
        result (fault/analyze-result epoch-events opts "/tmp/mydb/elle-p0")]
    (is (:valid? result) (pr-str result))
    (is (= 0 (get-in result [:isolation :filtered-operations])))
    (is (= 2 (count (get-in result [:isolation :epochs]))))
    (doseq [events [(assoc-in epoch-events [9 :evidence :fenced-exit-code] 0)
                    (assoc-in epoch-events [9 :evidence :candidate-lsn] 9)
                    (assoc-in epoch-events [9 :evidence :timeline-after] "old")
                    (assoc-in epoch-events [9 :evidence :loss-start-index] 5)
                    (assoc-in epoch-events [9 :allowed-loss] [[0 1] [1 2] [0 3] [1 4]])
                    (assoc-in epoch-events [13 :value] [[:r 0 [5]] [:r 1 [2]]])]]
      (is (false? (:valid? (fault/analyze-result events opts "/tmp/mydb/elle-p0")))))))

(deftest index-and-artifact-failures-have-nonzero-verdicts
  (let [directory (java.nio.file.Files/createTempDirectory
                    (java.nio.file.Path/of "/tmp/mydb" (make-array String 0))
                    "checker-negative-" (make-array java.nio.file.attribute.FileAttribute 0))
        index (str directory "/index.edn")]
    (try
      (spit index "[]")
      (is (= 1 (fault/run-index! index)))
      (spit index (pr-str [(str directory) (str directory)]))
      (is (= 1 (fault/run-index! index)))
      (spit (str directory "/history.edn") "[{:type :invoke")
      (spit (str directory "/options.edn") "{:expected-keys [0 1]}")
      (spit index (pr-str [(str directory)]))
      (is (= 1 (fault/run-index! index)))
      (is (false? (:valid? (clojure.edn/read-string (slurp (str directory "/results.edn"))))))
      (finally (doseq [file (reverse (file-seq (.toFile directory)))] (clojure.java.io/delete-file file))))))
