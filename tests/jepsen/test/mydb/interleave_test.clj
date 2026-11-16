(ns mydb.interleave-test
  (:require [clojure.test :refer :all]
            [mydb.workload.interleave :as interleave]))

(defn complete-history []
  (mapv (fn [spec]
          {:f :probe :type :ok
           :value {:scenario (:name spec)
                   :checks (mapv #(hash-map :case % :valid? true)
                                 (interleave/required-checks spec))}})
        interleave/scenarios))

(deftest every-scenario-and-every-boundary-is-required
  (let [history (complete-history)]
    (is (= 17 (count interleave/scenarios)))
    (is (:valid? (interleave/result history)))
    (is (false? (:valid? (interleave/result []))))
    (doseq [i (range (count history))]
      (is (false? (:valid? (interleave/result (vec (concat (take i history) (drop (inc i) history))))))))
    (is (false? (:valid? (interleave/result (update-in history [0 :value :checks] pop)))))
    (is (false? (:valid? (interleave/result (update-in history [0 :value :checks] conj
                                                     (get-in history [0 :value :checks 0]))))))
    (is (false? (:valid? (interleave/result (assoc-in history [0 :value :scenario] :unknown)))))
    (is (false? (:valid? (interleave/result (assoc-in history [0 :value :checks 0 :valid?] false)))))))

(deftest missing-wait-is-not-a-passing-interleaving
  (let [task (doto (promise) (deliver :completed))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"finished before"
          (interleave/wait-until! task :lock (constantly nil))))))

(deftest focused-reproduction-does-not-claim-full-coverage
  (let [selected (interleave/selected-scenarios {:interleave-case :scan-heap})
        history (filterv #(= :scan-heap (get-in % [:value :scenario])) (complete-history))]
    (is (= [:scan-heap] (mapv :name selected)))
    (is (:valid? (interleave/result history selected)))
    (is (false? (:valid? (interleave/result history))))))
