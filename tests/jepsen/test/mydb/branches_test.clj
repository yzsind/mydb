(ns mydb.branches-test
  (:require [clojure.test :refer :all]
            [mydb.workload.branches :as branches]
            [mydb.workload.interleave :as interleave]))

(defn complete-history [specs]
  (mapv (fn [spec]
          {:type :ok :f :probe :value {:scenario (:name spec)
                                      :checks (mapv #(hash-map :case % :valid? true) (branches/required-checks spec))}})
        specs))

(deftest protocol-matrix-has-explicit-coverage
  (let [all (branches/selected-scenarios {})
        text (branches/selected-scenarios {:branch-protocol :text})
        prepared (branches/selected-scenarios {:branch-protocol :prepared})]
    (is (= 39 (count all)))
    (is (= 18 (count text)))
    (is (= 21 (count prepared)))
    (is (:valid? (branches/result (complete-history all) all)))
    (is (false? (:valid? (branches/result [] all))))
    (is (false? (:valid? (branches/result [] []))))
    (is (false? (:valid? (branches/result (complete-history text) all))))
    (is (empty? (branches/selected-scenarios {:branch-protocol :text :branch-case :prepared-cursor})))))

(deftest missing-cases-or-protocol-proof-cannot-pass
  (let [specs branches/scenarios
        history (complete-history specs)]
    (doseq [i (range (count specs))]
      (is (false? (:valid? (branches/result (vec (concat (take i history) (drop (inc i) history))) specs)))))
    (is (false? (:valid? (branches/result
                         (update-in history [0 :value :checks] #(filterv (fn [c] (not= :protocol-path (:case c))) %)) specs))))
    (is (false? (:valid? (branches/result (assoc-in history [0 :value :checks 0 :valid?] false) specs))))
    (is (false? (:valid? (branches/result (update-in history [0 :value :checks] conj
                                                   (get-in history [0 :value :checks 0])) specs))))))

(deftest absent-protocol-counter-is-an-error
  (is (thrown? clojure.lang.ExceptionInfo (branches/delta {} {} :Com_stmt_execute)))
  (is (= 2 (branches/delta {:Com_stmt_execute 10} {:Com_stmt_execute 12} :Com_stmt_execute)))
  (is (not= (branches/digest (byte-array 0)) (branches/digest (byte-array [0])))))

(defn deadlock-fixture
  "Exercise the real scenario with either SQL victim, without thread scheduling."
  [victim corruption]
  (let [committed (atom {}) pending (atom {}) savepoints (atom #{}) checks (atom [])
        check! (fn record-check!
                 ([label expected actual] (record-check! label expected actual (= expected actual)))
                 ([label expected actual valid?]
                  (swap! checks conj {:case label :expected expected :actual actual :valid? valid?})))
        ctx {:a :a :b :b :c :c :base 0 :kind :deadlock :protocol :prepared
             :ids {:a 1 :b 2} :check! check!}
        sql-ex (fn [code] (java.sql.SQLException. "fixture" "40001" code))]
    (with-redefs [branches/insert! (fn [_ conn id v]
                                   (if (= :c conn) (swap! committed assoc id v)
                                       (swap! pending assoc-in [conn id] v)) 1)
                  branches/update! (fn [_ conn id v]
                                     (if (and (#{12 21} v)
                                              (or (= victim conn) (= victim :both)))
                                       (do (when-not (= corruption :retained-write)
                                             (swap! pending dissoc conn))
                                           (when-not (= corruption :retained-savepoint)
                                             (swap! savepoints disj conn))
                                           (throw (sql-ex (if (= corruption :timeout) 1205 1213))))
                                       (do (swap! pending assoc-in [conn id] v)
                                           (if (= corruption :zero-updated) 0 1))))
                  branches/control! (fn [conn sql]
                                      (case sql
                                        "BEGIN" (swap! pending assoc conn {})
                                        "SAVEPOINT victim" (swap! savepoints conj conn)
                                        "ROLLBACK TO SAVEPOINT victim"
                                        (when-not (contains? @savepoints conn) (throw (sql-ex 1305)))
                                        "COMMIT" (do (when-not (= corruption :lost-commit)
                                                       (swap! committed merge (get @pending conn)))
                                                     (swap! pending dissoc conn))
                                        (throw (ex-info "Unexpected fixture SQL" {:sql sql}))))
                  branches/rows (fn [_ conn]
                                  (mapv vec (sort (merge @committed (get @pending conn)))))
                  branches/spawn! (fn [_ _ f] (delay (f)))
                  interleave/await! deref
                  branches/wait-for-lock! (fn [_ _ requester blocker]
                                           (check! :wait-observed #{[requester blocker]}
                                                   #{[requester blocker]}))]
      (try (branches/locks! ctx)
           {:checks @checks}
           (catch Exception e {:checks @checks :error e})))))

(deftest deadlock-scenario-accepts-either-victim
  (doseq [victim [:a :b]]
    (let [{:keys [checks error]} (deadlock-fixture victim nil)]
      (is (nil? error) (str victim ": " error))
      (is (every? :valid? checks) (str checks))
      (is (= (get branches/case-checks :deadlock) (set (map :case checks))))
      (is (= (count checks) (count (set (map :case checks))))))))

(deftest deadlock-scenario-rejects-invalid-outcomes
  (doseq [[victim corruption] [[:none nil] [:both nil] [:a :timeout] [:b :timeout]
                              [:a :retained-write] [:b :retained-write]
                              [:a :retained-savepoint] [:b :retained-savepoint]
                              [:a :zero-updated] [:b :zero-updated]
                              [:a :lost-commit] [:b :lost-commit]]]
    (let [{:keys [checks error]} (deadlock-fixture victim corruption)]
      (is (or error (some #(not (:valid? %)) checks)) (str victim " " corruption)))))
