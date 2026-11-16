(ns mydb.checker
  (:require [elle.list-append :as elle]
            [clojure.set :as set]
            [jepsen.checker :as checker]
            [jepsen.store :as store]))

(defn append-checker []
  (reify checker/Checker
    (check [_ test history opts]
      (elle/check {:consistency-models [:read-committed]
                   ;; Optional rendering must not turn a valid RC history with
                   ;; allowed G2 cycles into :unknown on a host without dot.
                   :max-plot-bytes (if (:plots test) 65536 0)
                   :directory (str (store/path! test (:subdirectory opts) "elle"))}
                  history))))

(defn health-result
  "Require real coverage, and reject unexpected client exceptions."
  [history final-read-required?]
  (let [completed (filter #(#{:ok :fail :info} (:type %)) history)
        successful (filter #(and (= :ok (:type %)) (not (:final-read? %))) completed)
        unexpected (filter #(or (:unexpected-error? %)
                                (= :info (:type %))) completed)
        final-ok? (boolean (some #(and (:final-read? %) (= :ok (:type %))) completed))]
    {:valid? (boolean (and (seq successful) (empty? unexpected)
                           (or (not final-read-required?) final-ok?)))
     :successful-operations (count successful)
     :indeterminate-or-unexpected (vec unexpected)
     :final-read-ok? final-ok?}))

(defn health-checker [final-read-required?]
  (reify checker/Checker
    (check [_ _ history _] (health-result history final-read-required?))))

(defn readback-result
  "Require a complete, successful, duplicate-free snapshot of the expected keys."
  [op expected-keys]
  (let [mops (:value op)
        keys (map second mops)
        invalid (remove (fn [[f _ vs :as mop]]
                          (and (= 3 (count mop)) (= :r f) (vector? vs)
                               (every? #(and (integer? %) (pos? %)) vs)
                               (= (count vs) (count (set vs))))) mops)]
    {:valid? (boolean (and op (= :ok (:type op)) (:final-read? op)
                           (vector? mops) (seq expected-keys)
                           (= (set expected-keys) (set keys))
                           (= (count keys) (count (set keys)))
                           (empty? invalid)))
     :expected-keys (vec (sort expected-keys)) :actual-keys (vec keys)
     :invalid-reads (vec invalid)}))

(defn final-state-result
  "Every final snapshot must contain exactly the required keys and writes."
  ([events] (final-state-result events nil))
  ([events expected-keys] (final-state-result events expected-keys (constantly #{})))
  ([events expected-keys allowed-at]
   (let [completed (filter #(and (= :txn (:f %)) (#{:ok :info :fail} (:type %))) events)
         keys (or expected-keys (set (for [op events [_ k] (:value op)] k)))
         writes (fn [ops] (set (for [op ops [f k v] (:value op) :when (= :append f)] [k v])))
         snapshots (vec (for [[i op] (map-indexed vector events) :when (and (:final-read? op) (not= :invoke (:type op)))]
                          (let [prefix (filter #(and (= :txn (:f %)) (#{:ok :info :fail} (:type %))) (take i events))
                                required (writes (filter #(= :ok (:type %)) prefix))
                                possible (writes (filter #(#{:ok :info} (:type %)) prefix))
                                actual (set (for [[f k vs] (:value op) :when (= :r f) v vs] [k v]))
                                missing (set/difference required actual (allowed-at op))
                                unexpected (set/difference actual possible)
                                shape (readback-result op keys)]
                            {:valid? (boolean (and (:valid? shape) (empty? missing) (empty? unexpected)))
                             :index i :readback shape :missing (vec (sort missing))
                             :unexpected (vec (sort unexpected))})))
         required (writes (filter #(= :ok (:type %)) completed))
         reads (count (for [op completed :when (and (= :ok (:type op)) (not (:final-read? op)))
                           [f] (:value op) :when (= :r f)] f))]
     {:valid? (boolean (and (seq snapshots) (seq required) (pos? reads)
                            (:final-read? (last completed))
                            (every? #(true? (:valid? %)) snapshots)))
      :acknowledged-appends (count required) :concurrent-reads reads
      :snapshots snapshots
      :missing (vec (mapcat :missing snapshots)) :unexpected (vec (mapcat :unexpected snapshots))})))

(defn final-state-checker []
  (reify checker/Checker
    (check [_ _ history _] (final-state-result history))))

(defn probe-result [history]
  (let [ops (filter #(and (= :probe (:f %)) (= :ok (:type %))) history)
        checks (mapcat (fn [op]
                         (map #(assoc % :iteration (get-in op [:value :iteration])
                                        :history-index (:index op) :process (:process op))
                              (get-in op [:value :checks]))) ops)
        failures (remove #(true? (:valid? %)) checks)]
    {:valid? (and (pos? (count checks)) (empty? failures))
     :iterations (count ops)
     :assertions (count checks)
     :cases (frequencies (map :case checks))
     :failures (vec failures)}))

(def baseline-probe-cases
  #{:dirty-update :dirty-delete :dirty-insert :secondary-index-dirty-read
    :own-update :own-delete :own-null-insert :rollback
    :before-concurrent-commit :fresh-statement-snapshot :intermediate-write
    :own-latest-write :before-final-commit :committed-final-write
    :partial-transaction-invisible :uncommitted-transaction-invisible :atomic-commit-visible})

(defn baseline-probe-result [history]
  (let [base (probe-result history)
        incomplete (vec (for [op history :when (and (= :probe (:f op)) (= :ok (:type op)))
                              :let [labels (map :case (get-in op [:value :checks]))]
                              :when (or (not= baseline-probe-cases (set labels))
                                        (not= (count labels) (count (set labels))))]
                          {:index (:index op) :process (:process op) :checks (vec labels)}))]
    (assoc base :valid? (boolean (and (:valid? base) (empty? incomplete)))
           :incomplete-operations incomplete)))

(defn probe-checker []
  (reify checker/Checker
    (check [_ _ history _] (baseline-probe-result history))))
