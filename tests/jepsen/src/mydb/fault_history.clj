(ns mydb.fault-history
  "Analyze complete recorded histories with explicit, independently checked epochs."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [elle.list-append :as elle]
            [jepsen.history :as history]
            [mydb.checker :as check]
            [mydb.recovery :as recovery]))

(defn writes [op]
  (set (for [[f k v] (:value op) :when (= :append f)] [k v])))

(defn shape-result [events]
  (try
    (let [pending (atom {}) errors (atom [])
          txn-events (filter #(= :txn (:f %)) events)]
      (doseq [[i op] (map-indexed vector events)]
        (when-not (and (= i (:index op)) (integer? (:time op))
                       (or (zero? i) (<= (:time (nth events (dec i))) (:time op))))
          (swap! errors conj {:index i :error :invalid-index-or-time}))
        (when (= :txn (:f op))
          (let [process (:process op) invocation (get @pending process)]
            (cond
              (= :invoke (:type op))
              (if invocation (swap! errors conj {:index i :error :overlapping-process})
                (swap! pending assoc process op))
              (#{:ok :fail :info} (:type op))
              (do
                (when-not (and invocation (= (:epoch invocation 0) (:epoch op 0))
                               (= (mapv #(subvec % 0 2) (:value invocation))
                                  (mapv #(subvec % 0 2) (:value op)))
                               (= (writes invocation) (writes op)))
                  (swap! errors conj {:index i :error :unpaired-or-changed-operation}))
                (swap! pending dissoc process))
              :else (swap! errors conj {:index i :error :invalid-completion})))))
      {:valid? (boolean (and (seq txn-events) (empty? @errors) (empty? @pending)))
       :errors @errors :pending-processes (vec (keys @pending))})
    (catch Exception e {:valid? false :error (str e)})))

(defn final-result
  ([events allowed-loss] (final-result events allowed-loss nil nil))
  ([events allowed-loss boundary expected-keys]
   (let [allowed (set allowed-loss)
         confirmed (filter #(= :ok (:type %)) events)
         acknowledged (apply set/union #{} (map writes confirmed))
         whole? (every? (fn [op] (let [loss (set/intersection allowed (writes op))]
                                  (or (empty? loss) (= loss (writes op))))) confirmed)
         atomic? (every? (fn [op]
                          (if-not (and (:final-read? op) (= :ok (:type op))) true
                            (let [actual (set (for [[f k vs] (:value op) :when (= :r f) v vs] [k v]))]
                              (every? (fn [tx] (let [present (set/intersection (writes tx) actual)]
                                                (or (empty? present) (= present (writes tx))))) confirmed)))) events)
         eligible? (or (empty? allowed) (nil? boundary)
                       (every? #(< (:index %) boundary) (filter #(seq (set/intersection allowed (writes %))) confirmed)))
         result (check/final-state-result events expected-keys
                  (fn [op] (if (or (nil? boundary) (> (:index op) boundary)) allowed #{})))]
     (assoc result :valid? (boolean (and (:valid? result) whole? atomic? eligible?
                                        (set/subset? allowed acknowledged)))
            :allowed-async-loss (vec (sort allowed))))))

(defn epoch-result [events options directory]
  (let [transitions (filter #(= :epoch (:f %)) events)
        allowed (set (:allowed-loss options))
        expected-keys (set (:expected-keys options))
        transition (first transitions)
        evidence (:evidence transition)
        epochs (partition-by #(get % :epoch 0) events)
        transition-valid? (if (empty? allowed)
                            (and (empty? transitions) (= 1 (count epochs)))
                            (and (= 1 (count transitions)) (= 2 (count epochs))
                                 (= [0 1] (mapv #(get (first %) :epoch 0) epochs))
                                 (= 1 (:epoch transition))
                                 (= allowed (set (:allowed-loss transition)))
                                 (pos-int? (:fenced-pid evidence))
                                 (= (:candidate-lsn evidence) (:partition-lsn evidence))
                                 (< (:candidate-lsn evidence) (:source-lsn evidence))
                                 (= -9 (:fenced-exit-code evidence))
                                 (= 1 (count (filter #(= :loss-window (:f %)) events)))
                                 (let [start (:loss-start-index evidence)
                                       window (get events start)]
                                   (and (integer? start) (< start (:index transition))
                                        (= :loss-window (:f window))
                                        (= (:candidate-lsn evidence) (:candidate-lsn window))
                                        (every? #(<= start (:index %))
                                                (filter #(seq (set/intersection allowed (writes %))) events))))
                                 (seq (:timeline-before evidence)) (seq (:timeline-after evidence))
                                 (not= (:timeline-before evidence) (:timeline-after evidence))
                                 (:valid? (check/readback-result
                                            {:type :ok :final-read? true :value (:value transition)} expected-keys))))
        results (mapv
                  (fn [segment]
                    (let [epoch (get (first segment) :epoch 0)
                          txns (filterv #(= :txn (:f %)) segment)
                          ;; Explicit initial writes describe the independently validated
                          ;; candidate snapshot. No real operations or reads are discarded.
                          initial (when (pos? epoch)
                                    (vec (mapcat
                                           (fn [[_ k vs]]
                                             [{:process 2000000 :type :invoke :f :txn
                                               :value (mapv #(vector :append k %) vs)}
                                              {:process 2000000 :type :ok :f :txn
                                               :value (mapv #(vector :append k %) vs)}]) (:value transition))))
                          analyzed (vec (concat initial txns))
                          reads (count (for [op txns :when (= :ok (:type op)) [f] (:value op) :when (= :r f)] f))
                          final? (some #(and (:final-read? %) (= :ok (:type %))) txns)
                          result (if (and transition-valid? (seq txns) (pos? reads) final?)
                                   (elle/check {:consistency-models [:read-committed] :max-plot-bytes 0
                                                :directory (str directory "/elle-epoch-" epoch)}
                                               (history/history (mapv #(cond-> (dissoc % :index)
                                                                           (:index %) (assoc :source-index (:index %))) analyzed)))
                                   {:valid? false :error :incomplete-epoch})]
                      {:epoch epoch :valid? (true? (:valid? result)) :operations (count txns)
                       :synthetic-initial-operations (count initial) :isolation result})) epochs)]
    {:valid? (boolean (and transition-valid? (seq results) (every? #(true? (:valid? %)) results)))
     :transition-valid? (boolean transition-valid?) :epochs results
     :filtered-operations 0 :input-operations (count events)}))

(defn analyze-result [events options directory]
  (let [shape (shape-result events)
        boundary (:index (first (filter #(= :epoch (:f %)) events)))
        final (final-result events (:allowed-loss options) boundary (:expected-keys options))
        ;; Atomicity/stability applies separately within each timeline.
        atomicity (mapv #(recovery/recovery-result
                          (filterv (fn [op] (= :txn (:f op))) %) :either)
                        (partition-by #(get % :epoch 0) events))
        isolation (try (epoch-result events options directory)
                       (catch Throwable e {:valid? false :error (str e)}))
        unexpected (filter :unexpected-error? events)]
    {:valid? (boolean (and (seq (:expected-keys options)) (:valid? shape) (empty? unexpected)
                           (every? #(true? (:valid? %)) (concat [final isolation] atomicity))))
     :history-shape shape :final-state final :atomicity atomicity :isolation isolation
     :isolation-scope :explicit-epochs :unexpected (vec unexpected)}))

(defn analyze! [directory]
  (let [result (try
                 (let [events (edn/read-string (slurp (io/file directory "history.edn")))
                       options (edn/read-string (slurp (io/file directory "options.edn")))]
                   (analyze-result events options directory))
                 (catch Throwable e {:valid? false :status :incomplete :error (str e)}))]
    (spit (io/file directory "results.edn") (pr-str result))
    (println "FAULT-HISTORY" directory (:valid? result))
    result))

(defn run-index! [index-file]
  (try
    (let [directories (edn/read-string (slurp index-file))]
      (if (and (vector? directories) (seq directories)
               (= (count directories) (count (set directories)))
               (every? #(true? (:valid? %)) (mapv analyze! directories))) 0 1))
    (catch Throwable e (.printStackTrace e) 1)))

(defn -main [index-file] (System/exit (run-index! index-file)))
