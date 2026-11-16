(ns mydb.workload.interleave
  "RC overlaps confirmed by server lock state, with two writers and an observer."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [mydb.checker :as check]
            [mydb.jdbc :as jdbc])
  (:import (java.sql Connection SQLException)
           (java.util UUID)))

(def scenarios
  (vec (concat (for [kind [:pk-insert :unique-insert :delete-reinsert :key-move
                           :update-recheck :delete-recheck]
                    commit? [true false]]
                {:name (keyword (str (name kind) (if commit? "-commit" "-rollback")))
                 :kind kind :commit? commit?})
               [{:name :cross-table-commit :kind :cross-table :commit? true}
                {:name :cross-table-rollback :kind :cross-table :commit? false}]
               (for [path [:primary :secondary :heap]]
                 {:name (keyword (str "scan-" (name path))) :kind :scan :path path}))))

(defn required-checks [{:keys [kind]}]
  (case kind
    :scan #{:access-plan :statement-gate-observed :scan-original-snapshot :next-statement-fresh}
    :cross-table #{:partial-invisible :uncommitted-invisible :statement-gate-observed
                   :snapshot-before-resolution :fresh-after-resolution}
    (cond-> #{:wait-observed :uncommitted-primary :uncommitted-unique
              :contender-outcome :final-primary :final-unique}
      (= kind :key-move) (conj :own-moved-primary :own-moved-unique))))

(defn selected-scenarios [test]
  (if (#{nil :all} (:interleave-case test)) scenarios
      (filterv #(= (:name %) (:interleave-case test)) scenarios)))

(defn result
  ([history] (result history scenarios))
  ([history selected]
  (let [base (check/probe-result history)
        ops (filter #(and (= :probe (:f %)) (= :ok (:type %))) history)
        specs (into {} (map (juxt :name identity) selected))
        seen (frequencies (map #(get-in % [:value :scenario]) ops))
        incomplete (vec
                     (for [op ops
                           :let [scenario (get-in op [:value :scenario])
                                 spec (get specs scenario)
                                 actual (map :case (get-in op [:value :checks]))]
                           :when (or (nil? spec)
                                     (not= (required-checks spec) (set actual))
                                     (not= (count actual) (count (set actual))))]
                       {:scenario scenario :history-index (:index op) :checks (vec actual)}))
        missing (set/difference (set (keys specs)) (set (keys seen)))]
    (assoc base :valid? (and (:valid? base) (empty? incomplete) (empty? missing))
           :scenarios seen :missing-scenarios (vec (sort missing)) :incomplete-operations incomplete))))

(defn interleave-checker []
  (reify checker/Checker
    (check [_ test history _] (result history (selected-scenarios test)))))

(defn setup! [conn]
  (doseq [sql ["CREATE TABLE rc_interleave (id BIGINT PRIMARY KEY, bucket BIGINT NOT NULL, u VARCHAR(64) NOT NULL, v BIGINT, UNIQUE KEY by_u(u))"
               "CREATE TABLE rc_left (id BIGINT PRIMARY KEY, v BIGINT)"
               "CREATE TABLE rc_right (id BIGINT PRIMARY KEY, v BIGINT)"
               "CREATE TABLE rc_scan (bucket BIGINT NOT NULL, id BIGINT NOT NULL, grp BIGINT NOT NULL, v BIGINT, note VARCHAR(32), PRIMARY KEY(bucket,id), KEY by_group(bucket,grp,id))"]]
    (jdbc/execute! conn sql)))

(defn await! [task]
  (let [value (deref task 12000 ::timeout)]
    (when (= ::timeout value) (throw (ex-info "Concurrent SQL did not finish" {})))
    value))

(defn wait-until! [task description poll]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (let [evidence (poll)]
        (cond
          evidence evidence
          (realized? task) (throw (ex-info "SQL finished before required interleaving"
                                         {:boundary description :result (await! task)}))
          (> (System/nanoTime) deadline) (throw (ex-info "Required server state not observed"
                                                                      {:boundary description}))
          ;; Polling backoff only: the observed state, not elapsed time, releases the owner.
          :else (do (Thread/sleep 2) (recur)))))))

(defn resolve! [^Connection conn commit?]
  (if commit? (.commit conn) (.rollback conn)))

(defn contender! [^Connection conn sql params]
  (let [committing? (volatile! false)]
    (try
      (let [rows (apply jdbc/execute! conn sql params)]
        (vreset! committing? true)
        (.commit conn)
        {:outcome :ok :rows rows})
      (catch SQLException e
        (if (and (not @committing?) (#{1062 1213} (.getErrorCode e)) (jdbc/rollback! conn))
          {:outcome (if (= 1062 (.getErrorCode e)) :duplicate :retry) :code (.getErrorCode e)}
          (throw e))))))

(defn conflict! [a b observer ids iteration {:keys [kind commit?]} check!]
  (let [id (* 4 iteration) moved (inc id)
        old-u (str "key_" id) new-u (str "key_" moved)
        initial? (not (#{:pk-insert :unique-insert} kind))
        initial [[id old-u 10]]
        primary #(jdbc/query % "SELECT id,u,v FROM rc_interleave WHERE id BETWEEN ? AND ? ORDER BY id" id moved)
        unique #(jdbc/query % "SELECT id,u,v FROM rc_interleave FORCE INDEX(by_u) WHERE u IN (?,?) ORDER BY id" old-u new-u)]
    (when initial?
      (jdbc/execute! observer "INSERT INTO rc_interleave VALUES (?,?,?,10)" id iteration old-u))
    (case kind
      (:pk-insert :unique-insert)
      (jdbc/execute! a "INSERT INTO rc_interleave VALUES (?,?,?,10)" id iteration old-u)
      :delete-reinsert (jdbc/execute! a "DELETE FROM rc_interleave WHERE id=?" id)
      :key-move (do (jdbc/execute! a "UPDATE rc_interleave SET id=?,u=?,v=30 WHERE id=?" moved new-u id)
                    (check! :own-moved-primary [[moved new-u 30]] (primary a))
                    (check! :own-moved-unique [[moved new-u 30]] (unique a)))
      (:update-recheck :delete-recheck) (jdbc/execute! a "UPDATE rc_interleave SET v=20 WHERE id=?" id))
    (let [[sql params]
          (case kind
            :update-recheck ["UPDATE rc_interleave SET v=v+1 WHERE id=? AND v=10" [id]]
            :delete-recheck ["DELETE FROM rc_interleave WHERE id=? AND v=10" [id]]
            ["INSERT INTO rc_interleave VALUES (?,?,?,40)" [(if (= kind :unique-insert) moved id) iteration old-u]])
          task (future (contender! b sql params))]
      (try
        (let [waits (wait-until! task :data-lock
                      #(let [rows (jdbc/query observer
                                    "SELECT REQUESTING_THREAD_ID,BLOCKING_THREAD_ID FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=? AND BLOCKING_THREAD_ID=?"
                                    (:b ids) (:a ids))]
                         (when (seq rows) rows)))]
          (check! :wait-observed #{[(:b ids) (:a ids)]} (set waits)))
        (check! :uncommitted-primary (if initial? initial []) (primary observer))
        (check! :uncommitted-unique (if initial? initial []) (unique observer))
        (resolve! a commit?)
        (let [outcome (await! task)
              duplicate? (case kind
                           (:pk-insert :unique-insert) commit?
                           (:delete-reinsert :key-move) (not commit?)
                           false)
              ;; A conflicting insert may return mydb's documented 1213/40001
              ;; retryable abort. No retry is performed; its writes must be absent.
              expected-outcome (if duplicate? #{{:outcome :duplicate :code 1062} {:outcome :retry :code 1213}}
                                   #{{:outcome :ok :rows (if (and (#{:update-recheck :delete-recheck} kind) commit?) 0 1)}})
              expected (case kind
                         :pk-insert [[id old-u (if commit? 10 40)]]
                         :unique-insert [[(if commit? id moved) old-u (if commit? 10 40)]]
                         :delete-reinsert [[id old-u (if commit? 40 10)]]
                         :key-move (if commit? [[id old-u 40] [moved new-u 30]] initial)
                         :update-recheck [[id old-u (if commit? 20 11)]]
                         :delete-recheck (if commit? [[id old-u 20]] []))]
          (check! :contender-outcome expected-outcome outcome (contains? expected-outcome outcome))
          (check! :final-primary expected (primary observer))
          (check! :final-unique expected (unique observer)))
        (finally
          (jdbc/rollback! a)
          ;; Release the owner before draining a waiter even if an assertion/query throws.
          (try (await! task) (catch Throwable _ (future-cancel task))))))))

(def gate-expression
  "CASE WHEN GET_LOCK(CONCAT(?, '_reached'),0)=1 THEN GET_LOCK(?,10) ELSE 0 END")

(defn gated-query! [owner observer reader reader-id sql params transition! check!]
  (let [gate (str "ji_" (UUID/randomUUID))
        held (jdbc/query owner "SELECT CAST(GET_LOCK(?,0) AS SIGNED)" gate)]
    (when-not (= [[1]] held) (throw (ex-info "Could not acquire scan gate" {:actual held})))
    (let [task (future (apply jdbc/query reader sql (conj params gate gate)))]
      (try
        ;; Streaming execution may retain the outer "Sending data" process
        ;; stage. Observe a connection-owned progress lock acquired at the
        ;; designated row instead of relying on a transient stage label.
        (let [holder (wait-until! task :statement-progress-lock
                       #(let [holder (ffirst (jdbc/query observer
                                               "SELECT CAST(IS_USED_LOCK(?) AS SIGNED)" (str gate "_reached")))]
                          (when (= reader-id holder) holder)))]
          (check! :statement-gate-observed [reader-id false] [holder (realized? task)]))
        (transition!)
        (when-not (= [[1]] (jdbc/query owner "SELECT CAST(RELEASE_LOCK(?) AS SIGNED)" gate))
          (throw (ex-info "Could not release scan gate" {})))
        (let [rows (await! task)]
          (when-not (= [[1]] (jdbc/query reader "SELECT CAST(RELEASE_LOCK(?) AS SIGNED)" gate))
            (throw (ex-info "Scan failed to acquire gate" {})))
          rows)
        (finally
          (jdbc/query owner "SELECT RELEASE_LOCK(?)" gate)
          (try (await! task) (catch Throwable _ (future-cancel task)))
          (when (realized? task)
            (jdbc/query reader "SELECT RELEASE_ALL_LOCKS()")))))))

(defn cross-table! [a b reader ids iteration {:keys [commit?]} check!]
  (jdbc/execute! b "INSERT INTO rc_left VALUES (?,10)" iteration)
  (jdbc/execute! b "INSERT INTO rc_right VALUES (?,10)" iteration)
  (.commit ^Connection b)
  (let [sql "SELECT l.v,r.v FROM rc_left l JOIN rc_right r ON l.id=r.id WHERE l.id=?"
        read-pair #(jdbc/query reader sql iteration)]
    (jdbc/execute! a "UPDATE rc_left SET v=20 WHERE id=?" iteration)
    (check! :partial-invisible [[10 10]] (read-pair))
    (jdbc/execute! a "UPDATE rc_right SET v=20 WHERE id=?" iteration)
    (check! :uncommitted-invisible [[10 10]] (read-pair))
    (check! :snapshot-before-resolution [[10 10]]
            (gated-query! a b reader (:c ids) (str sql " AND CASE WHEN l.v=10 THEN (" gate-expression ") ELSE 0 END=1")
                          [iteration] #(resolve! a commit?) check!))
    (check! :fresh-after-resolution [(if commit? [20 20] [10 10])] (read-pair))))

(defn explain-key [conn sql params]
  (with-open [stmt (.prepareStatement ^Connection conn (str "EXPLAIN " sql))]
    (.setQueryTimeout stmt 10)
    (doseq [[i value] (map-indexed vector params)] (.setObject stmt (inc i) value))
    (with-open [rs (.executeQuery stmt)]
      (when-not (.next rs) (throw (ex-info "EXPLAIN returned no plan" {})))
      (.getString rs "key"))))

(defn scan! [a b reader ids iteration {:keys [path]} n check!]
  (doseq [chunk (partition-all 128 (range 1 (inc n)))]
    (jdbc/execute! b (str "INSERT INTO rc_scan VALUES "
                         (str/join "," (map #(str "(" iteration "," % ",10," % ",'before')") chunk)))))
  (.commit ^Connection b)
  (let [hint (case path :primary "FORCE INDEX(PRIMARY)" :secondary "FORCE INDEX(by_group)"
                         :heap "IGNORE INDEX(PRIMARY,by_group)")
        sql (str "SELECT id,grp,v,note FROM rc_scan " hint " WHERE bucket=? AND grp=10")
        gated (str sql " AND CASE WHEN id=? THEN (" gate-expression ") ELSE 1 END=1 ORDER BY id")
        original (mapv #(vector % 10 % "before") (range 1 (inc n)))
        changed (->> (range 1 (inc n))
                     (remove #{n (- n 2)})
                     (map #(vector (if (= % (dec n)) (+ n 2) %) 10 (+ % 100) nil))
                     (cons [(inc n) 10 777 "inserted"])
                     (sort-by first) vec)]
    ;; EXPLAIN the gated statement itself; merely requesting an index isn't coverage.
    (let [explain-gate (str "je_" (UUID/randomUUID))
          key (explain-key b gated [iteration (quot n 2) explain-gate explain-gate])
          locks (ffirst (jdbc/query b "SELECT CAST(RELEASE_ALL_LOCKS() AS SIGNED)"))]
      (check! :access-plan {:key (case path :primary "PRIMARY" :secondary "by_group" :heap nil) :explain-locks 0}
              {:key key :explain-locks locks}))
    (check! :scan-original-snapshot original
            (gated-query! a b reader (:c ids) gated [iteration (quot n 2)]
              #(do (jdbc/execute! a "UPDATE rc_scan SET v=v+100,note=NULL WHERE bucket=?" iteration)
                   (jdbc/execute! a "DELETE FROM rc_scan WHERE bucket=? AND id=?" iteration n)
                   (jdbc/execute! a "UPDATE rc_scan SET id=? WHERE bucket=? AND id=?" (+ n 2) iteration (dec n))
                   (jdbc/execute! a "UPDATE rc_scan SET grp=20 WHERE bucket=? AND id=?" iteration (- n 2))
                   (jdbc/execute! a "INSERT INTO rc_scan VALUES (?,?,10,777,'inserted')" iteration (inc n))
                   (.commit ^Connection a)) check!))
    (check! :next-statement-fresh changed (jdbc/query reader (str sql " ORDER BY id") iteration))))

(defn run-probe! [a b c ids test iteration]
  (let [selected (selected-scenarios test)
        spec (nth selected (mod iteration (count selected)))
        checks (atom [])
        failed? (atom false)
        check! (fn record-check!
                 ([label expected actual] (record-check! label expected actual (= expected actual)))
                 ([label expected actual valid?]
                  (swap! checks conj {:case label :expected expected :actual actual :valid? valid?})))]
    (doseq [conn [a b c]] (.setAutoCommit ^Connection conn true))
    (.setAutoCommit ^Connection a false)
    (.setAutoCommit ^Connection b false)
    ;; Reader retains a single RC transaction across the before/after statements.
    (when (#{:cross-table :scan} (:kind spec)) (.setAutoCommit ^Connection c false))
    (try
      (case (:kind spec)
        :cross-table (cross-table! a b c ids iteration spec check!)
        :scan (scan! a b c ids iteration spec (:scan-rows test) check!)
        (conflict! a b c ids iteration spec check!))
      {:iteration iteration :scenario (:name spec) :connections ids :checks @checks}
      (catch Throwable e
        (reset! failed? true)
        (throw (ex-info "Interleave scenario failed"
                        {:iteration iteration :scenario (:name spec) :connections ids :checks @checks} e)))
      (finally
        (try
          (doseq [conn [a b c]] (jdbc/rollback! conn))
          ;; Bound retained data, including the full-scan path. Cleanup errors
          ;; must not mask the original failure and its interleaving evidence.
          (.setAutoCommit ^Connection c true)
          (case (:kind spec)
            :scan (jdbc/execute! c "DELETE FROM rc_scan WHERE bucket=?" iteration)
            :cross-table (do (jdbc/execute! c "DELETE FROM rc_left WHERE id=?" iteration)
                             (jdbc/execute! c "DELETE FROM rc_right WHERE id=?" iteration))
            (jdbc/execute! c "DELETE FROM rc_interleave WHERE bucket=?" iteration))
          (catch Throwable e
            (when-not @failed?
              (throw (ex-info "Interleave cleanup failed"
                              {:iteration iteration :scenario (:name spec) :checks @checks} e)))))))))

(defrecord InterleaveClient [connections ids]
  client/Client
  (open! [this test _]
    (let [opened (atom [])]
      (try
        (dotimes [_ 3] (swap! opened conj (jdbc/open! test)))
        (assoc this :connections @opened
               :ids (zipmap [:a :b :c] (map #(ffirst (jdbc/query % "SELECT CONNECTION_ID()")) @opened)))
        (catch Throwable e (doseq [conn @opened] (.close ^Connection conn)) (throw e)))))
  (setup! [this _] this)
  (invoke! [_ test op]
    (let [[a b c] connections]
      (assoc op :type :ok :value (run-probe! a b c ids test (:value op)))))
  (teardown! [_ _])
  (close! [_ _] (doseq [conn connections] (.close ^Connection conn))))
