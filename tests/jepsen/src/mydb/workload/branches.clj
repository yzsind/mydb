(ns mydb.workload.branches
  "Transaction state and real text/server-prepared protocol branches."
  (:require [clojure.set :as set]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [mydb.checker :as check]
            [mydb.jdbc :as jdbc]
            [mydb.workload.interleave :as interleave])
  (:import (java.sql Connection PreparedStatement ResultSet SQLException Timestamp Types)
           (java.math BigDecimal)
           (java.io ByteArrayInputStream)
           (java.security MessageDigest)
           (java.time LocalDateTime Instant)
           (java.time.format DateTimeFormatter)
           (java.util Calendar HexFormat TimeZone UUID)
           (com.mysql.cj.jdbc ServerPreparedStatement)))

(def common-cases
  [:savepoint-rollback :savepoint-release-reuse :savepoint-locks
   :xa-commit :xa-rollback :xa-one-phase :xa-invalid :xa-unsupported
   :for-update :for-share :nowait :skip-locked :lock-timeout :deadlock
   :duplicate-statement :check-statement :syntax-error :autocommit-enable])

(def scenarios
  (vec (concat (for [protocol [:text :prepared] kind common-cases]
                 {:name (keyword (str (name kind) "-" (name protocol))) :kind kind :protocol protocol})
               (for [kind [:prepared-rebind :prepared-long-data :prepared-cursor]]
                 {:name kind :kind kind :protocol :prepared}))))

(def case-checks
  {:savepoint-rollback #{:own-restored :later-savepoint-gone :uncommitted-hidden :final}
   :savepoint-release-reuse #{:later-savepoint-works :released-savepoint-gone :replacement-boundary :final}
   :savepoint-locks #{:own-restored :lock-retained :lock-released :final}
   :xa-commit #{:prepared-hidden :recover-present :prepared-lock :origin-closed :recover-removed :resolved-xid-error :final}
   :xa-rollback #{:prepared-hidden :recover-present :prepared-lock :origin-closed :recover-removed :resolved-xid-error :final}
   :xa-one-phase #{:uncommitted-hidden :recover-absent :resolved-xid-error :final}
   :xa-invalid #{:prepare-active-error :wrong-xid-error :duplicate-xid-error :recover-present :recover-removed :final}
   :xa-unsupported #{:join-error :resume-error :suspend-error :normal-start-works :final}
   :for-update #{:consistent-read-old :wait-observed :locking-read-fresh :final}
   :for-share #{:shared-compatible :wait-observed :writer-upgraded :final}
   :nowait #{:nowait-error :prior-write-retained :prior-write-hidden :prior-write-committed :final}
   :skip-locked #{:skipped-subset :returned-row-locked :all-locks-released :final}
   :lock-timeout #{:wait-observed :timeout-error :prior-write-retained :prior-write-hidden
                   :savepoint-retained :prior-lock-retained :prior-write-committed :lock-released :final}
   :deadlock #{:wait-observed :deadlock-error :survivor-finished :victim-rolled-back
               :survivor-committed :victim-savepoint-gone :victim-reusable :final}
   :duplicate-statement #{:statement-error :own-prior-write :other-prior-hidden :failed-write-lock-released :final}
   :check-statement #{:statement-error :own-prior-write :other-prior-hidden :failed-write-lock-released :final}
   :syntax-error #{:syntax-error :prior-write-retained :prior-write-hidden :final}
   :autocommit-enable #{:pending-hidden :enable-commits :autocommit-state :next-write-commits :final}
   :prepared-rebind #{:live-server-statement :single-prepare :three-executes :close-command :closed-statement :typed-values :final}
   :prepared-long-data #{:long-data-commands :reset-command :close-command :payloads :final}
   :prepared-cursor #{:first-row :cursor-old-snapshot :fetch-commands :fresh-next-statement :final}})

(defn required-checks [spec] (conj (get case-checks (:kind spec)) :protocol-path))

(defn selected-scenarios [test]
  (let [kind (or (:branch-case test) :all)
        protocol (or (:branch-protocol test) :both)]
    (filterv #(and (or (= :all kind) (= kind (:kind %)))
                   (or (= :both protocol) (= protocol (:protocol %)))) scenarios)))

(defn result [history selected]
  (let [base (check/probe-result history)
        ops (filter #(and (= :probe (:f %)) (= :ok (:type %))) history)
        specs (into {} (map (juxt :name identity) selected))
        seen (frequencies (map #(get-in % [:value :scenario]) ops))
        incomplete (vec (for [op ops
                              :let [spec (get specs (get-in op [:value :scenario]))
                                    labels (map :case (get-in op [:value :checks]))]
                              :when (or (nil? spec) (not= (required-checks spec) (set labels))
                                        (not= (count labels) (count (set labels))))]
                          {:scenario (get-in op [:value :scenario]) :checks (vec labels)}))
        missing (set/difference (set (keys specs)) (set (keys seen)))]
    (assoc base :valid? (boolean (and (seq selected) (:valid? base) (empty? missing) (empty? incomplete)))
           :scenarios seen :missing-scenarios (vec (sort missing)) :incomplete-operations incomplete)))

(defn branch-checker []
  (reify checker/Checker
    (check [_ test history _] (result history (selected-scenarios test)))))

(defn control! [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.setQueryTimeout stmt 10)
    (.execute stmt sql)))

(defn raw-query [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.setQueryTimeout stmt 10)
    (with-open [rs (.executeQuery stmt sql)]
      (let [n (.getColumnCount (.getMetaData rs))]
        (loop [rows []]
          (if (.next rs) (recur (conj rows (mapv #(.getObject rs (int %)) (range 1 (inc n))))) rows))))))

(defn counters [conn]
  (into {} (map (fn [[k v]] [(keyword k) (parse-long (str v))])
                (raw-query conn "SHOW SESSION STATUS LIKE 'Com_stmt_%'"))))

(defn delta [before after key]
  (when-not (and (contains? before key) (contains? after key))
    (throw (ex-info "Missing protocol counter" {:counter key})))
  (- (get after key) (get before key)))

(defn digest [^bytes bytes]
  {:length (alength bytes) :sha256 (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes))})

(defn normalize [value]
  (cond
    (bytes? value) (digest value)
    (instance? BigDecimal value) (.toPlainString (.stripTrailingZeros ^BigDecimal value))
    (instance? LocalDateTime value) (.format ^LocalDateTime value (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    (instance? Timestamp value) (normalize (.toLocalDateTime ^Timestamp value))
    :else value))

(defn prepare! [^Connection conn protocol sql]
  (let [stmt (.prepareStatement conn sql)
        native? (instance? ServerPreparedStatement stmt)]
    (when-not (= (= protocol :prepared) native?)
      (.close stmt)
      (throw (ex-info "Unexpected JDBC prepared-statement path" {:protocol protocol :class (str (class stmt)) :sql sql})))
    (.setQueryTimeout stmt 10)
    stmt))

(defn execute! [conn protocol sql & params]
  (with-open [stmt (prepare! conn protocol sql)]
    (doseq [[i value] (map-indexed vector params)] (.setObject ^PreparedStatement stmt (inc i) value))
    (.executeUpdate ^PreparedStatement stmt)))

(defn query [conn protocol sql & params]
  (with-open [stmt (prepare! conn protocol sql)]
    (doseq [[i value] (map-indexed vector params)] (.setObject ^PreparedStatement stmt (inc i) value))
    (with-open [rs (.executeQuery ^PreparedStatement stmt)]
      (let [n (.getColumnCount (.getMetaData rs))]
        (loop [rows []]
          (if (.next rs)
            (recur (conj rows (mapv #(normalize (.getObject rs (int %))) (range 1 (inc n))))) rows))))))

(defn sql-error [f]
  (try {:code nil :sql-state nil :value (f)}
       (catch SQLException e {:code (.getErrorCode e) :sql-state (.getSQLState e) :message (.getMessage e)})))

(defn error! [check! label code f]
  (let [actual (sql-error f)]
    (check! label code (:code actual))
    (when-not (= code (:code actual))
      (throw (ex-info "Unexpected SQL outcome" (assoc actual :case label :expected-code code))))))

(defn setup! [conn]
  (doseq [sql ["CREATE TABLE branch_rows (id BIGINT PRIMARY KEY,bucket BIGINT NOT NULL,u VARCHAR(64) NOT NULL,v BIGINT NOT NULL,UNIQUE KEY by_u(u),CHECK(v>=0))"
               "CREATE TABLE branch_types (id BIGINT PRIMARY KEY,bucket BIGINT NOT NULL,n BIGINT,d DECIMAL(18,4),s VARCHAR(128),b VARBINARY(64),ts DATETIME(6),nullable_val VARCHAR(32))"
               "CREATE TABLE branch_payload (id BIGINT PRIMARY KEY,bucket BIGINT NOT NULL,payload LONGBLOB)"]]
    (control! conn sql)))

(defn rows [ctx conn]
  (query conn (:protocol ctx) "SELECT id,v FROM branch_rows WHERE bucket=? ORDER BY id" (:iteration ctx)))

(defn insert! [ctx conn offset value]
  (execute! conn (:protocol ctx) "INSERT INTO branch_rows VALUES (?,?,?,?)"
            (+ (:base ctx) offset) (:iteration ctx) (str "u_" (+ (:base ctx) offset)) value))

(defn update! [ctx conn offset value]
  (execute! conn (:protocol ctx) "UPDATE branch_rows SET v=? WHERE id=?" value (+ (:base ctx) offset)))

(defn locking-read [ctx conn offset suffix]
  (query conn (:protocol ctx) (str "SELECT id,v FROM branch_rows WHERE id=? " suffix) (+ (:base ctx) offset)))

(defn wait-for-lock! [ctx task requester blocker]
  (let [actual (interleave/wait-until! task :branch-data-lock
                 #(let [r (jdbc/query (:c ctx)
                            "SELECT REQUESTING_THREAD_ID,BLOCKING_THREAD_ID FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=? AND BLOCKING_THREAD_ID=?"
                            requester blocker)]
                    (when (seq r) r)))]
    ((:check! ctx) :wait-observed #{[requester blocker]} (set actual))))

(defn spawn! [ctx owner f]
  (let [task (future (f))] (swap! (:tasks ctx) conj [task owner]) task))

(defn savepoints! [{:keys [a b c base kind check!] :as ctx}]
  (if (= kind :savepoint-locks)
    (do
      (insert! ctx c 1 10)
      (control! a "BEGIN") (control! a "SAVEPOINT s")
      (update! ctx a 1 20) (control! a "ROLLBACK TO SAVEPOINT s")
      (check! :own-restored [[(inc base) 10]] (rows ctx a))
      (error! check! :lock-retained 3572 #(locking-read ctx b 1 "FOR UPDATE NOWAIT"))
      (control! a "COMMIT")
      (check! :lock-released [[(inc base) 10]] (locking-read ctx b 1 "FOR UPDATE NOWAIT")))
    (do
      (control! a "BEGIN") (insert! ctx a 1 10) (control! a "SAVEPOINT s")
      (update! ctx a 1 20)
      (if (= kind :savepoint-rollback)
        (do
          (insert! ctx a 2 30) (control! a "SAVEPOINT later")
          (execute! a (:protocol ctx) "DELETE FROM branch_rows WHERE id=?" (inc base))
          (control! a "ROLLBACK TO SAVEPOINT s")
          (check! :own-restored [[(inc base) 10]] (rows ctx a))
          (error! check! :later-savepoint-gone 1305 #(control! a "ROLLBACK TO SAVEPOINT later"))
          (check! :uncommitted-hidden [] (rows ctx c)))
        (do
          (control! a "SAVEPOINT later") (update! ctx a 1 30)
          (control! a "RELEASE SAVEPOINT s") (control! a "ROLLBACK TO SAVEPOINT later")
          (check! :later-savepoint-works [[(inc base) 20]] (rows ctx a))
          (error! check! :released-savepoint-gone 1305 #(control! a "ROLLBACK TO SAVEPOINT s"))
          (update! ctx a 1 40) (control! a "SAVEPOINT later")
          (update! ctx a 1 50) (control! a "ROLLBACK TO SAVEPOINT later")
          (check! :replacement-boundary [[(inc base) 40]] (rows ctx a))))
      (control! a "COMMIT")))
  (check! :final [[(inc base) (if (= kind :savepoint-release-reuse) 40 10)]] (rows ctx c)))

(defn recovered? [conn xid]
  (boolean (some #(= xid (str (last %))) (raw-query conn "XA RECOVER"))))

(defn await-server-disconnect! [observer connection-id]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (if (not-any? #(= connection-id (first %)) (raw-query observer "SHOW FULL PROCESSLIST"))
        true
        (if (> (System/nanoTime) deadline)
          (throw (ex-info "Origin connection remains registered after close" {:connection-id connection-id}))
          (do (Thread/sleep 2) (recur)))))))

(defn xa! [{:keys [a b c base kind check!] :as ctx}]
  (let [xid (str "jb_" (.replace (str (UUID/randomUUID)) "-" ""))
        id (str "'" xid "'")]
    (swap! (:xids ctx) conj id)
    (insert! ctx c 1 10)
    (when (= kind :xa-unsupported)
      (error! check! :join-error 1235 #(control! a (str "XA START " id " JOIN")))
      (error! check! :resume-error 1235 #(control! a (str "XA START " id " RESUME")))
      (error! check! :suspend-error 1235 #(control! a (str "XA END " id " SUSPEND"))))
    (control! a (str "XA START " id))
    (update! ctx a 1 20) (insert! ctx a 2 30)
    (when (= kind :xa-invalid)
      (error! check! :prepare-active-error 1399 #(control! a (str "XA PREPARE " id)))
      (error! check! :wrong-xid-error 1397 #(control! a (str "XA END 'missing_" xid "'"))))
    (when (= kind :xa-one-phase)
      (check! :uncommitted-hidden [[(inc base) 10]] (rows ctx c)))
    (when (= kind :xa-unsupported)
      (check! :normal-start-works [[(inc base) 20] [(+ base 2) 30]] (rows ctx a)))
    (control! a (str "XA END " id))
    (cond
      (= kind :xa-one-phase)
      (do (control! a (str "XA COMMIT " id " ONE PHASE"))
          (check! :recover-absent false (recovered? c xid)))
      (= kind :xa-unsupported)
      (control! a (str "XA ROLLBACK " id))
      :else
      (do
        (control! a (str "XA PREPARE " id))
        (check! :recover-present true (recovered? c xid))
        (if (= kind :xa-invalid)
          (error! check! :duplicate-xid-error 1440 #(control! b (str "XA START " id)))
          (do
            (check! :prepared-hidden [[(inc base) 10]] (rows ctx c))
            (.close ^Connection a)
            (check! :origin-closed {:client true :server true}
                    {:client (.isClosed ^Connection a) :server (await-server-disconnect! c (get-in ctx [:ids :a]))})
            (error! check! :prepared-lock 3572 #(locking-read ctx b 1 "FOR UPDATE NOWAIT"))))
        (control! b (str (if (= kind :xa-commit) "XA COMMIT " "XA ROLLBACK ") id))
        (check! :recover-removed false (recovered? c xid))))
    (when (#{:xa-commit :xa-rollback :xa-one-phase} kind)
      (error! check! :resolved-xid-error 1397 #(control! b (str "XA COMMIT " id))))
    (swap! (:xids ctx) disj id)
    (check! :final (if (#{:xa-commit :xa-one-phase} kind)
                    [[(inc base) 20] [(+ base 2) 30]] [[(inc base) 10]]) (rows ctx c))))

(defn locks! [{:keys [a b c base kind protocol ids check!] :as ctx}]
  (insert! ctx c 1 10)
  (when (#{:skip-locked :deadlock :lock-timeout} kind) (insert! ctx c 2 20))
  (control! a "BEGIN") (control! b "BEGIN")
  (case kind
    :for-update
    (do
      (update! ctx a 1 20)
      (check! :consistent-read-old [[(inc base) 10]] (rows ctx b))
      (let [task (spawn! ctx a #(locking-read ctx b 1 "FOR UPDATE"))]
        (wait-for-lock! ctx task (:b ids) (:a ids))
        (control! a "COMMIT")
        (check! :locking-read-fresh [[(inc base) 20]] (interleave/await! task)))
      (update! ctx b 1 21) (control! b "COMMIT")
      (check! :final [[(inc base) 21]] (rows ctx c)))
    :for-share
    (do
      (locking-read ctx a 1 "FOR SHARE")
      (check! :shared-compatible [[(inc base) 10]] (locking-read ctx b 1 "FOR SHARE"))
      (let [task (spawn! ctx a #(update! ctx b 1 11))]
        (wait-for-lock! ctx task (:b ids) (:a ids))
        (control! a "ROLLBACK")
        (check! :writer-upgraded 1 (interleave/await! task)))
      (control! b "COMMIT") (check! :final [[(inc base) 11]] (rows ctx c)))
    :nowait
    (do
      (locking-read ctx a 1 "FOR UPDATE") (insert! ctx b 2 30)
      (error! check! :nowait-error 3572 #(locking-read ctx b 1 "FOR UPDATE NOWAIT"))
      (check! :prior-write-retained [[(inc base) 10] [(+ base 2) 30]] (rows ctx b))
      (check! :prior-write-hidden [[(inc base) 10]] (rows ctx c))
      (control! b "COMMIT")
      (check! :prior-write-committed [[(inc base) 10] [(+ base 2) 30]] (rows ctx c))
      (control! a "ROLLBACK")
      (check! :final [[(inc base) 10] [(+ base 2) 30]] (rows ctx c)))
    :skip-locked
    (do
      (locking-read ctx a 1 "FOR UPDATE")
      (check! :skipped-subset [[(+ base 2) 20]]
              (query b protocol "SELECT id,v FROM branch_rows WHERE bucket=? ORDER BY id FOR UPDATE SKIP LOCKED" (:iteration ctx)))
      (error! check! :returned-row-locked 3572 #(locking-read ctx c 2 "FOR UPDATE NOWAIT"))
      (control! a "COMMIT") (control! b "COMMIT")
      (check! :all-locks-released [[(inc base) 10] [(+ base 2) 20]]
              (query c protocol "SELECT id,v FROM branch_rows WHERE bucket=? ORDER BY id FOR UPDATE NOWAIT" (:iteration ctx)))
      (check! :final [[(inc base) 10] [(+ base 2) 20]] (rows ctx c)))
    :lock-timeout
    (do
      (insert! ctx c 3 30)
      (locking-read ctx a 1 "FOR UPDATE") (update! ctx b 3 31)
      (control! b "SAVEPOINT victim") (control! b "SET SESSION innodb_lock_wait_timeout=1")
      (let [task (spawn! ctx a #(sql-error (fn [] (update! ctx b 1 99))))]
        (wait-for-lock! ctx task (:b ids) (:a ids))
        (check! :timeout-error 1205 (:code (interleave/await! task))))
      (check! :prior-write-retained [[(inc base) 10] [(+ base 2) 20] [(+ base 3) 31]] (rows ctx b))
      (check! :prior-write-hidden [[(inc base) 10] [(+ base 2) 20] [(+ base 3) 30]] (rows ctx c))
      (update! ctx b 2 22)
      (control! b "ROLLBACK TO SAVEPOINT victim")
      (check! :savepoint-retained [[(inc base) 10] [(+ base 2) 20] [(+ base 3) 31]] (rows ctx b))
      (error! check! :prior-lock-retained 3572 #(locking-read ctx c 3 "FOR UPDATE NOWAIT"))
      (control! b "COMMIT")
      (check! :prior-write-committed [[(inc base) 10] [(+ base 2) 20] [(+ base 3) 31]] (rows ctx c))
      (check! :lock-released [[(+ base 3) 31]] (locking-read ctx c 3 "FOR UPDATE NOWAIT"))
      (control! a "ROLLBACK")
      (control! b "BEGIN") (update! ctx b 2 21) (control! b "COMMIT")
      (check! :final [[(inc base) 10] [(+ base 2) 21] [(+ base 3) 31]] (rows ctx c)))
    :deadlock
    (do
      (update! ctx a 1 11) (update! ctx b 2 22)
      (doseq [conn [a b]] (control! conn "SAVEPOINT victim"))
      ;; Observing A -> B establishes contention, not which transaction will
      ;; detect the cycle. Capture both results before choosing the victim.
      (let [task (spawn! ctx b #(sql-error (fn [] (update! ctx a 2 12))))]
        (wait-for-lock! ctx task (:a ids) (:b ids))
        (let [b-result (sql-error #(update! ctx b 1 21))
              a-result (interleave/await! task)
              codes [(:code a-result) (:code b-result)]
              valid? (contains? #{[1213 nil] [nil 1213]} codes)]
          (check! :deadlock-error #{[1213 nil] [nil 1213]} codes valid?)
          (when-not valid?
            (throw (ex-info "Expected exactly one deadlock victim and one successful writer"
                            {:a a-result :b b-result})))
          (let [a-victim? (= 1213 (:code a-result))
                victim (if a-victim? a b)
                survivor (if a-victim? b a)
                survivor-result (if a-victim? b-result a-result)
                expected (if a-victim? [[(inc base) 21] [(+ base 2) 22]]
                                      [[(inc base) 11] [(+ base 2) 12]])]
            (check! :survivor-finished 1 (:value survivor-result))
            ;; Check before BEGIN can hide a partial rollback. The survivor's
            ;; writes remain uncommitted, so the victim must see original rows.
            (check! :victim-rolled-back [[(inc base) 10] [(+ base 2) 20]] (rows ctx victim))
            (error! check! :victim-savepoint-gone 1305
                    #(control! victim "ROLLBACK TO SAVEPOINT victim"))
            (control! survivor "COMMIT")
            (check! :survivor-committed expected (rows ctx c))
            (control! victim "BEGIN")
            (check! :victim-reusable 1 (insert! ctx victim 3 30))
            (control! victim "COMMIT")
            (check! :final (conj expected [(+ base 3) 30]) (rows ctx c))))))))

(defn statement-errors! [{:keys [a b c base kind protocol iteration check!] :as ctx}]
  (when (#{:duplicate-statement :check-statement} kind) (insert! ctx c 2 200))
  (control! a "BEGIN") (insert! ctx a 1 10)
  (if (= kind :syntax-error)
    (do
      (error! check! :syntax-error 1064 #(query a protocol "SELECT (1 +"))
      (check! :prior-write-retained [[(inc base) 10]] (rows ctx a))
      (check! :prior-write-hidden [] (rows ctx c))
      (control! a "COMMIT") (check! :final [[(inc base) 10]] (rows ctx c)))
    (do
      (error! check! :statement-error (if (= kind :duplicate-statement) 1062 3819)
              #(execute! a protocol "INSERT INTO branch_rows VALUES (?,?,?,30),(?,?,?,?)"
                         (+ base 3) iteration (str "u_" (+ base 3))
                         (+ base 4) iteration (str "u_" (+ base (if (= kind :duplicate-statement) 2 4)))
                         (if (= kind :duplicate-statement) 40 -1)))
      (check! :own-prior-write [[(inc base) 10] [(+ base 2) 200]] (rows ctx a))
      (check! :other-prior-hidden [[(+ base 2) 200]] (rows ctx c))
      (control! b "SET SESSION innodb_lock_wait_timeout=1")
      (check! :failed-write-lock-released 1 (insert! ctx b 3 31))
      (insert! ctx a 5 50) (control! a "COMMIT")
      (check! :final [[(inc base) 10] [(+ base 2) 200] [(+ base 3) 31] [(+ base 5) 50]] (rows ctx c)))))

(defn autocommit! [{:keys [a c base check!] :as ctx}]
  (.setAutoCommit ^Connection a false) (insert! ctx a 1 10)
  (check! :pending-hidden [] (rows ctx c))
  (.setAutoCommit ^Connection a true)
  (check! :enable-commits [[(inc base) 10]] (rows ctx c))
  (check! :autocommit-state true (.getAutoCommit ^Connection a))
  (update! ctx a 1 20)
  (check! :next-write-commits [[(inc base) 20]] (rows ctx c))
  (check! :final [[(inc base) 20]] (rows ctx c)))

(defn rebind! [{:keys [a c base iteration check!] :as ctx}]
  (let [before (counters a)
        stmt (prepare! a :prepared "INSERT INTO branch_types VALUES (?,?,?,?,?,?,?,?)")
        bytes1 (byte-array [0 127 -128 -1])
        bytes3 (byte-array [9 0 -1])
        utc (Calendar/getInstance (TimeZone/getTimeZone "UTC"))]
    (try
      (check! :live-server-statement [[1]]
              (query c :prepared "SELECT COUNT(*) FROM performance_schema.prepared_statements_instances WHERE OWNER_THREAD_ID=?"
                     (get-in ctx [:ids :a])))
      (doseq [i (range 3)]
        (.clearParameters ^PreparedStatement stmt)
        (.setLong ^PreparedStatement stmt 1 (+ base i 1))
        (.setLong ^PreparedStatement stmt 2 iteration)
        (case i
          0 (do (.setLong ^PreparedStatement stmt 3 (dec Long/MAX_VALUE))
                (.setBigDecimal ^PreparedStatement stmt 4 (BigDecimal. "123456789012.3456"))
                (.setString ^PreparedStatement stmt 5 "中文 Ω ' \u0000")
                (.setBytes ^PreparedStatement stmt 6 bytes1)
                (.setTimestamp ^PreparedStatement stmt 7 (Timestamp/from (Instant/parse "2024-02-29T12:34:56.123456Z")) utc)
                (.setNull ^PreparedStatement stmt 8 Types/VARCHAR))
          1 (do (.setNull ^PreparedStatement stmt 3 Types/BIGINT)
                (.setBigDecimal ^PreparedStatement stmt 4 (BigDecimal. "-0.0100"))
                (.setString ^PreparedStatement stmt 5 "")
                (.setBytes ^PreparedStatement stmt 6 (byte-array 0))
                (.setNull ^PreparedStatement stmt 7 Types/TIMESTAMP)
                (.setString ^PreparedStatement stmt 8 "second"))
          2 (do (.setInt ^PreparedStatement stmt 3 -7)
                (.setDouble ^PreparedStatement stmt 4 1.25)
                (.setString ^PreparedStatement stmt 5 "emoji🦀")
                (.setBytes ^PreparedStatement stmt 6 bytes3)
                (.setTimestamp ^PreparedStatement stmt 7 (Timestamp/from (Instant/parse "2025-01-01T00:00:00.000001Z")) utc)
                (.setNull ^PreparedStatement stmt 8 Types/VARCHAR)))
        (when-not (= 1 (.executeUpdate ^PreparedStatement stmt))
          (throw (ex-info "Prepared INSERT did not affect one row" {:row i}))))
      (finally (.close ^PreparedStatement stmt)))
    (let [after (counters a)]
      (check! :single-prepare 1 (delta before after :Com_stmt_prepare))
      (check! :three-executes 3 (delta before after :Com_stmt_execute))
      (check! :close-command 1 (delta before after :Com_stmt_close)))
    (check! :closed-statement {:client true :server [[0]]}
            {:client (.isClosed ^PreparedStatement stmt)
             :server (query c :prepared "SELECT COUNT(*) FROM performance_schema.prepared_statements_instances WHERE OWNER_THREAD_ID=?"
                            (get-in ctx [:ids :a]))})
    (check! :typed-values
            [[(inc base) (dec Long/MAX_VALUE) "123456789012.3456" "中文 Ω ' \u0000" (digest bytes1) "2024-02-29 12:34:56.123456" nil]
             [(+ base 2) nil "-0.01" "" (digest (byte-array 0)) nil "second"]
             [(+ base 3) -7 "1.25" "emoji🦀" (digest bytes3) "2025-01-01 00:00:00.000001" nil]]
            (query c :prepared "SELECT id,n,d,s,b,ts,nullable_val FROM branch_types WHERE bucket=? ORDER BY id" iteration))
    (check! :final [[3]] (query c :prepared "SELECT COUNT(*) FROM branch_types WHERE bucket=?" iteration))))

(defn long-data! [{:keys [a c base iteration check!] :as ctx}]
  (let [payload (byte-array (map unchecked-byte (range 32768)))
        before (counters a)
        stmt (prepare! a :prepared "INSERT INTO branch_payload VALUES (?,?,?)")
        expected [[(inc base) (digest payload)] [(+ base 2) (digest (byte-array 0))]]]
    (try
      (.setLong ^PreparedStatement stmt 1 (inc base))
      (.setLong ^PreparedStatement stmt 2 iteration)
      (with-open [stream (ByteArrayInputStream. payload)]
        (.setBinaryStream ^PreparedStatement stmt (int 3) stream)
        (.executeUpdate ^PreparedStatement stmt))
      ;; Connector/J sends COM_STMT_RESET here when the previous binding was
      ;; long data. Assert the server counter instead of assuming that fact.
      (.clearParameters ^PreparedStatement stmt)
      (.setLong ^PreparedStatement stmt 1 (+ base 2))
      (.setLong ^PreparedStatement stmt 2 iteration)
      (.setBytes ^PreparedStatement stmt 3 (byte-array 0))
      (.executeUpdate ^PreparedStatement stmt)
      (finally (.close ^PreparedStatement stmt)))
    (let [after (counters a)]
      (let [sends (delta before after :Com_stmt_send_long_data)
            resets (delta before after :Com_stmt_reset)]
        (check! :long-data-commands :positive sends (pos? sends))
        (check! :reset-command :positive resets (pos? resets)))
      (check! :close-command 1 (delta before after :Com_stmt_close)))
    (check! :payloads expected (query c :prepared "SELECT id,payload FROM branch_payload WHERE bucket=? ORDER BY id" iteration))
    (control! a "BEGIN")
    (execute! a :prepared "UPDATE branch_payload SET payload=? WHERE id=?" (byte-array [9]) (inc base))
    (control! a "ROLLBACK")
    (check! :final expected (query c :prepared "SELECT id,payload FROM branch_payload WHERE bucket=? ORDER BY id" iteration))))

(defn cursor! [{:keys [a c base iteration check!] :as ctx}]
  (doseq [offset (range 1 6)] (insert! ctx c offset 10))
  (control! a "BEGIN")
  (let [before (counters a)]
    (with-open [stmt (prepare! a :prepared "SELECT id,v FROM branch_rows WHERE bucket=? ORDER BY id")]
      (.setLong ^PreparedStatement stmt 1 iteration)
      (.setFetchSize ^PreparedStatement stmt 2)
      (with-open [rs (.executeQuery ^PreparedStatement stmt)]
        (when-not (.next rs) (throw (ex-info "Cursor returned no first row" {})))
        (let [first-row [(.getLong rs 1) (.getLong rs 2)]]
          (check! :first-row [(inc base) 10] first-row)
          (update! ctx c 5 99)
          (check! :cursor-old-snapshot (mapv #(vector (+ base %) 10) (range 1 6))
                  (loop [out [first-row]]
                    (if (.next rs) (recur (conj out [(.getLong rs 1) (.getLong rs 2)])) out))))))
    (let [fetches (delta before (counters a) :Com_stmt_fetch)]
      (check! :fetch-commands :positive fetches (pos? fetches))))
  (let [expected (mapv #(vector (+ base %) (if (= % 5) 99 10)) (range 1 6))]
    (check! :fresh-next-statement expected (rows ctx a))
    (control! a "COMMIT")
    (check! :final expected (rows ctx c))))

(defn cleanup! [{:keys [a b c tasks xids iteration]}]
  ;; Finish any XA branch before ordinary rollback/connection teardown. These
  ;; XIDs are generated by this operation and never refer to another test.
  (doseq [id @xids]
    (let [outcomes (for [conn [a b c] :when (not (.isClosed ^Connection conn))]
                     (sql-error #(control! conn (str "XA ROLLBACK " id))))]
      (when-not (some #(or (nil? (:code %)) (= 1397 (:code %))) outcomes)
        (throw (ex-info "Could not clean up owned XA branch" {:xid id :outcomes (vec outcomes)})))))
  (doseq [[task owner] @tasks]
    (when-not (realized? task) (sql-error #(control! owner "ROLLBACK")))
    (try (interleave/await! task) (catch Throwable _ (future-cancel task))))
  (doseq [conn [a b c] :when (not (.isClosed ^Connection conn))]
    (control! conn "ROLLBACK"))
  (doseq [table ["branch_rows" "branch_types" "branch_payload"]]
    (jdbc/execute! c (str "DELETE FROM " table " WHERE bucket=?") iteration)))

(defn run-probe! [test iteration]
  (let [selected (selected-scenarios test)
        spec (nth selected (mod iteration (count selected)))
        opts (assoc test :server-prepared? (= :prepared (:protocol spec)))
        checks (atom [])
        failed? (atom false)
        check! (fn record-check!
                 ([label expected actual] (record-check! label expected actual (= expected actual)))
                 ([label expected actual valid?]
                  (swap! checks conj {:case label :expected expected :actual actual :valid? valid?})))]
    (with-open [a (jdbc/open! opts) b (jdbc/open! opts) c (jdbc/open! opts)]
      (let [ids (zipmap [:a :b :c] (map #(ffirst (raw-query % "SELECT CONNECTION_ID()")) [a b c]))
            ctx (merge spec {:a a :b b :c c :base (* 16 iteration) :iteration iteration
                             :check! check! :ids ids :tasks (atom []) :xids (atom #{})})
            before (counters c)]
        (try
          (cond
            (#{:savepoint-rollback :savepoint-release-reuse :savepoint-locks} (:kind spec)) (savepoints! ctx)
            (#{:xa-commit :xa-rollback :xa-one-phase :xa-invalid :xa-unsupported} (:kind spec)) (xa! ctx)
            (#{:for-update :for-share :nowait :skip-locked :lock-timeout :deadlock} (:kind spec)) (locks! ctx)
            (#{:duplicate-statement :check-statement :syntax-error} (:kind spec)) (statement-errors! ctx)
            (= :autocommit-enable (:kind spec)) (autocommit! ctx)
            (= :prepared-rebind (:kind spec)) (rebind! ctx)
            (= :prepared-long-data (:kind spec)) (long-data! ctx)
            (= :prepared-cursor (:kind spec)) (cursor! ctx))
          (let [after (counters c)
                prepares (delta before after :Com_stmt_prepare)
                executes (delta before after :Com_stmt_execute)]
            (check! :protocol-path
                    (if (= :prepared (:protocol spec)) {:prepare :positive :execute :positive} {:prepare 0 :execute 0})
                    {:prepare prepares :execute executes}
                    (if (= :prepared (:protocol spec)) (and (pos? prepares) (pos? executes))
                        (and (zero? prepares) (zero? executes)))))
          {:iteration iteration :scenario (:name spec) :protocol (:protocol spec) :connections ids :checks @checks}
          (catch Throwable e
            (reset! failed? true)
            (throw (ex-info "Transaction/protocol branch failed"
                            {:iteration iteration :scenario (:name spec) :connections ids :checks @checks} e)))
          (finally
            (try (cleanup! ctx)
                 (catch Throwable e
                   (when-not @failed?
                     (throw (ex-info "Branch cleanup failed" {:scenario (:name spec) :checks @checks} e)))))))))))

(defrecord BranchClient []
  client/Client
  (open! [this _ _] this)
  (setup! [this _] this)
  (invoke! [_ test op] (assoc op :type :ok :value (run-probe! test (:value op))))
  (teardown! [_ _])
  (close! [_ _]))
