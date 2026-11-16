(ns mydb.core
  (:gen-class)
  (:require [jepsen.checker :as checker]
            [clojure.java.io :as io]
            [jepsen.cli :as cli]
            [jepsen.core :as jepsen]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.store :as store]
            [jepsen.tests :as tests]
            [mydb.checker :as check]
            [mydb.jdbc :as jdbc]
            [mydb.workload.append :as append]
            [mydb.workload.branches :as branches]
            [mydb.workload.interleave :as interleave]
            [mydb.workload.probe :as probe])
  (:import (java.sql Connection)
           (java.util UUID)))

(def cli-options
  [[nil "--host HOST" "MySQL endpoint (no SSH or process management)." :default "127.0.0.1"]
   [nil "--port PORT" "MySQL port." :default 3307 :parse-fn parse-long
    :validate [#(<= 1 % 65535) "Must be between 1 and 65535"]]
   [nil "--db-user USER" "Password comes from JEPSEN_MYDB_PASSWORD." :default "root"]
   [nil "--jdbc-ssl-mode MODE" "Connector/J SSL mode." :default "DISABLED"
    :validate [#{"DISABLED" "REQUIRED" "VERIFY_CA" "VERIFY_IDENTITY"} "Invalid SSL mode"]]
   [nil "--workload NAME" "append, rc-probe, rc-interleave or rc-branches." :default :append :parse-fn keyword
    :validate [#{:append :rc-probe :rc-interleave :rc-branches} "Unknown workload"]]
   [nil "--branch-protocol NAME" "both, text or prepared." :default :both :parse-fn keyword
    :validate [#{:both :text :prepared} "Must be both, text or prepared"]]
   [nil "--branch-case NAME" "all, or one transaction/protocol branch." :default :all :parse-fn keyword
    :validate [(conj (set (keys branches/case-checks)) :all) "Unknown branch case"]]
   [nil "--scan-rows NUMBER" "Rows per interleaved scan." :default 512 :parse-fn parse-long
    :validate [#(<= 128 % 8192) "Must be 128..8192"]]
   [nil "--interleave-case NAME" "all, or one named interleave scenario for reproduction."
    :default :all :parse-fn keyword
    :validate [(conj (set (map :name interleave/scenarios)) :all) "Unknown interleave scenario"]]
   [nil "--seed NUMBER" "Seed for generated operation plans." :default 1 :parse-fn parse-long]
   [nil "--key-count NUMBER" "Pre-created append keys." :default 16 :parse-fn parse-long
    :validate [pos? "Must be positive"]]
   [nil "--max-txn-length NUMBER" "Maximum micro-operations per transaction."
    :default 4 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--abort-percent NUMBER" "Percentage of intentional transaction rollbacks."
    :default 20 :parse-fn parse-long :validate [#(<= 0 % 100) "Must be 0..100"]]
   [nil "--rate NUMBER" "Approximate total operations/s, including rolled back transactions."
    :default 100 :parse-fn parse-double :validate [#(and (Double/isFinite %) (pos? %)) "Must be positive and finite"]]
   [nil "--plots" "Generate performance and anomaly plots (requires gnuplot and Graphviz)."]])

(defrecord Schema []
  db/DB
  (setup! [_ test _]
    ;; Generated identifier only. Never reuse, truncate, or drop a user's schema.
    (with-open [^Connection conn (jdbc/open! test "")]
      (jdbc/execute! conn (str "CREATE DATABASE " (:database test))))
    (with-open [^Connection conn (jdbc/open! test)]
      (case (:workload test)
        :append
        (do
          (jdbc/execute! conn "CREATE TABLE rc_append (id BIGINT PRIMARY KEY, v LONGTEXT NOT NULL)")
          (dotimes [k (:key-count test)]
            (jdbc/execute! conn "INSERT INTO rc_append (id,v) VALUES (?, '')" k)))
        :rc-branches (branches/setup! conn)
        :rc-interleave (interleave/setup! conn)
        :rc-probe
        (jdbc/execute! conn "CREATE TABLE rc_probe (id BIGINT PRIMARY KEY, grp BIGINT NOT NULL, v BIGINT, KEY by_group(grp))"))))
  (teardown! [_ _ _]))

(defn test-map [options]
  (when (and (= :rc-branches (:workload options)) (empty? (branches/selected-scenarios options)))
    (throw (ex-info "No branch scenarios match the selected protocol" {})))
  (let [append? (= :append (:workload options))
        operations (if append?
                     (append/transactions options)
                     (map (fn [i] {:type :invoke :f :probe :value i}) (range)))
        workload-gen (->> operations
                          (gen/stagger (/ 1.0 (:rate options)))
                          (gen/time-limit (:time-limit options))
                          gen/clients)
        checks (cond-> {:stats (checker/stats)
                        :health (check/health-checker append?)
                        :isolation (case (:workload options)
                                     :append (check/append-checker)
                                     :rc-interleave (interleave/interleave-checker)
                                     :rc-branches (branches/branch-checker)
                                     (check/probe-checker))}
                 append? (assoc :final-state (check/final-state-checker)))]
    (merge tests/noop-test options
           {:name (str "mydb-read-committed-" (name (:workload options)))
            :database (str "jepsen_rc_" (.replace (str (UUID/randomUUID)) "-" ""))
            :nodes [(:host options)]
            :ssh {:dummy? true}
            :db (->Schema)
            :client (case (:workload options)
                      :append (append/->AppendClient nil)
                      :rc-interleave (interleave/->InterleaveClient nil nil)
                      :rc-branches (branches/->BranchClient)
                      (probe/->ProbeClient nil nil))
            :generator (if append?
                         (gen/phases workload-gen
                                     (gen/clients (gen/once (append/final-read options))))
                         workload-gen)
            :checker (checker/compose (cond-> checks (:plots options) (assoc :perf (checker/perf))))})))

(defn -main [& args]
  ;; Keep direct CLI runs outside the checkout; runners override per workload.
  (alter-var-root #'store/base-dir
                  (constantly (System/getProperty "mydb.jepsen.store"
                                                  "/tmp/mydb/jepsen/store")))
  (let [commands (cli/single-test-cmd {:opt-spec cli-options :test-fn test-map
                                      :usage (cli/test-usage)})]
    ;; Upstream `analyze` saves results but does not set a failure exit code.
    (cli/run! (update-in commands ["analyze" :run]
                        (fn [analyze]
                          (fn [parsed]
                            (let [target (get-in parsed [:options :test])
                                  result (if (string? target)
                                           (let [directory (.getCanonicalFile (io/file target))]
                                             (when-not (.isFile (io/file directory "test.jepsen"))
                                               (throw (ex-info "Expected a Jepsen test directory" {:path target})))
                                             ;; Upstream resolves output relative to cwd even
                                             ;; when its input is an absolute directory.
                                             (with-redefs [store/base-dir (.getPath (.getParentFile (.getParentFile directory)))]
                                               (analyze parsed)))
                                           (analyze parsed))
                                  test (jepsen/log-results result)]
                              (case (get-in test [:results :valid?])
                                true nil
                                false (System/exit 1)
                                (System/exit 2))))))
              args)))
