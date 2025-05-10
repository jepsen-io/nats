(ns jepsen.nats.cli
  "Command-line entry point for NATS tests"
  (:gen-class)
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [control :as control]
                    [generator :as gen]
                    [nemesis :as jepsen.nemesis]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.nats [db :as db]
                         [nemesis :as nemesis]
                         [queue :as queue]]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none          (constantly tests/noop-test)
   :queue         queue/workload})

(def standard-workloads
  "All the workloads we run by default."
  [:queue])

(def nemeses
  "Basic nemeses we have available."
  #{:kill
    :pause
    :partition
    :packet
    :clock})

(def db-node-targets
  "Different ways we can target single nodes for database faults."
  #{:one
    :minority
    :majority
    :all})

(def standard-nemeses
  "Combinations of nemeses we run by default."
  [; Nothing
   []
   ; One fault at a time
   [:partition]
   [:kill]
   [:pause]
   [:packet]
   [:clock]
   ; General chaos
   [:partition :pause :kill :clock :packet]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all (peek standard-nemeses)})

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map keyword)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(defn test-name
  "Takes CLI options and constructs a test name as a string."
  [opts]
  (str (:version opts) " "
       (name (:workload opts))
       (when (:lazyfs opts)
         " lazyfs")
       (when-let [s (:sync-interval opts)]
         (str " s=" s))
       (let [n (:nemesis opts)]
         (when (seq n)
           (str " " (->> n (map name) sort (str/join ",")))))))

(defn nats-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [workload-name   (:workload opts)
        workload        ((workloads workload-name) opts)
        db              (db/db opts)
        os              debian/os
        nemesis         (nemesis/package
                          {:db            db
                           :nodes         (:nodes opts)
                           :stable-period (:nemesis-stable-period opts)
                           :interval      (:nemesis-interval opts)
                           :faults        (:nemesis opts)
                           :partition     {:targets [:one :majority]}
                           :pause         {:targets (:db-node-targets opts)}
                           :kill          {:targets (:db-node-targets opts)}
                           :packet        {:targets [:one :minority :all]
                                           :behaviors
                                           [{:delay {:time   "100ms"
                                                     :jitter "50ms"}}]}})
        workload-gen (->> (:generator workload)
                          (gen/stagger (/ (:rate opts)))
                          gen/clients)
        final-gen   (:final-generator workload)
        nemesis-gen (->> (:generator nemesis)
                         gen/nemesis)
        gen (gen/phases
              ; Main phase
              (->> workload-gen
                   (gen/nemesis
                     (gen/phases
                       (gen/sleep (:initial-quiet-period opts))
                       (:generator nemesis)))
                   (gen/time-limit (:time-limit opts)))
              ; Recover
              (gen/nemesis
                [(:final-generator nemesis)
                 (gen/log "Beginning final generator")])
              ; Final gen
              (gen/clients final-gen))]
    (merge tests/noop-test
           opts
           {:name     (test-name opts)
            :os       os
            :db       db
            :checker  (checker/compose
                        {:perf       (checker/perf)
                         :clock      (checker/clock-plot)
                         :stats      (checker/stats)
                         :exceptions (checker/unhandled-exceptions)
                         :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis jepsen.nemesis/noop)
            :plot      {:nemeses (:perf nemesis)}
            :generator gen})))

(def cli-opts
  "Command-line option specification"
  [[nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "3n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--db-node-targets TARGETS" "A comma-separated list of ways to target DB nodes for faults, like 'one,majority'"
    :default  [:one :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-node-targets) (cli/one-of db-node-targets)]]

   [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
    :default  20
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--initial-quiet-period SECONDS" "How long to wait before beginning faults"
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--[no-]lazyfs" "Mounts data dir in a lazy filesystem that can lose writes on kill."]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :default  #{}
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str (cli/one-of nemeses) " or the special nemeses, which " (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
    :default  nil
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default  10000
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--sync-interval SECS" "The default fsync/sync interval for the filestore page cache. Passed directly to NATS's config file. Try 'always', '10s', or the NATS default, '2m'. If unset, lets NATS choose."]

   [nil "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  300
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--timeout MILLIS" "Client timeout, in milliseconds"
    :default  5000
    :parse-fn parse-long
    :validate [pos? "Must be positive."]]

   ["-v" "--version VERSION" "What version should we test?"
    :default "2.10.20"]

   ["-w" "--workload NAME" "What workload should we run?"
    :default  :queue
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] standard-nemeses)
        workloads (if-let [w (:workload opts)] [w] standard-workloads)
        lazyfs    (let [l (:lazyfs opts)]
                    (if (nil? l)
                      [false true]
                      [l]))]
    (for [i     (range (:test-count opts))
          l     lazyfs
          n     nemeses
          w     workloads]
      (nats-test (-> opts
                       (assoc :lazyfs l
                              :nemesis  n
                              :workload w))))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  nats-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd
                     {:tests-fn all-tests
                      :opt-spec (cli/without-defaults-for
                                  [:workload :nemesis :lazyfs]
                                  cli-opts)})
                   (cli/serve-cmd))
            args))
