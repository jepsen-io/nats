(ns jepsen.nats.nemesis
  "Fault injection"
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap loopr]]
            [jepsen [control :as c]
                    [db :as jdb]
                    [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]
                    [role :as role]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis [combined :as nc]
                            [membership :as m]
                            [file :as nf]
                            [time :as nt]]
            [jepsen.nats [db :as db]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn package-gen-helper
  "Helper for package-gen. Takes a collection of packages and draws a random
  nonempty subset of them."
  [packages]
  (when (seq packages)
    (let [pkgs (->> packages
                    ; And pick a random subset of those
                    util/random-nonempty-subset
                    vec)]
      ; If we drew nothing, try again.
      (if (seq pkgs)
        pkgs
        (do ; (info "no draw, retrying")
            (recur packages))))))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  ; We want a sequence of random subsets of packages
  (repeatedly
    (fn rand-pkgs []
      (let [; Pick packages
            pkgs (if (< (rand) 1/4)
                   ; Roughly 1/4 of the time, pick no pkgs
                    []
                    (package-gen-helper packages))
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (map :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of nemeses: "
                       (pr-str (map (comp n/fs :nemesis) pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

(defn nodes
  "Figuring out what nodes are in the cluster is... complicated."
  [js]
  (into (sorted-set)
        ; Nodes in the meta cluster
        (concat (let [mc (-> js :data :meta_cluster)]
                  (->> (map :name (:replicas mc))
                       (cons (:leader mc))))
                ; Nodes in each stream
                (loopr [nodes (sorted-set)]
                       [ad      (-> js :data :account_details)
                        sd      (:stream_detail ad)
                        replica (-> sd :cluster :replicas)]
                       (do ;(info :sd (with-out-str (pprint sd)))
                           (recur
                             (-> nodes
                                 ; Ugh, they put leaders in a
                                 ; whole diff structure
                                 (conj (:leader (:cluster sd)))
                                 (conj (:name replica)))))))))

(defrecord MemberState
  [node-views
   view
   pending]

  m/State
  (setup! [this test]
    (info "Setup membership")
    this)

  (node-view [this test node]
    (-> (c/on-nodes
          test [node]
          (fn [_ _]
            (try+
              (let [js (db/jetstream)]
                ; Guessing that these are monotone? We're not doing clock skew
                ; yet so it's probably fine.
                (when (:now (:data js))
                  {:time (:now (:data js))
                   :nodes (nodes js)}))
              (catch [:type :jepsen.control/nonzero-exit] e
                nil))))
        first
        val))

  (merge-views [this test]
    ; Since we're asking the Raft leader, we'll use the latest by timestamp
    (:nodes (last (sort-by :time (vals node-views)))))

  (fs [this]
    #{:join :leave})

  (op [this test]
    ; One thing at a time; I'm not at all confident in doing multiple
    ; membership ops concurrently
    (if (seq pending)
      :pending
      ; Always leave at least 3 nodes. No idea how to tell what NATS needs as a
      ; minimum
      (if (< 3 (count view))
        ; We can remove a node
        {:type :info, :f :leave, :value (rand-nth (vec view))}
        :pending)))

  (invoke! [this test op]
    (case (:f op)
      :leave
      (let [leaver (:value op)
            v (c/on-nodes test [leaver] db/wipe!)
            v (c/on-nodes test [(rand-nth (vec (disj view leaver)))]
                                 (fn [_ _]
                                   (try+ (db/leave! test leaver)
                                         (catch [:type :jepsen.control/nonzero-exit] e
                                           (:err e)))))]
               (assoc op :value [(:value op) (val (first v))]))))

  (resolve [this test]
    this)

  (resolve-op [this test [op op']]
    (info "Try to resolve" op "(view is" view ")")
    (case (:f op)
      :leave
      (let [node (:value op)]
        ; Gone from the view?
        (if (not (contains? view node))
          this
          nil ; Waiting
          ))))

  (teardown! [this test]))

(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        packages
        (->> (concat
               ; Standard packages
               (nc/nemesis-packages opts)
               ; Custom packages
               [(m/package
                  (assoc opts
                         :membership {:state (map->MemberState
                                               {:node-views {}
                                                :view (sorted-set)
                                                :pending #{}})
                                      :log-resolve-op? true
                                      :log-resolve? true
                                      :log-node-views? true
                                      :log-view? true}))
                ]))
        nsp (:stable-period opts)]
    ;(info :packages (map (comp n/fs :nemesis) packages))
    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
