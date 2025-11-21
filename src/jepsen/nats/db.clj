(ns jepsen.nats.db
  "Database automation"
  (:require [cheshire.core :as json]
            [clojure [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c :refer [|]]
                    [core :as jepsen]
                    [db :as db]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [await-fn meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.nats [client :as client]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir
  "The top-level directory."
  "/opt/nats")

(def bin-name
  "The name of the server binary."
  "nats-server")

(def bin
  "Path to the server binary."
  (str dir "/" bin-name))

(def config-file
  "Path to the NATS config file"
  (str dir "/nats.conf"))

(def log-file
  "Where we direct logs from stdout/stderr"
  (str dir "/nats.log"))

(def pid-file
  "Where we write the process' pidfile"
  (str dir "/nats.pid"))

(def data-dir
  "NATS data directory"
  (str dir "/data"))

(defn install!
  "Install nats"
  [test]
  (c/su
    ; Server
    (let [url (str "https://binaries.nats.dev/nats-io/nats-server/v2@v"
                   (:version test))
          path (cu/cached-wget! url)]
      (c/exec :mkdir :-p dir)
      (c/exec :cp path bin)
      (c/exec :chmod :+x bin))
    ; Client
    (let [version "0.2.2"
          url (str "https://github.com/nats-io/natscli/releases/download/v"
                   version "/nats-" version "-amd64.deb")
          path (cu/cached-wget! url)]
      (c/exec :dpkg :-i path))))

(defn routes
  "The routes config snippet for the given node in the given test."
  [test node]
  (->> (:nodes test)
       (remove #{node})
       (map (fn [node]
              ; Bit of a hack: file might be indentation-sensitive, but we know
              ; exactly where this will be used
              (str "nats://" node ":6222")))
       (str/join "\n    ")))

(defn init-node-names
  "A map of nodes to their initial names."
  [nodes]
  (->> nodes
       (map (fn [node] [node (str node "-0")]))
       (into {})))

(defn node->name
  "NATS requires that we assign a unique node name to each node when we wipe
  it. We store this mapping in a test atom :node-names."
  [test node]
  (get @(:node-names test) node))

(defn name->node
  "Looks up a node from a name. Node names are always of the form `n1-4`, so we
  simply strip off the -4."
  [name]
  (second (re-find #"^(.+)-\d+$" name)))

(defn bump-node-name!
  "Increments the node name for the given node, mutating the test's node-names
  map."
  [test node]
  (let [r (swap! (:node-names test)
                 (fn bump [node-names]
                   (let [counter (->> (node-names node)
                                      (re-find #"-(\d+)$")
                                      second
                                      parse-long)]
                     (assoc node-names node (str node "-" (inc counter))))))]
    (info "Node" node "now has name" (r node))))

(defn configure!
  "Writes out the config file for a node."
  [test node]
  (-> (io/resource "nats.conf")
      slurp
      (str/replace #"%NAME" (node->name test node))
      (str/replace #"%ROUTES" (routes test node))
      (str/replace #"%SYNC_INTERVAL" (if-let [s (:sync-interval test)]
                                       (str "sync_interval: " s)
                                       ""))
      (cu/write-file! config-file)))

(defn nats*!
  "Runs a NATS CLI command without a user. NATS is weird: you can't administer
  Jetstream using an admin user, because admin users can't have Jetstream
  enabled. But you can't administer anything else using a regular user. So you
  need two admin users? :-/"
  [& args]
  (apply c/exec :nats :-s c/*host* :--password "jepsenpw" :--timeout "100ms" args))

(defn nats!
  "Runs a NATS CLI command."
  [& args]
  (apply nats*! :--user "sys" args))

(defn jetstream
  "Requests Jetstream info from the current node, asking for the leader's view"
  []
  (-> (nats! :server :request :jetstream :--leader :--streams)
      (json/parse-string true)))

(defn jetstream-health
  "Requests Jetstream health info from the current node."
  []
  (-> (nats! :server :request :jetstream-health)
      (str/split #"\n")
      (->> (mapv (fn [line]
                   (json/parse-string line true))))))

(defn leave!
  "Tells NATS that the given node is gone."
  [test node]
  ; Zero clue how to do this safely. Probably involves parsing a zillion admin
  ; commands.
  (info "Telling" c/*host* "that" (node->name test node) "is gone")
  (swap! (:living (:db test)) disj node)
  (nats! :server :raft :peer-remove :-f :-j (node->name test node))
  :removed)

(defn wipe!
  "Kills and wipes data on a node."
  [test node]
  (db/kill! (:db test) test node)
  (c/su (c/exec :rm :-rf data-dir))
  :wiped)

(defn reconfigure!
      "Reconfigures the node after peer-removal. Bumps the node name and rewrites the config file."
      [test node]
      (bump-node-name! test node)
      (c/with-node test node
                   (configure! test node)))

(defn join!
  "Joins the currently-bound node to the cluster."
  [test node]
  (swap! (:living (:db test)) conj node)
  (db/start! (:db test) test node))

(defrecord DB [peer-ids lazyfs living]
  db/DB
  (setup! [this test node]
    (install! test)
    (configure! test node)
    (when (:lazyfs test)
      (db/setup! lazyfs test node))
    (c/su
      ; On first run, I think the binary... even though you downloaded it... it
      ; downloads and replaces itself with something else?
      (c/cd dir (c/exec bin)))

    (db/start! this test node)
    (cu/await-tcp-port client/port
                       {:log-interval 10000}))

  (teardown! [this test node]
    ; Always tear down, in case we have a previous, crashed state left over
    (db/teardown! lazyfs test node)
    (c/su (cu/grepkill! :kill bin-name)
          (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    (c/su
      {config-file              "nats.conf"
       log-file                 "nats.log"
       (cu/tarball! data-dir)   "data.tar.gz"}))

  db/Primary
  (primaries [_ test])
  (setup-primary! [_ test node])

  db/Pause
  (pause! [_ test node]
    (c/su (cu/grepkill! :stop bin-name))
    :paused)

  (resume! [_ test node]
    (c/su (cu/grepkill! :cont bin-name))
    :resumed)

  db/Kill
  (kill! [_ test node]
    (info "Killing nats")
    (c/su (cu/grepkill! :kill bin-name))
    (when (:lazyfs test)
      (lazyfs/lose-unfsynced-writes! lazyfs))
    :killed)

  (start! [_ test node]
    (c/su
      (cu/start-daemon!
        {:logfile log-file
         :pidfile pid-file
         :chdir   dir}
         bin
         :-config config-file))))

(defn db
  "Constructs a new DB for the test, given CLI opts."
  [opts]
  (map->DB {:living (atom #{})
            :lazyfs (lazyfs/db {:dir data-dir})}))
