(ns jepsen.nats.db
  "Database automation"
  (:require [clojure [string :as str]]
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

(defn configure!
  "Writes out the config file for a node."
  [test node]
  (-> (io/resource "nats.conf")
      slurp
      (str/replace #"%NAME" node)
      (str/replace #"%ROUTES" (routes test node))
      (str/replace #"%SYNC_INTERVAL" (if-let [s (:sync-interval test)]
                                       (str "sync_interval: " s)
                                       ""))
      (cu/write-file! config-file)))

(defrecord DB [peer-ids lazyfs]
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
    (cu/await-tcp-port client/port)
    )

  (teardown! [this test node]
    (db/teardown! lazyfs test node)
    (c/su (cu/grepkill! :kill bin-name)
          (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    {config-file "nats.conf"
     log-file    "nats.log"})

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
  (map->DB {:lazyfs (lazyfs/db {:dir data-dir})}))
