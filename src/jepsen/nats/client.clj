(ns jepsen.nats.client
  "Wrapper for the NATS Java client."
  (:require [dom-top.core :as dt]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.codec :as codec]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (io.nats.client Connection
                           JetStream
                           JetStreamApiException
                           JetStreamManagement
                           JetStreamSubscription
                           Message
                           Nats
                           Options
                           PullSubscribeOptions
                           Subscription)
           (io.nats.client.impl NatsConnection)
           (io.nats.client.api DiscardPolicy
                               PurgeResponse
                               StorageType
                               StreamConfiguration
                               StreamInfo)
           (java.time Duration)))

(def port
  "Client port for NATS"
  4222)

(defrecord Client [^Connection conn
                   ^JetStream  js])

(defn init!
  "NATS connection code all goes through
  https://github.com/nats-io/nats.java/blob/c42c086230b0b6e42b0917977e7b27383e115f67/src/main/java/io/nats/client/impl/NatsImpl.java#L29,
  which calls connect--and if it explodes, doesn't close the client. I'm seeing
  massive thread leaks in tests, and I suspect it's because each new client
  creates threads it doesn't clean up. To work around this, we're hacking our
  way in to the private constructor of impl.NatsConnection and calling it
  directly, but in a way that lets us close the conn if it fails."
  []
  ; TODO, hmm maybe I can get away with using reconnectOnConnect?
  )

(defn open
  "Opens a connection to the given node. Our clients are a map of :conn (the
  connection itself) and :js (a JetStream context)."
  [test node]
  ; I suspect Nats/connect may leak threads, so let's be extra careful here
  (let [opts (.. (Options/builder)
                 (server (str "nats://" node ":" port))
                 (userInfo "jepsen" "jepsenpw")
                 (ignoreDiscoveredServers) ; Stick to the server we say please
                 (pingInterval (Duration/ofMillis 5000))
                 (connectionTimeout 5000)
                 (socketWriteTimeout 10000)
                 (socketReadTimeoutMillis 10000)
                 ; I'm hoping that letting NATS do infinite reconnects will
                 ; save us from the thread leak issue
                 (maxReconnects -1)
                 (reconnectWait (Duration/ofSeconds 1))
                 (build))
        conn (Nats/connect opts)]
    (try
      ;(.connect conn false)
      (Client. conn
               (.jetStream conn))
      (catch Throwable t
        (.close conn)
        (throw t)))))

(defn close!
  "Closes a client."
  [^Client client]
  (.close ^Connection (.conn client)))

(defn ^JetStreamManagement jsm
  "Returns a JetStreamManagement object on a client."
  [^Client client]
  (.jetStreamManagement ^Connection (.conn client)))

(defn create-stream!
  "Makes a new stream, given a client and an options map. Options are:

    :name       The name of the stream
    :subjects   A collection of subject strings
    :replicas   The number of replicas (default 3)"
  [client opts]
  (let [stream-config (.. (StreamConfiguration/builder)
                          (name          (:name opts))
                          (storageType   StorageType/File)
                          (subjects      ^java.util.Collection (:subjects opts))
                          (replicas      (:replicas opts 3))
                          (discardPolicy DiscardPolicy/New)
                          (build))]
    (.addStream (jsm client) stream-config)))

(defn publish!
  "Sends a message to a subject."
  [^Client client ^String subject message]
  (.publish ^JetStream (.js client) subject ^bytes (codec/encode message)))

(defn subscribe!
  "Opens a subscription on the given client for the given subject."
  [^Client client subject]
  ; (info "Subscribe to" subject)
  (let [opts (.. (PullSubscribeOptions/builder)
                 (build))]
    (.subscribe ^JetStream (.js client)
                subject
                opts)))

(defn unsubscribe!
  "Closes a subscription."
  [^Subscription subscription]
  ; (info "Unsubscribe from" subscription)
  (.unsubscribe subscription))

(defn fetch
  "Fetches up to n messages from the given subscription, waiting up to n
  millis."
  ([sub]
   (fetch sub 128))
  ([sub batch-size]
   (fetch sub batch-size 10))
  ([^JetStreamSubscription subscription, ^long batch-size, ^long timeout]
   (.fetch subscription batch-size timeout)))

(defn next-message
  "Returns the next message from the given subscription, waiting up to n
  millis. Returns nil for timeout."
  ([sub]
   (next-message sub 0))
  ([^JetStreamSubscription subscription, ^long timeout]
   (.nextMessage subscription timeout)))

(defn ack!
  "Acknowledges a Message."
  [^Message msg]
  (.ack msg))

(defn data
  "Gets the data from a Message."
  [^Message msg]
  (codec/decode (.getData msg)))

(defn stream-names
  "Retursn a list of stream names"
  [client]
  (vec (.getStreamNames (jsm client))))

(defn stream-info
  "Returns information about a stream."
  [client stream-name]
  (.getStreamInfo (jsm client) stream-name))

(defmacro with-retry
  "Evaluates body repeatedly, retrying IOExceptions a few times."
  [& body]
  `(dt/with-retry [tries# 10]
     ~@body
     (catch JetStreamApiException e#
       (if (pos? tries#)
         (do (info (.getMessage e#) "- retrying")
             (Thread/sleep 1000)
             (~'retry (dec tries#)))
         (throw e#)))
     (catch java.io.IOException e#
       (if (pos? tries#)
         (do (info (.getMessage e#) "- retrying")
             (Thread/sleep 1000)
             (~'retry (dec tries#)))
         (throw e#)))))

(defmacro with-errors
  "Takes an operation and a body. Evals body, converting common errors to :type
  :fail or :type :info."
  [op & body]
  `(try ~@body
        (catch java.lang.IllegalStateException e#
          (Thread/sleep (rand-int 2000))
          (condp re-find (.getMessage e#)
            #"Connection is Closed"
            (assoc ~op :type :fail, :error [:conn-closed])

            (throw e#)))

        (catch java.io.IOException e#
          (Thread/sleep (rand-int 2000))
          (condp re-find (.getMessage e#)
            #"503 No Responders Available For Request"
            (assoc ~op :type :fail, :error [:no-responders])

            #"Timeout or no response waiting for NATS"
            (assoc ~op :type :info, :error [:timeout])

            (throw e#)))

        (catch JetStreamApiException e#
          (Thread/sleep (rand-int 2000))
          (condp re-find (.getMessage e#)
            #"(?i)jetstream system temporarily unavailable"
            (assoc ~op :type :fail, :error [:jetstream-temporarily-unavailable])

            (throw e#)))
        ))
