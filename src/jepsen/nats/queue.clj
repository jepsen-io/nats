(ns jepsen.nats.queue
  "A simple queue workload"
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :refer [meh linear-time-nanos secs->nanos]]]
            [jepsen.nats [client :as c]]
            [tesser.core :as t]))

(def stream-name "jepsen-stream")
(def subjects ["jepsen.*"])

(defn k->subject
  "Turns an integer key into a string subject."
  [k]
  (str "jepsen." k))

(defn subject->k
  "Turns a string subject into an integer key."
  [subject]
  (let [[m k] (re-find #"^jepsen\.(\d+)$" subject)]
    (assert m (str "unable to find key in subject " (pr-str subject)))
    (parse-long k)))

(defn sub!
  "Takes a client, and tries to get its subscription, creating it if not
  already present. Returns subscription, or nil."
  [client]
  (let [subscription (:subscription client)]
    (when-not (realized? subscription)
      (when-let [sub (try (c/subscribe! (:client client) (k->subject 0))
                          (catch java.lang.IllegalStateException e
                            (info "Can't subscribe," (.getMessage e)))
                          (catch java.io.IOException e
                            (info "Can't subscribe," (.getMessage e))))]
        (deliver subscription sub)))
    (when (realized? subscription)
      @subscription)))

(defrecord Client [client
                   ; Promise; delivered on first use
                   subscription]
  client/Client
  (open! [this test node]
    (let [client (c/open test node)]
      (assoc this
             :client       (c/open test node)
             :subscription (promise))))

  (setup! [this test]
    (c/with-retry
      (c/create-stream! client
                        {:name      stream-name
                         :subjects  subjects
                         :replicas  (max 3
                                         (count (:nodes test)))})))

  (invoke! [this test {:keys [f value] :as op}]
    (c/with-errors op
      (case f
        ; Synthetic process crash
        :crash (assoc op :type :info, :error :crash)

        :publish
        (let [r (c/publish! client (k->subject 0) value)]
          (assoc op :type :ok))

        :next-message
        (if-let [sub (sub! this)]
          (if-let [m (c/next-message sub 1000)]
            (do (c/ack! m)
                (assoc op :type :ok, :value (c/data m)))
            (assoc op :type :fail, :error :caught-up))
          (do (Thread/sleep (rand-int 2000))
              (assoc op :type :info, :error :not-subscribed)))

        :fetch
        (if-let [sub (sub! this)]
          (let [batch (c/fetch sub)]
            (if (empty? batch)
              (assoc op :type :fail, :error :caught-up)
              (do ; Ack all
                  (mapv c/ack! batch)
                  (assoc op :type :ok, :value (mapv c/data batch)))))
          (do (Thread/sleep (rand-int 2000))
              (assoc op :type :info, :error :not-subscribed))))))

  (teardown! [this test])

  (close! [this test]
    (when (realized? subscription)
      (meh (c/unsubscribe! @subscription)))
    (c/close! client))

  client/Reusable
  (reusable? [client test]
    ; Trying to re-use clients as aggressively as possible--the client appears
    ; to leak threads like crazy and we'll OOM or blow out the thread limits
    ; within a few hundred seconds if we close and re-open
    true))

(defn stop-when-caught-up
  "Wraps a generator, ending it when an operation returns :error :caught-up"
  [gen]
  (gen/on-update
    (fn update [this test ctx op]
      (if (= :caught-up (:error op))
        nil ; Done
        (-> gen
            (gen/update test ctx op)
            stop-when-caught-up)))
    gen))

(defrecord StopWhenSubFails [deadline gen]
  gen/Generator
  (op [this test ctx]
    (if-not deadline
      ; Lazy init deadline
      (let [limit    (:final-time-limit test)
            deadline (+ (:time ctx) (secs->nanos limit))]
        (gen/op (assoc this :deadline deadline) test ctx))
      ; Pass through
      (when-let [[op gen'] (gen/op gen test ctx)]
        [op (assoc this :gen gen')])))

  (update [this test ctx op]
    (if (and deadline
             (< deadline (:time ctx))
             (= :not-subscribed (:error op)))
      ; Exhausted
      (gen/log (str "Giving up on drain; still don't have a subscription after "
                    (:final-time-limit test) " seconds"))
      (assoc this :gen (gen/update gen test ctx op)))))

(defn stop-when-sub-fails
  "When subscriptions fail for more than the final time limit, stop trying."
  [gen]
  (StopWhenSubFails. nil gen))

(defn drain
  "Emits dequeue opts until caught up on every thread."
  [opts]
  (gen/each-thread
    (->> (gen/repeat {:f :fetch})
         stop-when-caught-up
         stop-when-sub-fails)))

(defn generator
  [opts]
  (let [writes (->> (range)
                    (map (fn [x] {:f :publish, :value x})))
        reads (repeat (:concurrency opts)
                       (gen/repeat {:f :fetch}))]
  ;(gen/mix (vec (cons writes reads)))
  writes
  ))

(deftype Checker []
  checker/Checker
  (check [this test history opts]
    (let [{:keys [attempts published next-messaged fetched]}
          (->> (t/fuse
                 {:attempts (->> (t/filter (h/has-f? :publish))
                                 (t/filter h/invoke?)
                                 (t/map :value)
                                 (t/set))
                  :published (->> (t/filter (h/has-f? :publish))
                                  (t/filter h/ok?)
                                  (t/map :value)
                                  (t/set))
                  :next-messaged (->> (t/filter (h/has-f? :next-message))
                                      (t/map :value)
                                      (t/set))
                  :fetched (->> (t/filter (h/has-f? :fetch))
                                (t/filter h/ok?)
                                (t/mapcat :value)
                                (t/set))})
               (h/tesser history))

          ; Everything we read via either next-message or fetch
          read       (set/union next-messaged fetched)

          ; The OK set is every attempt that was read
          ok         (set/intersection attempts read)

          ; Unexpected records are those we never published. Maybe
          ; leftovers from some earlier state. Definitely don't want your
          ; queue emitting records from nowhere!
          unexpected (set/difference read attempts)

          ; Lost records are ones which we definitely enqueued but never
          ; came out.
          lost       (set/difference published read)

          ; Recovered records are dequeues where we didn't know if the enqueue
          ; suceeded or not, but an attempt took place.
          recovered  (set/difference ok published)]

      {:valid?             (and (empty? lost) (empty? unexpected))
       :attempt-count      (count attempts)
       :acknowledged-count (count published)
       :read-count         (count read)
       :ok-count           (count ok)
       :unexpected-count   (count unexpected)
       :lost-count         (count lost)
       :recovered-count    (count recovered)
       :lost               lost
       :unexpected         unexpected})))

(defn workload
  "Takes CLI options, returns a workload with a client and generator."
  [opts]
  {:generator       (generator opts)
   :final-generator (drain opts)
   :client          (Client. nil nil)
   :checker         (Checker.)})
