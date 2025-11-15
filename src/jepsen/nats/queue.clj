(ns jepsen.nats.queue
  "A simple queue workload"
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [antithesis :as a]
                    [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :refer [meh linear-time-nanos
                                  nanos->secs secs->nanos]]]
            [jepsen.checker [perf :as perf]
                            [plot :as plot]]
            [jepsen.nats [client :as c]]
            [slingshot.slingshot :refer [try+ throw+]]
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

(defn pair->process
  "Takes a 'process.counter' pair and returns the process."
  [pair]
  (parse-long (re-find #"^\d+" pair)))

(defn pair->counter
  "Takes a 'process.counter' pair and returns the counter."
  [pair]
  (parse-long (re-find #"\d+$" pair)))

(defn pair
  "Constructs a pair string from a process and counter"
  [process counter]
  (str process "-" counter))

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
                            (info "Can't subscribe," (.getMessage e)))
                          (catch io.nats.client.JetStreamApiException e
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
      (try
        (assoc this
               :client       (c/open test node)
               :subscription (promise))
        (catch java.io.IOException e
          ; Don't spam connections
          (Thread/sleep 1000)
          (throw e)))))

  (setup! [this test]
    (c/with-retry
      (c/create-stream! client
                        {:name      stream-name
                         :subjects  subjects
                         :replicas  (max 3
                                         (count (:nodes test)))})))

  (invoke! [this test {:keys [f process value] :as op}]
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
            (when (not (seq batch))
              ; Empty; let's not spam requests constantly
              (Thread/sleep 1000))
            ; Ack all
            (mapv c/ack! batch)
            (assoc op :type :ok, :value (mapv c/data batch)))
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

(defrecord TrackCounters [; The key we assign in the context
                          context-key
                          ; The set of :f's we care about
                          fs
                          ; A map of process to highest counter
                          counters
                          gen]
  gen/Generator
  (op [this test ctx]
    (let [ctx' (assoc ctx context-key counters)]
      (when-let [[op gen'] (gen/op gen test ctx')]
        [op (TrackCounters. context-key fs counters gen')])))

  (update [this test ctx {:keys [f type value] :as event}]
    (if (and (identical? type :ok)
             (contains? fs f))
      (let [pairs   (case f
                      :publish      [value]
                      :next-message [value]
                      :fetch        value)
            counters' (reduce (fn [counters pair]
                                (let [process (pair->process pair)
                                      counter (max (pair->counter pair)
                                                   (get counters process 0))]
                                  (assoc counters process counter)))
                              counters
                              pairs)
            ctx'      (assoc ctx context-key counters')
            gen'      (gen/update gen test ctx' event)]
        (TrackCounters. context-key fs counters' gen'))
      ; Not interested, pass through
      (let [ctx' (assoc ctx context-key counters)
            gen' (gen/update gen test ctx' event)]
        (TrackCounters. context-key fs counters gen')))))

(defn track-writes
  "Wraps a generator, tracking the counters written via :publish"
  [gen]
  (TrackCounters. :writes #{:publish} {} gen))

(defn track-reads
  "Wraps a generator, tracking the counters read via :next-message or :fetch"
  [gen]
  (TrackCounters. :reads #{:fetch :next-message} {} gen))

(defn caught-up?
  "Takes a generator context map, and returns true if the reads of that context
  are at least as high, for each process, as the writes."
  [{:keys [reads writes] :as ctx}]
  (every? (fn [[process counter]]
            (<= counter (get reads process -1)))
          writes))

(defn until-caught-up
  "Wraps a generator, ending it when the generator has read, for each process,
  a record at least as high as the highest record we know that process wrote."
  [gen]
  (gen/on-update
    (fn update [this test ctx op]
      (if (caught-up? ctx)
        (gen/log "Caught up")
        ; Not done; propagate
        (-> gen
            (gen/update test ctx op)
            until-caught-up)))
    gen))

(defrecord UntilSubFails [deadline gen]
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

(defn until-sub-fails
  "When subscriptions fail for more than the final time limit, stop trying."
  [gen]
  (UntilSubFails. nil gen))

(defn drain
  "Emits dequeue opts until caught up on every thread."
  [opts]
  (gen/each-thread
    (->> (gen/repeat {:f :fetch})
         until-caught-up
         until-sub-fails
         track-reads)))

; Our write generator emits process.some-int, where some-int is 0,1,2,... for
; that process. This lets us do high-watermark tracking.
(defrecord Writes [; A map of process -> next counter to emit
                   next-counters]
  gen/Generator
  (op [this test ctx]
    (if-let [p (gen/some-free-process ctx)]
      (let [counter (get next-counters p 0)
            ncs'    (assoc next-counters p (inc counter))]
        [(h/->Op -1
                (:time ctx)
                :invoke
                p
                :publish
                (pair p counter))
         (Writes. ncs')])
      [:pending this]))

  (update [this test ctx event]
    this))

(defn generator
  [opts]
  (let [writes (Writes. {})
        reads (repeat (:concurrency opts)
                       (gen/repeat {:f :fetch}))]
  ;(gen/mix (vec (cons writes reads)))
  writes
  ))

(defrecord Checker []
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
          recovered  (set/difference ok published)

          ; Unknown records were attempts which were not ok or lost.
          unknown    (set/difference attempts ok lost)

          ; What's the highest read counter for each process?
          highest-reads (loopr [hrs (transient {})]
                               [pair read]
                               (recur
                                 (let [process (pair->process pair)
                                       counter (pair->counter pair)]
                                   ; Taking advantage of the fact that "" is
                                   ; less than any string
                                   (if (< (get hrs process -1) counter)
                                     (assoc! hrs process counter)
                                     hrs)))
                               (persistent! hrs))

          ; Did we lose anything *below* the highest reads?
          holes (->> lost
                     (filter (fn [pair]
                               (let [highest (get highest-reads
                                                  (pair->process pair))]
                                 (and highest
                                      (< (pair->counter pair) highest)))))
                     set)]
      (plot/op-color-plot!
        test history
        (assoc opts
               :filename "set.png"
               :title    (str (:name test) " set")
               :groups {:ok         {:color "#81BFFC"}
                        :hole       {:color "#D41EFF"}
                        :lost       {:color "#FF1E90"}
                        :unexpected {:color "#00FFA4"}
                        :unknown    {:color "#FFA400"}}
               :group-fn (fn group [op]
                           (when (identical? :publish (:f op))
                             (let [v (:value op)]
                               (cond
                                 (unexpected v) :unexpected
                                 (holes v)      :hole
                                 (lost v)       :lost
                                 (ok v)         :ok
                                 true           :unknown))))))

      {:valid?             (and (empty? lost) (empty? unexpected))
       :attempt-count      (count attempts)
       :acknowledged-count (count published)
       :read-count         (count read)
       :ok-count           (count ok)
       :unexpected-count   (count unexpected)
       :lost-count         (count lost)
       :hole-count         (count holes)
       :recovered-count    (count recovered)
       :lost               (into (sorted-set) (take 32 lost))
       :unexpected         (into (sorted-set) (take 32 unexpected))
       :holes              (into (sorted-set) (take 32 holes))})))

(defn workload
  "Takes CLI options, returns a workload with a client and generator."
  [opts]
  {:generator       (generator opts)
   :final-generator (drain opts)
   :wrap-generator  track-writes
   :client          (Client. nil nil)
   :checker         (a/checker (Checker.))})
