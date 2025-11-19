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

(defn lost-breakout
  "Takes a set of lost values, and a set of read ones. Partitions lost values
  into three sets, based on the order in which the process wrote them. For each
  writing process, these sets are:

    :lost-prefix    All lost values before the first ok read
    :lost-middle    Those after the first ok read and before the last ok read
    :lost-postfix   Those after the last ok read (or all values, if
                    nothing read)

  Lost-prefix hints that NATS 'started over', throwing away everything before
  some time. Lost-postfix might (might!) be a consequence of a slow reader.
  Lost-middle is especially bad: we read things before and after that value, so
  we *definitely* should have read the value in the middle, per NATS'
  Serializability claims."
  [lost read]
  ; First, we need to find the high and low watermarks for each writer process
  (loopr [; Maps of process->counter
          first-reads (transient {})
          last-reads  (transient {})]
         [pair read]
         (let [process (pair->process pair)
               counter (pair->counter pair)]
           (recur
             (if (< counter (first-reads process Long/MAX_VALUE))
               (assoc! first-reads process counter)
               first-reads)
             (if (< (last-reads process Long/MIN_VALUE) counter)
               (assoc! last-reads process counter)
               last-reads)))
         ; Partition
         (loopr [prefix  (transient #{})
                 middle  (transient #{})
                 postfix (transient #{})]
                [pair lost]
                (let [process (pair->process pair)
                      counter (pair->counter pair)]
                  (cond
                    ; Before first read
                    (< counter (first-reads process Long/MIN_VALUE))
                    (recur (conj! prefix pair) middle postfix)

                    ; Before last read
                    (< counter (last-reads process Long/MIN_VALUE))
                    (recur prefix (conj! middle pair) postfix)

                    ; After last read
                    true
                    (recur prefix middle (conj! postfix pair))))
                {:lost        lost
                 :lost-prefix (persistent! prefix)
                 :lost-middle (persistent! middle)
                 :lost-postfix (persistent! postfix)})))

(defn lost-plot!
  "Renders an op color plot to show values which were lost during a test.
  Returns a Jepsen history task. Takes read, lost-prefix, lost-middle, and
  lost-postfix sets, in addition to the usual options for op-color-plot!."
  [test history {:keys [read lost-prefix lost-middle lost-postfix] :as opts}]
  (assert (set? read))
  (assert (set? lost-prefix))
  (assert (set? lost-middle))
  (assert (set? lost-postfix))
  (h/task history op-color-plot []
          (plot/op-color-plot!
            test history
            (assoc opts
                   :groups {:ok           {:color "#81BFFC"}
                            :lost-prefix  {:color "#8C0348"}
                            :lost-middle  {:color "#FF1E90"}
                            :lost-postfix {:color "#FF74BA"}
                            :unknown      {:color "#FFA400"}}
                   :group-fn (fn group [op]
                               (when (identical? :publish (:f op))
                                 (let [v (:value op)]
                                   (cond
                                     (read v)         :ok
                                     (lost-prefix v)  :lost-prefix
                                     (lost-middle v)  :lost-middle
                                     (lost-postfix v) :lost-postfix
                                     true             :unknown))))))))

(defrecord Checker []
  checker/Checker
  (check [this test history opts]
    (let [nodes    (:nodes test)
          n        (count nodes)
          op->node (fn op->node [op]
                     (nth nodes (mod (:process op) n)))
          {:keys [attempts published next-messaged fetched
                  fetched-by-node]}
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
                                (t/set))
                  ; Note: for now I'm *just* doing the per-node divergence
                  ; checks based on fetch, not next-message. This is because
                  ; I'm specifically worried about the final reads diverging.
                  ; Later it might be good to break this out in more detail.
                  :fetched-by-node (->> (t/filter h/ok?)
                                        (t/filter (h/has-f? :fetch))
                                        (t/group-by op->node)
                                        (t/mapcat :value)
                                        (t/set))})
                  (h/tesser history))
          ; Complete fetched-by-node if any nodes were missing
          fetched-by-node (->> fetched-by-node
                               (merge (zipmap (:nodes test) (repeat #{})))
                               (into (sorted-map)))

          ; Everything we read via either next-message or fetch
          read       (set/union next-messaged fetched)

          ; The OK set is every attempt that was read
          ok         (set/intersection attempts read)

          ; Unexpected records are those we never published. Maybe
          ; leftovers from some earlier state. Definitely don't want your
          ; queue emitting records from nowhere!
          unexpected (set/difference read attempts)

          ; Recovered records are dequeues where we didn't know if the enqueue
          ; suceeded or not, but an attempt took place.
          recovered  (set/difference ok published)

          ; Lost records are ones which we definitely enqueued but never
          ; came out anywhere.
          lost       (set/difference published read)

          ; Unknown records were attempts which were not ok or lost.
          unknown    (set/difference attempts ok lost)

          ; Which we break out into three types
          lost       (lost-breakout lost read)

          ; We can also lose a record on just one node.
          lost-on-node (update-vals fetched-by-node
                                    (partial set/difference published))
          ; Which, again, we break out
          lost-on-node (into (sorted-map)
                             (map (fn [[node lost]]
                                    [node
                                     (lost-breakout lost
                                                    (fetched-by-node node))])
                                  lost-on-node))

          ; Render plots
          plot (lost-plot! test history
                           (merge opts
                                  lost
                                  {:title    (str (:name test) " lost")
                                   :filename "lost.png"
                                   :read      read}))
          node-plots
          (mapv (fn [node]
                  (lost-plot! test history
                              (merge opts
                                     (lost-on-node node)
                                     {:title (str (:name test) " lost " node)
                                      :filename (str "lost " node ".png")
                                      :read (fetched-by-node node)})))
                (keys lost-on-node))

          ; Summarize a lost map
          short-lost (fn [lost]
                       (reduce (fn [m k]
                                 (assoc m k (into (sorted-set)
                                                  (take 32 (lost k)))

                                        (keyword (str (name k) "-count"))
                                        (count (lost k))))
                               (sorted-map)
                               (keys lost)))]
      ; Wait for plots
      @plot
      (mapv deref node-plots)

      {:valid?             (and (empty? (:lost lost))
                                (empty? unexpected)
                                (every? empty? (map :lost (vals lost-on-node))))
       :attempt-count      (count attempts)
       :acknowledged-count (count published)
       :read-count         (count read)
       :ok-count           (count ok)
       :unexpected-count   (count unexpected)
       :recovered-count    (count recovered)
       :lost               (short-lost lost)
       :lost-on-node       (update-vals lost-on-node short-lost)
       :unexpected         (into (sorted-set) (take 32 unexpected))})))

(defn workload
  "Takes CLI options, returns a workload with a client and generator."
  [opts]
  {:generator       (generator opts)
   :final-generator (drain opts)
   :wrap-generator  track-writes
   :client          (Client. nil nil)
   :checker         (a/checker (Checker.))})
