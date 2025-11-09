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
            [jepsen.checker [perf :as perf]]
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

(defn viz-points
  "Takes a history and sets of unexpected and lost records. Returns a map of
  three series of datapoints: :ok, :lost, and :unexpected"
  [test history lost unexpected]
  (let [t0 (:time (first history))
        t1 (:time (peek history))
        dt (- t1 t0)
        ; Size of the time windows. This is sort of arbitrary; we're just
        ; saying the graph is divided into 100 vertical slices.
        window-count 512
        window-size (/ dt window-count)
        ; A vector of windows, where each window is a map of {:ok, :lost, and
        ; :unexpected}, each of those being a vector of times. We'll derive the
        ; y coordinates once the windows are built.
        empty-window {:ok [], :lost [], :unexpected []}
        empty-windows (vec (repeat window-count empty-window))
        windows
        (->> ;(t/take 10)
             (t/filter (h/has-f? :publish))
             (t/filter h/invoke?)
             (t/fold
               {:identity (constantly empty-windows)
                :reducer  (fn reducer [windows op]
                            (let [window (-> (:time op)
                                             (- t0)
                                             (/ dt)
                                             (* window-count)
                                             Math/floor
                                             long)
                                  value (:value op)
                                  type (cond (lost value)       :lost
                                             (unexpected value) :unexpected
                                             true               :ok)]
                              (update windows window
                                      update type
                                      conj (nanos->secs (:time op)))))
                :combiner (fn combiner [windows1 windows2]
                            (mapv (fn merge [win1 win2]
                                    (merge-with into win1 win2))
                                  windows1
                                  windows2))})
             (h/tesser history))
        ; How many points in the biggest window?
        max-window-count (->> windows
                              (map (fn c [window]
                                     (reduce + (map count (vals window)))))
                              (reduce max 0))
        ; How many Hz is the biggest window?
        max-window-hz (/ max-window-count (nanos->secs window-size))]
    ; Now we'll unfurl each window into [x y] points, where the y coordinates
    ; are scaled relative to the window with the most points. This a.) spreads
    ; out points, and b.) means the y axis of the graph gives a feeling for
    ; overall throughput over time.
    (loopr [series empty-window]
           [window windows]
           ; With this window, give each point an increasing counter, carrying
           ; that counter i across all three types.
           (recur
             (loopr [i      0
                     series series]
                    [[type times] window
                     t            times]
                    (let [y (float (* max-window-hz (/ i max-window-count)))]
                      (recur (inc i)
                             (update series type conj [t y])))
                    series)))))

(defn viz!
  "Writes out a graph for checker results, showing which records were preserved
  and lost over time."
  [test history {:keys [subdirectory nemeses]} lost unexpected]
  (let [nemeses  (or nemeses (:nemeses (:plot test)))
        _ (prn :nemeses nemeses)
        datasets (viz-points test history lost unexpected)
        output   (.getCanonicalPath (store/path! test subdirectory "set.png"))
        preamble (concat (perf/preamble output)
                         [[:set :title (str (:name test) " set")]
                          '[[set ylabel "Throughput (Hz)"]]])
        series   (for [[type points] datasets]
                   {:title (name type)
                    :with 'points
                    :linetype (perf/type->color
                                (case type
                                  :ok :ok
                                  :unexpected :info
                                  :lost :fail))
                    :pointtype 0
                    :data points})]
    (-> {:preamble preamble
         :series series}
        perf/with-range
        (perf/with-nemeses history nemeses)
        perf/plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))

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
          recovered  (set/difference ok published)]

      (viz! test history opts lost unexpected)

      {:valid?             (and (empty? lost) (empty? unexpected))
       :attempt-count      (count attempts)
       :acknowledged-count (count published)
       :read-count         (count read)
       :ok-count           (count ok)
       :unexpected-count   (count unexpected)
       :lost-count         (count lost)
       :recovered-count    (count recovered)
       :lost               (into (sorted-set) (take 32 lost))
       :unexpected         (into (sorted-set) (take 32 unexpected))})))

(defn workload
  "Takes CLI options, returns a workload with a client and generator."
  [opts]
  {:generator       (generator opts)
   :final-generator (drain opts)
   :wrap-generator  track-writes
   :client          (Client. nil nil)
   :checker         (a/checker (Checker.))})
