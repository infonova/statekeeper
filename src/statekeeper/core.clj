(ns statekeeper.core
  (:require [riemann.streams :refer [call-rescue expired? periodically-until-expired]]))

;
; changed
;

(defonce changed-stream-state (atom {}))

(defn default-pred-fn
  [event]
  (str (:host event) (:service event)))

(defn changed-state
  "Evaluates event state changes.

  Takes a pred function to determine the key for the event stored
  inside the changed-stream-state.

  Optionally takes a map with the following properties:

  - :init   Expected initial state value (defaults to \"ok\")
  - :pairs? Receive the state of the previous event, in addition to the current event, as a vector. Defaults to false.

  Example:

    ```clojure
    (changed-state (str (:host event) (:service event)) {:init \"full\" :pairs? true})
    ```

  When the state changes emits a vector with the event and the previous
  state.
  "
  [pred & children]
  (fn stream [event']
    (let [ckey (pred event')
          options  (if (map? (first children)) (first children) {:init "ok"})
          children (if (map? (first children))
                     (rest children)
                     children)
          value'(:state event')
          value (get @changed-stream-state ckey (:init options))]
      (when-not (= value value')
        (swap! changed-stream-state assoc ckey value')
        (call-rescue (if (:pairs? options false)
                       [event' value]
                       event')
                     children)))))

;
; coalesce
;

(defonce coalesce-stream-state (atom {}))

(defn set-coalesce-stream-state!
  "Ensure that there is a stored value for a named coalesce stream.
   Yields the stored value."
  [coalesce-stream-name state]
  (get
   (swap! coalesce-stream-state assoc coalesce-stream-name state)
   coalesce-stream-name))

(defn reset-coalesce-stream-states!
  "Clear previously saved stream states"
  []
  (reset! coalesce-stream-state {}))

(defn named-coalesce-stream-state
  "Get a coalesce stream by name, if no previous value existed for this
   named stream, use the 0-arity constructor `ctor` to initialize it."
  [coalesce-stream-name ctor]
  (if-let [state (get @coalesce-stream-state coalesce-stream-name)]
    state
    (set-coalesce-stream-state! coalesce-stream-name (ctor))))

(defn expire-coalesce-stream-state
  "Utility function to expire a coalesce stream state by name.
   This is meant to be called after a reload if a stream has changed name,
   to expire the previous one."
  [coalesce-stream-name]
  (swap! coalesce-stream-state dissoc coalesce-stream-name))

(defn extract-coalesce-args
  "returns a map containing the coalesce args."
  [args children]
  (cond
    (number? args) {:dt args :children children}
    (map? args)    (assoc args :dt (:dt args 1) :children children)
    :default       {:dt 1 :children (cons args children)}))

(defn coalesce
  "Combines events over time. Coalesce remembers the most recent event for each
  service/host combination that passes through it (limited by :ttl). Every dt
  seconds (default to 1 second), it passes on *all* events it remembers. When
  events expire, they are included in the emitted sequence of events *once*,
  and removed from the state table thereafter.
  Use coalesce to combine states that arrive at different times--for instance,
  to average the CPU use over several hosts.
  Every 10 seconds, print a sequence of events including all the events which
  share the same :foo and :bar attributes:
  (by [:foo :bar]
  (coalesce 10 prn))

  The first parameter can be a number (dt value) or a map. The map keys are `dt` and `stream-name`. The `stream-name` value is used to keep the stream state between Riemann reloads.

  Coalesce call by called with :

  ```clojure
  (coalesce children)                             ;; default dt = 1
  (coalesce 10 children)                          ;; dt = 10
  (coalesce {:dt 5 :stream-name :name} children)   ;; dt = 5 and the stream name is :name
  ```
  "
  [& [dt & children]]
  (let [{:keys [dt stream-name children]} (extract-coalesce-args dt children)
        ctor  (fn [] (java.util.concurrent.ConcurrentHashMap.))
        chm   (if stream-name (named-coalesce-stream-state stream-name ctor) (ctor))
        callback (fn callback []
                   (let [es (vec (.values chm))
                         expired (filter expired? es)]
                     (doseq [e expired
                             :let [s (:service e)
                                   h (:host e)]]
                       (.remove chm [s h] e))
                     (call-rescue es children)))
        period-manager (periodically-until-expired dt callback)]
    (fn [e]
      (.put chm [(:service e) (:host e)] e)
      (period-manager e))))

;
; moving-event-window
;

(defonce mew-stream-state (atom {}))

(defn extract-moving-event-window-args
  [args children]
  (cond
    (number? args) {:n args :stream-name :default :children children}
    (map? args)    (assoc args :stream-name (keyword (get args :stream-name :default)) :children children)))

(defn moving-event-window
  "A sliding window of the last few events. Every time an event arrives, calls
     children with a vector of the last n events, from oldest to newest. Ignores
     event times. Examples:

     (moving-event-window 5 (smap folds/mean index))
     (moving-event-window {:n 5 :stream-name :mystream} (smap folds/mean index))"
  [n & children]
  (let [{:keys [n stream-name children]} (extract-moving-event-window-args n children)]
    (fn stream [event]
      (swap! mew-stream-state
             assoc stream-name (vec (take-last n (conj (get @mew-stream-state stream-name) event))))
      (let [events (get @mew-stream-state stream-name)]
        (info @mew-stream-state)
        (call-rescue events children)))))

;
; predict-linear
;

(defn predict-linear
  "Stream that performs OLS regression. Uses a moving-event-window
  of :n events and emits an event with a prediction for :metric of :s
  seconds in the future. If the optional model rebuild interval :r
  (in seconds) is specified the model will be rebuild periodically and
  not on every arriving event.

  E.g. predict the metric of service \"fs-usage\" 30 minutes in the
  future grouped by host:

  ```clojure
  (where (service \"fs-usage\")
    (by :host
      (predict-linear {:n 10 :s 1800 :stream-name :mystream}
        #(info %))))
  ```
  "
  [args & children]
  (assert (> (get args :n) 1))
  (let [n (get args :n)
        s (get args :s)
        stream-name (get args :stream-name :default)
        rebuild-interval (* (get args :r 0) 1000)
        last-rebuild (atom 0)
        coefs (atom {:intercept 0 :slope 0})]
    (moving-event-window
      {:n n :stream-name stream-name}
      (smap
        (fn [events]
          ; Makes no sense to build model for one event
          (when (> (count events) 1)
            (let [model (fn [events]
                          (let [x (map :time events)
                                x-avg (/ (reduce + x) (count x))
                                y (map :metric events)
                                y-avg (/ (reduce + y) (count y))
                                x-sub (map #(- % x-avg) x)
                                y-sub (map #(- % y-avg) y)
                                m (/
                                   (reduce + (map * x-sub y-sub))
                                   (reduce + (map * x-sub x-sub)))
                                b (- y-avg (* m x-avg))]
                            (reset! coefs {:slope m :intercept b})
                            (reset! last-rebuild (System/currentTimeMillis))
                            @coefs))
                  params (if (or
                               (= 0 rebuild-interval)
                               (= 0 @last-rebuild)
                               (> (- (System/currentTimeMillis) @last-rebuild) rebuild-interval))
                           (model events)
                           @coefs)]
              (let [prediction (+ (:intercept params) (* (+ (:time (last events)) s) (:slope params)))
                    event (assoc (last events) :metric prediction)]
                (call-rescue event children)))))))))
