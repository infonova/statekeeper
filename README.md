# statekeeper

A Riemann plugin called statekeeper

## Goal

Keep stream state during Riemann config reloads.

## Usage

### Build

```
lein uberjar
```

### Config example


```
; -*- mode: clojure; -*-
; vim: filetype=clojure
(load-plugins)

(require '[statekeeper.core :as sk])

(logging/init {:file "riemann.log"})

; Listen on the local interface over TCP (5555), UDP (5555), and websockets
; (5556)
(let [host "127.0.0.1"]
  (tcp-server {:host host})
  (udp-server {:host host})
  (ws-server  {:host host}))

(periodically-expire 5)

(let [index (index)]
  (streams
    (where (service "test.changed-state")
      (sk/changed-state sk/default-pred-fn
                        #(info %)))

    (where (service "test.coalesce")
           (by-builder [host :host]
                       (sk/coalesce {:dt 10 :stream-name (str "test.coalesce." host)}
                                    #(info %))))))
```

## License

Distributed under the Eclipse Public License, the same as Clojure.
