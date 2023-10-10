#lang racket

(require json try-catch)

(define-logger client)

(display "connecting...")

(define-values (in out) (tcp-connect "localhost" 20174))
(displayln "connected.")

(try [(for ([sql '("select * from test" "select data from test where data like 'bob%'")])
        (log-client-debug "sending...")
        (write-json (hash 'type "db" 'action "query" 'sql sql)
                    out)
        (log-client-debug "after write")
        (flush-output out)
        (log-client-debug "message sent.")

        (println (read-json in))
        (sleep 1))]
     [catch
         (any/c
          (begin
            (log-client-debug "boom!: ~v" e)
            (close-input-port in)
            (close-output-port out)))])
