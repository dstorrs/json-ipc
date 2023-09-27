#lang racket

(require json try-catch)

(display "connecting...")

(define-values (in out) (tcp-connect "localhost" 20174))
(displayln "connected.")

(try [(for ([sql '("select * from test" "select data from test where data like 'bob%'")])
        (display "sending...")
        (write-json (hash 'type "db" 'action "query" 'sql sql)
                    out)
        (displayln "after write")
        (flush-output out)
        (displayln "message sent.")

        (println (read-json in))
        (sleep 1))]
     [catch
         (any/c
          (begin
            (displayln (~a  "boom!: " (~v e)))
            (close-input-port in)
            (close-output-port out)))])
