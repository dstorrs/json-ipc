#lang racket

(require  racket/runtime-path
          db-helpers
          thread-with-id
          json
          try-catch
          )

(define-runtime-path here "./")

(define-logger server)
(define conn (tcp-listen 20174 100 #t))
(displayln "Waiting for connections...")

(define (close-connection in out)
  (log-server-debug "~a closing connection" (thread-id))
  (close-input-port in)
  (close-output-port out))

(define (server-start dispatch)
  (define db-path (build-path here "dbsqlite3.conf"))
  (log-server-debug "db-path is: ~a" db-path)
  (current-database-handle db-path)
  (log-server-debug "outside of accept loop.  getting test rows: ~v" (query-rows (dbh) "select * from test"))

  (let accept-loop ()
    (define-values (in out) (tcp-accept conn))
    (thread
     (thunk
      (define-values (lh lp remote-host remote-port)
        (tcp-addresses in #t))
      (log-server-debug "Incoming connection from ~a:~a..." remote-host remote-port)

      (let msg-loop ([msg-num 1])
        (displayln "waiting for message...")
        (try [
              (define msg (read-json in))
              (cond [(eof-object? msg)  (close-connection in out)]
                    [else
                     (log-server-debug "~a msg is: ~v" (thread-id) msg)
                     (define reply (dispatch msg msg-num))
                     (log-server-debug "~a reply is: '~v'" (thread-id) reply)
                     (match reply
                       ['close-connection  (close-connection in out)]
                       [reply
                        (write-json (if (void? reply)
                                        "done"
                                        reply)
                                    out)
                        ;;  R's readLines() function will not return until it finds a \n
                        ;;  or until the 1-minute timeout on the socket pops, the socket is
                        ;;  closed, and remaining data in the socket buffer is returned.
                        ;;  Therefore, let's send a newline.
                        (display "\n" out)
                        (flush-output out)
                        (msg-loop (add1 msg-num))])])]
             [catch
                 (exn:break? raise)
               (any/c (let ()
                        (log-server-debug "~a exception thrown.  Message: ~a"
                                          (thread-id)
                                          (if (exn? e)
                                              (exn-message e)
                                              e))
                        (write-json "invalid message" out)
                        (flush-output out)))]))))
    (accept-loop)))

(define (dispatch msg msg-num)
  (log-server-debug "~a entering dispatch with msg ~v " (thread-id) msg)

  ;; Messages are always hashes.  Racket's JSON hashes have symbols for keys.
  ;;
  ;;   Grammar for messages:  <> are for grouping in the grammar, not literals.
  ;;   | indicates alternatives, one of which must be present
  ;;   ? indicates the prior item is optional
  ;;   ...   indicates 0 or more of the prior item
  ;;   ...+  indicates 1 or more of the prior item
  ;;
  ;; Recognized messages:
  ;;
  ;; * run a SQL command, either a SELECT ("query"), or UPDATE/INSERT ("execute")
  ;; (hash 'type "db" 'action <"query" | "execute"> 'sql "SQL string")
  ;;       <'values (list item ...+)>?
  ;;       <'config config-hash>?  )
  ;;
  ;; IMPORTANT: Use "query" for anything that returns data, including 'INSERT ... RETURNING'
  ;;
  ;; Return value for a "query" type is always a list of lists, even if there is only one row.
  ;;
  ;; config-hash contains details on how to connect to the database, either for SQLite or
  ;; Pg.  The string values are literals, everything else is an example:
  ;;
  ;;    (hash  'connect-function   connect-func-string
  ;;           'database           "./foo.db")
  ;;
  ;; or (hash  'connect-function   connect-func-string
  ;;           'dbname             "lims"
  ;;           'host               "192.168.1.24"
  ;;           'port               5432
  ;;           'user               "bob"
  ;;           'password           "password123")
  ;;
  ;; connect-func-string           <"sqlite3-connect" | "postgresql-connect">

  (match msg
    [(hash-table('type "db") ('action action) ('sql sql))
     #:when (member action '("query" "execute"))
     (log-server-debug "~a message is a valid DB message" (thread-id))
     (define func (if (equal? action "query")
                      query-rows
                      query-exec))
     (define config (hash-ref msg 'config #f))
     (define vals   (hash-ref msg 'values '()))

     (log-server-debug "~a chose func: ~v" (thread-id) func)
     (log-server-debug "~a config is: ~v " (thread-id) config)
     (log-server-debug "~a vals are: ~v " (thread-id) vals)

     (parameterize ([current-database-handle (if config (dbh config) (dbh))])
       (define reply (apply func (append (list (dbh) sql) vals)))
       (match reply
         [(? list?)              (map vector->list reply)]
         [(? void?)              reply]
         [result
          (raise-arguments-error 'dispatch
                                 "result was neither list nor void"
                                 "result" result)]))]
    ;
    [(? string?)
     (log-server-debug "~a prepping reply." (thread-id))
     (~a "reply #" msg-num)]
    ;
    [_ (log-server-debug "~a invalid message: ~v" (thread-id) msg)]))

(server-start dispatch)
(tcp-close conn)
