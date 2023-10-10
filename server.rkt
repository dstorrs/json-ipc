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

(define (send-message msg out)
  (write-json (if (or (void?  msg)
                      (equal? msg "\n"))
                  "done"
                  msg)
              out)
  ;;  R's readLines() function will not return until it finds a \n
  ;;  or until the 1-minute timeout on the socket pops, the socket is
  ;;  closed, and remaining data in the socket buffer is returned.
  ;;  Therefore, let's send a newline.
  (display "\n" out)
  (flush-output out))

(define (server-start dispatch)
  (define db-path (build-path here "db.conf"))
  (log-server-debug "db-path is: ~a" db-path)

  (current-database-handle db-path)

  (let accept-loop ()
    (define-values (in out) (tcp-accept conn))
    (thread
     (thunk
      (define-values (lh lp remote-host remote-port)
        (tcp-addresses in #t))
      (log-server-debug "Incoming connection from ~a:~a..." remote-host remote-port)

      (let msg-loop ([msg-num 1])
        (displayln "waiting for message...")
        (try [(define msg (read-json in))
              (log-server-debug "~a msg is: ~v" (thread-id) msg)
              (match msg
                [(? eof-object?)  (close-connection in out)]
                ["\n" (send-message "done" out)
                      (msg-loop (add1 msg-num))]
                [(app (curryr dispatch msg-num) reply)
                 #:when (not (equal? reply 'close-connection))
                 (log-server-debug "~a got a message: ~v" (thread-id) msg)
                 (send-message reply out)
                 (msg-loop (add1 msg-num))]
                    [else
                 (log-server-debug "~a closing connection" (thread-id))
                 (send-message "FIN" out)
                 (close-connection in out)])]
             [catch
                 (exn:break? raise)
               (any/c (let ()
                        (log-server-debug "~a exception thrown.  Message: ~a"
                                          (thread-id)
                                          (if (exn? e)
                                              (exn-message e)
                                              e))
                        (send-message "invalid message" out)                        
                        (msg-loop (add1 msg-num))))]))))
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
  ;; * establish the DB connection parameters for this connection
  ;; (hash 'type "db" 'action "config" 'config config-hash)
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
  ;; Pg.  Note that the sqlite version refers to a config file on the server.
  ;;
  ;;    (hash  'connect-function   "sqlite3-connect"
  ;;           'database           "./foo.db")
  ;;
  ;; or (hash  'connect-function   "postgresql-connect"
  ;;           'dbname             "lims"
  ;;           'host               "192.168.1.24"
  ;;           'port               5432
  ;;           'user               "bob"
  ;;           'password           "password123")
  ;;
  (match msg
    [(hash-table ('type "db") ('action "config") ('config config-hash))
     (current-database-handle config-hash)]
    ;
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
         [(? list?)
          (for*/list ([row (map vector->list reply)]
                      [val row])
            (if (sql-null? val) "sql-null" val))]
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
