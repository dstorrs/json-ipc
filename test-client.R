library(rjson)

client <- function(){
  conn <- socketConnection(host="localhost", port = 20174, blocking=TRUE,
                           server=FALSE, open="r+")
  f <- file("stdin")
  open(f)
  while(TRUE){
    print("Enter SQL to be sent, q to quit")
    sql <- readLines(f, n=1)
    print(paste("msg is: '", sql, "'"))
    if(tolower(sql)=="q") {
      break
    }
    str = toJSON(list(type="db", action="query", sql="select * from test where data = $1",
                      values=list("row 1")))
    print(paste("json'd out str is: ", str))
    print(paste("json'd out str class is: ", class(str)))
    
    ##  Send to server
    write_resp <- writeLines(str, con=conn)
    print("done with write")
    
    ##  Wait for response
    in_str = readLines(conn, 1)
    print(paste("raw in str is: ", in_str))
    in_str = fromJSON(in_str)
    print(paste("json'd in str is: ", in_str))
    server_resp <- in_str

    print(paste("Response:  ", server_resp))
  }
  close(f)
  close(conn)
}
client()

