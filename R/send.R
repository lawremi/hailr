### =========================================================================
### Sending
### -------------------------------------------------------------------------
###
### Utlitlies for sending data somewhere and still being able to use it.
### 

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### send(): the top-level user entry point
###

send <- function(x, dest) {
    marshalled <- marshal(x, dest)
    remote <- transmit(marshalled, dest)
    unmarshal(remote, x)
}

setGeneric("marshal", function(x, dest) x)

setGeneric("transmit", function(x, dest) standardGeneric("transmit"))

setGeneric("unmarshal", function(x, skeleton) x)
