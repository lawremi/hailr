### =========================================================================
### Copying
### -------------------------------------------------------------------------
###
### Utlitlies for copying data somewhere and still being able to use it.
### 

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### copy(): the top-level user entry point
###

copy <- function(x, dest) {
    ### Strategy:
    
    ### 1) Call marshal(). Default coerces to DataFrame first, then
    ###    recalls the generic. For DataFrame, we create a data.frame.
    ### 2) Copy the marshalled object to Hail (and thus Spark).
    ###    We get a Hail table back.
    ### 3) Call unmarshal(). It is passed the result of coercion in
    ###    (1) and the Hail table reference. There will generally be
    ###    recursion that ends up creating the result, probably a
    ###    projected version of the Hail table.

    marshalled <- marshal(x, dest)
    remote <- transmit(marshalled, dest)
    unmarshal(remote, x)
}

setGeneric("marshal", function(x, dest) standardGeneric("marshal"))

setGeneric("unmarshal", function(x, skeleton) standardGeneric("unmarshal"))



