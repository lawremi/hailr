### =========================================================================
### Promise objects
### -------------------------------------------------------------------------
###
### Temporarily copy/pasted from rsolr
###

setClass("Promise")

setClass("SimplePromise",
         slots=c(expr="Expression",
                 context="Context"),
         contains="Promise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

expr <- function(x) x@expr
`expr<-` <- function(x, value) {
    x@expr <- value
    x
}
context <- function(x) x@context
`context<-` <- function(x, value) {
    x@context <- value
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Fulfillment
###

setGeneric("fulfill", function(x, ...) standardGeneric("fulfill"))
setMethod("fulfill", "Promise", function(x) {
    eval(expr(x), context(x))
})
setMethod("fulfill", "ANY", function(x) x)

setMethod("as.logical", "Promise", function(x) as.logical(fulfill(x)))
setMethod("as.integer", "Promise", function(x) as.integer(fulfill(x)))
setMethod("as.numeric", "Promise", function(x) as.numeric(fulfill(x)))
setMethod("as.character", "Promise", function(x) as.character(fulfill(x)))
setMethod("as.factor", "Promise", function(x) as.factor(fulfill(x)))
setMethod("as.vector", "Promise",
          function(x, mode = "any") as.vector(fulfill(x), mode=mode))
as.Date.Promise <- function(x, ...) as.Date(fulfill(x))
as.POSIXct.Promise <- function(x, ...) as.POSIXct(fulfill(x))
as.POSIXlt.Promise <- function(x, ...) as.POSIXlt(fulfill(x))
as.data.frame.Promise <- function(x, row.names = NULL, optional = FALSE, ...) {
    as.data.frame(fulfill(x), row.names=row.names, optional=optional, ...)
}

as.list.Promise <- function(x, ...) as.list(fulfill(x))
