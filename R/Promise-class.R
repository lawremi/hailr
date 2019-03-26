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

## captures a call to order(); a start on a general algebra...
setClass("OrderPromise", contains="Promise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

expr <- function(x) x@expr
`expr<-` <- function(x, value) {
    x@expr <- value
    x
}

setGeneric("context", function(x) NULL)

setMethod("context", "Promise", function(x) x@context)

`context<-` <- function(x, value) {
    x@context <- value
    x
}

setMethod("[", "Promise", function(x, i, j, ..., drop = TRUE) {
    if (!missing(j) || length(list(...)) > 0L || !missing(drop)) {
        stop("'[' only accepts x[i] or x[] syntax")
    }
    if (missing(i)) {
        return(x)
    }
    if (is(i, "Promise")) {
        ctx <- resolveContext(list(x, i))
    } else {
        ctx <- context(x)
    }
    context(x) <- ctx[i,]
    x
})

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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### NSBS wrapper
###

setClass("PromiseNSBS", contains="NSBS", slots=c(subscript="Promise"))

PromiseNSBS <- function(subscript, upper_bound, upper_bound_is_strict, has_NAs)
    new2("PromiseNSBS", subscript=subscript,
         upper_bound=upper_bound,
         upper_bound_is_strict=upper_bound_is_strict,
         has_NAs=has_NAs,
         check=FALSE)

setMethod("NSBS", "Promise",
          function(i, x, exact=TRUE, strict.upper.bound=TRUE, allow.NAs=FALSE) {
              ### FIXME: check 'i' is within upper bound
              ### FIXME: check for NAs if !allow.NAs
              PromiseNSBS(i, NROW(x), strict.upper.bound, FALSE)
          })

setMethod("length", "PromiseNSBS", function(x) length(x@subscript))

setMethod("as.integer", "PromiseNSBS", function(x) as.integer(x@subscript))

setMethod("replaceROWS", c(i="PromiseNSBS"), function(x, i, value ) {
    replaceROWS(x, i@subscript, value)
})

setMethod("extractROWS", c(i="PromiseNSBS"), function(x, i) {
    extractROWS(x, i@subscript)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### show()
###

## makes Promises mostly invisible to the user
setMethod("show", "Promise", function(object) {
    show(fulfill(object))
})

setGeneric("inspect", function(x) standardGeneric("inspect"))

setMethod("inspect", "Promise", function(x) {
    cat(paste(class(x), "object"),
        paste("expr:", as.character(expr(x))),
        paste("context:", class(context(x))),
        sep="\n")
})
