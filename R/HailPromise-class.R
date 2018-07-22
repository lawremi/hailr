### =========================================================================
### HailPromise objects
### -------------------------------------------------------------------------
###
### Promises defer operations by building a tree of expressions. Upon
### collection, the promise evaluates the expression in the assigned context.
###

setClass("HailPromise",
         slots=c(expr="HailExpression"),
         contains="SimplePromise")

setClass("BooleanPromise", contains="HailPromise")

setClass("NumericPromise", contains="HailPromise")
setClass("Float32Promise", contains="NumericPromise")
setClass("Float64Promise", contains="NumericPromise")

setClass("IntegralPromise", contains="HailPromise")
setClass("Int32Promise", contains="IntegralPromise")
setClass("Int64Promise", contains="IntegralPromise")

setClass("StringPromise", contains="HailPromise")

setClass("BinaryPromise", contains="HailPromise")

setClass("ContainerPromise",
         slots=c(elementType="HailType"),
         contains="HailPromise")
setClass("ArrayPromise", contains="ContainerPromise")
setClass("SetPromise", contains="ContainerPromise")
setClass("DictPromise", contains="ContainerPromise")
setClass("AggregablePromise", contains="ContainerPromise") # a matrix

.HailPromiseList <- setClass("HailPromiseList",
                             prototype=prototype(elementClass="HailPromise"),
                             contains="SimpleList")

setClass("BaseStructPromise", contains=c("HailPromise", "HailPromiseList"),
         slots=c(fieldTypes="HailTypeList"))

## list-like with unique names
setClass("StructPromise", contains="BaseStructPromise",
         validity=function(object) {
             c(if (length(object) > 0L && is.null(names(object)))
                 "StructPromise must have names; see TuplePromise",
               if (anyDuplicated(names(object)))
                 "StructPromise must have unique names")
         })

## list-like with no names
setClass("TuplePromise", contains="BaseStructPromise",
         validity=function(object) {
             if (!is.null(names(object)))
                 "TuplePromise must not have names; see StructPromise"
         })

## These should be a GenomicRanges, but do not confuse with HailGRanges
## Might need a common parent class

setClass("IntervalPromise", contains="HailPromise")
setClass("CallPromise", contains="HailPromise")
setClass("LocusPromise", contains="HailPromise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction.
###

setGeneric("Promise", function(type, expr, context) {
    expr <- as(expr, expressionClass(context))
    new(promiseClass(type), expr=expr, context=context)
}, signature="type")

setMethod("Promise", "TBaseStruct", function(type, expr, context) {
    promise <- callNextMethod()
    subpromises <- HailPromiseList(lapply(names(type), function(nm) {
        Promise(type[[nm]], expr(promise)[[nm]], context)
    }))
    names(subpromises) <- names(type)
    initialize(promise, subpromises)
})

setMethod("Promise", "TContainer", function(type, expr, context) {
    initialize(callNextMethod(), elementType=elementType(type))
})

HailPromiseList <- function(...) {
    .HailPromiseList(List(...))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Basic accessors
###

setMethod("length", "HailPromise", function(x) {
    hailCall("size", x)
})

setMethod("length", "BaseStructPromise", function(x) {
    length(as.list(x))
})

setMethod("lengths", "ContainerPromise", function(x, use.names = TRUE) {
    ## TODO
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

setMethod("head", "HailPromise", function(x, n) {
    initialize(x, context=head(context(x), n))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities.
###

## Compare to solrCall()
hailCall <- function(fun, type, ...) {
    args <- list(...)
    promises <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    args[promises] <- lapply(args[promises], expr)
    ctx <- resolveContext(...)
    expr <- HailCall(fun, args)
    Promise(type, expr, ctx)
}

## From rsolr
resolveContext <- function(...) {
    args <- list(...)
    isProm <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    ctx <- Filter(Negate(is.null), lapply(args[isProm], context))
    if (any(!vapply(ctx[-1], compatible, ctx[[1L]], FUN.VALUE=logical(1L)))) {
        stop("cannot combine promises from different contexts")
    }
    ctx[[1L]]
}
