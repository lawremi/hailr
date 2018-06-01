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

setClass("IntegralPromise", contains="HailPromise")

setClass("StringPromise", contains="HailPromise")

setClass("BinaryPromise", contains="HailPromise")

setClass("ContainerPromise", contains="HailPromise") # list-like

setClass("StructPromise", contains="HailPromise") # high-level vector

## These should be a GenomicRanges, but do not confuse with HailGRanges
## Might need a common parent class

setClass("IntervalPromise", contains="HailPromise")
setClass("CallPromise", contains="HailPromise")
setClass("LocusPromise", contains="HailPromise")

setClass("AggregablePromise", contains="HailPromise") # a matrix

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction.
###

promises <- function(context) {
    type <- hailType(context)
    mapply(Promise, type, names(type), MoreArgs=list(context=context))
}

Promise <- function(type, expr, context) {
    new(promiseClass(type), expr=expr, context=context)
}
