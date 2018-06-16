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
setClass("TFloat32Promise", contains="NumericPromise")
setClass("TFloat64Promise", contains="NumericPromise")

setClass("IntegralPromise", contains="HailPromise")
setClass("Int32Promise", contains="IntegralPromise")
setClass("Int64Promise", contains="IntegralPromise")

setClass("StringPromise", contains="HailPromise")

setClass("BinaryPromise", contains="HailPromise")

setClass("ContainerPromise", contains="HailPromise") # list-like
setClass("ArrayPromise", contains="ContainerPromise")
setClass("SetPromise", contains="ContainerPromise")
setClass("DictPromise", contains="ContainerPromise")
setClass("AggregablePromise", contains="ContainerPromise") # a matrix

setClass("StructPromise", contains="HailPromise") # high-level vector

## These should be a GenomicRanges, but do not confuse with HailGRanges
## Might need a common parent class

setClass("IntervalPromise", contains="HailPromise")
setClass("CallPromise", contains="HailPromise")
setClass("LocusPromise", contains="HailPromise")

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
