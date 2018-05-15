### =========================================================================
### HailPromise objects
### -------------------------------------------------------------------------
###
### Promises defer operations by building a tree of expressions. Upon
### collection, the promise evaluates the expression in the assigned context.
###

### BEGIN rsolr - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
setClass("Promise")

setClass("SimplePromise",
         slots=c(expr="Expression",
                 context="Context"),
         contains="Promise")
### END rsolr - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

setClass("HailPromise",
         slots=c(expr="HailExpression"),
         contains="SimplePromise")

## It seems cleanest for promises to be classed by data type. In
## rsolr, we classed promises by their language (as they are
## essentially expression factories). To unify the two frameworks, we
## could have a 'generator' layer that separates how expressions are
## constructed from data type constraints. This makes S4 do the data
## type checking for us.

## Should these promises have a reference to their Hail type? 

setClass("HailBooleanPromise", contains="HailPromise")

setClass("HailNumericPromise", contains="HailPromise")

setClass("HailIntegralPromise", contains="HailPromise")

setClass("HailStringPromise", contains="HailPromise")

setClass("HailBinaryPromise", contains="HailPromise")

setClass("HailContainerPromise", contains="HailPromise") # list-like

setClass("HailStructPromise", contains="HailPromise") # high-level vector

## These should be a GenomicRanges, but do not confuse with HailGRanges
## Might need a common parent class

setClass("HailIntervalPromise", contains="HailPromise")
setClass("HailCallPromise", contains="HailPromise")
setClass("HailLocusPromise", contains="HailPromise")

setClass("HailAggregablePromise", contains="HailPromise") # a matrix

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction.
###

setGeneric("promise", function(expr, context) standardGeneric("promise"))

setMethod("promise", "ANY", "is.hail.table.Table", function(expr, context) {
    new("HailVectorPromise", expr=as(expr, "HailExpression"), context=context)
})
