### =========================================================================
### Context objects
### -------------------------------------------------------------------------
###
### Borrowed from rsolr
###

setClassUnion("Context", "environment")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Context inheritance
###

setGeneric("parent", function(x) standardGeneric("parent"))

setMethod("parent", "Context", function(x) { NULL }) # {() for bug in methods

derivesFrom <- function(x, p) {
    !is.null(x) && (same(parent(x), p) || derivesFrom(parent(x), p))
}

setGeneric("same", function(x, y) identical(x, y))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Aggregation (often requires delegation from e.g. row-level to context)
###

setGeneric("contextualLength", function(x, context) length(x))
