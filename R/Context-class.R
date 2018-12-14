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

setMethod("parent", "Context", function(x) NULL)

derivesFrom <- function(x, p) {
    !is.null(x) && (identical(parent(x), p) || derivesFrom(parent(x), p))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Aggregation (often requires delegation from e.g. row-level to context)
###

setGeneric("contextualLength", function(x, context) length(x))
