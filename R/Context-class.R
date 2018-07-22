### =========================================================================
### Context objects
### -------------------------------------------------------------------------
###
### Borrowed from rsolr
###

setClassUnion("Context", "environment")

setGeneric("compatible", function(x, y, ...) standardGeneric("compatible"))

setMethod("compatible", c("Context", "Context"),
          function(x, y) {
              identical(x, y)
          })
