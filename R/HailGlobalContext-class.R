### =========================================================================
### HailGlobalContext objects
### -------------------------------------------------------------------------
###
### Represents the global metadata on a Hail object (Table or MatrixTable)
###

.HailGlobalContext <- setClass("HailGlobalContext",
                               slots=c(src="ANY"),
                               contains="HailExpressionContext")

HailGlobalContext <- function(src) {
    .HailGlobalContext(src=src)
}

setMethod("hailType", "HailGlobalContext", function(x) hailType(x@src))
