### =========================================================================
### HailMapGlobalsContext objects
### -------------------------------------------------------------------------
###
### Represents the context of a HailMapGlobals() call. Shared between
### HailTable and HailMatrixTable (and presumably any other object
### with global metadata).
###

.HailMapGlobalsContext <- setClass("HailMapGlobalsContext",
                                   slots=c(src="ANY"),
                                   contains="HailExpressionContext")

HailMapGlobalsContext <- function(src) {
    .HailMapGlobalsContext(src=src)
}

setMethod("hailType", "HailMapGlobalsContext", function(x) hailType(x@src))
