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

src <- function(x) x@src

setMethod("hailType", "HailMapGlobalsContext", function(x) hailType(src(x)))

setMethod("hailTypeEnv", "HailMapGlobalsContext",
          function(x) hailTypeEnv(hailType(x)))

setMethod("parent", "HailMapGlobalsContext", function(x) context(src(x)))
