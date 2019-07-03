### =========================================================================
### HailSymbol objects
### -------------------------------------------------------------------------
###
### Symbols in the Hail language.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

.HailSymbol <- setClass("HailSymbol", contains="SimpleSymbol")

.HailSymbolList <- setClass("HailSymbolList",
                            prototype=
                                prototype(elementType="HailSymbol"),
                            contains="SimpleList")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

HailSymbol <- function(name) {
    .HailSymbol(name=name)
}

HailSymbolList <- function(...) {
    args <- list(...)
    if (length(args) == 1L && is.list(args[[1L]]))
        args <- args[[1L]]
    listData <- lapply(args, as, "HailSymbol", strict=FALSE)
    .HailSymbolList(listData=listData)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("character", "HailSymbol", function(from) HailSymbol(from))

