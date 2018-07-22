### =========================================================================
### HailExpression objects
### -------------------------------------------------------------------------
###
### Expressions in the Hail language.
###

setClass("HailExpression", contains=c("Expression", "VIRTUAL"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

.HailSymbol <- setClass("HailSymbol",
                        contains=c("SimpleSymbol", "HailExpression"))

.HailCall <- setClass("HailCall", contains=c("SimpleCall", "HailExpression"))

.HailSelect <- setClass("HailSelect",
                        slots=c(container="HailExpression",
                                element="character"),
                        contains="HailExpression",
                        validity=function(object) {
                            if (!isSingleStrong(object@element))
                                "@element must be a single, non-NA string"
                        })

setClass("HailExpressionList",
         prototype=prototype(elementType="HailExpression"),
         contains="SimpleList")

.HailStructDeclaration <- setClass("HailStructDeclaration",
                                   contains=c("HailExpression",
                                              "HailExpressionList"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

HailSymbol <- function(name) {
    .HailSymbol(SimpleSymbol(name))
}

HailCall <- function(name, args) {
    .HailCall(SimpleCall(name, args))
}

HailSelect <- function(container, element) {
    .HailSelect(container=container, element=element)
}

HailStructDeclaration <- function(...) {
    .HailStructDeclaration(List(...))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("[[", "HailExpression", function(x, i, j, ...) {
    stopifnot(missing(j), missing(...), isSingleString(i))
    HailSelect(x, i)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("character", "HailExpression", function(from) {
    HailSymbol(from)
})

setMethod("as.character", "HailSelect",
          function(x) paste0(as.character(x@container), ".",
                             as.character(x@element)))
