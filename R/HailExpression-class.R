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

.HailRef <- setClass("HailRef",
                     contains=c("SimpleSymbol", "HailExpression"))

.HailApply <- setClass("HailApply",
                       contains=c("SimpleCall", "HailExpression"))

.HailGetField <- setClass("HailGetField",
                          slots=c(element="character",
                                  container="HailExpression"),
                          contains="HailExpression",
                          validity=function(object) {
                              if (!isSingleString(object@element))
                                  "@element must be a single, non-NA string"
                        })

.HailExpressionList <- setClass("HailExpressionList",
                                prototype=
                                    prototype(elementType="HailExpression"),
                                contains="SimpleList")

.HailMakeStruct <- setClass("HailMakeStruct",
                            contains=c("HailExpression",
                                       "HailExpressionList"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

HailRef <- function(name) {
    .HailRef(SimpleSymbol(name))
}

HailApply <- function(name, args) {
    .HailApply(SimpleCall(name, args))
}

HailGetField <- function(container, element) {
    .HailGetField(container=container, element=element)
}

HailExpressionList <- function(...) {
    listData <- lapply(list(...), as, "HailExpression", strict=TRUE)
    .HailExpressionList(listData=listData)
}

HailMakeStruct <- function(...) {
    .HailMakeStruct(HailExpressionList(...))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("[[", "HailExpression", function(x, i, j, ...) {
    stopifnot(missing(j), missing(...), isSingleString(i))
    HailGetField(x, i)
})

setMethod("[[", "HailMakeStruct", function(x, i, j, ...) {
    stopifnot(missing(j), missing(...))
    as.list(x)[[i]]
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("character", "HailExpression", function(from) {
    HailRef(from)
})

ir_name <- function(x) sub("^Hail", "", class(x))

setGeneric("to_ir", function(x) as.character(x))

setMethod("to_ir", "HailExpression",
          function(x) to_ir(c(ir_name(x), ir_args(x))))

setMethod("to_ir", "list",
          function(x) paste0("(",
                             paste(vapply(x, to_ir, character(1L)),
                                   collapse=" "),
                             ")"))

setGeneric("ir_args", function(x) standardGeneric("ir_args"))

setMethod("ir_args", "HailExpression",
          function(x) lapply(slotNames(x), slot, object=x))

setMethod("ir_args", "HailApply", function(x) c(x@name, x@args))

setMethod("ir_args", "HailMakeStruct",
          function(x) as.list(zipup(Pairs(names(x), as.list(x)))))

setMethod("as.character", "HailExpression", function(x) to_ir(x))
