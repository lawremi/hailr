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

.HailSymbol <- setClass("HailSymbol", contains="SimpleSymbol")

.HailI32 <- setClass("HailI32", slots=c(x="integer"),
                     contains="HailExpression")

.HailF64 <- setClass("HailF64", slots=c(x="numeric"),
                     contains="HailExpression")

.HailStr <- setClass("HailStr", slots=c(x="character"),
                     contains="HailExpression")

.HailFalse <- setClass("HailFalse", contains="HailExpression")
.HailTrue <- setClass("HailTrue", contains="HailExpression")

.HailNA <- setClass("HailNA", slots=c(type="HailType"),
                    contains="HailExpression")

setClassUnion("HailType_OR_NULL", c("HailType", "NULL"))

.HailRef <- setClass("HailRef",
                     slots=c(symbol="HailSymbol"),
                     contains="HailExpression")

.HailApply <- setClass("HailApply",
                       contains=c("HailExpression", "SimpleCall"))

.HailGetField <- setClass("HailGetField",
                          slots=c(element="HailSymbol",
                                  container="HailExpression"),
                          contains="HailExpression")

.HailExpressionList <- setClass("HailExpressionList",
                                prototype=
                                    prototype(elementType="HailExpression"),
                                contains="SimpleList")

.HailVarArgs <- setClass("HailVarArgs", contains="HailExpressionList")

.HailMakeArray <- setClass("HailMakeArray",
                           slots=c(type="HailType", elements="HailVarArgs"),
                           contains="HailExpression")

.HailMakeStruct <- setClass("HailMakeStruct",
                            contains=c("HailExpression",
                                       "HailExpressionList"))

setClass("HailBinaryOp",
         slots=c(op="HailSymbol",
                 left="ANY", right="ANY"),
         contains=c("HailExpression", "VIRTUAL"))

.HailApplyComparisonOp <- setClass("HailApplyComparisonOp",
                                   contains="HailBinaryOp")

.HailApplyBinaryPrimOp <- setClass("HailApplyBinaryPrimOp",
                                   contains="HailBinaryOp")

.HailArrayMap <- setClass("HailArrayMap",
                          slots=c(name="HailSymbol",
                                  array="HailExpression",
                                  body="HailExpression"),
                          contains="HailExpression")

.HailArrayFilter <- setClass("HailArrayFilter",
                             slots=c(name="HailSymbol",
                                     array="HailExpression",
                                     body="HailExpression"),
                             contains="HailExpression")

.HailArrayLen <- setClass("HailArrayLen",
                          slots=c(array="HailExpression"),
                          contains="HailExpression")

.HailIf <- setClass("HailIf",
                    slots=c(cond="HailExpression",
                            cnsq="HailExpression",
                            altr="HailExpression"),
                    contains="HailExpression")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

HailSymbol <- function(name) {
    .HailSymbol(name=name)
}

HailI32 <- function(x) {
    .HailI32(x=as.integer(x))
}

HailF64 <- function(x) {
    .HailF64(x=as.numeric(x))
}

HailStr <- function(x) {
    .HailStr(x=x)
}

HailFalse <- function() {
    .HailFalse()
}

HailTrue <- function() {
    .HailTrue()
}

HailNA <- function(x) {
    .HailNA(type=hailType(x))
}

HailRef <- function(symbol) {
    .HailRef(symbol=symbol)
}

HailApply <- function(name, args) {
    .HailApply(SimpleCall(name, args))
}

HailGetField <- function(container, element) {
    .HailGetField(container=container, element=element)
}

HailExpressionList <- function(data) {
    listData <- lapply(data, as, "HailExpression", strict=FALSE)
    .HailExpressionList(listData=listData)
}

HailVarArgs <- function(args) {
    .HailVarArgs(HailExpressionList(args))
}

HailMakeArray <- function(type, elements) {
    .HailMakeArray(type=type, elements=HailVarArgs(elements))
}

HailMakeStruct <- function(data) {
    .HailMakeStruct(HailExpressionList(data))
}

HailApplyComparisonOp <- function(op, left, right) {
    .HailApplyComparisonOp(op=op, left=left, right=right)
}

HailApplyBinaryPrimOp <- function(op, left, right) {
    .HailApplyBinaryPrimOp(op=op, left=left, right=right)
}

HailArrayMap <- function(name, array, body) {
    .HailArrayMap(name=name, array=array, body=body)
}

HailArrayFilter <- function(name, array, body) {
    .HailArrayFilter(name=name, array=array, body=body)
}

HailArrayLen <- function(array) {
    .HailArrayLen(array=array)
}

HailIf <- function(cond, cnsq, altr) {
    .HailIf(cond=cond, cnsq=cnsq, altr=altr)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("[[", "HailExpression", function(x, i, j, ...) {
    stopifnot(missing(j), missing(...), isSingleString(i))
    HailGetField(x, HailSymbol(i))
})

setMethod("[[", "HailMakeStruct", function(x, i, j, ...) {
    stopifnot(missing(j), missing(...))
    as.list(x)[[i]]
})

setMethod("hailType", "HailExpression", function(x) x@type)

symbol <- function(x) x@symbol

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

hailLiteral <- function(from, CONSTRUCTOR) {
    stopifnot(length(from) == 1L)
    if (is.na(from))
        HailNA(from)
    else CONSTRUCTOR(from)
}

setAs("character", "HailExpression", function(from) {
    hailLiteral(from, HailStr)
})

setAs("integer", "HailI32", function(from) {
    hailLiteral(from, HailI32)
})

setAs("numeric", "HailF64", function(from) {
    hailLiteral(from, HailF64)
})

setAs("logical", "HailI32", function(from) {
    hailLiteral(from, function(from) {
        if (from)
            HailTrue()
        else HailFalse()
    })
})

setAs("list", "HailExpression", function(from) {
    stopifnot(length(from) == 1L)
    HailMakeArray(hailType(from),
                  lapply(from[[1L]], as, "HailExpression"))
})

ir_name <- function(x) sub("^Hail", "", class(x))

setGeneric("to_ir", function(x) as.character(x))

setMethod("to_ir", "HailSymbol", function(x) escape_id(name(x)))

setMethod("to_ir", "HailExpression",
          function(x) to_ir(c(ir_name(x), ir_args(x))))

setMethod("to_ir", "list",
          function(x) paste0("(",
                             paste(vapply(x, to_ir, character(1L)),
                                   collapse=" "),
                             ")"))

setMethod("to_ir", "HailVarArgs",
          function(x) paste(vapply(x, to_ir, character(1L)), collapse=" "))

setGeneric("ir_args", function(x) standardGeneric("ir_args"))

setMethod("ir_args", "HailExpression",
          function(x) Filter(Negate(is.null),
                             setNames(lapply(slotNames(x), slot, object=x),
                                      slotNames(x))))

setMethod("ir_args", "HailApply", function(x) c(x@name, x@args))

setMethod("ir_args", "HailMakeStruct",
          function(x) as.list(zipup(Pairs(names(x), as.list(x)))))

setMethod("ir_args", "HailStr", function(x) list(paste0("\"", x@x, "\"")))

escape_id <- function(x) {
    ans <- capture.output(print(as.name(x)))
    if (startsWith(ans, "."))
        ans <- paste0("`", ans, "`")
    ans
}

setMethod("as.character", "HailExpression", function(x) to_ir(x))

setMethod("show", "HailExpression",
          function(object) cat(as.character(object), "\n"))
