### =========================================================================
### HailExpression objects
### -------------------------------------------------------------------------
###
### Expressions in the Hail language.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

setClass("is.hail.expr.ir.BaseIR", contains="JavaObject")

setClass("HailExpression", contains=c("Expression", "VIRTUAL"))

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

.HailRef <- setClass("HailRef",
                     slots=c(symbol="HailSymbol"),
                     contains="HailExpression")

.HailApply <- setClass("HailApply",
                       slots=c(name="HailSymbol"),
                       contains=c("HailExpression", "SimpleCall"))

.HailApplySeeded <- setClass("HailApplySeeded", contains="HailApply")

.HailGetField <- setClass("HailGetField",
                          slots=c(element="HailSymbol",
                                  container="HailExpression"),
                          contains="HailExpression")

.HailExpressionList <- setClass("HailExpressionList",
                                prototype=
                                    prototype(elementType="HailExpression"),
                                contains="SimpleList")

.HailSymbolList <- setClass("HailSymbolList",
                            prototype=
                                prototype(elementType="HailSymbol"),
                            contains="HailExpressionList")

setClassUnion("HailExpressionList_OR_NULL", c("HailExpressionList", "NULL"))

.HailVarArgs <- setClass("HailVarArgs", contains="HailExpressionList")

.HailMakeArray <- setClass("HailMakeArray",
                           slots=c(type="HailType", elements="HailVarArgs"),
                           contains="HailExpression")

.HailMakeStruct <- setClass("HailMakeStruct",
                            contains=c("HailExpression",
                                       "HailVarArgs"))

setClass("HailBinaryOp",
         slots=c(op="HailSymbol",
                 left="ANY", right="ANY"),
         contains=c("HailExpression", "VIRTUAL"))

.HailApplyComparisonOp <- setClass("HailApplyComparisonOp",
                                   contains="HailBinaryOp")

.HailApplyBinaryPrimOp <- setClass("HailApplyBinaryPrimOp",
                                   contains="HailBinaryOp")

.HailApplyUnaryPrimOp <- setClass("HailApplyUnaryPrimOp",
                                  slots=c(op="HailSymbol", x="ANY"),
                                  contains="HailExpression")

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

.HailInsertFields <- setClass("HailInsertFields",
                              slots=c(old="HailExpression",
                                      field_order="NULL",
                                      fields="HailVarArgs"),
                              contains="HailExpression")

.HailSelectFields <- setClass("HailSelectFields",
                              slots=c(old="HailExpression",
                                      fields="HailSymbolList"),
                              contains="HailExpression")

.Accumulation <- setClass("Accumulation",
                          slots=c(op="character",
                                  constructor_args="HailExpressionList",
                                  init_op_args="HailExpressionList_OR_NULL",
                                  seq_op_args="HailExpressionList"))

.HailApplyScanOp <- setClass("HailApplyScanOp",
                             contains=c("Accumulation", "HailExpression"))

.HailApplyAggOp <- setClass("HailApplyAggOp",
                            contains=c("Accumulation", "HailExpression"))

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

HailApply <- function(name, args = list()) {
    .HailApply(SimpleCall(as(name, "HailSymbol"), args))
}

ensureSeed <- function() {
    seed <- .GlobalEnv$.Random.seed
    if (is.null(seed)) {
        set.seed(NULL)
        seed <- Recall()
    }
    seed
}

HailApplySeeded <- function(name, args = list()) {
    .HailApplySeeded(SimpleCall(as(name, "HailSymbol"), c(ensureSeed(), args)))
}

HailGetField <- function(container, element) {
    .HailGetField(container=container, element=element)
}

HailExpressionList <- function(...) {
    args <- list(...)
    if (length(args) == 1L && is.list(args[[1L]]))
        args <- args[[1L]]
    listData <- lapply(args, as, "HailExpression", strict=FALSE)
    .HailExpressionList(listData=listData)
}

HailVarArgs <- function(...) {
    .HailVarArgs(HailExpressionList(...))
}

HailMakeArray <- function(type, ...) {
    .HailMakeArray(type=type, elements=HailVarArgs(...))
}

HailMakeStruct <- function(...) {
    .HailMakeStruct(HailVarArgs(...))
}

HailApplyComparisonOp <- function(op, left, right) {
    .HailApplyComparisonOp(op=op, left=left, right=right)
}

HailApplyBinaryPrimOp <- function(op, left, right) {
    .HailApplyBinaryPrimOp(op=op, left=left, right=right)
}

HailApplyUnaryPrimOp <- function(op, x) {
    .HailApplyUnaryPrimOp(op=op, x=x)
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

HailInsertFields <- function(old, fields) {
    ### TODO: optimize by coalescing previous InsertFields
    .HailInsertFields(old=old, fields=HailVarArgs(fields))
}

HailSelectFields <- function(old, fields) {
    ### TODO: optimize by coalescing previous Select()
    ### TODO: optimize by coalescing InsertFields (drop the unneeded ones)
    .HailSelectFields(old=old, fields=fields)
}

Accumulation <- function(op, constructor_args = HailExpressionList(),
                         init_op_args = NULL,
                         seq_op_args = HailExpressionList())
{
    .Accumulation(op=op, constructor_args=constructor_args,
                  init_op_args=init_op_args, seq_op_args=seq_op_args)
}

HailApplyScanOp <- function(accumulation) {
    .HailApplyScanOp(accumulation)
}

HailApplyAggOp <- function(accumulation) {
    .HailApplyAggOp(accumulation)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("$", "HailExpression", function(x, name) x[[name]])

setMethod("[[", "HailExpression", function(x, i, j, ...) {
    stopifnot(missing(j), missing(...), isSingleString(i))
    HailGetField(x, HailSymbol(i))
})

symbol <- function(x) x@symbol

setMethod("hailType", "HailExpression",
          function(x, env = emptyenv()) inferHailType(x, env))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Type inference
###

### Potential caching functionality; not yet used

cacheType <- function(expr) {
    cache <- parent.frame()$x@.cache
    if (!exists("type", cache))
        cache$type <- expr
    cache$type
}

## setMethod("initialize", "HailExpression", function(.Object, ...) {
##     .Object <- callNextMethod()
##     .Object@.cache <- new.env(emptyenv())
##     .Object
## })

setGeneric("inferHailType",
           function(x, env = emptyenv()) standardGeneric("inferHailType"),
           signature="x")

setMethod("inferHailType", "HailI32", function(x, env) TINT32)
setMethod("inferHailType", "HailF64", function(x, env) TFLOAT64)
setMethod("inferHailType", "HailStr", function(x, env) TSTRING)
setMethod("inferHailType", "HailFalse", function(x, env) TBOOLEAN)
setMethod("inferHailType", "HailTrue", function(x, env) TBOOLEAN)
setMethod("inferHailType", "HailMakeStruct",
          function(x, env) TStruct(lapply(x, inferHailType, env)))
setMethod("inferHailType", "HailApplyComparisonOp", function(x, env) TBOOLEAN)
setMethod("inferHailType", "HailApplyBinaryPrimOp",
          function(x, env) merge(inferHailType(x@left),
                                 inferHailType(x@right, env)))
setMethod("inferHailType", "HailApplyUnaryPrimOp",
          function(x, env) inferHailType(x@x, env))
setMethod("inferHailType", "HailArrayMap", function(x, env) {
    env <- list2env(setNames(list(x@array), x@name), parent=env)
    TArray(inferHailType(x@body, env))
})
setMethod("inferHailType", "HailArrayFilter",
          function(x, env) inferHailType(x@array, env))
setMethod("inferHailType", "HailArrayLen", function(x, env) TINT64)
setMethod("inferHailType", "HailIf",
          function(x, env) inferHailType(x@cnsq, env))
setMethod("inferHailType", "HailInsertFields", function(x, env) {
    type <- inferHailType(x@old, env)
    type[names(x@fields)] <- lapply(x@fields, inferHailType, env)
    type
})
setMethod("inferHailType", "HailSelectFields",
          function(x, env) inferHailType(x@old, env)[names(x@fields)])
setMethod("inferHailType", "Accumulation", function(x, env) {
    functionReturnType(x@op, x@seq_op_args, functionTag(x))
})
setMethod("inferHailType", "HailNA", function(x, env) x@type)
setMethod("inferHailType", "HailMakeArray", function(x, env) x@type)
setMethod("inferHailType", "HailGetField",
          function(x, env) inferHailType(x@container, env)[[name(x@element)]])

### NOTE: The type of a reference is only inferrable if we know the
### environment in which to resolve it (such as in map operations).
setMethod("inferHailType", "HailRef", function(x, env) {
    env[[symbol]]
})

setMethod("inferHailType", "HailApply", function(x, env) {
    functionReturnType(x@name, x@args, functionTag(x))
})

setGeneric("functionTag", function(x) NULL)
setMethod("functionTag", "Accumulation", function(x) "agg")
setMethod("functionTag", "HailApplySeeded", function(x) "seeded")

lookupFunction <- function(name, tag = NULL) {
    prefix <- "ht"
    if (!is.null(tag))
        prefix <- paste0(prefix, "_", tag)
    match.fun(paste0(prefix, "_", name))
}

functionReturnType <- function(name, args, tag = NULL) {
    f <- lookupFunction(name, tag)
    tryCatch(do.call(f, as.list(args)),
             error=stop("Function ",
                        paste0(name, "(", paste(args, collapse=", "),
                               ")"),
                        " not found"))
}

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

setAs("integer", "HailExpression", function(from) {
    hailLiteral(from, HailI32)
})

setAs("numeric", "HailExpression", function(from) {
    hailLiteral(from, HailF64)
})

setAs("logical", "HailExpression", function(from) {
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

setGeneric("to_ir", function(x, ...) as.character(x))

setMethod("to_ir", "HailSymbol", function(x, ...) escape_id(name(x)))

setMethod("to_ir", "HailExpression",
          function(x, ...) to_ir(c(ir_name(x), ir_args(x)), ...))

setMethod("to_ir", "list",
          function(x, ...) paste0("(",
                                  paste(vapply(x, to_ir, character(1L), ...),
                                        collapse=" "),
                                  ")"))

setMethod("to_ir", "List", function(x, ...) to_ir(as.list(x), ...))

setMethod("to_ir", "HailVarArgs",
          function(x, ...) {
              if (is.character(names(x)))
                  x <- mapply(list, lapply(names(x), HailSymbol), x)
              paste(vapply(x, to_ir, character(1L), ...),
                    collapse=" ")
          })

setMethod("to_ir", "NULL", function(x, ...) "None")

setMethod("to_ir", "logical", function(x, ...) {
    stopifnot(isTRUEorFALSE(x))
    if (x) "True" else "False"
})

setGeneric("ir_args", function(x) standardGeneric("ir_args"))

setMethod("ir_args", "HailExpression",
          function(x) setNames(lapply(slotNames(x), slot, object=x),
                               slotNames(x)))

setMethod("ir_args", "HailApply", function(x) c(x@name, x@args))

setMethod("ir_args", "HailStr", function(x) list(paste0("\"", x@x, "\"")))

escape_id <- function(x) {
    underscored <- startsWith(x, "_")
    if (underscored) {
        x <- paste0("x", x)
    }
    ans <- capture.output(print(as.name(x)))
    if (underscored) {
        ans <- substring(x, 2L)
    } else if (startsWith(ans, ".")) {
        ans <- paste0("`", ans, "`")
    }
    ans
}

setMethod("as.character", "HailExpression", function(x) to_ir(x))

setAs("character", "HailSymbol", function(from) HailSymbol(from))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### show()
###

setMethod("show", "HailExpression",
          function(object) cat(as.character(object), "\n"))
