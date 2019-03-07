### =========================================================================
### HailPromise objects
### -------------------------------------------------------------------------
###
### Promises defer operations by building a tree of expressions. Upon
### collection, the promise evaluates the expression in the assigned context.
###

### Note on missingness:

### Hail propagates missingness in a way that is mostly compatible
### with R. One inconsistency is that all aggregators effectively have
### na.rm=TRUE. To handle na.rm=FALSE, we will need to check for
### missingness explicitly. No big deal.

setClass("HailPromise",
         slots=c(expr="HailExpression"),
         contains=c("SimplePromise", "VIRTUAL"))

setClass("HailAtomicPromise", contains=c("HailPromise", "Vector", "VIRTUAL"))

setClass("BooleanPromise", contains="HailAtomicPromise")

setClass("NumericPromise", contains=c("HailAtomicPromise", "VIRTUAL"))
setClass("Float32Promise", contains="NumericPromise")
setClass("Float64Promise", contains="NumericPromise")

setClass("IntegralPromise", contains=c("NumericPromise", "VIRTUAL"))
setClass("Int32Promise", contains="IntegralPromise")
setClass("Int64Promise", contains="IntegralPromise")

setClass("StringPromise", contains="HailAtomicPromise")

setClass("BinaryPromise", contains="HailPromise")

setClass("HailPromiseList",
         prototype=prototype(elementType="HailPromise"),
         contains="List")

### Typically we could infer the element HailType from the
### @elementType, but not in general, e.g., we could have an array of
### tables. Thus, we need a @elementHailType.
setClass("ContainerPromise",
         slots=c(elementHailType="HailType"),
         prototype=prototype(elementHailType=TBOOLEAN),
         contains=c("HailPromise", "HailPromiseList", "VIRTUAL"))
## in general ragged but could be a matrix
setClass("ArrayPromise", contains="ContainerPromise")
setClass("NumericArrayPromise", contains="ArrayPromise")
setClass("SetPromise", contains="ContainerPromise")
setClass("DictPromise", contains="ContainerPromise")

.SimpleHailPromiseList <- setClass("SimpleHailPromiseList",
                                   contains=c("HailPromiseList", "SimpleList"))

## The struct promises have the same structure across rows, so we
## treat them as a scalar collection of vectors, in the same way that
## a JSON array of consistent objects can be twisted into a data.frame.
setClass("BaseStructPromise",
         contains=c("HailPromise", "SimpleHailPromiseList", "VIRTUAL"),
         slots=c(fieldTypes="HailTypeList"))

## list-like with unique names
setClass("StructPromise", contains="BaseStructPromise",
         validity=function(object) {
             c(if (length(object) > 0L && is.null(names(object)))
                 "StructPromise must have names; see TuplePromise",
               if (anyDuplicated(names(object)))
                 "StructPromise must have unique names")
         })

## list-like with no names
setClass("TuplePromise", contains="BaseStructPromise",
         validity=function(object) {
             if (!is.null(names(object)))
                 "TuplePromise must not have names; see StructPromise"
         })

## These should be a GenomicRanges, but do not confuse with HailGRanges
## Might need a common parent class

setClass("IntervalPromise", contains="HailAtomicPromise")
setClass("CallPromise", contains="HailAtomicPromise")
setClass("LocusPromise", contains="HailAtomicPromise")

setClass("HailOrderPromise", contains=c("HailPromise", "OrderPromise"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction.
###

### FIXME: 'type' should come last since we can often infer the type from expr
setGeneric("Promise", function(type, expr, context) {
    expr <- as(expr, expressionClass(context), strict=FALSE)
    new2(promiseClass(type), expr=expr, context=context, check=FALSE)
}, signature="type")

setMethod("Promise", "TBaseStruct", function(type, expr, context) {
    promise <- callNextMethod()
    subpromises <- HailPromiseList(lapply(names(type), function(nm) {
        Promise(type[[nm]], expr(promise)[[nm]], context)
    }))
    names(subpromises) <- names(type)
    initialize(promise, subpromises)
})

setMethod("Promise", "TContainer", function(type, expr, context) {
    initialize(callNextMethod(), elementHailType=elementType(type),
               elementType=promiseClass(elementType(type)))
})

setMethod("Promise", "missing", function(type, expr, context) {
    Promise(hailType(expr), expr, context)
})

HailPromiseList <- function(...) {
    .SimpleHailPromiseList(List(...))
}

dummyPromise <- function(context) {
    Promise(context, FALSE, TBOOLEAN)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Basic accessors
###

setGeneric("size", function(x) length(x))

setMethod("size", "HailPromise", function(x) {
    promiseMethodCall("size", TINT32, x)
})

### Our promises are typed according to the type of data they
### contain. Whether a promise corresponds to a vector or scalar in
### Hail depends on the context.

setMethod("length", "HailPromise", function(x) {
    contextualLength(x, context(x))
})

## We think of a struct in the table(row) context like a nested table
setMethod("length", "BaseStructPromise", function(x) {
    length(as.list(x))
})

setMethod("lengths", "ContainerPromise", function(x, use.names = TRUE) {
    ## TODO
})

setMethod("dim", "HailPromise", function(x) {
    contextualDim(x, context(x))
})

setMethod("[", "ArrayPromise", function(x, i, j, ..., drop = TRUE) {
    if (length(list(...)) > 0L || !missing(drop)) {
        stop("'drop' and arguments in '...' not supported")
    }
    x <- callNextMethod(x, i)
    if (!missing(j)) {
        ### TODO: normalize 'j' to integer
        if (length(j) == 1L) {
            method <- paste0("[", j, "]")
        } else if(isSlice(j)) {
            method <- paste0("[", head(j, 1L), ":", tail(j, 1L), "]")
        } else {
            stop("'j' must be a scalar or (k:l)")
        }
        x <- promiseMethodCall(method, elementType(hailType(x)), x)
    }
    x
})

setMethod("extractROWS", c("ArrayPromise", "ArrayPromise"),
          function(x, i) {

          })

setMethod("extractROWS", "HailPromise",
          function(x, i) {
              ### By filtering the context, we keep things simple, but
              ### we also promote filtering before projection. In many
              ### cases, that is an accidental optimization, but it's
              ### not generally correct. The filter expression will
              ### always embed any projection, but if the projection
              ### crosses rows (i.e., embeds an aggregation like
              ### computing the length), then things might
              ### break. Aggregations over the entire table can
              ### probably be computed eagerly and the result inlined,
              ### surely something will fail eventually.
              extractROWS(context(x), i)
          })

setReplaceMethod("[", "HailPromise", function(x, i, j, ..., value) {
    stopifnot(missing(j), missing(...))
    if (missing(i))
        value
    else if (is.logical(i) || is(i, "BooleanPromise"))
        ifelse(i, value, x)
    else if (is.numeric(i) || is(i, "NumericPromise")) {
        ### Probably deferrable using a join followed by ifelse, but for
        ### now just fulfill() the promises.
        x <- fulfill(x)
        x[fulfill(i)] <- fulfill(value)
        x
    } else stop("unsupported type for 'i'")
})

setMethod("hailType", "Int32Promise", function(x) TINT32)
setMethod("hailType", "Int64Promise", function(x) TINT64)
setMethod("hailType", "Float32Promise", function(x) TFLOAT32)
setMethod("hailType", "Float64Promise", function(x) TFLOAT64)
setMethod("hailType", "BooleanPromise", function(x) TBOOLEAN)
setMethod("hailType", "StringPromise", function(x) TSTRING)
setMethod("hailType", "StructPromise", function(x) new("TStruct", x@fieldTypes))
setMethod("hailType", "ContainerPromise",
          function(x) get(typeClass(x), mode="function")(x@elementHailType))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion.
###

setGeneric("cast", function(x, type) as(x, promiseClass(type)))

setMethod("cast", c("HailPromise", "HailPrimitiveType"), function(x, type) {
    if (!identical(vectorMode(hailType(x)), vectorMode(type))) {
        promiseMethodCall(x, paste0("to", type), type)
    } else {
        x
    }
})

setMethod("cast", c("ArrayPromise", "TArray"), function(x, type) {
    lapply(x, cast, elementType(type))
})

setMethod("cast", c("ANY", "HailPrimitiveType"), function(x, type) {
    if (is.null(x) && optional(type)) NULL else as.vector(x, vectorMode(type))
})

setMethod("cast", c("list", "TArray"), function(x, type) {
    as.typed_list(x, vectorMode(elementType(type)))
})

setMethod("cast", c("data.frame", "TStruct"), function(x, type) {
    x[] <- Map(cast, x, type)
    x
})

setMethod("as.list", "ContainerPromise", as.list.Promise)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Iteration
###

.ArrayApplyContext <- setClass("ArrayApplyContext",
                               slots=c(arrayPromise="ArrayPromise",
                                       argName="HailSymbol"),
                               contains="HailExpressionContext")

ArrayApplyContext <- function(arrayPromise, argName = HailSymbol(uuid("xi"))) {
    .ArrayApplyContext(arrayPromise=arrayPromise, argName=argName)
}

arrayPromise <- function(x) x@arrayPromise
`arrayPromise<-` <- function(x, value) {
    x@arrayPromise <- value
    x
}

argName <- function(x) x@argName

setMethod("parent", "ArrayApplyContext",
          function(x) {
              context(arrayPromise(x))
          })

setMethod("extractROWS", c("ArrayApplyContext", "BooleanPromise"),
          function(x, i) {
              stopifnot(derivesFrom(context(i), x))
              expr <- HailArrayFilter(arrayPromise(x), argName(x), expr(i))
              ### We are still inside of the iteration, so we just
              ### replace the underlying array promise, preserving the
              ### expr and keeping it in an ArrayApplyContext. This is
              ### probably asking for trouble.
              arrayPromise(x) <- Promise(hailType(array), expr,
                                         context(arrayPromise(x)))
              x
          })

setMethod("contextualLength",
          c("HailPromise", "ArrayApplyContext"),
          function(x, context) {
              arrayPromise <- arrayMap(x)
              Promise(TINT32, HailArrayLen(expr(arrayPromise)),
                      context(arrayPromise))
          })

setGeneric("deriveTable",
           function(context, expr) standardGeneric("deriveTable"),
           signature="context")

setMethod("deriveTable", "ArrayApplyContext",
          function(context, expr) {
              deriveTable(context(arrayPromise(context)),
                          arrayMapExpr(context, expr))
          })

setClass("AtomicApplyContext", contains="HailContext")

setMethod("contextualLength", c("HailPromise", "AtomicApplyContext"),
          function(x, context) 1L)

elementPromise <- function(context) {
    Promise(elementType(hailType(arrayPromise(context))),
            HailRef(argName(context)),
            context)
}

arrayMapExpr <- function(context, expr) {
    argName <- argName(context)
    ans <- expr(arrayPromise(context))
    identity <- identical(expr, HailRef(argName))
    if (!identity) {
        ans <- HailArrayMap(argName, ans, expr)
    }
    ans
}

arrayMap <- function(body) {
    expr <- arrayMapExpr(context(body), expr(body))
    Promise(TArray(hailType(body)), expr,
            context(arrayPromise(context(body))))
}

## Another case we may want to handle: df$array[[1]]. Calling `[[` on
## an ArrayPromise could first filter to row 'i', then do
## elementPromise(ArrayApplyContext(x)), but the array map would be
## deferred to fulfill().

setMethod("lapply", "ArrayPromise", function(X, FUN, ...) {
    ans <- FUN(elementPromise(ArrayApplyContext(X)), ...)
    if (is(context(ans), "ArrayApplyContext"))
        ans <- arrayMap(ans)
    ans
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accumulation
###

.AccumulationContext <- setClass("AccumulationContext",
                                 slots=c(parent="HailExpressionContext"),
                                 contains="HailExpressionContext")

.ScanContext <- setClass("ScanContext", contains="AccumulationContext")

AccumulationContext <- function(parent) .AccumulationContext(parent=parent)

ScanContext <- function(parent) .ScanContext(parent=parent)

setMethod("parent", "AccumulationContext", function(x) x@parent)

setMethod("expressionClass", "ScanContext", function(x) "HailApplyScanOp")

promiseAccumulation <- function(accumulation, context, type) {
    Promise(type, as(accumulation, expressionClass(context)), parent(context))
}

setMethod("contextualLength", c("HailPromise", "AccumulationContext"),
          function(x, context) {
              promiseAccumulation(Accumulation("Count"), context, TINT64)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Comparison
###

## Need to emulate the Compare group generic here, since there are
## operator-specific methods for Vector.
lapply(c("<", ">", ">=", "<=", "==", "!="), function(op) {
    setMethods(op,
               list(c("HailPromise", "ANY"),
                    c("ANY", "HailPromise"),
                    c("HailPromise", "HailPromise")),
               function(e1, e2) promiseComparisonOpCall(op, e1, e2))
})

setMethods("Arith",
           list(c("HailPromise", "ANY"),
                c("ANY", "HailPromise"),
                c("HailPromise", "HailPromise")),
           function(e1, e2) {
               op <- .Generic
               if (op == "%%")
                   op <- "%"
               else if (op == "%/%")
                   op <- "//"
               else if (op == "^")
                   op <- "**"
               if (missing(e2)) {
                   if (op == "+") e1 else promiseUnaryOpCall(op, e1)
               } else {
                   promiseBinaryPrimOpCall(op, e1, e2)
               }
           })

setMethods("Logic",
           list(c("HailPromise", "ANY"),
                c("ANY", "HailPromise"),
                c("HailPromise", "HailPromise")),
           function(e1, e2) {
               op <- paste0(.Generic, .Generic)
               promiseMethodCall(e1, op, TBOOLEAN, e2)
           })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transforming
###

setMethod("ifelse2", "BooleanPromise", function(test, yes, no) {
    returnType <- unifyTypes(test, yes, no)
    promiseCall(HailIf, returnType,
                test,
                cast(yes, returnType),
                cast(no, returnType))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

setMethod("head", "HailPromise", function(x, n) {
    initialize(x, context=head(context(x), n))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### String manipulation
###

setGeneric("strsplit", signature="x")

### NOTE: splitting an empty string in Hail yields an array containing
### only the empty string, instead of an empty array.

setMethod("strsplit", "StringPromise",
          function (x, split, fixed = FALSE, perl = FALSE, useBytes = FALSE) {
              stopifnot(identical(fixed, FALSE),
                        identical(perl, FALSE),
                        identical(useBytes, FALSE),
                        isSingleString(split))
              promiseMethodCall(x, "split", TArray(TSTRING), split)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities.
###

## Compare to solrCall()

promiseCall <- function(fun, type, ..., CALL_CONSTRUCTOR) {
    args <- list(...)
    ctx <- resolveContext(args)
    promises <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    args[promises] <- lapply(args[promises], expr)
    args <- lapply(args, as, expressionClass(ctx), strict=FALSE)
    if (missing(CALL_CONSTRUCTOR)) {
        expr <- do.call(fun, args)
    } else {
        expr <- CALL_CONSTRUCTOR(fun, args)
    }
    Promise(type, expr, ctx)
}

promiseStaticCall <- function(fun, type, ...) {
    promiseCall(fun, type, CALL_CONSTRUCTOR=HailApply, ...)
}

promiseMethodCall <- function(target, fun, type, ...) {
    promiseCall(fun, type, CALL_CONSTRUCTOR=HailApply, target, ...)
}

promiseBinaryOpCall <- function(op, left, right, returnType, OP_CONSTRUCTOR) {
    promiseCall(op, returnType, CALL_CONSTRUCTOR=function(op, args) {
        OP_CONSTRUCTOR(HailSymbol(op), args[[1L]], args[[2L]])
    }, left, right)
}

promiseComparisonOpCall <- function(op, left, right) {
    promiseBinaryOpCall(op, left, right, TBOOLEAN, HailApplyComparisonOp)
}

cast_recycle <- function(x, type) {
    if (is(type, "TContainer") && !is(x, "ContainerPromise")) {
        type <- elementType(type)
    }
    cast(x, type)
}

promiseBinaryPrimOpCall <- function(op, left, right) {
    returnType <- numericType(unifyTypes(left, right))
    promiseBinaryOpCall(op,
                        cast_recycle(left, returnType),
                        cast_recycle(right, returnType),
                        returnType, HailApplyBinaryPrimOp)
}

unifyTypes <- function(...) {
    Reduce(merge, lapply(list(...), hailType))
}

promiseUnaryOpCall <- function(op, x) {
    promiseCall(op, hailType(x), CALL_CONSTRUCTOR=function(op, args) {
        HailApplyUnaryPrimOp(HailSymbol(op), args[[1L]])
    }, x)
}

resolveContext <- function(objs) {
    isProm <- vapply(objs, is, "Promise", FUN.VALUE=logical(1L))
    ctxs <- Filter(Negate(is.null), lapply(objs[isProm], context))
    ans <- ctxs[[1L]]
    for (ctx in ctxs[-1L]) {
        if (!identical(ans, ctx)) {
            if (derivesFrom(ctx, ans)) {
                ans <- ctx
            } else {
                stop("cannot combine promises from different contexts")
            }
        }
    }
    ans
}
