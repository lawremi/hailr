### =========================================================================
### HailPromise objects
### -------------------------------------------------------------------------
###
### Promises defer operations by building a tree of expressions. Upon
### collection, the promise evaluates the expression in the assigned context.
###

### Hail has two types of data structures: scalar and container. The R
### API is vectorized, so we treat these data structures as vectors
### (typically columns in a Table). Thus, scalars become atomic
### vectors, and containers become lists of containers. The latter is
### somewhat complicated in that the R user will naturally need to
### apply a function over the list in order to manipulate the
### containers themselves. The most complicated case is the "dict"
### container, which we model as a named vector, but only in the
### context of applying over the list. Two types of containers,
### "struct" and "tuple", are along the second (column) dimension, so
### they are implemented as actual R lists of promises. A Hail
### "array", if rectangular (every array in the list has the same
### length), can represent an R matrix, but only the HailMatrix
### wrapper will behave like a matrix.

### Note on missingness:

### Hail propagates missingness in a way that is mostly compatible
### with R. One inconsistency is that all aggregators effectively have
### na.rm=TRUE. To handle na.rm=FALSE, we will need to check for
### missingness explicitly. No big deal.

setClass("HailPromise",
         slots=c(expr="HailExpression",
                 NAMES="HailExpression_OR_NULL"),
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
setClass("IterablePromise", contains="ContainerPromise")
setClass("ArrayPromise", contains="IterablePromise")
setClass("NumericArrayPromise", contains="ArrayPromise")
setClass("SetPromise", contains="IterablePromise")
setClass("DictPromise", contains="ContainerPromise")

.SimpleHailPromiseList <- setClass("SimpleHailPromiseList",
                                   contains=c("HailPromiseList", "SimpleList"))

## The struct promises have the same structure across rows, so we
## treat them as a scalar collection of vectors, in the same way that
## a JSON array of consistent objects can be twisted into a data.frame.
setClass("BaseStructPromise",
         contains=c("SimpleHailPromiseList", "HailPromise", "VIRTUAL"))

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

setClassUnion("Logical", c("logical", "BooleanPromise"))
setClassUnion("Numeric", c("numeric", "NumericPromise"))
setClassUnion("Character", c("character", "StringPromise"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction.
###

Promise <- function(expr, context) {
    type <- hailType(expr, as.environment(hailType(context)))
    makePromise(type, expr, context)
}

setGeneric("makePromise", function(type, expr, context) {
    expr <- coalesce(as(expr, expressionClass(context), strict=FALSE))
    new2(promiseClass(type), expr=expr, context=context, check=FALSE)
}, signature="type")

setMethod("makePromise", "TBaseStruct", function(type, expr, context) {
    promise <- callNextMethod()
    subpromises <- HailPromiseList(lapply(names(type), function(nm) {
        makePromise(type[[nm]], HailGetField(expr(promise), HailSymbol(nm)),
                    context)
    }))
    names(subpromises) <- names(type)
    initialize(promise, subpromises)
})

setMethod("makePromise", "TContainer", function(type, expr, context) {
    initialize(callNextMethod(), elementHailType=elementType(type),
               elementType=promiseClass(elementType(type)))
})

HailPromiseList <- function(...) {
    .SimpleHailPromiseList(List(...))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Basic accessors
###

setGeneric("size", function(x) length(x))

setMethod("size", "HailPromise", function(x) {
    promiseMethodCall("size", x)
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

setMethod("lengths", "IterablePromise", function(x, use.names = TRUE) {
    promiseCall(HailArrayLen, x)
})

setMethod("dim", "HailPromise", function(x) {
    contextualDim(x, context(x))
})

setMethod("names", "HailPromise",
          function(x) if (!is.null(x@NAMES)) Promise(x@NAMES, context(x)))

setMethod("names", "StructPromise", function(x) names(as.list(x)))

setMethod("extractCOLS", "ArrayPromise", function(x, i) {
    stopifnot(is.numeric(i), !anyNA(i), all(i > 0)) # TODO: support logical
    if (length(i) == 1L) {
        promiseMethodCall("indexArray", x, i)
    }
    else if(isSlice(i)) {
        promiseMethodCall("[*:*]", x, head(i, 1L), tail(i, 1L))
    } else {
        stop("'j' must be a scalar or (k:l)")
    }
})

setGeneric("heads", signature="x")

setMethod("heads", "ArrayPromise", function(x, n=6L) {
    n <- unique(n)
    stopifnot(length(n) == 1L && n > 0L)
    end <- n
    promiseMethodCall("[:*]", x, end - 1L)
})

setGeneric("tails", signature="x")

setMethod("tails", "ArrayPromise", function(x, n=6L) {
    n <- unique(n)
    stopifnot(length(n) == 1L && n < 0L)
    start <- abs(n)
    promiseMethodCall("[*:]", x, start)
})

setMethod("[", "ArrayPromise", function(x, i, j, ..., drop = TRUE) {
    if (length(list(...)) > 0L || !missing(drop)) {
        stop("'drop' and arguments in '...' not supported")
    }
    x <- callNextMethod(x, i)
    if (!missing(j)) {
        extractCOLS(x, j)
    }
    x
})

setMethod("extractROWS", c("ArrayPromise", "ArrayPromise"),
          function(x, i) {
              ### TODO
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

setMethod("mergeROWS", "HailPromise", function(x, i, value) {
    mergeROWS(fulfill(x), fulfill(i), fulfill(value))
})

setMethod("bindROWS", "HailPromise",
          function(x, objects=list(), use.names=TRUE, ignore.mcols=FALSE,
                   check=TRUE)
          {
              bindROWS(fulfill(x), lapply(object, fulfill), use.names,
                       ignore.mcols, check)
          })

selectFields <- function(x, fields) {
    if (!identical(names(x), fields))
        promiseCall(HailSelectFields, x, HailSymbolList(fields))
    else x
}

setMethod("extractROWS", "StructPromise", function(x, i) {
    nsbs <- normalizeSingleBracketSubscript(i, x)
    if (missing(i) || !is.character(i)) {
        i <- names(x)[nsbs]
    }
    selectFields(x, i)
})

insertFields <- function(x, fields) {
    promiseCall(HailInsertFields, x, args=c(list(x), as.list(fields)))
}

setMethod("mergeROWS", "StructPromise", function(x, i, value) {
    nsbs <- normalizeSingleBracketSubscript(i, x, allow.append=is.character(i))
    if (missing(i) || !is.character(i)) {
        i <- names(x)[nsbs]
    }
    names(value) <- i
    insertFields(x, value)
})

setMethod("bindROWS", "StructPromise",
          function(x, objects=list(), use.names=TRUE, ignore.mcols=FALSE,
                   check=TRUE)
          {
              ans <- callNextMethod()
              if (anyDuplicated(names(ans)))
                  stop("duplicate names not allowed")
              insertFields(x, tail(ans, length(ans) - length(x)))
          })

### Promise creation infers the type from the expression. In theory we
### could dynamically infer the type here, but we explicitly return
### the type corresponding to each promise type, because it is often
### obvious from the class. It only gets complicated when we need
### additional information beyond the class, such as the types of the
### fields in a struct. Dynamic type inference would obviate the need
### to store that information on the promise.
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
### Fulfillment.
###

### Collecting a Hail promise is analogous to Solr: we derive a new
### table representing the promise, collect that table and extract the
### column.

deriveTable <- function(x) {
    contextualDeriveTable(context(x), x)
}

setGeneric("contextualDeriveTable",
           function(context, x) standardGeneric("contextualDeriveTable"),
           signature="context")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Pushing from one context to another
###

setGeneric("push", function(x, remote) standardGeneric("push"))

setMethod("push", c("Promise", "ANY"),
          function(x, remote) {
              if (identical(context(x), remote))
                  return(x)
              pushFrom(x, context(x), remote)
          })

setGeneric("pushFrom", function(x, origin, remote) standardGeneric("pushFrom"))

setMethod("pushFrom", c("HailPromise", "HailMapGlobalsContext",
                        "HailExpressionContext"),
          function(x, origin, remote) {
              promiseCall(HailGetGlobals, src(origin)$projectGlobals(x = x))$x
          })

setMethod("push", c("HailPromise", "HailContext"), function(x, remote) {
    if (derivesFrom(context(x), remote))
        return(x)
    stop("cannot push promise to remote")
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion.
###

setGeneric("cast", function(x, type) as(x, promiseClass(type)))

setMethod("cast", c("HailPromise", "HailPrimitiveType"), function(x, type) {
    if (!identical(vectorMode(hailType(x)), vectorMode(type))) {
        promiseMethodCall(x, castMethod(type))
    } else {
        x
    }
})

setMethod("cast", c("ContainerPromise", "TContainer"), function(x, type) {
    ht <- hailType(x)
    if (identical(ht, type))
        x
    else if (identical(elementType(ht), elementType(type)))
        promiseCall(castContainerOp(type), x)
    else cast(lapply(x, cast, elementType(type)), type)
})

setMethod("cast", c("HailPromise", "TContainer"), function(x, type) {
    cast(promiseCall(HailMakeArray, type, x), type)
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

## In theory, stuff like endoapply() should just work with this
setMethod(S4Vectors:::coerce2, "HailPromise",
          function(from, to) {
              casted <- cast(from, hailType(to))
              if (!is(casted, "HailPromise"))
                  Promise(casted, context(to))
              else casted
          })

setMethod("as.list", "ContainerPromise", as.list.Promise)

setAs("DataFrame", "StructPromise", function(from) {
    promiseCall(HailMakeStruct, args=as.list(from))
})

setAs("HailPromise", "ArrayPromise", function(from) {
    cast(from, TArray(hailType(from)))
})

setAs("ContainerPromise", "ArrayPromise", function(from) {
    cast(from, TArray(elementType(hailType(from))))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Iteration
###

.ApplyContext <- setClass("ApplyContext",
                          slots=c(containerPromise="ContainerPromise",
                                  argName="HailSymbol"),
                          contains="HailExpressionContext")

ApplyContext <- function(containerPromise, argName = HailSymbol(uuid("xi"))) {
    .ApplyContext(containerPromise=containerPromise, argName=argName)
}

containerPromise <- function(x) x@containerPromise
`containerPromise<-` <- function(x, value) {
    x@containerPromise <- value
    x
}

argName <- function(x) x@argName

setMethod("parent", "ApplyContext",
          function(x) {
              context(containerPromise(x))
          })

setMethod("extractROWS", c("ApplyContext", "BooleanPromise"),
          function(x, i) {
              stopifnot(derivesFrom(context(i), x))
              expr <- HailArrayFilter(containerPromise(x), argName(x), expr(i))
              ### We are still inside of the iteration, so we just
              ### replace the underlying array promise, preserving the
              ### expr and keeping it in an ApplyContext. This is
              ### probably asking for trouble.
              containerPromise(x) <- Promise(expr, context(containerPromise(x)))
              x
          })

setMethod("contextualLength",
          c("HailPromise", "ApplyContext"),
          function(x, context) {
              lengths(arrayMap(x))
          })

setGeneric("contextualNames",
           function(x, context) standardGeneric("contextualNames"))

setMethod("contextualNames", c("HailPromise", "HailExpressionContext"),
          function(x, context) NULL)

setMethod("contextualNames", c("DictPromise", "ApplyContext"),
          function(x, context) {
              cast(promiseMethodCall(x, "keys"), TArray(TSTRING))
          })

setMethod("contextualDeriveTable", "ApplyContext",
          function(context, x) {
              deriveTable(arrayMap(x))
          })

setClass("AtomicApplyContext", contains="HailContext")

setMethod("contextualLength", c("HailPromise", "AtomicApplyContext"),
          function(x, context) 1L)

.ApplyType <- setClass("ApplyType",
                       slots=c(env="environment"),
                       contains="HailType")

setMethod("hailType", "ApplyContext",
          function(x) {
              container <- containerPromise(x)
              parent <- as.environment(hailType(context(container)))
              env <- new.env(parent=parent)
              env[[as.character(argName(x))]] <-
                  elementType(hailType(container))
              .ApplyType(env=env)
          })

as.environment.ApplyType <- function(x) x@env

elementPromise <- function(context) {
    promise <- Promise(HailRef(argName(context)), context)
    if (is(containerPromise(context), "DictPromise"))
        promise <- initialize(promise$value, NAMES=expr(promise$key))
    promise
}

arrayMapExpr <- function(context, expr) {
    argName <- argName(context)
    container <- containerPromise(context)
    identity <- identical(expr, HailRef(argName))
    if (identity) {
        expr(container)
    } else {
        array <- expr(as(container, "ArrayPromise"))
        HailArrayMap(argName, array, expr)
    }
}

arrayMap <- function(body) {
    expr <- arrayMapExpr(context(body), expr(body))
    Promise(expr, context(containerPromise(context(body))))
}

## Another case we may want to handle: df$array[[1]]. Calling `[[` on
## an ArrayPromise could first filter to row 'i', then do
## elementPromise(ApplyContext(x)), but the array map would be
## deferred to fulfill().

## FIXME: lapply() should really return a list, not an ArrayPromise.

## Note that we wrap scalars into arrays for consistency. unlist()
## could be smart enough to coalesce with MakeArray() when it is
## called with a single argument.

setMethod("lapply", "ContainerPromise", function(X, FUN, ...) {
    ans <- FUN(elementPromise(ApplyContext(X)), ...)
    if (is(context(ans), "ApplyContext"))
        arrayMap(ans)
    else as(ans, "ArrayPromise")
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

promiseAccumulation <- function(accumulation, context) {
    Promise(as(accumulation, expressionClass(context)), parent(context))
}

setMethod("contextualLength", c("HailPromise", "AccumulationContext"),
          function(x, context) {
              promiseAccumulation(Accumulation("Count"), context)
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
               promiseMethodCall(e1, op, e2)
           })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transforming
###

setMethod("ifelse2", "BooleanPromise", function(test, yes, no) {
    returnType <- unifyTypes(test, yes, no)
    promiseCall(HailIf,
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
              promiseMethodCall(x, "split", split)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities.
###

## Compare to solrCall()

promiseCall <- function(fun, ..., args = list(...), CALL_CONSTRUCTOR) {
    ctx <- resolveContext(args)
    promises <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    args[promises] <- lapply(args[promises], function(p) expr(push(p, ctx)))
    args <- lapply(args, as, languageClass(ctx), strict=FALSE)
    if (missing(CALL_CONSTRUCTOR)) {
        expr <- do.call(fun, args)
    } else {
        expr <- CALL_CONSTRUCTOR(fun, args)
    }
    Promise(expr, ctx)
}

promiseSeededCall <- function(fun, ...) {
    promiseCall(fun, CALL_CONSTRUCTOR=HailApplySeeded, ...)
}

promiseStaticCall <- function(fun, ...) {
    promiseCall(fun, CALL_CONSTRUCTOR=HailApply, ...)
}

promiseMethodCall <- function(target, fun, ...) {
    promiseCall(fun, CALL_CONSTRUCTOR=HailApply, target, ...)
}

promiseBinaryOpCall <- function(op, left, right, OP_CONSTRUCTOR) {
    promiseCall(op, CALL_CONSTRUCTOR=function(op, args) {
        OP_CONSTRUCTOR(HailSymbol(op), args[[1L]], args[[2L]])
    }, left, right)
}

promiseComparisonOpCall <- function(op, left, right) {
    promiseBinaryOpCall(op, left, right, HailApplyComparisonOp)
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
                        HailApplyBinaryPrimOp)
}

unifyTypes <- function(...) {
    Reduce(merge, lapply(list(...), hailType))
}

promiseUnaryOpCall <- function(op, x) {
    promiseCall(op, CALL_CONSTRUCTOR=function(op, args) {
        HailApplyUnaryPrimOp(HailSymbol(op), args[[1L]])
    }, x)
}
