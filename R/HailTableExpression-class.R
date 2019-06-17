### =========================================================================
### HailTableExpression objects
### -------------------------------------------------------------------------
###
### Expressions in the Hail language that manipulate HailTables.
###
### Separated from the rest of the HailExpression framework for clarity.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

setClass("is.hail.expr.ir.TableIR", contains="is.hail.expr.ir.BaseIR")

setClass("HailTableExpression",
         contains=c("HailExpression", "VIRTUAL"))

setClassUnion("HailTableExpression_OR_TableIR",
              c("HailTableExpression", "is.hail.expr.ir.TableIR"))

setClass("UnaryHailTableExpression",
         slots=c(child="HailTableExpression_OR_TableIR"),
         contains=c("HailTableExpression", "VIRTUAL"))

## Embeds a literal HailTable that is substituted into the expression
## on the JVM side.
.HailJavaTable <- setClass("HailJavaTable",
                           slots=c(child="is.hail.expr.ir.TableIR"),
                           contains="UnaryHailTableExpression")

.HailTableKeyBy <- setClass("HailTableKeyBy",
                            slots=c(keys="CharacterList", is_sorted="logical"),
                            prototype=prototype(is_sorted=FALSE),
                            contains="UnaryHailTableExpression",
                            validity=function(object) {
                                c(if (!isTRUEorFALSE(object@is_sorted))
                                    "@is_sorted must be TRUE or FALSE",
                                  if (anyNA(unlist(object@keys)))
                                    "@keys must not contain NAs")  
                            })

.HailTableMapRows <- setClass("HailTableMapRows",
                              slots=c(expr="HailExpression"),
                              contains="UnaryHailTableExpression")

.HailTableMapGlobals <- setClass("HailTableMapGlobals",
                                 slots=c(expr="HailExpression"),
                                 contains="UnaryHailTableExpression")

.HailTableFilter <- setClass("HailTableFilter",
                             slots=c(pred="HailExpression"),
                             contains="UnaryHailTableExpression")

.HailTableJoin <- setClass("HailTableJoin",
                           slots=c(type="character", key="character",
                                   left="HailTableExpression",
                                   right="HailTableExpression"),
                           contains="HailTableExpression",
                           validity=function(object) {
                               c(if (!isSingleString(object@type))
                                   "@type must be single, non-NA string",
                                 if (anyNA(object@keys))
                                   "@keys must not contain NAs")
                           })

.HailTableHead <- setClass("HailTableHead",
                           slots=c(n="integer"),
                           validity=function(object) {
                               if (!isSingleInteger(object@n))
                                   "@n must be single, non-NA integer"
                           },
                           contains="UnaryHailTableExpression")

.HailTableCollect <- setClass("HailTableCollect",
                              slots=c(child="HailTableExpression"),
                              contains="HailExpression")

.HailTableCount <- setClass("HailTableCount",
                            slots=c(child="HailTableExpression"),
                            contains="HailExpression")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

HailJavaTable <- function(child) {
    .HailJavaTable(child=child)
}

HailTableMapRows <- function(child, expr) {
    .HailTableMapRows(child=child, expr=expr)
}

HailTableMapGlobals <- function(child, expr) {
    .HailTableMapGlobals(child=child, expr=expr)
}

HailTableKeyBy <- function(child, keys, is_sorted=FALSE) {
    .HailTableKeyBy(child=as(child, "HailTableExpression", strict=FALSE),
                    keys=as(keys, "CharacterList"), is_sorted=is_sorted)
}

HailTableFilter <- function(child, pred) {
    .HailTableFilter(child=child, pred=pred)
}

HailTableJoin <- function(left, right, type, key) {
    .HailTableJoin(left=left, right=right, type=type, key=key)
}

HailTableCount <- function(child) {
    .HailTableCount(child=child)
}

HailTableHead <- function(child, n) {
    .HailTableHead(child=child, n=n)
}

HailTableCollect <- function(child) {
    .HailTableCollect(child=child)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

child <- function(x) x@child

setMethod("context", "is.hail.expr.ir.TableIR", function(x) {
    HailContext(jvm(x))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Type inference
###

setMethod("hailType", "is.hail.expr.ir.TableIR", function(x) as(x$typ(), "HailType"))

setMethod("inferHailType", "UnaryHailTableExpression", function(x, env) {
    hailType(child(x))
})

as.environment.TableType <- function(x) {
    list(global=globalType(x), row=rowType(x))
}

setMethod("inferHailType", "HailTableMapRows", function(x, env) {
    old_type <- callNextMethod()
    initialize(old_type,
               rowType = inferHailType(expr(x), as.environment(old_type)))
})

setMethod("inferHailType", "HailTableMapGlobals", function(x, env) {
    old_type <- callNextMethod()
    initialize(old_type,
               globalType = inferHailType(expr(x), as.environment(old_type)))
})

setMethod("inferHailType", "HailTableKeyBy", function(x, env) {
    type <- callNextMethod()
    keys(type) <- x@keys
    type
})

setMethod("inferHailType", "HailTableJoin", function(x, env) {
    lt <- hailType(x@left)
    rt <- hailType(x@right)
    TableType(rowType=c(keyType(lt), valueType(lt), valueType(rt)),
              globalType=c(globalType(lt), globalType(rt)),
              keys=unique(c(keys(lt), keys(rt))))
})

setMethod("inferHailType", "HailTableCount", function(x, env) TINT64)

setMethod("inferHailType", "HailTableCollect", function(x, env) {
    ttable <- hailType(child(x))
    TStruct(rows=TArray(rowType(ttable)), globals=globalType(ttable))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("to_ir", "is.hail.expr.ir.TableIR", function(x, substitutor) {
    substitutor$substitute(x, "table")
})

setMethod("toJava", "HailExpression", function(x, jvm) {
    substitutor <- Substitutor()
    ir <- to_ir(x, substitutor)
    PARSE <- jvm$is$hail$expr$ir$IRParser[[parseMethod(x)]]
    jir <- PARSE(ir, JavaHashMap(list()),
                 JavaHashMap(substitutor$substitutions))
    toJava(jir)
})

setGeneric("parseMethod", function(x) standardGeneric("parseMethod"))
setMethod("parseMethod", "HailExpression", function(x) "parse_value_ir")
setMethod("parseMethod", "HailTableExpression", function(x) "parse_table_ir")

setMethod("toJava", "HailJavaTable", function(x, jvm) toJava(child(x)))

setMethod("eval", c("HailTableExpression", "HailContext"),
          function(expr, envir, enclos) {
              hc <- envir$impl
              HailTable(jvm(hc)$is$hail$table$Table$new(hc, expr))
          })

setAs("is.hail.expr.ir.TableIR", "HailTableExpression", function(from) {
    HailJavaTable(from)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

.Substitutor <-
    setRefClass("Substitutor",
                fields = c(count="integer",
                           substitutions="list"),
                methods = list(
                    substitute = function(x, label) {
                        key <- paste0("__", label, "_", .self$count)
                        .self$substitutions[[key]] <- x
                        .self$count <- .self$count + 1L
                        key
                    }
                ))

Substitutor <- function() {
    .Substitutor(count=0L)
}
