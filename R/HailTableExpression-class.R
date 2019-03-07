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

setClass("is.hail.expr.ir.TableIR", contains="JavaObject")

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

.HailMapRows <- setClass("HailMapRows",
                         slots=c(expr="HailExpression"),
                         contains="UnaryHailTableExpression")

.HailMapGlobals <- setClass("HailMapGlobals",
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

HailJavaTable <- function(child) {
    .HailJavaTable(child=child)
}

HailMapRows <- function(child, expr) {
    .HailMapRows(child=child, expr=expr)
}

HailMapGlobals <- function(child, expr) {
    .HailMapGlobals(child=child, expr=expr)
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

child <- function(x) x@child

setMethod("hailType", "is.hail.expr.ir.TableIR",
          function(x) as(x$typ(), "HailType"))

setMethod("hailType", "UnaryHailTableExpression", function(x) {
    hailType(child(x))
})

setMethod("hailType", "HailTableKeyBy", function(x) {
    type <- callNextMethod()
    keys(type) <- x@keys
    type
})

setMethod("hailType", "HailTableJoin", function(x) {
    lt <- hailType(x@left)
    rt <- hailType(x@right)
    TableType(rowType=c(keyType(lt), valueType(lt), valueType(rt)),
              globalType=c(globalType(lt), globalType(rt)),
              keys=unique(c(keys(lt), keys(rt))))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("to_ir", "is.hail.expr.ir.TableIR", function(x, substitutor) {
    substitutor$substitute(x)
})

setMethod("toJava", "HailTableExpression", function(x, jvm) {
    substitutor <- Substitutor("table")
    ir <- to_ir(x, substitutor)
    jir <- jvm$is$hail$expr$ir$IRParser$
        parse_table_ir(ir, JavaHashMap(list()),
                       JavaHashMap(substitutor$substitutions))
    toJava(jir)
})

setMethod("toJava", "HailJavaTable", function(x, jvm) toJava(child(x)))

setMethod("eval", c("HailTableExpression", "HailContext"),
          function(expr, envir, enclos) {
              hc <- envir$impl
              HailTable(jvm(hc)$is$hail$table$Table$new(hc, expr))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

.Substitutor <-
    setRefClass("Substitutor",
                fields = c(label="character",
                           count="integer",
                           substitutions="list"),
                methods = list(
                    substitute = function(x) {
                        key <- paste0("__", .self$label, "_", .self$count)
                        .self$substitutions[[key]] <- x
                        .self$count <- .self$count + 1L
                        key
                    }
                ))

Substitutor <- function(label) {
    .Substitutor(label=label, count=0L)
}
