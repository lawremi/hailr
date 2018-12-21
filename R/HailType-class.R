### =========================================================================
### HailType objects
### -------------------------------------------------------------------------
###
### Formal classes corresponding to types in the Hail expression
### language. There is a class for each relevant Java representation,
### and a corresponding high-level class that models the Java
### object. This makes the types self-documenting. At some point we
### might want to lazily construct and cache the Java representation,
### like the Python interface, but we are not optimizing yet.
###

setClass("is.hail.expr.types.virtual.Type", contains="JavaObject")
setClass("HailType")

setClass("HailTypeList", prototype=list(elementType="HailType"),
         contains="SimpleList")

setClass("HailPrimitiveType", contains=c("HailType", "VIRTUAL"))

setClass("is.hail.expr.types.virtual.TBoolean",
         contains="is.hail.expr.types.virtual.Type")
TBOOLEAN <- setClass("TBoolean", contains="HailPrimitiveType")()

setClass("is.hail.expr.types.virtual.TNumeric",
         contains="is.hail.expr.types.virtual.Type")
setClass("is.hail.expr.types.virtual.TFloat32",
         contains="is.hail.expr.types.virtual.TNumeric")
setClass("is.hail.expr.types.virtual.TFloat64",
         contains="is.hail.expr.types.virtual.TNumeric")
setClass("TNumeric", contains=c("HailPrimitiveType", "VIRTUAL"))
TFLOAT32 <- setClass("TFloat32", contains="TNumeric")()
TFLOAT64 <- setClass("TFloat64", contains="TNumeric")()

setClass("is.hail.expr.types.virtual.TIntegral",
         contains="is.hail.expr.types.virtual.Type")
setClass("is.hail.expr.types.virtual.TInt32",
         contains="is.hail.expr.types.virtual.TIntegral")
setClass("TIntegral", contains=c("TNumeric", "VIRTUAL"))
TINT32 <- setClass("TInt32", contains="TIntegral")()
TINT64 <- setClass("TInt64", contains="TIntegral")()

setClass("is.hail.expr.types.virtual.TString",
         contains="is.hail.expr.types.virtual.Type")
TSTRING <- setClass("TString", contains="HailPrimitiveType")()

## raw vector
setClass("is.hail.expr.types.virtual.TBinary",
         contains="is.hail.expr.types.virtual.Type")
setClass("TBinary", contains="HailType")

setClass("is.hail.expr.types.virtual.TContainer",
         contains="is.hail.expr.types.virtual.Type")
setClass("is.hail.expr.types.virtual.TArray",
         contains="is.hail.expr.types.virtual.TContainer")
setClass("is.hail.expr.types.virtual.TSet",
         contains="is.hail.expr.types.virtual.TContainer")
setClass("is.hail.expr.types.virtual.TDict",
         contains="is.hail.expr.types.virtual.TContainer")
setClass("TContainer", slots=c(elementType="HailType"),
         contains=c("HailType", "VIRTUAL"))
.TArray <- setClass("TArray", contains="TContainer")
setClass("TSet", contains="TContainer")
setClass("TDict", contains="TContainer")

setClass("is.hail.expr.types.virtual.TBaseStruct",
         contains="is.hail.expr.types.virtual.Type")
setClass("is.hail.expr.types.virtual.TTuple",
         contains="is.hail.expr.types.virtual.TBaseStruct")
setClass("is.hail.expr.types.virtual.TStruct",
         contains="is.hail.expr.types.virtual.TBaseStruct")
setClass("TBaseStruct",
         contains=c("HailType", "HailTypeList", "VIRTUAL"))
setClass("TStruct",
         contains="TBaseStruct",
         validity=function(object) {
             if (length(object) > 0L && is.null(names(object)))
                 "TStruct objects must have names"
         })
setClass("TTuple", contains="TBaseStruct")

## Better name might have been 'DecoratedType'; adds semantics
setClass("is.hail.expr.types.virtual.ComplexType",
         contains="is.hail.expr.types.virtual.Type")
setClass("ComplexType", slots=c(representationType="HailType"),
         contains="HailType")

## Basically a 'Ranges'
setClass("is.hail.expr.types.virtual.TInterval",
         contains="is.hail.expr.types.virtual.ComplexType")
setClass("TInterval", slots=c(pointType="HailType"), contains="ComplexType")

## Variant call
setClass("is.hail.expr.types.virtual.TCall",
         contains="is.hail.expr.types.virtual.ComplexType")
setClass("TCall", contains="ComplexType")

## Defined by a genome, chromosome, and position (like a SNP)
setClass("is.hail.expr.types.virtual.TLocus",
         contains="is.hail.expr.types.virtual.ComplexType")
setClass("TLocus", contains="ComplexType")

setClass("is.hail.expr.types.TableType",
         contains="is.hail.expr.types.virtual.Type")
setClass("TableType", slots=c(rowType="TStruct", globalType="TStruct",
                              keys="character"),
         contains="HailType")

## The schema of a MatrixTable
setClass("is.hail.expr.types.MatrixType",
         contains="is.hail.expr.types.virtual.Type")
setClass("MatrixType",
         slots=c(globalType="TStruct",
                 colTableType="TableType",
                 rowTableType="TableType",
                 entryType="TStruct",
                 rowPartitionKey="character"),
         contains="HailType")

## Hail supports registering functions for use in the expression runtime.
## We should be able to call these dynamically at least. Ideally,
## we could define functions using R code.
setClass("is.hail.expr.types.virtual.TFunction",
         contains="is.hail.expr.types.virtual.Type")
setClass("TFunction", slots=c(paramTypes="HailTypeList", returnType="HailType"),
         contains="HailType")

setClass("is.hail.expr.types.virtual.TVariable",
         contains="is.hail.expr.types.virtual.Type")
setClass("TVariable", contains="HailType")

setClass("is.hail.expr.types.virtual.TVoid",
         contains="is.hail.expr.types.virtual.Type")
setClass("TVoid", contains="HailType")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors (could be memoized)
###

setGeneric("hailType", function(x) standardGeneric("hailType"))

setMethod("hailType", "logical", function(x) TBOOLEAN)
setMethod("hailType", "numeric", function(x) TFLOAT64)
setMethod("hailType", "integer", function(x) TINT32)
setMethod("hailType", "character", function(x) TSTRING)
setMethod("hailType", "array", function(x) TTuple(hailType(x[0L])))
setMethod("hailType", "list",
          function(x) TArray(as(elementType(x), "HailType")))

## Compare to rsolr 'solrMode()'
setGeneric("vectorMode", function(x) standardGeneric("vectorMode"))

setMethod("vectorMode", "TBoolean", function(x) "logical")
setMethod("vectorMode", "TFloat64", function(x) "numeric")
setMethod("vectorMode", "TFloat32", function(x) "numeric")
setMethod("vectorMode", "TInt32", function(x) "integer")
setMethod("vectorMode", "TString", function(x) "character")

TArray <- function(elementType) {
    .TArray(elementType=elementType)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("elementType", "TContainer", function(x) x@elementType)

representationType <- function(x) x@representationType

## These types can be e.g. a TIntegral, or a TLocus (links)
pointType <- function(x) x@pointType

rowType <- function(x) x@rowType
globalType <- function(x) x@globalType
keys <- function(x) x@keys
keyType <- function(x) unname(as.list(rowType(x))[keys(x)])

colTableType <- function(x) x@colTableType
rowTableType <- function(x) x@rowTableType
entryType <- function(x) x@entryType
rowPartitionKey <- function(x) x@rowPartitionKey

paramTypes <- function(x) x@paramTypes
returnType <- function(x) x@returnType

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion (between Java and R representations)
###

setAs("is.hail.expr.types.virtual.Type", "HailType", function(from) {
    new(sub("^is\\.hail\\.expr\\.types(\\.virtual)?\\.", "", class(from)))
})

javaHailType <- function(x, jvm) {
    scala_object(jvm$is$hail$expr$types[[class(x)]])
}

setMethod("toJava", "HailType", function(x, jvm) {
    javaHailType(x, jvm)
})

setAs("is.hail.expr.types.virtual.TContainer", "HailType",
      function(from)
          initialize(callNextMethod(),
                     elementType=as(from$elementType(), "HailType")))

setMethod("toJava", "TContainer",
      function(x, jvm) {
          etype <- toJava(elementType(x), jvm)
          callNextMethod()$apply(etype, FALSE)
      })

setAs("is.hail.expr.types.virtual.TBaseStruct", "HailTypeList", function(from) {
    as(lapply(from$types(), as, "HailType"), "List")
})

setAs("is.hail.expr.types.virtual.TStruct", "HailTypeList", function(from) {
    setNames(callNextMethod(), from$fieldNames())
})

setAs("is.hail.expr.types.virtual.TBaseStruct", "HailType", function(from) {
    initialize(callNextMethod(), as(from, "HailTypeList"))
})

setMethod("toJava", "TBaseStruct",
      function(x, jvm) {
          fieldTypes <- lapply(x, toJava, jvm)
          obj <- callNextMethod()
          if (!is.null(names(x)))
              obj$apply(JavaArrayList(names(x)), JavaArrayList(fieldTypes),
                        FALSE)
          else obj$apply(JavaArrayList(fieldTypes), FALSE)
      })

setAs("is.hail.expr.types.virtual.ComplexType", "HailType",
      function(from) {
          representationType <- as(from$representation(), "HailType")
          initialize(callNextMethod(), representationType=representationType)
      })

setMethod("toJava", "TInterval",
      function(x, jvm) {
          pointType <- toJava(pointType(x), jvm)
          callNextMethod()$apply(pointType, FALSE)
      })

setAs("is.hail.expr.types.virtual.TInterval", "HailType",
      function(from) {
          pointType <- as(from$pointType(), "HailType")
          initialize(callNextMethod(), pointType=pointType)
      })

setAs("is.hail.expr.types.TableType", "HailType",
      function(from) {
          rowType <- as(from$rowType(), "HailType")
          globalType <- as(from$globalType(), "HailType")
          ## FIXME: as.character() needed due to limitation in sparklyr (#1558)
          keys <- character(0L) # as.character(from$key()$getOrElse(NULL))
          initialize(callNextMethod(), rowType=rowType, globalType=globalType,
                     keys=keys)
      })

setMethod("toJava", "TableType",
      function(x, jvm) {
          rowType <- toJava(rowType(x), jvm)
          keys <- keys(x)
          globalType <- toJava(globalType(x), jvm)
          callNextMethod()$apply(rowType, keys, globalType)
      })

setAs("is.hail.expr.types.virtual.TFunction", "HailType", function(from) {
    paramTypes <- lapply(from$paramTypes(), as, "HailType")
    returnType <- as(from$returnType(), "HailType")
    initialize(callNextMethod(), paramTypes=paramTypes, returnType=returnType)
})

setMethod("toJava", "TFunction",
      function(x, jvm) {
          paramTypes <- lapply(paramTypes(x), toJava, jvm)
          returnType <- toJava(returnType(x), jvm)
          callNextMethod()$apply(paramTypes, returnType)
      })

setAs("is.hail.expr.types.MatrixType", "HailType",
      function(from) {
          globalType <- as(from$globalType(), "HailType")
          colTableType <- as(from$colsTableType(), "HailType")
          rowTableType <- as(from$rowsTableType(), "HailType")
          entryType <- as(from$entryType(), "HailType")
          rowPartitionKey <- from$rowPartitionKey()
          initialize(callNextMethod(),
                     globalType=globalType,
                     colTableType=colTableType,
                     rowTableType=rowTableType,
                     entryType=entryType, rowPartitionKey=rowPartitionKey)
      })

setMethod("toJava", "MatrixType",
      function(x, jvm) {
          colTableType <- toJava(colTableType(x), jvm)
          globalType <- globalType(colTableType)
          colKey <- keys(colTableType)
          colType <- rowType(colTableType)
          rowPartitionKey <- rowPartitionKey(x)
          rowTableType <- toJava(rowTableType(x), jvm)
          rowKey <- keys(rowTableType)
          rvRowType <- rowType(rowTableType)
          entryType <- toJava(entryType(x), jvm)
          mt <- scala_object(jvm$is$hail$expr$types$MatrixType)
          rvRowType[[mt$entriesIdentifier()]] <- entryType
          callNextMethod()$apply(globalType, colKey, colType,
                                 rowPartitionKey, rowKey, rvRowType)
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("character", "HailType", function(from) as(getClass(from), "HailType"))

setAs("classRepresentation", "HailType", function(from) hailType(new(from)))

baseTypeName <- function(x) sub("^T", "", class(x))
setMethod("as.character", "HailType", baseTypeName)
setMethod("as.character", "TContainer", function(x) {
    paste0(callNextMethod(), "[", elementType(x), "]")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Merging and munging
###

setGeneric("numericType",
           function(x) stop("Type '", class(x), "' is not numeric"))

setMethod("numericType", "TBoolean", function(x) TINT32)
setMethod("numericType", "TNumeric", function(x) x)
setMethod("numericType", "TArray", function(x) numericType(elementType(x)))

typeOrder <- c("TBoolean", "TInt32", "TInt64", "TFloat32", "TFloat64",
               "TString")
setGeneric("typeOrdinal", function(x) standardGeneric("typeOrdinal"))
lapply(seq_along(typeOrder), function(i) {
    setMethod("typeOrdinal", typeOrder[i], function(x) i)
})
setMethod("typeOrdinal", "HailType", function(x) NA_integer_)

setMethod("merge", c("HailType", "HailType"), function(x, y) {
    stop("Cannot unify type '", x, "' with '", y, "'")
})

setMethod("merge", c("HailPrimitiveType", "HailPrimitiveType"), function(x, y) {
    if (typeOrdinal(x) > typeOrdinal(y)) x else y
})

setMethod("merge", c("TArray", "TArray"), function(x, y) {
    TArray(merge(elementType(x), elementType(y)))
})

setMethod("merge", c("HailType", "TArray"), function(x, y) {
    TArray(merge(x, elementType(y)))
})

setMethod("merge", c("TArray", "HailType"), function(x, y) {
    TArray(merge(elementType(x), y))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

promiseClass <- function(x) {
    paste0(baseTypeName(x), "Promise")
}

typeClass <- function(x) {
    paste0("T", sub("Promise$", "", class(x)))
}
