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

setClass("is.hail.expr.types.Type", contains="JavaObject")
setClass("HailType", slots=c(impl="is.hail.expr.types.Type"))

setClass("HailTypeList", prototype=list(elementType="HailType"),
         contains="SimpleList")

setClass("is.hail.expr.types.TBoolean", contains="is.hail.expr.types.Type")
.TBoolean <- setClass("TBoolean", contains="HailType")

setClass("is.hail.expr.types.TNumeric", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TFloat32", contains="is.hail.expr.types.TNumeric")
setClass("is.hail.expr.types.TFloat64", contains="is.hail.expr.types.TNumeric")
setClass("TNumeric", contains="HailType")
setClass("TFloat32", contains="TNumeric")
.TFloat64 <- setClass("TFloat64", contains="TNumeric")

setClass("is.hail.expr.types.TIntegral", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TInt32", contains="is.hail.expr.types.TIntegral")
setClass("TIntegral", contains="HailType")
.TInt32 <- setClass("TInt32", contains="TIntegral")
setClass("TInt64", contains="TIntegral")

setClass("is.hail.expr.types.TString", contains="is.hail.expr.types.Type")
.TString <- setClass("TString", contains="HailType")

## raw vector
setClass("is.hail.expr.types.TBinary", contains="is.hail.expr.types.Type")
setClass("TBinary", contains="HailType")

setClass("is.hail.expr.types.TContainer", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TArray", contains="is.hail.expr.types.TContainer")
setClass("is.hail.expr.types.TSet", contains="is.hail.expr.types.TContainer")
setClass("is.hail.expr.types.TDict", contains="is.hail.expr.types.TContainer")
setClass("TContainer", slots=c(elementType="HailType"), contains="HailType")
setClass("TArray", contains="TContainer")
setClass("TSet", contains="TContainer")
setClass("TDict", contains="TContainer")

## Describes a matrix
setClass("is.hail.expr.types.TAggregable",
         contains="is.hail.expr.types.TContainer")
setClass("TAggregable", contains="TContainer")

setClass("is.hail.expr.types.TBaseStruct", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TTuple", contains="is.hail.expr.types.TBaseStruct")
setClass("is.hail.expr.types.TStruct",
         contains="is.hail.expr.types.TBaseStruct")
setClass("TBaseStruct",
         prototype=prototype(elementType="HailType"),
         contains=c("HailType", "HailTypeList"))
setClass("TStruct", contains="TBaseStruct")
setClass("TTuple", contains="TBaseStruct")

## Better name might have been 'DecoratedType'; adds semantics
setClass("is.hail.expr.types.ComplexType", contains="is.hail.expr.types.Type")
setClass("TComplex", slots=c(representationType="HailType"),
         contains="HailType")

## Basically a 'Ranges'
setClass("is.hail.expr.types.TInterval",
         contains="is.hail.expr.types.ComplexType")
setClass("TInterval", slots=c(pointType="HailType"), contains="TComplex")

## Variant call
setClass("is.hail.expr.types.TCall", contains="is.hail.expr.types.ComplexType")
setClass("TCall", contains="TComplex")

## Defined by a genome, chromosome, and position (like a SNP)
setClass("is.hail.expr.types.TLocus", contains="is.hail.expr.types.ComplexType")
setClass("TLocus",  contains="TComplex")

setClass("is.hail.expr.types.TableType", contains="is.hail.expr.types.Type")
setClass("RowType", contains="TStruct")
setClass("GlobalType", contains="TStruct")
setClass("TableType", slots=c(rowType="RowType", globalType="GlobalType",
                              keys="character"),
         contains="HailType")

## The schema of a MatrixTable
setClass("is.hail.expr.types.MatrixType", contains="is.hail.expr.types.Type")
setClass("MatrixType", contains="HailType")

## Hail supports registering functions for use in the expression runtime.
## We should be able to call these dynamically at least. Ideally,
## we could define functions using R code.
setClass("is.hail.expr.types.TFunction", contains="is.hail.expr.types.Type")
setClass("TFunction", slots=c(paramTypes="HailTypeList", returnType="HailType"),
         contains="HailType")

setClass("is.hail.expr.types.TVariable", contains="is.hail.expr.types.Type")
setClass("TVariable", contains="HailType")

setClass("is.hail.expr.types.TVoid", contains="is.hail.expr.types.Type")
setClass("TVoid", contains="HailType")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors (could be memoized)
###

setGeneric("hailType", function(x) standardGeneric("hailType"))

setMethod("hailType", "logical", function(x) .TBoolean())
setMethod("hailType", "numeric", function(x) .TFloat64())
setMethod("hailType", "integer", function(x) .TInt32())
setMethod("hailType", "character", function(x) .TString())

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

colTableType <- function(x) x@colTableType
rowTableType <- function(x) x@rowTableType
entryType <- function(x) x@entryType
rowPartitionKey <- function(x) x@rowPartitionKey

paramTypes <- function(x) x@paramTypes
returnType <- function(x) x@returnType

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion (between Java and R representations)
###

setAs("is.hail.expr.types.Type", "HailType", function(from) {
    new(sub("^is.hail.expr.types.", "", class(from)))
})

javaHailType <- function(x) {
    scala_object(jvm(hail_context())$is$hail$expr$types[[class(x)]])
}

setAs("HailType", "is.hail.expr.types.Type", function(from) {
    javaHailType(from)
})

setAs("is.hail.expr.types.TContainer", "HailType",
      function(from)
          initialize(callNextMethod(),
                     elementType=as(from$elementType(), "HailType")))

setAs("TContainer", "is.hail.expr.types.Type",
      function(from) {
          etype <- as(elementType(from), "is.hail.expr.types.Type")
          callNextMethod()$apply(etype, FALSE)
      })

setAs("is.hail.expr.types.TBaseStruct", "HailType", function(from) {
    fieldTypes <- as(lapply(from$fieldTypes(), as, "HailType"), "List")
    initialize(callNextMethod(), fieldTypes)
})

setAs("TBaseStruct", "is.hail.expr.types.Type",
      function(from) {
          fieldTypes <- lapply(from, as, "is.hail.expr.types.Type")
          obj <- callNextMethod()
          if (!is.null(names(from)))
              obj$apply(JavaArrayList(names(from)), JavaArrayList(fieldTypes),
                        FALSE)
          else obj$apply(JavaArrayList(fieldTypes), FALSE)
      })

setAs("is.hail.expr.types.TStruct", "HailType",
      function(from) {
          ans <- callNextMethod()
          names(ans) <- from$fieldNames()
          ans
      })

setAs("is.hail.expr.types.TComplexType", "HailType",
      function(from) {
          representationType <- as(from$representation(), "HailType")
          initialize(callNextMethod(), representationType=representationType)
      })

setAs("TInterval", "is.hail.expr.types.Type",
      function(from) {
          pointType <- as(pointType(from), "is.hail.expr.types.Type")
          callNextMethod()$apply(pointType, FALSE)
      })

setAs("is.hail.expr.types.TInterval", "HailType",
      function(from) {
          pointType <- as(from$$pointType(), "HailType")
          initialize(callNextMethod(), pointType=pointType)
      })

setAs("is.hail.expr.types.TableType", "HailType",
      function(from) {
          rowType <- as(from$rowType(), "HailType")
          globalType <- as(from$globalType(), "HailType")
          keys <- from$keys()
          initialize(callNextMethod(), rowType=rowType, globalType=globalType,
                     keys=keys)
      })

setAs("TableType", "is.hail.expr.types.Type",
      function(from) {
          rowType <- as(rowType(from), "is.hail.expr.types.Type")
          keys <- keys(from)
          globalType <- as(globalType(from), "is.hail.expr.types.Type")
          callNextMethod()$apply(rowType, keys, globalType)
      })

setAs("is.hail.expr.types.TFunction", "HailType", function(from) {
    paramTypes <- lapply(from@$paramTypes(), as, "HailType")
    returnType <- as(from$returnType(), "HailType")
    initialize(callNextMethod(), paramTypes=paramTypes, returnType=returnType)
})

setAs("TFunction", "is.hail.expr.types.Type",
      function(from) {
          paramTypes <- lapply(paramTypes(from), as, "is.hail.expr.types.Type")
          returnType <- as(returnType(from), "is.hail.expr.types.Type")
          callNextMethod()$apply(paramTypes, returnType)
      })

setAs("is.hail.expr.types.MatrixType", "HailType",
      function(from) {
          colTableType <- as(from$colsTableType(), "HailType")
          rowTableType <- as(from$rowsTableType(), "HailType")
          entryType <- as(from$entryType(), "HailType")
          rowPartitionKey <- from$rowPartitionKey()
          initialize(callNextMethod(), colTableType=colTableType,
                     rowTableType=rowTableType,
                     entryType=entryType, rowPartitionKey=rowPartitionKey)
      })

setAs("MatrixType", "is.hail.expr.types.Type",
      function(from) {
          colTableType <- as(colTableType(from), "is.hail.expr.types.Type")
          globalType <- globalType(colTableType)
          colKey <- keys(colTableType)
          colType <- rowType(colTableType)
          rowPartitionKey <- rowPartitionKey(from)
          rowTableType <- as(rowTableType(from), "is.hail.expr.types.Type")
          rowKey <- keys(rowTableType)
          rvRowType <- rowType(rowTableType)
          entryType <- as(entryType(from), "is.hail.expr.types.Type")
          mt <- scala_object(jvm(hail_context())is$hail$expr$types$MatrixType)
          rvRowType[[mt$entriesIdentifier()]] <- entryType
          callNextMethod()$apply(globalType, colKey, colType,
                                 rowPartitionKey, rowKey, rvRowType)
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

promiseClass <- function(x) {
    paste0(sub("^T", "", class(x)), "Promise")
}
