### =========================================================================
### HailType objects
### -------------------------------------------------------------------------
###
### Formal classes corresponding to types in the Hail expression
### language. There is a class for each relevant Java representation,
### and a corresponding high-level class that wraps the Java
### object. The wrapper classes could cache more information about the
### type, but we have not gone there yet.
###

### Question: should the high-level HailType be backed by the Java
### object, or should it be independent, with coercions? The "fat" R
### object has the benefit of being self-documenting, with the typed
### slots listed. Are there cases where we will have the type at the R
### level, without need for a backing object? Often, the type will be
### indirectly stored via the promise type. The Python interface uses
### a fat Python representation, with lazy, cached construction of the
### Java type. We could do that with reference classes. We could also
### create singletons for the non-parameterized types.

setClass("is.hail.expr.types.Type", contains="JavaObject")
setClass("HailType", slots=c(impl="is.hail.expr.types.Type"))

setClass("HailTypeList", prototype=list(elementType="HailType"),
         contains="SimpleList")

## Except for TBinary, these all refer to scalars (per row)

setClass("is.hail.expr.types.TBoolean", contains="is.hail.expr.types.Type")
.TBoolean <- setClass("TBoolean", contains="HailType")
setAs("is.hail.expr.types.TBoolean", "HailType",
      function(from) .TBoolean())
setAs("TBoolean", "is.hail.expr.types.Type",
      function(from) .javaHailType(from))

setClass("is.hail.expr.types.TNumeric", contains="is.hail.expr.types.Type")
.HailNumericType <- setClass("HailNumericType", contains="HailType")
setAs("is.hail.expr.types.TNumeric", "HailType",
      function(from) .HailNumericType())
setAs("HailNumericType", "is.hail.expr.types.Type",
      function(from) .javaHailTypeForName("TFloat64"))

setClass("is.hail.expr.types.TIntegral", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TInt32", contains="is.hail.expr.types.TIntegral")
.HailIntegralType <- setClass("HailIntegralType", contains="HailType")
.HailInt32Type <- setClass("HailInt32Type", contains="HailIntegralType")
.HailInt64Type <- setClass("HailInt64Type", contains="HailIntegralType")

setAs("is.hail.expr.types.TIntegral", "HailType",
      function(from) .HailIntegralType())
setAs("HailInt32Type", "is.hail.expr.types.Type",
      function(from) .javaHailTypeForName("TInt32"))

setClass("is.hail.expr.types.TString", contains="is.hail.expr.types.Type")
.HailStringType <- setClass("HailStringType", contains="HailType")
setAs("is.hail.expr.types.TString", "HailType",
      function(from) .HailStringType())
setAs("HailStringType", "is.hail.expr.types.Type",
      function(from) .javaHailTypeForName("TString"))

## raw vector
setClass("is.hail.expr.types.TBinary", contains="is.hail.expr.types.Type")
.HailBinaryType <- setClass("HailBinaryType", contains="HailType")
setAs("is.hail.expr.types.TBinary", "HailType",
      function(from) .HailBinaryType())
setAs("HailBinaryType", "is.hail.expr.types.Type",
      function(from) .javaHailTypeForName("TBinary"))

setClass("is.hail.expr.types.TContainer", contains="is.hail.expr.types.Type")
.HailContainerType <- setClass("HailContainerType",
                               slots=c(elementType="HailType"),
                               contains="HailType")
setMethod("elementType", "HailContainerType", function(x) x@elementType)
setAs("is.hail.expr.types.TContainer", "HailType",
      function(from)
          .HailContainerType(elementType=as(from$elementType(), "HailType")))

.HailArrayType <- setClass("HailArrayType", contains="HailContainerType")

setAs("HailContainerType", "is.hail.expr.types.Type",
      function(from) {
          etype <- as(elementType(from), "is.hail.expr.types.Type")
          .javaHailTypeForName("TArray")$apply(etype, FALSE)
      })

## Describes a matrix
setClass("is.hail.expr.types.TAggregable",
         contains="is.hail.expr.types.TContainer")
.HailAggregableType <- setClass("HailAggregableType",
                                contains="HailContainerType")
setAs("is.hail.expr.types.TAggregable", "HailType",
      function(from) .HailAggregableType(callNextMethod()))

setClass("is.hail.expr.types.TBaseStruct", contains="is.hail.expr.types.Type")
.HailBaseStructType <- setClass("HailBaseStructType",
                                prototype=prototype(elementType="HailType"),
                                contains=c("HailType", "HailTypeList"))

setAs("is.hail.expr.types.TBaseStruct", "HailType", function(from) {
    fieldTypes <- lapply(from$fieldTypes(), as, "HailType")
    .HailBaseStructType(as(fieldTypes, "List"))
})

setClass("is.hail.expr.types.TStruct",
         contains="is.hail.expr.types.TBaseStruct")
.HailStructType <- setClass("HailStructType", contains="HailBaseStructType")
setAs("is.hail.expr.types.TStructType", "HailType",
      function(from) {
          ans <- .HailStructType(callNextMethod())
          names(ans) <- from$fieldNames()
          ans
      })

## Better name might have been 'DecoratedType'; adds semantics
setClass("is.hail.expr.types.ComplexType", contains="is.hail.expr.types.Type")
.HailComplexType <- setClass("HailComplexType",
                             slots=c(representationType = "HailType"),
                             contains="HailType")
setAs("is.hail.expr.types.TComplexType", "HailType",
      function(from) {
          .HailComplexType(representationType=as(from$representation(),
                                                 "HailType"))
      })

representationType <- function(x) x@representationType

## Basically a 'Ranges'
setClass("is.hail.expr.types.TInterval",
         contains="is.hail.expr.types.ComplexType")
.HailIntervalType <- setClass("HailIntervalType",
                              slots=c(startType="HailType",
                                      endType="HailType"),
                              contains="HailComplexType")
setAs("is.hail.expr.types.TInterval", "HailType",
      function(from) {
          startType <- as(from$representation()$field("start")$typ(),
                          "HailType")
          endType <- as(from$representation()$field("end")$typ(),
                        "HailType")
          .HailIntervalType(callNextMethod(),
                            startType=startType,
                            endType=endType)
      })

## These types can be e.g. a TIntegral, or a TLocus (links)
startType <- function(x) x@startType
endType <- function(x) x@endType

## Variant call
setClass("is.hail.expr.types.TCall", contains="is.hail.expr.types.ComplexType")
.HailCallType <- setClass("HailCallType", contains="HailComplexType")
setAs("is.hail.expr.types.TCall", "HailType",
      function(from) .HailCallType(callNextMethod()))

## Defined by a genome, chromosome, and position (like a SNP)
setClass("is.hail.expr.types.TLocus", contains="is.hail.expr.types.ComplexType")
setClass("HailLocusType",  contains="HailComplexType")
setAs("is.hail.expr.types.TLocus", "HailType",
      function(from) .HailLocusType(callNextMethod()))

setClass("is.hail.expr.types.TableType", contains="is.hail.expr.types.Type")
.HailTableType <- setClass("HailTableType",
                           slots=c(rowType="HailType", globalType="HailType",
                                   keys="character"),
                           contains="HailType")

setAs("is.hail.expr.types.TableType", "HailType",
      function(from) {
          rowType <- as(from$rowType(), "HailType")
          globalType <- as(from$globalType(), "HailType")
          keys <- from$keys()
          .HailTableType(rowType=rowType, globalType=globalType, keys=keys)
      })

rowType <- function(x) x@rowType
globalType <- function(x) x@globalType
keys <- function(x) x@keys

## The schema of a MatrixTable
setClass("is.hail.expr.types.MatrixType", contains="is.hail.expr.types.Type")
.HailMatrixType <- setClass("HailMatrixType", contains="HailType")
setAs("is.hail.expr.types.MatrixType", "HailType",
      function(from) {
          colTableType <- as(from$colsTableType(), "HailType")
          rowTableType <- as(from$rowsTableType(), "HailType")
          entryType <- as(from$entryType(), "HailType")
          rowPartitionKey <- from$rowPartitionKey()
          .HailMatrixType(colTableType=colTableType, rowTableType=rowTableType,
                          entryType=entryType, rowPartitionKey=rowPartitionKey)
      })

colTableType <- function(x) x@colTableType
rowTableType <- function(x) x@rowTableType
entryType <- function(x) x@entryType
rowPartitionKey <- function(x) x@rowPartitionKey

## Hail supports registering functions for use in the expression runtime.
## We should be able to call these dynamically at least. Ideally,
## we could define functions using R code.
setClass("is.hail.expr.types.TFunction", contains="is.hail.expr.types.Type")
.HailFunctionType <- setClass("HailFunctionType",
                              slots=c(paramTypes="HailTypeList",
                                      returnType="HailType"),
                              contains="HailType")
setAs("is.hail.expr.types.TFunction", "HailType", function(from) {
    paramTypes <- lapply(from@$paramTypes(), as, "HailType")
    returnType <- as(from$returnType(), "HailType")
    .HailFunctionType(paramTypes=paramTypes, returnType=returnType)
})

paramTypes <- function(x) x@paramTypes
returnType <- function(x) x@returnType

setClass("is.hail.expr.types.TVariable", contains="is.hail.expr.types.Type")
.HailVariableType <- setClass("HailVariableType", contains="HailType")
setAs("is.hail.expr.types.TVariable", "HailType",
      function(from) .HailVariableType())

setClass("is.hail.expr.types.TVoid", contains="is.hail.expr.types.Type")
.HailVoidType <- setClass("HailVoidType", contains="HailType")
setAs("is.hail.expr.types.TVoid", "HailType",
      function(from) .HailVoidType())

javaHailType <- function(x) {
    .javaHailTypeForName(.javaHailTypeName(x))
}

javaHailTypeName <- function(x) {
    paste0("T", sub("Hail(.*?)Type", "\\1", class(x)))
}

javaHailTypeForName <- function(typeName) {
    scala_object(jvm(hail_context())$is$hail$expr$types[[typeName]])
}
