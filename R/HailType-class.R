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

## Except for TBinary, these all refer to scalars (per row)

setClass("is.hail.expr.types.TBoolean", contains="is.hail.expr.types.Type")
setClass("HailBooleanType", slots=c(impl="is.hail.expr.types.TBoolean"),
         contains="HailType")

setClass("is.hail.expr.types.TNumeric", contains="is.hail.expr.types.Type")
setClass("HailNumericType", slots=c(impl="is.hail.expr.types.TNumeric"),
         contains="HailType")

setClass("is.hail.expr.types.TIntegral", contains="is.hail.expr.types.Type")
setClass("HailIntegralType", slots=c(impl="is.hail.expr.types.TIntegral"),
         contains="HailType")

setClass("is.hail.expr.types.TString", contains="is.hail.expr.types.Type")
setClass("HailStringType", slots=c(impl="is.hail.expr.types.TString"),
         contains="HailType")

## raw vector
setClass("is.hail.expr.types.TBinary", contains="is.hail.expr.types.Type")
setClass("HailBinaryType", slots=c(type="is.hail.expr.types.TBinary"),
         contains="HailType")

setClass("is.hail.expr.types.TContainer", contains="is.hail.expr.types.Type")
setClass("HailContainerType", slots=c(type="is.hail.expr.types.TContainer"),
         contains=c("HailType", "List"))
setMethod("elementType", "HailContainerType",
          function(x) impl(x)$elementType())

## Describes a matrix
setClass("is.hail.expr.types.TAggregable",
         contains="is.hail.expr.types.TContainer")
setClass("HailAggregableType", slots=c(type="is.hail.expr.types.TAggregable"),
         contains="HailType")

setClass("is.hail.expr.types.TBaseStruct",
         contains="is.hail.expr.types.Type")
setClass("HailBaseStructType", slots=type="is.hail.expr.types.TBaseStruct",
         contains="HailType")

setMethod("length", "HailBaseStructType",
          function(x) length(impl(x)$fieldTypes()))
setMethod("as.list", "HailBaseStructType",
          function(x) lapply(impl(x)$fieldTypes(), HailType))

setClass("is.hail.expr.types.TStruct",
         contains="is.hail.expr.types.TBaseStruct")
setClass("HailStructType", slots=c(type="is.hail.expr.types.TStruct"),
         contains="HailBaseStructType")

setMethod("names", "HailStructType", function(x) impl(x)$fieldNames())

## Better name might have been 'DecoratedType'; adds semantics
setClass("is.hail.expr.types.ComplexType", contains="is.hail.expr.types.Type")
setClass("HailComplexType", slots=c(type="is.hail.expr.types.ComplexType"),
         contains="HailType")

representationType <- function(x) HailType(impl(x)$representation())

## Basically a 'Ranges'
setClass("is.hail.expr.types.TInterval",
         contains="is.hail.expr.types.ComplexType")
setClass("HailIntervalType", slots=c(type="is.hail.expr.types.TInterval"),
         contains="HailComplexType")

## These types can be e.g. a TIntegral, or a TLocus (links)
startType <- function(x) impl(representationType(x))$field("start")$typ()
endType <- function(x) impl(representationType(x))$field("end")$typ()

## Variant call
setClass("is.hail.expr.types.TCall", contains="is.hail.expr.types.ComplexType")
setClass("HailCallType", slots=c(type="is.hail.expr.types.TCall"),
         contains="HailComplexType")

## Defined by a genome, chromosome, and position (like a SNP)
setClass("is.hail.expr.types.TLocus", contains="is.hail.expr.types.ComplexType")
setClass("HailLocusType", slots=c(type="is.hail.expr.types.TLocus"),
         contains="HailComplexType")

setClass("is.hail.expr.types.TableType", contains="is.hail.expr.types.Type")
setClass("HailTableType", slots=c(type="is.hail.expr.types.TableType"),
         contains="HailType")

rowType <- function(x) HailType(impl(x)$rowType())
globalType <- function(x) HailType(impl(x)$globalType())
keys <- function(x) impl(x)$keys()

## The schema of a MatrixTable
setClass("is.hail.expr.types.MatrixType", contains="is.hail.expr.types.Type")
setClass("HailMatrixType", slots=c(type="is.hail.expr.types.MatrixType"),
         contains="HailType")

colTableType <- function(x) HailType(impl(x)$colsTableType())
rowTableType <- function(x) HailType(impl(x)$rowsTableType())
entryType <- function(x) HailType(impl(x)$entryType())
rowPartitionKey <- function(x) impl(x)$rowPartitionKey()

## Hail supports registering functions for use in the expression runtime.
## We should be able to call these dynamically at least. Ideally,
## we could define functions using R code.
setClass("is.hail.expr.types.TFunction", contains="is.hail.expr.types.Type")
setClass("HailFunctionType", slots=c(type="is.hail.expr.types.TFunction"),
         contains="HailType")

paramTypes <- function(x) HailType(impl(x)$paramTypes())
returnType <- function(x) HailType(impl(x)$returnType())

setClass("is.hail.expr.types.TVariable", contains="is.hail.expr.types.Type")
setClass("HailVariableType", slots=c(type="is.hail.expr.types.TVariable"),
         contains="HailType")

setClass("is.hail.expr.types.TVoid", contains="is.hail.expr.types.Type")
setClass("HailVoidType", slots=c(type="is.hail.expr.types.TVoid"),
         contains="HailType")
