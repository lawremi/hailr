### =========================================================================
### HailType objects
### -------------------------------------------------------------------------
###
### Formal classes corresponding to types in the Hail expression
### language. For consistency, we name the classes according to what
### Hail calls the type. These typically begin with 'T'.
###
### These could have just been proxies of the Java object, but it
### seems best to declare them, as it is both self-documenting and
### probably more performant, as there is no need for round-tripping
### to Spark.

### Are we sure this is not just premature optimization? We can define
### proxy classes with convenience methods for R-like syntax, so that
### users are not aware of the object being backed by Java. We need
### those proxy classes anyway, so that we can define coercions
### between the low-level and high-level classes.

setClass("is.hail.expr.types.Type", contains="SparkObject")

## Except for TBinary, these all refer to scalars (per row)
setClass("is.hail.expr.types.TBoolean", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TNumeric", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TFloat32", contains="is.hail.expr.types.TNumeric")
setClass("is.hail.expr.types.TFloat64", contains="is.hail.expr.types.TNumeric")
setClass("is.hail.expr.types.TIntegral", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TInt32", contains="is.hail.expr.types.TIntegral")
setClass("is.hail.expr.types.TInt64", contains="is.hail.expr.types.TIntegral")
setClass("is.hail.expr.types.TString", contains="is.hail.expr.types.Type")
## raw vector
setClass("is.hail.expr.types.TBinary", contains="is.hail.expr.types.Type")

setClass("is.hail.expr.types.TContainer", contains="is.hail.expr.types.Type")
setMethod("elementType", "TContainer", function(x) x$elementType())

setClass("is.hail.expr.types.TIterable",
         contains="is.hail.expr.types.TContainer")
setClass("is.hail.expr.types.TArray", contains="is.hail.expr.types.TIterable")
setClass("is.hail.expr.types.TSet", contains="is.hail.expr.types.TIterable")

## Describes a matrix
setClass("is.hail.expr.types.TAggregable",
         contains="is.hail.expr.types.TContainer")

setClass("is.hail.expr.types.TDict", contains="is.hail.expr.types.TContainer")

setClass("is.hail.expr.types.TBaseStruct", contains="is.hail.expr.types.Type")
setGeneric("fieldTypes", function(x) standardGeneric("fieldTypes"))
setMethod("fieldTypes", "TBaseStruct", function(x) x$fieldTypes())

setClass("is.hail.expr.types.TStruct",
         contains="is.hail.expr.types.TBaseStruct")
setMethod("fieldTypes", "TStruct",
          function(x) setNames(callNextMethod(), x$fieldNames()))

setClass("is.hail.expr.types.TTuple", contains="is.hail.expr.types.TBaseStruct")

## Better name might have been 'DecoratedType'; adds semantics
setClass("is.hail.expr.types.ComplexType", contains="is.hail.expr.types.Type")
representationType <- function(x) x$representation()

## Basically a 'Ranges'
setClass("is.hail.expr.types.TInterval",
         contains="is.hail.expr.types.ComplexType")

startType <- function(x) representationType(x)$field("start")$typ()
endType <- function(x) representationType(x)$field("end")$typ()

## Variant call
setClass("is.hail.expr.types.TCall", contains="is.hail.expr.types.ComplexType")

## Defined by a genome, chromosome, and position (like a SNP)
setClass("is.hail.expr.types.TLocus", contains="is.hail.expr.types.ComplexType")

setClass("is.hail.expr.types.TableType", contains="is.hail.expr.types.Type")

rowType <- function(x) x$rowType()
globalType <- function(x) x$globalType()
keys <- function(x) x$keys()

## The schema of a MatrixTable
setClass("is.hail.expr.types.MatrixType", contains="is.hail.expr.types.Type")

colTableType <- function(x) x$colsTableType()
rowTableType <- function(x) x$rowsTableType()
entryType <- function(x) x$entryType()
rowPartitionKey <- function(x) x$rowPartitionKey()

## Hail supports registering functions for use in the expression runtime.
## We should be able to call these dynamically at least. Ideally,
## we could define functions using R code.
setClass("is.hail.expr.types.TFunction", contains="is.hail.expr.types.Type")
paramTypes <- function(x) x$paramTypes()
returnType <- function(x) x$returnType()
setClass("is.hail.expr.types.TVariable", contains="is.hail.expr.types.Type")
setClass("is.hail.expr.types.TVoid", contains="is.hail.expr.types.Type")
