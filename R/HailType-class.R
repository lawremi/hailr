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

setClass("HailType")

setClass("HailTypeList", prototype=list(elementType="HailType"),
         contains="SimpleList")

## Except for TBinary, these all refer to scalars (per row)
setClass("TBoolean", contains="HailType")
setClass("TNumeric", contains="HailType")
setClass("TFloat32", contains="TNumeric")
setClass("TFloat64", contains="TNumeric")
setClass("TIntegral", contains="HailType")
setClass("TInt32", contains="TIntegral")
setClass("TInt64", contains="TIntegral")
setClass("TString", contains="HailType")
setClass("TBinary", contains="HailType") # raw vector

setClass("TContainer", slots=c(elementType="HailType"), contains="Type")

setClass("TIterable", contains="TContainer")
setClass("TArray", contains="TIterable")
setClass("TSet", contains="TIterable")

## Describes a matrix
setClass("TAggregable", extends="TContainer")

setClass("TDict", slots=c(elementType="TStruct", keyType="HailType",
                          valueType="HailType"),
         extends="TContainer")

setClass("TBaseStruct", slots=c(types="HailTypeList"), contains="HailType")

## names(@types) represent field names
setClass("TStruct", contains="TBaseStruct")

setClass("TTuple", contains="TBaseStruct")

## Better name might have been 'DecoratedType'; adds semantics
setClass("ComplexType", slots=c(representation="HailType"), contains="HailType")

## Basically a 'Ranges'
setClass("TInterval", slots=c(representation="TStruct"), contains="ComplexType")

## Variant call
setClass("TCall", slots=c(representation="TInt32"), contains="ComplexType")

## Defined by a genome, chromosome, and position (like a SNP)
setClass("TLocus", slots=c(representation="TStruct", genome="character"),
         contains="ComplexType")

setClass("TableType", slots=c(rowType="TStruct", globalType="TStruct",
                              key="character"),
         contains="HailType")

## The schema of a MatrixTable
setClass("MatrixType",
         slots=c(globalType="TStruct", colKey="character", colType="TStruct",
                 rowPartitionKey="character", rowKey="character",
                 rowType="TStruct", entryType="TStruct"),
         contains="HailType")

## Language constructs?
setClass("TFunction", slot=c(paramTypes="HailTypeList", returnType="HailType"),
         contains="HailType")
setClass("TVariable", slot=c(name="character"), contains="HailType")

setClass("TVoid", contains="HailType")
