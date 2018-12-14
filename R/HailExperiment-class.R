### =========================================================================
### HailExperiment objects
### -------------------------------------------------------------------------
###
### Represents a Hail VariantDataset as a SummarizedExperiment derivative.
###

.HailExperiment <- setClass("HailExperiment",
                            slots=c(matrixTable="HailMatrixTable"),
                            contains="SummarizedExperiment")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailExperiment <- function(matrixTable) {
    se <- SummarizedExperiment(assays=matrixTable$entry(),
                               rowData=HailMetaDataFrame(matrixTable$row()),
                               colData=HailMetaDataFrame(matrixTable$col()),
                               metadata=as.list(matrixTable$globals()))
    .HailExperiment(se, matrixTable=matrixTable)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("unmarshal", c("HailMatrixTable", "ANY"),
          function(x, skeleton) HailExperiment(x))

setAs("ANY", "HailExperiment", function(from) {
    push(as(from, "SummarizedExperiment"), hail())
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessor methods.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailExperiment <- function(file, ...)
{
    HailExperiment(readMatrixTable(file, ...))
}

readHailExperimentFromVCF <- function(file, ...)
{
    HailExperiment(readMatrixTableFromVCF(file, ...))
}

