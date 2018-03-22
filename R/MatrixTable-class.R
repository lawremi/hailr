### =========================================================================
### MatrixTable objects
### -------------------------------------------------------------------------
###
### Proxy for the Hail MatrixTable, the central data structure of Hail.
### The user will typically interact with this through HailExperiment.
###

setClass("is.hail.variant.MatrixTable", contains="SparkObject")

MatrixTable <- function(.impl) {
    new("is.hail.variant.MatrixTable", SparkObject(.impl))
}

readMatrixTable <- function(file, format = c("VCF", "PLINK", "VDS"), ...,
                            hail = hail_context())
{
    ## Automatically determine import function from file extension?
    ## If VCF, hail$importVCF()
    ## If Plink, hail$importPlink()
    ## If VDS, hail$readVDS()

}

readMatrixTable <- function(file, drop.rows=FALSE, drop.cols=FALSE)
{
    hail$read(file, dropRows, dropCols)
}

readMatrixTableFromVCF <- function(file, path, force=FALSE,
                                   force.bgz=FALSE, header.file=NULL,
                                   min.partitions=NULL,
                                   drop.samples=FALSE, call.fields=character(),
                                   genome="default", contig.recoding=NULL,
                                   hail = hail_context())
{
    hail$importVCF(file, force, force.bgz, joption(header.file),
                   joption(min.partitions), drop.samples,
                   jset(call.fields),
                   joption(rg), joption(contig.recoding))
}
