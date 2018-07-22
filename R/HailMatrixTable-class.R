### =========================================================================
### HailMatrixTable objects
### -------------------------------------------------------------------------
###
### Direct mapping of Hail MatrixTable API to R. HailExperiment wraps
### this to provide the familiar SE API. Provides promises of its
### entries, i.e., assay matrices, and access to metadata tables.
###

.is.hail.variant.MatrixTable <- setClass("is.hail.variant.MatrixTable",
                                         contains="JavaObject")

.HailMatrixTable <- setRefClass("HailMatrixTable",
                                fields=c(impl="is.hail.variant.MatrixTable"))

HailMatrixTable <- function(.impl) {
    .is.hail.variant.MatrixTable(JavaObject(.impl))
}

readMatrixTable <- function(file, drop.rows=FALSE, drop.cols=FALSE)
{
    hail_context()$readMatrixTable(file, drop.rows, drop.cols)
}

genomeFromVCFHeader <- function(file) {
    if (requireNamespace("VariantAnnotation")) {
        genome <- unique(genome(VariantAnnotation::scanVcfHeader(file)))
        if (length(genome) != 1L) {
            stop("Unable to determine 'genome' from VCF header, please specify")
        }
        genome
    } else {
        stop("If 'genome' is missing, the VariantAnnotation package is ",
             "required to extract the genome from the VCF header")
    }
}

readHailMatrixTableFromVCF <- function(file, genome=NA_character_)
{
    stopifnot(isSingleStringOrNA(genome))
    if (is.na(genome)) {
        genome <- genomeFromVCFHeader(file)
    }
    hail_context()$importVCF(file, genome)
}

## TODO: readMatrixTableFromPlink()
