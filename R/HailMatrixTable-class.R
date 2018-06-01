### =========================================================================
### HailMatrixTable objects
### -------------------------------------------------------------------------
###
### Direct mapping of Hail MatrixTable API to R. HailExperiment wraps
### this to provide the familiar SE API. Provides promises of its
### entries, i.e., assay matrices, and access to metadata tables.
###

setClass("is.hail.variant.MatrixTable", contains="JavaObject")

.HailMatrixTable <- setRefClass("HailMatrixTable",
                                fields=c(impl="is.hail.variant.MatrixTable"))

HailMatrixTable <- function(.impl) {
    new("is.hail.variant.MatrixTable", JavaObject(.impl))
}

readMatrixTable <- function(file, drop.rows=FALSE, drop.cols=FALSE)
{
    hail_context()$read(file, drop.rows, drop.cols)
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

readHailMatrixTableFromVCF <- function(file, force=FALSE,
                                       force.bgz=FALSE, header.file=NULL,
                                       min.partitions=NULL,
                                       drop.samples=FALSE,
                                       call.fields=character(),
                                       genome=NA_character_,
                                       contig.recoding=NULL)
{
    stopifnot(is.character(genome), length(genome) == 1L)
    if (is.na(genome)) {
        genome <- genomeFromVCFHeader(file)
    }
    jvm <- jvm(hail_context())
    genome <- jvm$is$hail$variant$ReferenceGenome$getReference(genome)
    hail_context()$importVCF(file, force, force.bgz, ScalaOption(header.file),
                             ScalaOption(min.partitions), drop.samples,
                             ScalaSet(call.fields),
                             ScalaOption(genome), ScalaOption(contig.recoding))
}

## TODO: readMatrixTableFromPlink()
