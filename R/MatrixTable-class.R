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

readMatrixTableFromVCF <- function(file, force=FALSE,
                                   force.bgz=FALSE, header.file=NULL,
                                   min.partitions=NULL,
                                   drop.samples=FALSE, call.fields=character(),
                                   genome=NA_character_, contig.recoding=NULL)
{
    stopifnot(is.character(genome), length(genome) == 1L)
    if (is.na(genome)) {
        genome <- genomeFromVCFHeader(file)
    }
    sc <- sparkConnection(hail_context())
    genome <- sc$is$hail$variant$ReferenceGenome$getReference(genome)
    hail_context()$importVCF(file, force, force.bgz, ScalaOption(header.file),
                             ScalaOption(min.partitions), drop.samples,
                             ScalaSet(call.fields),
                             ScalaOption(genome), ScalaOption(contig.recoding))
}

## TODO: readMatrixTableFromPlink()
