installed_sparks <- function() {
    sparklyr::spark_installed_versions()
}

install_spark <- function(version) {
    message("Installing Spark version '", version, "'")
    sparklyr::spark_install(version)
}

available_hails <- function() {
    data.frame(version       = "0.1",
               build_hash    = "20613ed50c74",
               spark_version = c("2.0.2", "2.2.1"))
}

hail_filename <- function(spark_version, hail_version, hail_build) {
    paste0("Hail-", hail_version, "-", hail_build, "-Spark-",
           spark_version, ".zip")
}

hail_url <- function(spark_version, hail_version, hail_build) {
    dist_url <- "https://storage.googleapis.com/hail-common/distributions"
    paste(dist_path, hail_version,
          hail_filename(spark_version, hail_version, hail_build),
          sep = "/")
}

download_hail <- function(hail) {
    dist <- do.call(hail_url, hail)
    dest_path <- file.path(tmpdir(), basename(dist))
    download.file(dist, dest_path)
    dest_path
}

select_hail <- function(hails, version) {
    if (!missing(version)) {
        stopifnot(is.character(version),
                  length(version) == 1L && !is.na(version))
        hails <- hails[hails$version == version,]
        if (nrow(hails) == 0L) {
            stop("No Hail available with version '", version, "'")
        }
    }
    sparks <- installed_sparks()
    valid_sparks <- intersect(sparks$version %in% hails$spark_version)
    if (length(valid_sparks) == 0L) {
        hail <- tail(hails, 1L)
        message("No valid Spark version installed for Hail '", version, "'")
        install_spark(hail$spark_version)
        valid_sparks <- hail$spark_version
    }
    tail(hail[hail$spark_version %in% valid_sparks,], 1L)
    if (nrow(ans) > 0L)
        ans
}

extract_hail <- function(file) {
    exdir <- file.path(hail_dir(), file_path_sans_ext(basename(dest)))
    if (file.exists(exdir))
        unlink(exdir)
    invisible(untar(file, exdir=exdir))
}

install_hail <- function(version) {
    if (!requireNamespace("sparklyr"))
        stop(strwrap(paste(
            "The sparklyr package must be installed to install hail",
            "Please install sparklyr or install Hail manually.")))
    hail <- select_hail(available_hails(), version)
    dest <- download_hail(hail)
    extract_hail(dest)
}

hail_dir <- function() {
    if (!requireNamespace("rappdirs"))
        stop(strwrap(paste("The rappdirs package must be installed to use ",
                           "the automatically installed Hail.\nPlease ",
                           "install rappdirs or use a system Hail.")))
    file.path(rappdirs::user_data_dir("hailr"), "hail")
}

## the installed hails specific to this version of the package
installed_hails <- function() {
    dirs <- dir(hail_dir(), full.names=TRUE)
    hails <- available_hails()
    hail_filenames <- do.call(hail_filename, hails)
    m <- match(hail_filenames, basename(dirs), 0L)
    cbind(hails[m > 0L,], home=dirs[m])
}
