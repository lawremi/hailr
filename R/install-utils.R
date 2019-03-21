installed_sparks <- function() {
    sparklyr::spark_installed_versions()
}

install_spark <- function(version) {
    message("Installing Spark version '", version, "'")
    sparklyr::spark_install(version)
}

install_hail_artifact <- function(hail) {
    dest <- download_hail(hail)
    file.path(extract_hail(dest), paste0("hail-", hail$version))
}

hail_artifact <- function(version, hash, spark_version = "2.2.0") {
    data.frame(version       = version,
               hash          = hash,
               spark_version = spark_version,
               stringsAsFactors = FALSE)
}

available_hails <- function() {                                 
    rbind(hail_artifact(
        "0.2.11",
        "71/b3/67bf8947a03c49a6f6c990defd739252a6a298689d108ed44e89b95a68a0"
    ))
}

hail_filename <- function(version, hash, spark_version) {
    paste0("hail-", version, ".tar.gz")
}

hail_url <- function(version, hash, spark_version) {
    dist_url <- "https://files.pythonhosted.org/packages"
    paste(dist_url, hash, hail_filename(version, spark_version), sep = "/")
}

download_hail <- function(hail) {
    dist <- do.call(hail_url, hail)
    dest_path <- file.path(tempdir(), basename(dist))
    download.file(dist, dest_path)
    dest_path
}

query_spark_version <- function() {
    bin <- file.path(spark_home(), "bin", "spark-submit")
    cmd <- paste(bin, "--version 2>&1")
    output <- system(cmd, intern=TRUE)
    sub(".*version ", "", grep("version", output, value=TRUE)[1L])
}

select_spark_versions <- function() {
    spark_versions <- spark_version()
    if (is.null(spark_versions)) {
        sparks <- installed_sparks()
        spark_versions <- sparks$spark
        if (nzchar(spark_home())) {
            m <- match(spark_home(), sparks$dir)
            if (!is.na(m)) {
                spark_versions <- spark_versions[m]
            } else {
                spark_versions <- query_spark_version()
            }
        }
    }
    spark_versions
}

select_hail <- function(hails, version) {
    if (!missing(version)) {
        stopifnot(is.character(version),
                  length(version) == 1L && !is.na(version))
        hails <- hails[hails$version == version,]
    }
    if (nrow(hails) == 0L) {
        return(NULL)
    }
    valid_sparks <- intersect(select_spark_versions(), hails$spark_version)
    if (length(valid_sparks) == 0L) {
        hail <- tail(hails, 1L)
        message("No supported Spark version installed for Hail version '",
                hail$version, "'")
        install_spark(hail$spark_version)
        valid_sparks <- hail$spark_version
    }
    ans <- tail(hails[hails$spark_version %in% valid_sparks,], 1L)
    if (nrow(ans) > 0L) {
        if (spark_master() == "local")
            use_local_spark(ans$spark_version)
        ans
    }
}

extract_hail <- function(file) {
    exdir <- hail_dir()
    if (file.exists(exdir))
        unlink(exdir)
    untar(file, exdir=exdir)
    invisible(exdir)
}

prompt_to_install_hail <- function() {
    if (!interactive()) {
        install_hail()
    } else {
        message("Install hail?")
        res <- readline("y/n: ")
        if (res == "y") {
            install_hail()
        } else {
            stop("Please install Hail to run tests/examples")
        }
    }
}

install_hail <- function(version) {
    if (!requireNamespace("sparklyr", quietly=TRUE))
        stop(strwrap(paste(
            "The sparklyr package must be installed to install hail",
            "Please install sparklyr or install Hail manually.")))
    hail <- select_hail(available_hails(), version)
    install_hail_artifact(hail)
}

hail_dir <- function() {
    if (!requireNamespace("rappdirs", quietly=TRUE))
        stop(strwrap(paste("The rappdirs package must be installed to use ",
                           "the automatically installed Hail.\nPlease ",
                           "install rappdirs or use a system Hail.")))
    file.path(rappdirs::user_data_dir("R"), "hailr", "hail")
}

## the installed hails specific to this version of the package
installed_hails <- function() {
    dirs <- dir(hail_dir(), full.names=TRUE)
    hails <- available_hails()
    hail_filenames <- do.call(hail_filename, hails)
    m <- match(file_path_sans_ext(hail_filenames, compression=TRUE),
               basename(dirs), 0L)
    ans <- hails[m > 0L,]
    ans$home <- dirs[m]
    ans
}
