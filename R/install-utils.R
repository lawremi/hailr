installed_sparks <- function() {
    sparklyr::spark_installed_versions()
}

install_spark <- function(version) {
    message("Installing Spark version '", version, "'")
    sparklyr::spark_install(version)
}

available_hails <- function() {
    rbind(data.frame(version       = "0.1",
                     build         = "20613ed50c74",
                     spark_version = c("2.0.2", "2.1.0")),
          data.frame(version       = "devel",
                     build         = "fdf130b2f5d4",
                     spark_version = c("2.2.0")))
}

hail_filename <- function(version, build, spark_version) {
    paste0("Hail-", version, "-", build, "-Spark-",
           spark_version, ".zip")
}

hail_url <- function(version, build, spark_version) {
    dist_url <- "https://storage.googleapis.com/hail-common/distributions"
    paste(dist_url, version,
          hail_filename(version, build, spark_version),
          sep = "/")
}

download_hail <- function(hail) {
    dist <- do.call(hail_url, hail)
    dest_path <- file.path(tempdir(), basename(dist))
    download.file(dist, dest_path)
    dest_path
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
    sparks <- installed_sparks()
    valid_sparks <- intersect(sparks$spark, hails$spark_version)
    if (length(valid_sparks) == 0L) {
        hail <- tail(hails, 1L)
        message("No supported Spark version installed for Hail version '",
                hail$version, "'")
        install_spark(hail$spark_version)
        valid_sparks <- hail$spark_version
    }
    ans <- tail(hails[hails$spark_version %in% valid_sparks,], 1L)
    if (nrow(ans) > 0L) {
        ans
    }
}

extract_hail <- function(file) {
    exdir <- file.path(hail_dir(), file_path_sans_ext(basename(file)))
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
    file.path(rappdirs::user_data_dir("R"), "hailr", "hail")
}

## the installed hails specific to this version of the package
installed_hails <- function() {
    dirs <- dir(hail_dir(), full.names=TRUE)
    hails <- available_hails()
    hail_filenames <- do.call(hail_filename, hails)
    m <- match(file_path_sans_ext(hail_filenames), basename(dirs), 0L)
    ans <- hails[m > 0L,]
    ans$home <- dirs[m]
    ans
}
