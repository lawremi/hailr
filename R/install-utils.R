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

hail_distribution <- function(spark_version, hail_version, hail_build) {
    dist_url <- "https://storage.googleapis.com/hail-common/distributions"
    paste(dist_path, hail_version,
          paste0("Hail-", hail_version, "-", hail_build, "-Spark-",
                 spark_version, ".zip"),
          sep = "/")
}

download_hail <- function(hail) {
    dist <- do.call(hail_distribution, hail)
    dest_path <- file.path(tmpdir(), dist)
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
    file.path(user_data_dir("hailr"), "hail")
}

installed_hails <- function() {
    dirs <- dir(hail_dir(), full.names=TRUE)
    cbind(strcapture("Hail-(.*?)-(.*?)-Spark-(.*?)", dirs,
                     data.frame(version=character(),
                                build_hash=character(),
                                spark_version=character())),
          home=dirs)
}

use_hail <- function(home) {
    if (missing(home)) {
        home <- select_hail(installed_hails())$home
        if (length(home) == 0L) {
            home <- install_hail()
        }
    }
    stopifnot(is.character(home),
              length(home) == 1L && !is.na(home))
    if (!file.exists(hail_jar(home)))
        stop("No Hail installation found at '", home, "'")
    Sys.setenv(HAIL_HOME=home)
    invisible(home)
}

hail_jar <- function(home = hail_home()) {
    file.path(home, "jars", "hail-all-spark.jar")
}

hail_home <- function() {
    home <- Sys.getenv("HAIL_HOME")
    if (home == "") {
        home <- use_hail()
    }
    home
}
