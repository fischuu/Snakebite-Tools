#' Analyse Snakemake runs
#' 
#' Get more inforamtion about the status of your snakemake run
#' 
#' @param project_folder Location of your project
#' @param print_feedback logical, should the sacct command be printed
#' @param no_return logical, turn off the return value of the function
#' 
#' @return A data.frame with the job information from the latest log file
#' 
#' @export

snakemake_log_analyser <- function(project_folder, print_sacct=TRUE, no_return=TRUE){
  
  # Get the logfiles
  #project_folder <- "/scratch/project_2010176/Peltoniemi_Aviti"
  #project_folder <- "/scratch/project_2001829/RuminomicsHighLow/"
  #project_folder <- "/scratch/project_2009831/Holoruminant_metaG_analysis/"
  logfile_folder <- file.path(project_folder, ".snakemake", "log")
  logfiles <- list.files(logfile_folder)
  
  # Get the latest logfile  
  timestamps <- as.POSIXct(sub("\\.snakemake\\.log$", "", logfiles), format = "%Y-%m-%dT%H%M%OS")
  latest_file <- logfiles[which.max(timestamps)]
  
  # Import the logfile
  log_in.file <- file.path(logfile_folder, latest_file)
  log_in <- readLines(log_in.file)
  
  # Extract job IDs
  job_ids <- as.numeric(sapply(strsplit(log_in[grep("jobid: ", log_in)], ": "), "[", 2))
  finished_job_ids <- as.numeric(gsub("\\.", "", sapply(strsplit(log_in[grep("Finished job", log_in)], " "), "[", 3)))
  
  # Extract submitted job information
  submitted_job_lines <- log_in[grep("Submitted job ", log_in)]
  submitted_jobs_tmp <- strsplit(sapply(strsplit(submitted_job_lines, "Submitted job "), "[", 2), " ")
  
  submitted_jobs <- data.frame(
    jobid = as.numeric(sapply(submitted_jobs_tmp, "[", 1)),
    slurmid = as.numeric(gsub("'\\.", "", sapply(submitted_jobs_tmp, "[", 8))),
    hasFinished = FALSE
  )
  submitted_jobs$hasFinished <- is.element(submitted_jobs$jobid, finished_job_ids)
  
  submitted_jobs$hasFinished <- is.element(submitted_jobs$jobid, finished_job_ids)
  
  # Extract rule associated with each job ID
  rule_lines <- log_in[grep("^rule ", log_in)]
  rule_info <- data.frame(
    jobid = NA, #as.numeric(gsub(".*jobid: ([0-9]+).*", "\\1", rule_lines)),
    rule = gsub(".*rule ([_a-zA-Z0-9]+):.*", "\\1", rule_lines)
  )
  
  # Extract submission times
  timestamps <- as.character(gsub("\\[|\\]", "", sapply(strsplit(submitted_job_lines, "\\["), "[", 2)))
  
  submitted_jobs$timestamp <- timestamps
  submitted_jobs <- merge(submitted_jobs, rule_info, by = "jobid", all.x = TRUE)
  
  # Give the slurm command to get additional information in the non-finished jobs
  if(print_sacct){
    if(sum(submitted_jobs$hasFinished==FALSE)>0){
      cat("There are", sum(submitted_jobs$hasFinished==FALSE), "unfinished job(s) in your project log. Check their SLURM status by running: \n\n")
      cat('sacct -j',paste(submitted_jobs$slurmid[submitted_jobs$hasFinished==FALSE], collapse=","),'--format="JobID,State,JobName%50" \n')  
    } else {
      message("Great, all your jobs are finished!")
    }
  }
  
  if(!no_return) return(submitted_jobs)
}
