library(sjlabelled)
library(splitstackshape)
library(data.table)
library(doParallel)
library(parallel)
library(iterators)
library(foreach)
library(haven)
library(tidyverse)

select <- dplyr::select

enrollment_dates <- function(data_dirname){
  enrollment <- read_csv(paste0(data_dirname,"enrollment.csv")) %>%
    select(file_id = PID_master,start_date = appstartdate,stop_date = appenddate,
           end_date = Date_Visit2, hours_up = 'wake-sleepDIF', true_wake = waketime,
           true_sleep = sleeptime) %>%
    mutate(start_date = as.Date(start_date, format = "%m/%d/%y", tz = "America/Los Angeles", origin = "1970-01-01"),
           stop_date = as.Date(stop_date, format = "%m/%d/%y", tz = "America/Los Angeles", origin = "1970-01-01"),
           end_date = as.Date(end_date, format = "%m/%d/%y", tz = "America/Los Angeles", origin = "1970-01-01"),
           hours_up = as.integer(str_split_fixed(hours_up,":",2)[,1]) + 
             ifelse(str_split_fixed(hours_up,":",2)[,2] == "30",.5,0)) %>%
    filter(!is.na(start_date)) %>%
    mutate(true_end = ifelse(is.na(end_date),stop_date,ifelse(
      end_date < stop_date,end_date,stop_date)),
      true_end = as.Date(true_end, origin = "1970-01-01", tz = "America/Los Angeles"),
      ema_days = true_end - start_date,
      hours_down = 24-hours_up) %>%
    filter(as.integer(file_id)<3000) %>%
    select(file_id,enroll_start = start_date,enroll_end = true_end)
  
  write_csv(as.data.frame(enrollment$file_id),paste0(data_dirname,"pids.txt"),col_names = FALSE)
  
  return(enrollment)
}

read_gps <- function(data_dirname,gps_dirname,gps_manual_dirname){
  data_files <- dir(paste0(data_dirname,gps_dirname),pattern = "lml_com$")
  gps_dirs <- paste0(data_dirname,gps_dirname,data_files)
  if(.Platform$OS.type == "windows"){
    date_files <- dir(paste0(gps_dirs), full.names = TRUE)
  } else {
    date_files <- dir(paste0(gps_dirs,"/data"), full.names = TRUE)
  }
  hour_files <- dir(date_files, full.names = TRUE)
  gps_files <- list.files(hour_files,pattern = "GPS.csv$", full.names = TRUE, include.dirs = FALSE)
  
  manual_data_files <- dir(paste0(data_dirname,gps_manual_dirname),pattern = "lml_com$")
  manual_gps_dirs <- paste0(data_dirname,gps_manual_dirname,manual_data_files)
  if(.Platform$OS.type == "windows"){
    manual_date_files <- dir(paste0(manual_gps_dirs), full.names = TRUE)
  } else {
    manual_date_files <- dir(paste0(manual_gps_dirs,"/data"), full.names = TRUE)
  }
  manual_hour_files <- dir(manual_date_files, full.names = TRUE)
  manual_gps_files <- list.files(manual_hour_files,pattern = "GPS.csv", full.names = TRUE, include.dirs = FALSE, recursive = TRUE)
  
  # Switching to Data.Table Paradigm for Fast GPS Processing
  pre_raw_gps_log <- rbindlist(lapply(gps_files,skip_fread, data_dirname = data_dirname, suffix_dirname = gps_dirname), fill = TRUE)
  manual_raw_gps_log <- rbindlist(lapply(manual_gps_files,skip_fread, data_dirname = data_dirname, suffix_dirname = gps_manual_dirname), fill = TRUE)
  merge_manual <- anti_join(manual_raw_gps_log,pre_raw_gps_log, by = c("file_id","V1"))
  raw_gps_log <- bind_rows(pre_raw_gps_log,merge_manual)
  names(raw_gps_log) <- c("a","b","c","d","e","f","file_id")
  raw_gps_log[, log_time := as.POSIXct(a, tz = "America/Los_Angeles", format = "%Y-%m-%d %H:%M:%S", origin = "1970-01-01")]
  raw_gps_log[, measure_time := as.POSIXct(b, tz = "America/Los_Angeles", format = "%Y-%m-%d %H:%M:%S", origin = "1970-01-01")]
  raw_gps_log[is.na(log_time), log_time := as.POSIXct(as.numeric(a)/1000, tz = "America/Los_Angeles", origin = "1970-01-01")]
  raw_gps_log[is.na(measure_time), measure_time := as.POSIXct(as.numeric(b)/1000, tz = "America/Los_Angeles", origin = "1970-01-01")]
  raw_gps_log[, time_diff := log_time - measure_time]
  
  raw_gps_log[, latitude := as.numeric(c)]
  raw_gps_log[, longitude := as.numeric(d)]
  raw_gps_log[, accuracy := as.numeric(e)]
  raw_gps_log[, c("a","b","c","d","e","f") := NULL]
  raw_gps_log[, gps_time_valid := as.integer(time_diff <= 300)]
  raw_gps_log[, gps_accuracy_valid := as.integer(accuracy <= 100)]
  raw_gps_log[, fulltime := measure_time]
  
  return(raw_gps_log)
}

skip_fread <- function(file, data_dirname, suffix_dirname){
  if(file.size(file) > 0){
    try({
      return_csv <- fread(file, colClasses = 'character')
      idstringlength <- str_length(paste0(data_dirname,suffix_dirname))
      id <- str_sub(file,idstringlength+4,idstringlength+7)
      return_csv[, file_id := id]
      new_return_csv <- return_csv[!is.na(V2)]
      if(nrow(new_return_csv)>0){
        return(new_return_csv)
      } else {
        return(NULL)
      }
    })
    return(NULL)
  } else {return(NULL)}
}

read_sni <- function(sni_stata_filename) {
  sni_data <- read_dta(sni_stata_filename) %>%
    select(SNI_PID,starts_with("SNI_ID_")) %>%
    rename(
      subject_id = 1,
      sni1 = 2,
      sni2 = 3,
      sni3 = 4,
      sni4 = 5,
      sni5 = 6
    ) %>%
    mutate_all(funs(tolower)) %>%
    mutate_all(funs(str_replace(.,"\\.",""))) %>%
    mutate_all(funs(str_replace(.," ",""))) %>%
    mutate_all(funs(str_replace(.,"\\.",""))) %>%
    filter(!is.na(subject_id))
  return(sni_data)
}

read_ema <- function(file_to_read, data_dirname, suffix_dirname){
  discard <- 1
  return_ema_file <- as.tibble(data.frame(discard))
  try({
    file_cols <- ncol(read_csv(file_to_read))
    return_ema_file <- read_csv(file_to_read, col_types = str_flatten(rep("c",file_cols)))
    idstringlength <- str_length(paste0(data_dirname,suffix_dirname))
    id <- str_sub(file_to_read,idstringlength+4,idstringlength+7)
    return_ema_file$system_file <- id
    return_ema_file$discard <- NULL
    return(return_ema_file)  
  }, silent = TRUE)
  return(NULL)
}

