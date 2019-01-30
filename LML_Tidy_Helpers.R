library(sjlabelled)
library(splitstackshape)
library(data.table)
library(doParallel)
library(parallel)
library(iterators)
library(foreach)
library(haven)
library(tidyverse)
library(bit64)

select <- dplyr::select

write_gps_logs <- function(data_dirname, wockets_dirname, manual_dirname = NULL, skip_manual = TRUE){
  gps_files <- read_file_list(data_dirname,wockets_dirname,"data/","lml_com$","GPS.csv$")
  pre_raw_gps_log <- rbindlist(lapply(gps_files,skip_fread, data_dirname = data_dirname, suffix_dirname = wockets_dirname), fill = TRUE)
  
  if(!skip_manual){
    manual_gps_files <- read_file_list(data_dirname,manual_dirname,"data","lml_com$","GPS.csv$")
    manual_raw_gps_log <- rbindlist(lapply(manual_gps_files,skip_fread, data_dirname = data_dirname, suffix_dirname = manual_dirname), fill = TRUE)
    # Switching to Data.Table Paradigm for Fast GPS Processing
    merge_manual <- anti_join(manual_raw_gps_log,pre_raw_gps_log, by = c("file_id","V1"))
    raw_gps_log <- bind_rows(pre_raw_gps_log,merge_manual)
  } else {
    raw_gps_log <- pre_raw_gps_log
  }
  write_csv(raw_gps_log,paste(data_dirname,"gps_logs.csv", sep = "/"))
  return(raw_gps_log)
}


write_daily_responses <- function(data_dirname, wockets_dirname, manual_dirname = NULL, skip_manual = TRUE){
  dailylog_response_files <- read_file_list(data_dirname,wockets_dirname,"surveys","lml_com$","PromptResponses_Dailylog.csv$", hour_filter = FALSE)
  raw_dailylog <- lapply(dailylog_response_files,read_ema, data_dirname = data_dirname, suffix_dirname = wockets_dirname)
  
  if(!skip_manual){
    manual_dailylog_response_files <- read_file_list(data_dirname,manual_dirname,"surveys","lml_com$","PromptResponses_Dailylog.csv$", hour_filter = FALSE)
    manual_raw_dailylog <- lapply(manual_dailylog_response_files,read_ema, data_dirname = data_dirname, suffix_dirname = manual_dirname)
  }
  
  dailylog_responses <- raw_dailylog[[1]]
  for(i in 2:length(raw_dailylog)){
    dailylog_responses <- bind_rows(dailylog_responses,raw_dailylog[[i]])
  }
  if(!skip_manual){
    manual_dailylog_responses <- manual_raw_dailylog[[1]]
    for(i in 2:length(manual_raw_dailylog)){
      manual_dailylog_responses <- bind_rows(manual_dailylog_responses,manual_raw_dailylog[[i]])
    }
    merge_responses <- anti_join(manual_dailylog_responses,dailylog_responses, by = c("system_file","PromptTime"))
    pre_filtered_dailylog <- bind_rows(dailylog_responses,merge_responses)
  } else {
    pre_filtered_dailylog <- dailylog_responses
  }
  write_csv(pre_filtered_dailylog,paste(data_dirname,"daily_responses.csv", sep = "/"))
  return(pre_filtered_dailylog)
}

write_ema_responses <- function(data_dirname, wockets_dirname, id_varstub, filename_varstub, manual_dirname = NULL, skip_manual = TRUE, prepend_surveys = ""){
  #ids <- read_delim(paste(data_dirname,"file_ids.txt",sep = "/"), delim = ",", col_names = "id")
  ema_response_files <- read_file_list(data_dirname,wockets_dirname,paste(prepend_surveys,"surveys",sep = "/"),id_varstub,filename_varstub, hour_filter = FALSE)
  raw_ema <- lapply(ema_response_files,read_ema, data_dirname = data_dirname, suffix_dirname = wockets_dirname)
  
  if(!skip_manual){
    manual_ema_response_files <- read_file_list(data_dirname,manual_dirname,paste(prepend_surveys,"surveys",sep = "/"),id_varstub, filename_varstub, hour_filter = FALSE)
    manual_raw_ema <- lapply(manual_ema_response_files,read_ema, data_dirname = data_dirname, suffix_dirname = manual_dirname)
  }
  
  ema_responses <- raw_ema[[1]]
  for(i in 2:length(raw_ema)){
    ema_responses <- bind_rows(ema_responses,raw_ema[[i]])
  }
  if(!skip_manual){
    manual_ema_responses <- manual_raw_ema[[1]]
    for(i in 2:length(manual_raw_ema)){
      manual_ema_responses <- bind_rows(manual_ema_responses,manual_raw_ema[[i]])
    }
    merge_responses <- anti_join(manual_ema_responses,ema_responses, by = c("system_file","PromptTime"))
    pre_filtered_ema <- bind_rows(ema_responses,merge_responses)
  } else {
    pre_filtered_ema <- ema_responses
  }
  write_csv(pre_filtered_ema,paste(data_dirname,"ema_responses.csv", sep = "/"))
  return(pre_filtered_ema)
}

prebind_data <- function(filtered_data, variable_prefix, name_keys = "", name_value_pairs = "", return_name_columns = FALSE, separator = ",|"){
  if(sum(!is.na(filtered_data %>% select(!!variable_prefix))>0)){
    filtered_newvars <- cSplit_e(filtered_data, variable_prefix,type = "character", fill = 0, sep = separator)
    names(filtered_newvars) <- enc2native(names(filtered_newvars))
    selected_data <- filtered_newvars %>%
      mutate_at(vars(starts_with(paste0(variable_prefix,"_"))),funs(ifelse(is.na(eval(parse(text = variable_prefix))),NA,.))) %>%
      select(starts_with(paste0(variable_prefix,"_")))
    if(return_name_columns){
      return(names(selected_data))
    }
    names(selected_data) <- name_keys[names(selected_data)]
    new_return <- generate_missing_column(selected_data,get_labels(name_keys))
    return_data <- new_return %>%
      set_label(unlist(name_value_pairs))
  } else {
    new_return <- generate_missing_column(filtered_data,get_labels(name_keys))
    return_data <- new_return %>%
      select(starts_with(paste0(variable_prefix,"_"))) %>%
      set_label(unlist(name_value_pairs))
  }
  return(return_data)
}

generate_missing_column <- function(data_name, column_names){
  return_data_name <- data_name
  for(i in column_names){
    if(!(i %in% names(return_data_name))){
      return_data_name <- mutate(return_data_name, !!i := NA)
    }
  }
  return(return_data_name)
}

enrollment_dates <- function(data_dirname){
  enrollment <- read_csv(paste(data_dirname,"enrollment.csv", sep = "/")) %>%
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
  
  write_csv(as.data.frame(enrollment$file_id),paste(data_dirname,"pids.txt",sep = "/"),col_names = FALSE)
  
  return(enrollment)
}

read_file_list <- function(data_dirname, 
                           midpoint_dirname, 
                           end_dirname,
                           id_filter, 
                           file_filter, 
                           hour_filter = TRUE){
  data_files <- dir(paste(data_dirname,midpoint_dirname, sep = "/"),pattern = id_filter)
  data_dirs <- paste(data_dirname,midpoint_dirname,data_files, sep = "/")
  date_files <- dir(paste(data_dirs,end_dirname, sep = "/"), full.names = TRUE)
  if(hour_filter){
    date_files <- dir(date_files, full.names = TRUE)
  }
  return_files <- list.files(date_files,pattern = file_filter, full.names = TRUE, include.dirs = FALSE, recursive = TRUE)
  return(return_files)
}

read_gps <- function(data_dirname,wockets_dirname,manual_dirname, skip_manual = FALSE){
  gps_files <- read_file_list(data_dirname,wockets_dirname,"data/","lml_com$","GPS.csv$")
  pre_raw_gps_log <- rbindlist(lapply(gps_files,skip_fread, data_dirname = data_dirname, suffix_dirname = wockets_dirname), fill = TRUE)
  
  if(!skip_manual){
  manual_gps_files <- read_file_list(data_dirname,manual_dirname,"data","lml_com$","GPS.csv$")
  manual_raw_gps_log <- rbindlist(lapply(manual_gps_files,skip_fread, data_dirname = data_dirname, suffix_dirname = manual_dirname), fill = TRUE)
  # Switching to Data.Table Paradigm for Fast GPS Processing
  merge_manual <- anti_join(manual_raw_gps_log,pre_raw_gps_log, by = c("file_id","V1"))
  raw_gps_log <- bind_rows(pre_raw_gps_log,merge_manual)
  } else {
    raw_gps_log <- pre_raw_gps_log
  }
  
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
      return_csv <- fread(file, colClasses = 'character', encoding = "UTF-8")
      idstringlength <- str_length(paste(data_dirname,suffix_dirname,"", sep = "/"))
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
    idstringlength <- str_length(paste(data_dirname,suffix_dirname,"", sep = "/"))
    id <- str_sub(file_to_read,idstringlength+4,idstringlength+7)
    return_ema_file$system_file <- id
    return_ema_file$discard <- NULL
    return(return_ema_file)  
  }, silent = TRUE)
  return(NULL)
}

