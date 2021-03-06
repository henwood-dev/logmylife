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


write_prompt_responses <- function(data_dirname, wockets_dirname, manual_dirname = NULL, skip_manual = TRUE){
  #ids <- read_delim(paste(data_dirname,"file_ids.txt",sep = "/"), delim = ",", col_names = "id")
  prompt_response_files <- read_file_list(data_dirname,wockets_dirname,"surveys","lml_com$","Prompts.csv$", hour_filter = FALSE)
  raw_prompts <- lapply(prompt_response_files,read_ema, data_dirname = data_dirname, suffix_dirname = wockets_dirname)
  
  if(!skip_manual){
    manual_prompt_response_files <- read_file_list(data_dirname,manual_dirname,"surveys","lml_com$","Prompts.csv$", hour_filter = FALSE)
    manual_raw_prompts <- lapply(manual_prompt_response_files,read_ema, data_dirname = data_dirname, suffix_dirname = manual_dirname)
  }
  
  prompt_responses <- raw_prompts[[1]]
  for(i in 2:length(raw_prompts)){
    prompt_responses <- bind_rows(prompt_responses,raw_prompts[[i]])
  }
  if(!skip_manual){
    manual_prompt_responses <- manual_raw_prompts[[1]]
    for(i in 2:length(manual_raw_prompts)){
      manual_prompt_responses <- bind_rows(manual_prompt_responses,manual_raw_prompts[[i]])
    }
    merge_responses <- anti_join(manual_prompt_responses,prompt_responses, by = c("system_file","TimeStampPrompted"))
    pre_filtered_prompts <- bind_rows(prompt_responses,merge_responses)
  } else {
    pre_filtered_prompts <- prompt_responses
  }
  write_csv(pre_filtered_prompts,paste(data_dirname,"prompt_responses.csv", sep = "/"))
  return(pre_filtered_ema)
}

pipe_print <- function(data_to_pass, string_to_print){
  print(string_to_print)
  return(data_to_pass)
}

stata_varcheck_setnames <- function(dataset){
  longvars <- c("longvars")
  for(i in 1:ncol(dataset)){
    if(str_length(names(dataset[i])) > 32) {
      longvars <- append(longvars,names(dataset[i]))
    }
  }
  longvars[1] <- NA
  return(longvars)
}


simplify_sni_id <- function(sni_id){
  remove_spaces <- gsub(" ","",sni_id)
  remove_periods <- gsub("\\.","",remove_spaces)
  remove_quote <- gsub("'","",remove_periods)
  make_lower <- tolower(remove_quote)
  return(make_lower)
}

write_master_logs <- function(data_dirname, wockets_dirname, manual_dirname = NULL, skip_manual = TRUE,
                              master_filter = "master"){
  masterlog_files <- read_file_list(data_dirname,wockets_dirname,"logs","lml_com$",paste0(master_filter,".log.csv$"))
  pre_master_log <- rbindlist(lapply(masterlog_files,skip_fread, data_dirname = data_dirname, suffix_dirname = wockets_dirname), fill = TRUE)
  
  if(!skip_manual){
    manual_masterlog_files <- read_file_list(data_dirname,manual_dirname,"logs","lml_com$","master.log.csv$")
    manual_master_log <- rbindlist(lapply(manual_masterlog_files,skip_fread, data_dirname = data_dirname, suffix_dirname = manual_dirname), fill = TRUE)
    # Switching to Data.Table Paradigm for Fast Master Processing
    merge_manual <- anti_join(manual_master_log,pre_master_log, by = c("file_id","V1"))
    raw_master_log <- bind_rows(pre_master_log,merge_manual)
  } else {
    raw_master_log <- as.data.table(pre_master_log)
  }
  raw_master_log[, V2:= NULL]

  write_csv(raw_master_log,paste0(data_dirname,"/",master_filter,"_logs.csv"))
  return(raw_master_log)
}

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

read_file_list <- function(data_dirname, 
                           midpoint_dirname = NULL, 
                           end_dirname = NULL,
                           id_filter = "*", 
                           file_filter, 
                           hour_filter = TRUE,
                           recursive = TRUE){
  if(!is.null(midpoint_dirname) & !is.null(end_dirname)){
    data_files <- dir(paste(data_dirname,midpoint_dirname, sep = "/"),pattern = id_filter)
    data_dirs <- paste(data_dirname,midpoint_dirname,data_files, sep = "/")
    date_files <- dir(paste(data_dirs,end_dirname, sep = "/"), full.names = TRUE)
  
    if(hour_filter){
      date_files <- dir(date_files, full.names = TRUE)
    }
    return_files <- list.files(date_files,pattern = file_filter, full.names = TRUE, include.dirs = FALSE, recursive = recursive)
  } else {
    return_files <- list.files(data_dirname,pattern = file_filter, full.names = TRUE, include.dirs = FALSE, recursive = recursive)
  }
  return(return_files)
}

read_gps <- function(data_dirname,wockets_dirname,manual_dirname, skip_manual = FALSE, fast_mode = FALSE){
  if(!fast_mode){
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
  }
  else{
    raw_gps_log <- fread(paste(data_dirname,"gps_logs.csv",sep = "/"), colClasses = rep("chr",7))
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

skip_fread <- function(file, data_dirname, suffix_dirname, supply_header = FALSE){
  if(file.size(file) > 0){
    try({
      return_csv <- fread(file, colClasses = 'character', encoding = "UTF-8", header = supply_header)
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

