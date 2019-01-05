# Libraries
library(sjlabelled)
library(splitstackshape)
library(data.table)
library(doParallel)
library(parallel)
library(iterators)
library(foreach)
library(haven)
library(tidyverse)
library(lubridate)
library(geosphere)
library(bit64)

# Override
select <- dplyr::select

# Imports
source("LML_Tidy_Helpers.R", encoding = "UTF-8")
source("LML_Tidy_EMA.R", encoding = "UTF-8")
source("LML_Tidy_Daily.R", encoding = "UTF-8")

# Keys
if(.Platform$OS.type == "windows"){
  data_dirname <- "C:/Users/dzubur/Desktop/LML Raw Data"
} else {
  data_dirname <- "/Users/Eldin/Downloads/Data"
}
if(.Platform$OS.type == "windows"){
  sni_stata_filename <- "C:/Users/dzubur/SharePoint/T/Team/Eldin/LML Data Management/Data/SNI.dta"
} else {
  sni_stata_filename <- "/Users/Eldin/Downloads/Data/SNI.dta"
}
wockets_dirname <- "Wockets"
manual_dirname <- "Manual"
id_varstub <- "lml_com$"
filename_varstub <- "PromptResponses_EMA.csv$"

dummy3 <- function() {
  data_dirname <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Prompt Level"
  ema <- tidy_ema(data_dirname, wockets_dirname,manual_dirname,TRUE,FALSE,FALSE,TRUE)

  #ema <- tidy_ema(data_dirname, wockets_dirname, manual_dirname)
  #enroll_filepath <- "C:/Users/dzubur/Desktop/LML Raw Data/enroll_sheet.csv"
  enroll_filepath <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Prompt Level/enroll_sheet.csv"
  enroll <- read_csv(enroll_filepath) %>%
    transmute(system_file = as.character(PID_master),
              enroll = as_date(Date_PhoneSetup, format = "%m/%d/%y", tz = ""),
              end = enroll + 8)
  ema_new <- ema %>%
    left_join(enroll, by = "system_file") %>%
    mutate(date = as_date(PromptDate, tz = ""),
           day = date - enroll + 1) %>%
    filter(date <= end & date >= enroll)
  write_dta(ema_new,"/Users/eldin/University of Southern California/LogMyLife Project - Documents/Team/Sara/ema_data.dta")
  #write_dta(ema_new,"C:/Users/dzubur/Desktop/ema_data.dta")
  
  gps <- read_gps(data_dirname,wockets_dirname,manual_dirname, skip_manual)
  daily <- tidy_daily(data_dirname, wockets_dirname, manual_dirname, sni_stata_filename)
  daily_sleep <- daily %>%
    filter(prompt_status == "Completed") %>%
    select(subject_id,prompt_date,wake_hour,wake_minute,sleep_hour,sleep_minute,no_sleep) %>%
    mutate(date_now = as.Date(prompt_date, origin = "1970-01-01", tz = "America/Los_Angeles"),
           date_yesterday = date_now - days()) %>%
    mutate(wake_hour = ifelse(wake_hour > 12, wake_hour - 12, wake_hour),
           sleep_hour = ifelse(sleep_hour > 12, sleep_hour - 12, sleep_hour)) %>%
    mutate(wake_am_time = paste0(format(date_now,"%Y-%m-%d")," ",str_pad(wake_hour,2,pad="0"),":",str_pad(wake_minute,2,pad = "0")," AM"),
           wake_pm_time = paste0(format(date_now,"%Y-%m-%d")," ",str_pad(wake_hour,2,pad="0"),":",str_pad(wake_minute,2,pad = "0"), " PM"),
           sleep_am_time = paste0(format(date_now,"%Y-%m-%d")," ",str_pad(sleep_hour,2,pad="0"),":",str_pad(sleep_minute,2,pad = "0")," AM"),
           sleep_pm_time = paste0(format(date_yesterday,"%Y-%m-%d")," ",str_pad(sleep_hour,2,pad="0"),":",str_pad(sleep_minute,2,pad = "0"), " PM")) %>%
    mutate_at(vars(ends_with("time")),funs(as.POSIXct(., format = "%Y-%m-%d %I:%M %p", origin = "1970-01-01", tz = "America/Los_Angeles"))) %>%
    mutate(wakeam_sleepam = difftime(wake_am_time,sleep_am_time,units = "hours"),
           wakeam_sleeppm = difftime(wake_am_time,sleep_pm_time,units = "hours"),
           wakepm_sleepam = difftime(wake_pm_time,sleep_am_time,units = "hours"),
           wakepm_sleeppm = difftime(wake_pm_time,sleep_pm_time,units = "hours")) %>%
    mutate(aa_bin = ifelse(wakeam_sleepam > 0 & wakeam_sleepam < 24,1,0),
           ap_bin = ifelse(wakeam_sleeppm > 0 & wakeam_sleeppm < 24,1,0),
           pa_bin = ifelse(wakepm_sleepam > 0 & wakepm_sleepam < 24,1,0),
           pp_bin = ifelse(wakepm_sleeppm > 0 & wakepm_sleeppm < 24,1,0))
  
  sleep_hour <- daily %>%
    select(subject_id,prompt_date,prompt_time,prompt_status) %>%
    filter(prompt_status == "Completed") %>%
    mutate(prompt_time = str_replace(prompt_time,"PDT",""),
           prompt_time = str_replace(prompt_time,"PST",""),
           fulltime = as.POSIXct(prompt_time, format = "%a %b %d %H:%M:%S %Y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           survey_hour = hour(fulltime)) %>%
    select(subject_id,prompt_date,survey_hour)
  
  merge_gps <- gps
  merge_gps[, date := as.Date(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")]
  
  df_count <- nrow(daily_sleep)
  temp_daily <- as.data.table(daily_sleep)
  core_count <- detectCores() - 1L
  registerDoParallel(cores = core_count)
  
  # WAKE AM SLEEP AM
  time_amam <- foreach(i=1:df_count, .combine = rbind, .packages = c("data.table")) %dopar% {
    if(!is.na(temp_daily[i,aa_bin]) && temp_daily[i,aa_bin] == 1){
      max_time <- temp_daily[i, wake_am_time]
      min_time <- temp_daily[i, sleep_am_time]
      temp_id <- temp_daily[i,subject_id]
      temp_date <- temp_daily[i,prompt_date]
      temp_gps <- merge_gps[fulltime <= max_time & fulltime >= min_time & file_id == temp_id]
      if(nrow(temp_gps) > 0){
        temp_gps[, longitude_lag := shift(longitude, type = "lag")]
        temp_gps[, latitude_lag := shift(latitude, type = "lag")]
        temp_gps[, fulltime_lag := shift(fulltime, type = "lag")]
        temp_gps[, distance := distHaversine(temp_gps[,.(longitude,latitude)],
                                             temp_gps[,.(longitude_lag,latitude_lag)])]
        temp_gps[, timedif := difftime(fulltime,fulltime_lag, units = "mins")]
        temp_gps[, velocity := distance/as.numeric(timedif)]
        collapsed_gps <- temp_gps[,lapply(.SD, mean, na.rm=TRUE),by = file_id]
        collapsed_gps[,.(subject_id = file_id, date_now = date, velocity_aa = velocity )]
      } else {
        temp_return <- temp_daily[i,.(subject_id,date_now)]
        temp_return[, velocity_aa := NA]
        temp_return  
      }
    } else {
      temp_return <- temp_daily[i,.(subject_id,date_now)]
      temp_return[, velocity_aa := NA]
      temp_return
    }
  }
  
  # WAKE AM SLEEP PM
  time_ampm <- foreach(i=1:df_count, .combine = rbind, .packages = c("data.table")) %dopar% {
    if(!is.na(temp_daily[i,ap_bin]) && temp_daily[i,ap_bin] == 1){
      max_time <- temp_daily[i, wake_am_time]
      min_time <- temp_daily[i, sleep_pm_time]
      temp_id <- temp_daily[i,subject_id]
      temp_date <- temp_daily[i,prompt_date]
      temp_gps <- merge_gps[fulltime <= max_time & fulltime >= min_time & file_id == temp_id]
      if(nrow(temp_gps) > 0){
        temp_gps[, longitude_lag := shift(longitude, type = "lag")]
        temp_gps[, latitude_lag := shift(latitude, type = "lag")]
        temp_gps[, fulltime_lag := shift(fulltime, type = "lag")]
        temp_gps[, distance := distHaversine(temp_gps[,.(longitude,latitude)],
                                             temp_gps[,.(longitude_lag,latitude_lag)])]
        temp_gps[, timedif := difftime(fulltime,fulltime_lag, units = "mins")]
        temp_gps[, velocity := distance/as.numeric(timedif)]
        collapsed_gps <- temp_gps[,lapply(.SD, mean, na.rm=TRUE),by = file_id]
        collapsed_gps[,.(subject_id = file_id, date_now = date, velocity_ap = velocity )]
      } else {
        temp_return <- temp_daily[i,.(subject_id,date_now)]
        temp_return[, velocity_ap := NA]
        temp_return  
      }
    } else {
      temp_return <- temp_daily[i,.(subject_id,date_now)]
      temp_return[, velocity_ap := NA]
      temp_return
    }
  }
  
  # WAKE PM SLEEP AM
  time_pmam <- foreach(i=1:df_count, .combine = rbind, .packages = c("data.table")) %dopar% {
    if(!is.na(temp_daily[i,pa_bin]) && temp_daily[i,pa_bin] == 1){
      max_time <- temp_daily[i, wake_pm_time]
      min_time <- temp_daily[i, sleep_am_time]
      temp_id <- temp_daily[i,subject_id]
      temp_date <- temp_daily[i,prompt_date]
      temp_gps <- merge_gps[fulltime <= max_time & fulltime >= min_time & file_id == temp_id]
      if(nrow(temp_gps) > 0){
        temp_gps[, longitude_lag := shift(longitude, type = "lag")]
        temp_gps[, latitude_lag := shift(latitude, type = "lag")]
        temp_gps[, fulltime_lag := shift(fulltime, type = "lag")]
        temp_gps[, distance := distHaversine(temp_gps[,.(longitude,latitude)],
                                             temp_gps[,.(longitude_lag,latitude_lag)])]
        temp_gps[, timedif := difftime(fulltime,fulltime_lag, units = "mins")]
        temp_gps[, velocity := distance/as.numeric(timedif)]
        collapsed_gps <- temp_gps[,lapply(.SD, mean, na.rm=TRUE),by = file_id]
        collapsed_gps[,.(subject_id = file_id, date_now = date, velocity_pa = velocity )]
      } else {
        temp_return <- temp_daily[i,.(subject_id,date_now)]
        temp_return[, velocity_pa := NA]
        temp_return  
      }
    } else {
      temp_return <- temp_daily[i,.(subject_id,date_now)]
      temp_return[, velocity_pa := NA]
      temp_return
    }
  }
  
  # WAKE PM SLEEP PM
  time_pmpm <- foreach(i=1:df_count, .combine = rbind, .packages = c("data.table")) %dopar% {
    if(!is.na(temp_daily[i,pp_bin]) && temp_daily[i,pp_bin] == 1){
      max_time <- temp_daily[i, wake_pm_time]
      min_time <- temp_daily[i, sleep_pm_time]
      temp_id <- temp_daily[i,subject_id]
      temp_date <- temp_daily[i,prompt_date]
      temp_gps <- merge_gps[fulltime <= max_time & fulltime >= min_time & file_id == temp_id]
      if(nrow(temp_gps) > 0){
        temp_gps[, longitude_lag := shift(longitude, type = "lag")]
        temp_gps[, latitude_lag := shift(latitude, type = "lag")]
        temp_gps[, fulltime_lag := shift(fulltime, type = "lag")]
        temp_gps[, distance := distHaversine(temp_gps[,.(longitude,latitude)],
                                             temp_gps[,.(longitude_lag,latitude_lag)])]
        temp_gps[, timedif := difftime(fulltime,fulltime_lag, units = "mins")]
        temp_gps[, velocity := distance/as.numeric(timedif)]
        collapsed_gps <- temp_gps[,lapply(.SD, mean, na.rm=TRUE),by = file_id]
        collapsed_gps[,.(subject_id = file_id, date_now = date, velocity_pp = velocity )]
      } else {
        temp_return <- temp_daily[i,.(subject_id,date_now)]
        temp_return[, velocity_pp := NA]
        temp_return  
      }
    } else {
      temp_return <- temp_daily[i,.(subject_id,date_now)]
      temp_return[, velocity_pp := NA]
      temp_return
    }
  }
  
  enroll <- enrollment_dates(data_dirname) %>%
    mutate(subject_id = as.character(file_id)) %>%
    select(-file_id)
    
  
  enroll_sleep <- read_csv(paste(data_dirname,"enrollment.csv", sep = "/")) %>%
    select(subject_id = PID_master,sleeptime,waketime) %>%
    mutate(sleep = str_split_fixed(sleeptime,":",n=2)[,1],
           wake = str_split_fixed(waketime,":",n=2)[,1],
           duration = as.integer(wake)-as.integer(sleep),
           duration = ifelse(duration < 0,duration + 24,duration))
  
  merged_sleep <- daily_sleep %>%
    bind_cols(time_amam, time_ampm, time_pmam, time_pmpm,sleep_hour) %>%
    rowwise() %>%
    mutate(min_velocity= min(velocity_aa,velocity_ap,velocity_pa,velocity_pp, na.rm = TRUE))
  
  max_sleep = 16
  min_sleep = 0
  
  algo_sleep <- merged_sleep %>%
    mutate(hour_wake_morning = hour(wake_am_time),
           hour_wake_afternoon = hour(wake_pm_time)) %>%
    mutate(possible_aa = ifelse(survey_hour >= hour_wake_morning,1,0),
           possible_ap = ifelse(survey_hour >= hour_wake_morning,1,0),
           possible_pa = ifelse(survey_hour >= hour_wake_afternoon,1,0),
           possible_pp = ifelse(survey_hour >= hour_wake_afternoon,1,0)) %>%
    mutate(match_aa = ifelse(wakeam_sleepam > min_sleep & wakeam_sleepam < max_sleep && possible_aa == 1 && (!is.na(aa_bin) && aa_bin == 1) && (is.na(velocity_aa) | min_velocity == velocity_aa),"aa",""),
           match_ap = ifelse(wakeam_sleeppm > min_sleep & wakeam_sleeppm < max_sleep && possible_ap == 1 && (!is.na(ap_bin) && ap_bin == 1) && (is.na(velocity_ap) | min_velocity == velocity_ap),"ap",""),
           match_pa = ifelse(wakepm_sleepam > min_sleep & wakepm_sleepam < max_sleep && possible_pa == 1 && (!is.na(pa_bin) && pa_bin == 1) && (is.na(velocity_pa) | min_velocity == velocity_pa),"pa",""),
           match_pp = ifelse(wakepm_sleeppm > min_sleep & wakepm_sleeppm < max_sleep && possible_pp == 1 && (!is.na(pp_bin) && pp_bin == 1) && (is.na(velocity_pp) | min_velocity == velocity_pp),"pp","")) %>%
   mutate_at(vars(starts_with("match")),funs(ifelse(is.na(.),"",.))) %>%
    mutate(sleep_decision = paste(match_aa,match_ap,match_pa,match_pp,sep=",")) %>%
    mutate(time_aa = ifelse(possible_aa == 1 && (!is.na(aa_bin) && aa_bin == 1) && wakeam_sleepam > min_sleep && wakeam_sleepam < max_sleep,"aa",""),
           time_ap = ifelse(possible_ap == 1 && (!is.na(ap_bin) && ap_bin == 1) && wakeam_sleeppm > min_sleep && wakeam_sleeppm < max_sleep,"ap",""),
           time_pa = ifelse(possible_pa == 1 && (!is.na(pa_bin) && pa_bin == 1) && wakepm_sleepam > min_sleep && wakepm_sleepam < max_sleep,"pa",""),
           time_pp = ifelse(possible_pp == 1 && (!is.na(pp_bin) && pp_bin == 1) && wakepm_sleeppm > min_sleep && wakepm_sleeppm < max_sleep,"pp","")) %>%
    mutate(time_decision = paste(time_aa,time_ap,time_pa,time_pp,sep=","),
           new_sleep1 = ifelse(sleep_decision == "aa,,,",wakeam_sleepam,
                               ifelse(sleep_decision == ",ap,," || sleep_decision == ",,pa," || sleep_decision == ",ap,pa,",wakeam_sleeppm,
                                      ifelse(sleep_decision == ",,,pp",wakepm_sleeppm,NA))),
           has_decision = ifelse(!is.na(new_sleep1),1,0)) %>%
    mutate(mt_aa = ifelse(has_decision == 1,match_aa,time_aa), # ifelse(possible_aa == 1 && (!is.na(aa_bin) && aa_bin == 1) ,"aa",""), # 
           mt_ap = ifelse(has_decision == 1,match_ap,time_ap), # ifelse(possible_ap == 1 && (!is.na(ap_bin) && ap_bin == 1) ,"ap",""), # 
           mt_pa = ifelse(has_decision == 1,match_pa,time_pa), # ifelse(possible_pa == 1 && (!is.na(pa_bin) && pa_bin == 1) ,"pa",""), # 
           mt_pp = ifelse(has_decision == 1,match_pp,time_pp), # ifelse(possible_pp == 1 && (!is.na(pp_bin) && pp_bin == 1) ,"pp",""), # 
           mt_decision = paste(mt_aa,mt_ap,mt_pa,mt_pp,sep=","),
           new_sleep2 = ifelse(mt_decision == "aa,,,",wakeam_sleepam,
                       ifelse(mt_decision == ",ap,," || mt_decision == ",,pa," || mt_decision == ",ap,pa,",wakeam_sleeppm,
                       ifelse(mt_decision == ",,,pp",wakepm_sleeppm,NA))),
           new_sleep3 = ifelse(no_sleep == 1,0,new_sleep2)) %>%
    left_join(enroll,by=c("subject_id")) %>%
    filter(enroll_start <= prompt_date & enroll_end >= prompt_date) %>%
    select(subject_id,prompt_date,new_sleep_duration = new_sleep3)
  
  written_out <- daily %>%
    filter(prompt_status == "Completed") %>%
    left_join(algo_sleep,by = c("subject_id","prompt_date")) %>%
    left_join(enroll,by=c("subject_id")) %>%
    filter(enroll_start <= prompt_date & enroll_end >= prompt_date)
  
  write_dta(written_out,paste(data_dirname,"sleep_daily.dta", sep = "/"))
                                     
    
  
  

  
  
    
}

dummy2 <- function(){
daily <- tidy_daily(data_dirname, wockets_dirname, manual_dirname, sni_stata_filename)
new_gps <- tidy_gps(data_dirname, wockets_dirname, manual_dirname)
enroll <- enrollment_dates(data_dirname)
new_ema <- new_gps %>%
  mutate(file_id = as.integer(system_file)) %>%
  left_join(enroll, by = "file_id")
new_gps2 <- rename(new_ema,alcohol_social_other_stfriends = alcohol_social_other_streetfriends,
                   tempted_social_other_stfriends = tempted_social_other_streetfriends)
write_dta(new_gps2,paste(data_dirname,"gps.dta", sep = "/"))

new_daily <- daily %>%
  mutate(file_id = as.integer(subject_id)) %>%
  left_join(enroll, by = "file_id")
write_dta(new_daily,paste(data_dirname,"daily.dta", sep = "/"))

all_gps <- read_gps(data_dirname,wockets_dirname,manual_dirname, skip_manual)
write_dta(all_gps,paste(data_dirname,"gps_all.dta", sep = "/"))


  
#ema <- tidy_ema(data_dirname, wockets_dirname, manual_dirname)
#ema2 <- rename(ema,alcohol_social_other_stfriends = alcohol_social_other_streetfriends,
#               tempted_social_other_stfriends = tempted_social_other_streetfriends)
#write_dta(ema2,paste0(data_dirname,"ema.dta"))
#write_dta(daily,paste0(data_dirname,"daily.dta"))
#new_gps2 <- rename(new_gps,alcohol_social_other_stfriends = alcohol_social_other_streetfriends,
#               tempted_social_other_stfriends = tempted_social_other_streetfriends)
#write_dta(new_gps2,paste0(data_dirname,"gps.dta"))
}

daily_gps <- function(data_dirname, wockets_dirname, manual_dirname){
  
}


dummy_fnc <- function(data_dirname){
  
  
  new_gps <- tidy_gps(data_dirname, wockets_dirname, manual_dirname)

  
  non_valid_gps_prompts <- new_gps %>%
    transmute(system_file,
              date = as.Date(PromptDate, tz = "America/Los Angeles", origin = "1970-01-01"),
              PromptID,
              PromptTime,
              has_gps = ifelse(is.na(latitude),0,1)) %>%
    filter(has_gps == 0) 
  
  non_valid_gps_days <- new_gps %>%
    transmute(system_file,
              date = as.Date(PromptDate, tz = "America/Los Angeles", origin = "1970-01-01"),
              gps_count = ifelse(is.na(latitude),0,1),
              ema_count = 1) %>%
    group_by(system_file, date) %>%
    summarize_all(funs(sum)) %>%
    mutate(gps_prop = gps_count/ema_count,
           invalid_day = ifelse(gps_prop == 0,1,0)) %>%
    filter(invalid_day == 1) 
  
  non_valid_gps_ids <- non_valid_gps_days %>%
    select(-gps_count,-ema_count,-gps_prop,-date) %>%
    group_by(system_file) %>%
    summarize_all(funs(sum)) %>%
    mutate(invalid_id = ifelse(invalid_day > 2,1,0)) %>%
    filter(invalid_id == 1)
  
  write_csv(non_valid_gps_days,paste(data_dirname,"nonvalid_days.csv", sep = "/"))
  write_csv(non_valid_gps_ids,paste(data_dirname,"nonvalid_ids.csv", sep = "/"))
  write_csv(non_valid_gps_prompts,paste(data_dirname,"nonvalid_prompts.csv", sep = "/"))
  
  library(lubridate)
  library(aspace)
  testgps <- read_gps(data_dirname,wockets_dirname,manual_dirname, skip_manual)
  
  test <- testgps[file_id == "2020"]
  test[,date := as.Date(fulltime)]
  test[, minute := minute((fulltime))]
  test[, hour := hour((fulltime))]
  test[, newminute := (minute %/% 2)*2]
  new_test <- test %>%
    group_by(file_id,date,hour,newminute) %>%
    summarize_at(vars(fulltime,latitude,longitude,accuracy), funs(mean))
  test2 <- new_test %>%
    ungroup() %>%
    select(latitude,longitude)
  
  
  library(grDevices)
  library(sf)
  library(plotKML)
  library(maptools)
  library(leaflet)
  library(rgeos)
  # needs rgeos
  coordproj <- CRS("+proj=longlat +datum=WGS84")
  coordproj2 <- CRS("+init=epsg:4304")
  coordprojnew <- CRS("+proj=lcc +lat_1=34.03333333333333 +lat_2=35.46666666666667 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +units=m +no_defs")
  coordprojnew2 <- CRS("+proj=lcc +lat_1=35.46666666666667 +lat_2=34.03333333333333 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
  epsg3006 <- leafletCRS(crsClass = "L.Proj.CRS", code = "EPSG:26945",
                         proj4def = "+proj=lcc +lat_1=35.46666666666667 +lat_2=34.03333333333333 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
                         resolutions = 2^(13:-1))
  
  m <- leaflet() %>%
    addTiles() %>%
    addPolygons(data = sps) %>%
  addPolygons(lat = con.hull$latitude, lng = con.hull$longitude)
  
  
  
  library(grDevices) # load grDevices package
  all_gps <- read_gps(data_dirname,wockets_dirname,manual_dirname, skip_manual)
  all_gps[, date := as.Date(fulltime, tz = "America/Los Angeles", origin = "1970-01-01")]
  all_gps[, system_file := file_id]
  all_gps2 <- all_gps
  all_gps <- all_gps2[gps_time_valid == 1 & gps_accuracy_valid == 1]
  
  all_ema <- tidy_ema(data_dirname,wockets_dirname,manual_dirname)
  all_ema <- as.data.table(all_ema)
  all_ema[, PromptTime := str_replace(PromptTime,"PDT","")]
  all_ema[, ema_time := as.POSIXct(PromptTime, tz = "America/Los_Angeles", format = "%a %b %d %H:%M:%S %Y", origin = "1970-01-01")]
  all_ema[, date := as.Date(ema_time,  tz = "America/Los Angeles", origin = "1970-01-01")]
  
  ema_id_dates <- all_ema %>%
    group_by(system_file,date) %>%
    summarize()
  ema_id_dates <- as.data.table(ema_id_dates)
  num_id_dates <- nrow(ema_id_dates)
  
  i = 1
  temp_df <- all_gps[date == ema_id_dates$date[i] & system_file == ema_id_dates$system_file[i]]
  temp_df <- select(temp_df,latitude,longitude)
  
  con.hull.pos <- chull(temp_df) # find positions of convex hull
  if(!is_empty(con.hull.pos)){
    con.hull <- rbind(temp_df[con.hull.pos,],temp_df[con.hull.pos[1],]) # get coordinates for convex hull
    coordinates(con.hull) <- c("longitude","latitude")
    proj4string(con.hull) <- CRS("+init=epsg:4326")
    p <- Polygon(con.hull)
    ps <- Polygons(list(p),1)
    sps <- SpatialPolygons(list(ps))
    proj4string(sps) <- CRS("+init=epsg:4326")
    sps_proj <- spTransform(sps,CRS("+init=epsg:26945"))
    area_km <- gArea(sps_proj)/(1000*1000)
  } else{
    area_km <- NA
  }
  ema_id_dates[i,mch_area_km := area_km]
  
  for(i in 2:num_id_dates){
    print(i)
    temp_df <- all_gps[date == ema_id_dates$date[i] & system_file == ema_id_dates$system_file[i]]
    temp_df <- select(temp_df,latitude,longitude)
    
    con.hull.pos <- chull(temp_df) # find positions of convex hull
    if(!is_empty(con.hull.pos)){
      con.hull <- rbind(temp_df[con.hull.pos,],temp_df[con.hull.pos[1],]) # get coordinates for convex hull
      coordinates(con.hull) <- c("longitude","latitude")
      proj4string(con.hull) <- CRS("+init=epsg:4326")
      p <- Polygon(con.hull)
      ps <- Polygons(list(p),1)
      sps <- SpatialPolygons(list(ps))
      proj4string(sps) <- CRS("+init=epsg:4326")
      sps_proj <- spTransform(sps,CRS("+init=epsg:26945"))
      area_km <- gArea(sps_proj)/(1000*1000)
    } else{
      area_km <- NA
    }
    ema_id_dates[i,mch_area_km := area_km]
    
  }
  
  df <- test2
  con.hull.pos <- chull(df) # find positions of convex hull
  con.hull <- rbind(df[con.hull.pos,],df[con.hull.pos[1],]) # get coordinates for convex hull
  coordinates(con.hull) <- c("longitude","latitude")
  proj4string(con.hull) <- CRS("+init=epsg:4326")
  p <- Polygon(con.hull)
  ps <- Polygons(list(p),1)
  sps <- SpatialPolygons(list(ps))
  proj4string(sps) <- CRS("+init=epsg:4326")
  sps_proj <- spTransform(sps,CRS("+init=epsg:26945"))
  
  #coordinates(con.hull) <- ~ latitude+longitude
  #proj4string(con.hull) <- CRS("+init=epsg:4326")
  #data.proj <- spTransform(data, CRS("+init=epsg:2790"))
  #data.proj
  
  plot(latitude ~ longitude, data = df) # plot data
  lines(con.hull) # add lines for convex hull
 coordinates(con.hull) <- c("longitude","latitude")
 proj4string(con.hull) <- CRS("+init=epsg:4326")
 
 
  
  p <- Polygon(con.hull)
  ps <- Polygons(list(p),1)
  sls <- SpatialPoints(con.hull,proj4string=coordproj)
  sps <- SpatialPolygons(list(ps))
  proj4string(con.hull) <- CRS("+init=epsg:4326")
  
  dftest <- data.frame(test = 1)
  spsdf <- SpatialPolygonsDataFrame(sps,dftest)
  
}


## Livability
# Exc: Sleep apnea?
#load(paste0(export_dir,"transitions.rdata"))
#transitions <- as.tibble(transitions)


gps_fusion_adapter <- function(new_gps, export_dirname){
  selected_gps <- new_gps %>%
    mutate(SAFE_NOW = as.character(safe),
           WHERE_NOW = as.character(where), DOINGWHAT_NOW = as.character(what),
           HAPPY_NOW = as.character(happy), STRESSED_NOW = as.character(stressed),
           SAD_NOW = as.character(sad), IRRITATED_NOW = as.character(irritated),
           CALM_NOW = as.character(calm), EXCITED_NOW = as.character(excited),
           BORED_NOW = as.character(bored), HUNGRY_NOW = as.character(hungry),
           HAPPY_NOWDS = as.integer(happy), STRESSED_NOWDS = as.integer(stressed),
           SAD_NOWDS = as.integer(sad), IRRITATED_NOWDS = as.integer(irritated),
           CALM_NOWDS = as.integer(calm), EXCITED_NOWDS = as.integer(excited),
           BORED_NOWDS = as.integer(bored), HUNGRY_NOWDS = as.integer(hungry),
           DRINKS_2HR = as.character(alcohol), DRINKS_OTHERSDRINKING = as.character(alcohol_social_use),
           ANYDRUGS_2HR = as.character(drugs), DRUGS_OTHERSUSING = as.character(drugs_social_use),
           TEMPTED_2HR = as.character(tempted), OTHERSUSINGWHENTEMPTED = as.character(tempted_social_use),
           LOCTIMESTAMP = strftime(fulltime, format = "%a %b %d %T %Z %Y")) %>%
    select(PID = system_file, SURVEYNUMBER = PromptID,PROMPT_REPROMPT = PromptType,
           STATUS = Status, DATE = PromptDate, PROMPTTIME = PromptTime, LOCTIMESTAMP,
           LATITUDE = latitude, LONGITUDE = longitude, ACCURACY = accuracy, CLOSEST5_2HR = Q1_social,
           SOCIAL_OTHER_2HR = Q1_a_socialother, SAFE_NOW,
           WHERE_NOW, DOINGWHAT_NOW, HAPPY_NOW, STRESSED_NOW, SAD_NOW, IRRITATED_NOW,CALM_NOW,
           EXCITED_NOW, BORED_NOW, HUNGRY_NOW, HAPPY_NOWDS, STRESSED_NOWDS, SAD_NOWDS,
           IRRITATED_NOWDS, CALM_NOWDS, EXCITED_NOWDS, BORED_NOWDS, HUNGRY_NOWDS,
           IMPTEVENT_2HR = Q10_stressevents,
           TOBACCO_2HR = Q11_tobacco,
           DRINKS_2HR, DRINKS_WHERE_2HR = Q12_b_alcohol_where,
           DRINKS_WHOCLOSE5 = Q12_c_alcohol_who,
           DRINKS_WHOOTHER = Q12_d_alcohol_who_other,
           DRINKS_OTHERSDRINKING, ANYDRUGS_2HR, 
           DRUGSTYPE = Q13_a_drugs_type,
           DRUGSWHERE = Q13_b_drugs_where,
           DRUGSWHO_CLOSE5 = Q13_c_drugs_who,
           DRUGSWHO_OTHER = Q13_d_drugs_who_other,
           DRUGS_OTHERSUSING,TEMPTED_2HR, 
           TEMPTWHERE = Q14_a_tempted_where,
           TEMPTWHO_CLOSE5 = Q14_b_tempted_who,  
           TEMPTWHO_OTHER = Q14_c_tempted_who_other,
           OTHERSUSINGWHENTEMPTED) %>%
    mutate(PA_SUM = (HAPPY_NOWDS + CALM_NOWDS)/2,
           NA_SUM = (STRESSED_NOWDS + SAD_NOWDS + IRRITATED_NOWDS)/3,
           temp_long = runif(nrow(new_gps),-118.808275,-118.501087),
           temp_lat = runif(nrow(new_gps),33.720756,33.989567),
           LATITUDE = ifelse(is.na(LATITUDE),temp_lat,LATITUDE),
           LONGITUDE = ifelse(is.na(LONGITUDE), temp_long,LONGITUDE)) %>%
    select(-temp_lat,-temp_long) %>%
    filter(STATUS != "Never Started") 
  # 
  # selected_gps <- new_gps %>%
  #   mutate(WHERE_NOW = as.character(where), DOINGWHAT_NOW = as.character(what),
  #          HAPPY_NOW = as.character(happy), STRESSED_NOW = as.character(stressed),
  #          SAD_NOW = as.character(sad), IRRITATED_NOW = as.character(irritated),
  #          CALM_NOW = as.character(calm), EXCITED_NOW = as.character(excited),
  #          BORED_NOW = as.character(bored), HUNGRY_NOW = as.character(hungry),
  #          HAPPY_NOWDS = as.integer(happy), STRESSED_NOWDS = as.integer(stressed),
  #          SAD_NOWDS = as.integer(sad), IRRITATED_NOWDS = as.integer(irritated),
  #          CALM_NOWDS = as.integer(calm), EXCITED_NOWDS = as.integer(excited),
  #          BORED_NOWDS = as.integer(bored), HUNGRY_NOWDS = as.integer(hungry),
  #          DRINKS_2HR = as.character(alcohol), DRINKS_OTHERSDRINKING = as.character(alcohol_social_use),
  #          ANYDRUGS_2HR = as.character(drugs), DRUGS_OTHERSUSING = as.character(drugs_social_use),
  #          TEMPTED_2HR = as.character(tempted), OTHERSUSINGWHENTEMPTED = as.character(tempted_social_use),
  #          LOCTIMESTAMP = strftime(fulltime, format = "%a %b %d %T %Z %Y")) %>%
  #   select(PID = system_file, SURVEYNUMBER = PromptID,PROMPT_REPROMPT = PromptType,
  #          STATUS = Status, DATE = PromptDate, PROMPTTIME = PromptTime, LOCTIMESTAMP,
  #          LATITUDE = latitude, LONGITUDE = longitude, ACCURACY = accuracy, CLOSEST5_2HR = Q1_social,
  #          SOCIAL_OTHER_2HR_STAFF = social_other_staff, SOCIAL_OTHER_2HR_STAFF = social_other_staff,
  #          SOCIAL_OTHER_2HR_HOMEFRIENDS = social_other_homefriends, SOCIAL_OTHER_2HR_STREETFRIENDS = social_other_streetfriends,
  #          SOCIAL_OTHER_2HR_POLICE = social_other_police, SOCIAL_OTHER_2HR_COWORKER = social_other_coworker,
  #          SOCIAL_OTHER_2HR_PARTNER = social_other_partner, SOCIAL_OTHER_2HR_STRANGER = social_other_stranger,
  #          WHERE_NOW, SAFE_NOW, DOINGWHAT_NOW, HAPPY_NOW, STRESSED_NOW, SAD_NOW, IRRITATED_NOW,CALM_NOW,
  #          EXCITED_NOW, BORED_NOW, HUNGRY_NOW, HAPPY_NOWDS, STRESSED_NOWDS, SAD_NOWDS,
  #          IRRITATED_NOWDS, CALM_NOWDS, EXCITED_NOWDS, BORED_NOWDS, HUNGRY_NOWDS,
  #          IMPTEVENT_2HR_LAW = stress_events_law, IMPTEVENT_2HR_NONE = stress_events_none,
  #          IMPTEVENT_2HR_PHYSICALFIGHT = stress_events_physicalfight, IMPTEVENT_2HR_BADNEWS = stress_events_badnews,
  #          IMPTEVENT_2HR_GOODNEWS = stress_events_goodnews, IMPTEVENT_2HR_VERBALFIGHT = stress_events_verbalfight,
  #          TOBACCO_2HR_OTHER = tobacco_other, TOBACCO_2HR_CHEW = tobacco_chew,
  #          TOBACCO_2HR_VAPE = tobacco_vape, TOBACCO_2HR_NONE = tobacco_none,
  #          TOBACCO_2HR_PAPER = tobacco_paper, DRINKS_2HR, DRINKS_WHERE_2HR_TRANSIT = alcohol_where_transit,
  #          DRINKS_WHERE_2HR_APARTMENT = alcohol_where_apartment, DRINKS_WHERE_2HR_OTHER = alcohol_where_other,
  #          DRINKS_WHERE_2HR_OUTDOORS = alcohol_where_outdoors, DRINKS_WHERE_2HR_BUSINESS = alcohol_where_business,
  #          DRINKS_WHERE_2HR_OTHERRESIDENCE = alcohol_where_otherresidence, DRINKS_WHERE_2HR_SCHOOLWORK = alcohol_where_schoolwork,
  #          DRINKS_WHERE_2HR_SERVICE = alcohol_where_service, DRINKS_WHOCLOSE5 = Q12_c_alcohol_who,
  #          DRINKS_WHOOTHER_FAMILY = alcohol_social_other_family, DRINKS_WHOOTHER_HOMEFRIENDS = alcohol_social_other_homefriends,
  #          DRINKS_WHOOTHER_STREETFRIENDS = alcohol_social_other_streetfriends, DRINKS_WHOOTHER_COWORKER = alcohol_social_other_coworker,
  #          DRINKS_WHOOTHER_PARTNER = alcohol_social_other_partner, DRINKS_WHOOTHER_STRANGER = alcohol_social_other_stranger,
  #          DRINKS_OTHERSDRINKING, ANYDRUGS_2HR, DRUGSTYPE_ECSTACY = drugs_type_ecstasy,
  #          DRUGSTYPE_ECSTACY = drugs_type_ecstasy, DRUGSTYPE_HALLUCINOGENS = drugs_type_hallucinogens,
  #          DRUGSTYPE_MARIJUANA = drugs_type_marijuana, DRUGSTYPE_METH = drugs_type_meth,
  #          DRUGSTYPE_SPICE = drugs_type_spice, DRUGSTYPE_RX = drugs_type_rx,
  #          DRUGSTYPE_OTHER = drugs_type_other, DRUGSWHERE_TRANSIT = drugs_where_transit,
  #          DRUGSWHERE_APARTMENT = drugs_where_apartment, DRUGSWHERE_OTHER = drugs_where_other,
  #          DRUGSWHERE_OUTDOORS = drugs_where_outdoors, DRUGSWHERE_BUSINESS = drugs_where_business,
  #          DRUGSWHERE_OTHERRESIDENCE = drugs_where_otherresidence, DRUGSWHERE_SCHOOLWORK = drugs_where_schoolwork,
  #          DRUGSWHERE_SERVICE = drugs_where_service, DRUGSWHO_CLOSE5 = Q13_c_drugs_who,
  #          DRUGSWHO_OTHER_FAMILY = drugs_social_other_family, DRUGSWHO_OTHER_HOMEFRIENDS = drugs_social_other_homefriends,
  #          DRUGSWHO_OTHER_STREETFRIENDS = drugs_social_other_streetfriends, DRUGSWHO_OTHER_COWORKER = drugs_social_other_coworker,
  #          DRUGSWHO_OTHER_PARTNER = drugs_social_other_partner, DRUGSWHO_OTHER_STRANGER = drugs_social_other_stranger,
  #          DRUGS_OTHERSUSING,TEMPTED_2HR, TEMPTWHERE_TRANSIT = tempted_where_transit,
  #          TEMPTWHERE_APARTMENT = tempted_where_apartment, TEMPTWHERE_OTHER = tempted_where_other,
  #          TEMPTWHERE_OUTDOORS = tempted_where_outdoors, TEMPTWHERE_BUSINESS = tempted_where_business,
  #          TEMPTWHERE_OTHERRESIDENCE = tempted_where_otherresidence, TEMPTWHERE_SCHOOLWORK = tempted_where_schoolwork,
  #          TEMPTWHERE_SERVICE = tempted_where_service, TEMPTWHO_CLOSE5 = Q14_b_tempted_who,  
  #          TEMPTWHO_OTHER_FAMILY = tempted_social_other_family, TEMPTWHO_OTHER_HOMEFRIENDS = tempted_social_other_homefriends,
  #          TEMPTWHO_OTHER_STREETFRIENDS = tempted_social_other_streetfriends, TEMPTWHO_OTHER_COWORKER = tempted_social_other_coworker,
  #          TEMPTWHO_OTHER_PARTNER = tempted_social_other_partner, TEMPTWHO_OTHER_STRANGER = tempted_social_other_stranger,
  #          OTHERSUSINGWHENTEMPTED) %>%
  #   mutate(PA_SUM = (HAPPY_NOWDS + CALM_NOWDS)/2,
  #          NA_SUM = (STRESSED_NOWDS + SAD_NOWDS + IRRITATED_NOWDS)/3,
  #          temp_long = runif(nrow(new_gps),-118.808275,-118.501087),
  #          temp_lat = runif(nrow(new_gps),33.720756,33.989567),
  #          LATITUDE = ifelse(is.na(LATITUDE),temp_lat,LATITUDE),
  #          LONGITUDE = ifelse(is.na(LONGITUDE), temp_long,LONGITUDE)) %>%
  #   select(-temp_lat,-temp_long) %>%
  #   filter(STATUS != "Never Started") 
  
  mean_affect <- selected_gps %>%
    group_by(PID) %>%
    summarise_at(vars(PA_MEAN = PA_SUM,NA_MEAN = NA_SUM),funs(mean(.,na.rm = TRUE)))
  
  print_ema_fusion <- selected_gps %>%
    left_join(mean_affect, by = "PID") %>%
    mutate(PA_ABOVEAVG = ifelse(PA_SUM>PA_MEAN,"Yes","No"),
           NA_ABOVEAVG = ifelse(NA_SUM>NA_MEAN,"Yes","No"))
  
  write_csv(print_ema_fusion,paste(export_dirname,"ema_fusion.csv", sep = "/"), na = "")
  
  all_ids <- unique(print_ema_fusion$PID)
  for(id in all_ids){
    print(paste0("Writing ID number: ",id))
    temp_dataset <- filter(print_ema_fusion, PID == id)
    write_csv(temp_dataset,paste0(export_dirname,"/","ema_fusion_",id,".csv"), na = "")
  }
  
  return(print_ema_fusion)
}

tidy_gps <- function(data_dirname, wockets_dirname, manual_dirname = "", skip_manual = FALSE, retain_names = FALSE){
  ema <- tidy_ema(data_dirname = data_dirname, wockets_dirname = wockets_dirname,
                  manual_dirname = manual_dirname, skip_manual = skip_manual, retain_names = retain_names) %>%
    mutate(file_id = system_file)
  
  ema_dt <- as.data.table(ema)
  ema_dt[, merge_row := .I]

  gps <- read_gps(data_dirname, wockets_dirname, manual_dirname, skip_manual = skip_manual)
  ema_premerge <- ema_dt[, c("PromptTime","file_id","merge_row")]
  ema_premerge[, PromptTime := str_replace(PromptTime,"PDT","")]
  ema_premerge[, PromptTime := str_replace(PromptTime,"PST","")]
  ema_premerge[, ema_time := as.POSIXct(PromptTime, tz = "America/Los_Angeles", format = "%a %b %d %H:%M:%S %Y", origin = "1970-01-01")]

  num_ema <- nrow(ema_premerge)
  core_count <- detectCores() - 1L
  registerDoParallel(cores = core_count)
  
  
  lla <- foreach(i=1:num_ema, .combine = rbind, .packages = c("data.table")) %dopar% {
    temp_ema <- ema_premerge[i]
    merge_time <- temp_ema[1,ema_time]
    merge_time_low <- merge_time-60*15
    merge_time_high <- merge_time+60*15
    merge_id <- temp_ema[1,file_id]
    merge_rownum <- temp_ema[1,merge_row]
    temp_gps <- gps[file_id == merge_id & fulltime > merge_time_low & fulltime < merge_time_high]
    collapsed_gps <- temp_gps[,lapply(.SD, mean, na.rm=TRUE),by = file_id]
    
    if(nrow(collapsed_gps) == 0) {
      empty_gps <- as.data.table(merge_id)
      empty_gps[,file_id := merge_id]
      empty_gps[,merge_id := NULL]
      empty_gps[,fulltime := merge_time]
      empty_gps[,latitude := NA_real_]
      empty_gps[,longitude := NA_real_]
      empty_gps[,accuracy := NA_real_]
      empty_gps[,gps_accuracy_valid := NA_real_]
      empty_gps[,merge_row := merge_rownum]
      empty_gps[,merge_rownum := NULL]
      empty_gps
    } else{
      collapsed_gps[, log_time := NULL]
      collapsed_gps[, measure_time := NULL]
      collapsed_gps[, time_diff := NULL]
      collapsed_gps[, gps_time_valid := NULL]
      collapsed_gps[, merge_row := merge_rownum]
      collapsed_gps
    }
  }
  
  new_gps <- merge(ema_dt,lla,by = "merge_row")
  new_gps[, merge_row := NULL]
  new_gps[, file_id.x := NULL]
  new_gps[, file_id.y := NULL]
  write_csv(new_gps,paste(data_dirname,"ema_gps.csv", sep = "/"))
  return(new_gps)
}

export_person_level <- function(data_dirname, wockets_dirname, manual_dirname, 
                                sni_stata_filename = "", skip_sni = TRUE, skip_manual = FALSE,
                                export_dirname = data_dirname){
  ema <- tidy_gps(data_dirname = data_dirname, wockets_dirname = wockets_dirname,
                  manual_dirname = manual_dirname, skip_manual = skip_manual) %>%
  mutate(participant = system_file)
  
  ema_person_compliance <- ema %>%
    mutate(comply = ifelse(Status == "Completed",1,0),
           gps_bin = as.integer(!is.na(latitude))) %>%
    group_by(participant) %>%
    summarize_at(vars(ema_compliance = comply,
                      gps_availability = gps_bin),funs(mean(.,na.rm=TRUE)))
  
  ema_person <- ema %>%
    mutate(alcohol_bin = as.integer(alcohol) != 1,
           drugs_bin = as.integer(drugs) == 1,
           tobacco_bin = as.integer(tobacco_none) != 1,
           tempted_bin = as.integer(tempted) != 1
           marijuana_bin = as.integer(drugs_type_marijuana)) %>%
    group_by(participant) %>%
    summarise_at(vars(ema_alcohol_events = alcohol_bin,
                      ema_marijuana_events = marijuana_bin,
                      ema_tobacco_events = tobacco_bin,
                      ema_ecstasy_events = drugs_type_ecstasy,
                      ema_hallucinogens_events = drugs_type_hallucinogens,
                      ema_meth_events = drugs_type_meth,
                      ema_rx_events = drugs_type_rx,
                      ema_spice_events = drugs_type_spice,
                      ema_other_events = drugs_type_other)
                 ,funs(sum(., na.rm = TRUE))) %>%
    mutate(ema_sum_harddrug_events = ema_other_events + ema_spice_events + ema_rx_events +
             ema_meth_events + ema_hallucinogens_events + ema_ecstasy_events) %>%
    left_join(ema_person_compliance, by = "participant")
  
  daily <- tidy_daily(data_dirname = data_dirname, wockets_dirname = wockets_dirname,
                      manual_dirname = manual_dirname, sni_stata_filename = sni_stata_filename,
                      skip_manual = skip_manual, skip_sni = skip_sni) %>%
    mutate(participant = subject_id)
  
  daily_person_compliance <- daily %>%
    mutate(comply = ifelse(prompt_status == "Completed",1,0)) %>%
    group_by(participant) %>%
    summarize_at(vars(daily_compliance = comply),funs(sum(.,na.rm=TRUE))) %>%
    mutate(daily_compliance = daily_compliance/7)
  
  daily_person <- daily %>%
    group_by(participant) %>%
    summarise_at(vars(daily_alcohol_total = alcohol_use,
                      daily_marijuana_total = marijuana_use,
                      daily_hallucinogens_total = drugs_type_hallucinogens,
                      daily_meth_total = drugs_type_meth,
                      daily_rx_total = drugs_type_rx,
                      daily_spice_total = drugs_type_spice,
                      daily_cocaine_total = drugs_type_cocaine,
                      daily_other_total = drugs_type_other)
                 ,funs(sum(., na.rm = TRUE))) %>%
    mutate(daily_sum_harddrug_total = (daily_hallucinogens_total +
                                         daily_meth_total + daily_rx_total + daily_spice_total +
                                         daily_cocaine_total + daily_other_total)) %>%
    left_join(daily_person_compliance, by = "participant") %>%
    left_join(ema_person, by = "participant")
    
  write_csv(daily_person,paste(export_dirname,"export_person.csv", sep = "/"))
  return(TRUE)
}


tidy_daily <- function(data_dirname, wockets_dirname, 
                       manual_dirname, sni_stata_filename = "", 
                       skip_sni = FALSE, skip_manual = FALSE, fast_mode = FALSE){
  if(!fast_mode){
    pre_filtered_dailylog <- write_daily_responses(data_dirname = data_dirname,
                                            wockets_dirname = wockets_dirname,
                                            manual_dirname = manual_dirname,
                                            skip_manual = skip_manual)
  } else {
    pre_filtered_dailylog <- read_csv(paste(data_dirname,"daily_responses.csv",sep="/"))
  }
  
  pre_filtered_dailylog <- generate_missing_column(pre_filtered_dailylog,c(
    "Q4_10_a_coke","Q4_10_a_coke_number", "Q7_sex_exchange_where_other", "Q7_sex_exchange_where"
  ))
  
  filtered_dailylog <- pre_filtered_dailylog %>%
    filter(!is.na(Subject_ID)) %>%
    mutate_all(funs(ifelse(.=="question is not displayed",NA_character_,.))) %>%
    mutate_all(funs(ifelse(.=="question was not displayed",NA_character_,.))) %>%
    mutate_all(funs(ifelse(.=="skipped",NA_character_,.))) %>%
    mutate_all(funs(as.character)) %>%
    select(-Q0_welcome,-Q8_thankyou,-Q3_1_sleeploc_other,-PromptType,-PromptID,-Subject_ID) %>%
    rename(subject_id = system_file,
           prompt_date = PromptDate,
           prompt_time = PromptTime,
           prompt_status = Status) %>%
    mutate(Q4_10_a_coke = ifelse(is.na(Q4_10_a_coke),Q4_10_a_coke_number,Q4_10_a_coke)) %>%
    mutate(sleep_quality = factor(Q3_b_sleep_quality,levels = c("Very poor",
                                                          "Poor",
                                                          "Fair",
                                                          "Good",
                                                          "Very good"))) %>%
    mutate(alcohol_binge = factor(Q4_2_a_alcohol,levels = c("No",
                                                                "Yes",
                                                                "Not sure"))) %>%
    mutate(marijuana_concentrates = factor(Q4_3_a_marijuana,levels = c("No",
                                                            "Yes",
                                                            "Not sure"))) %>%
    mutate_at(vars(Q4_2_alcohol,Q4_3_marijuana,Q4_6_synthmj,Q4_4_meth,Q4_8_prescription,
              Q4_5_mdma,Q4_7_halluc,Q4_9_heroin,Q4_10_a_coke),
              funs(str_extract(.,"[0-9]{1,2}$"))) %>%
    mutate(sleep_location = as_factor(Q3_sleeploc),
           wake_hour = as.integer(str_extract(Q1_waketime,"[0-9]+(?=:)")),
           sleep_hour = as.integer(str_extract(Q2_sleeptime,"[0-9]+(?=:)")),
           wake_minute = as.integer(str_extract(Q1_waketime,"(?![0-9]+:)[0-9]+")),
           sleep_minute = as.integer(str_extract(Q2_sleeptime,"(?![0-9]+:)[0-9]+")),
           no_sleep = ifelse(Q1_waketime == "no sleep",1,0),
           drug_other_moa = as_factor(Q4_17_b_other),
           alcohol_use = as.integer(Q4_2_alcohol),
           marijuana_use = as.integer(Q4_3_marijuana),
           synthetic_use = as.integer(Q4_6_synthmj),
           meth_use = as.integer(Q4_4_meth),
           rx_use = as.integer(Q4_8_prescription),
           mdma_use = as.integer(Q4_5_mdma),
           hallucinogen_use = as.integer(Q4_7_halluc),
           heroin_use = as.integer(Q4_9_heroin),
           cocaine_use = as.integer(Q4_10_a_coke),
           meth_moa = as_factor(Q4_4_a_meth),
           rx_moa = as_factor(Q4_8_a_prescription),
           mdma_moa = as_factor(Q4_5_a_mdma),
           heroin_moa = as_factor(Q4_9_a_heroin),
           cocaine_moa = as_factor(Q4_10_b_coke),
           cocaine_type = as_factor(Q4_10_a_coke_type),
           sex_partners = as_factor(Q5_sex_partners),
           sex_exchange = as_factor(Q7_sex_exchange),
           sex_exchange_text_where = Q7_sex_exchange_where_other
           ) %>%
    bind_cols(drugs_type_daily(.)) %>%
    bind_cols(sex_partners_daily(.)) %>%
    bind_cols(sex_where_daily(.))
  if(!skip_sni){
    return_dailylog <- filtered_dailylog %>%
      bind_cols(social_daily(., sni_stata_filename, "core")) %>%
      bind_cols(social_daily(., sni_stata_filename, "alcohol")) %>%
      bind_cols(social_daily(., sni_stata_filename, "marijuana")) %>%
      bind_cols(social_daily(., sni_stata_filename, "synthetic")) %>%
      bind_cols(social_daily(., sni_stata_filename, "meth")) %>%
      bind_cols(social_daily(., sni_stata_filename, "rx")) %>%
      bind_cols(social_daily(., sni_stata_filename, "mdma")) %>%
      bind_cols(social_daily(., sni_stata_filename, "hallucinogen")) %>%
      bind_cols(social_daily(., sni_stata_filename, "heroin")) %>%
      bind_cols(social_daily(., sni_stata_filename, "cocaine")) %>%
      select(-Q3_sleeploc,-Q1_waketime,-Q2_sleeptime,-Q3_b_sleep_quality,
             -Q4a_substances,-Q4b_substances,-Q4_17_b_other,
             -Q4_2_alcohol,-Q4_2_a_alcohol,
             -Q4_3_marijuana,-Q4_3_a_marijuana,
             -Q4_6_synthmj,
             -Q4_4_meth,-Q4_4_a_meth,
             -Q4_8_prescription,-Q4_8_a_prescription,
             -Q4_5_mdma,-Q4_5_a_mdma,
             -Q4_7_halluc,
             -Q4_9_heroin,-Q4_9_a_heroin,
             -Q4_10_a_coke,-Q4_10_a_coke_type,-Q4_10_b_coke,-Q4_10_a_coke_number,
             -Q5_sex_partners,-Q5_sex_a_id,-Q7_sex_exchange,-Q7_sex_exchange_where,-Q7_sex_exchange_where_other,
             -starts_with("R",ignore.case = FALSE),
             -Q4_0_soccore,-Q4_2_b_alcohol_who,-Q4_3_b_marijuana_who,-Q4_6_b_synthmj_who,
             -Q4_4_b_meth_who,-Q4_8_b_prescription_who,-Q4_5_b_mdma_who,-Q4_7_b_halluc_who,
             -Q4_9_b_heroin_who,-Q4_10_c_coke_who
             ) 
  } else {
    return_dailylog <- filtered_dailylog %>%
      select(-Q3_sleeploc,-Q1_waketime,-Q2_sleeptime,-Q3_b_sleep_quality,
             -Q4a_substances,-Q4b_substances,-Q4_17_b_other,
             -Q4_2_alcohol,-Q4_2_a_alcohol,
             -Q4_3_marijuana,-Q4_3_a_marijuana,
             -Q4_6_synthmj,
             -Q4_4_meth,-Q4_4_a_meth,
             -Q4_8_prescription,-Q4_8_a_prescription,
             -Q4_5_mdma,-Q4_5_a_mdma,
             -Q4_7_halluc,
             -Q4_9_heroin,-Q4_9_a_heroin,
             -Q4_10_a_coke,-Q4_10_a_coke_type,-Q4_10_b_coke,-Q4_10_a_coke_number,
             -Q5_sex_partners,-Q5_sex_a_id,-Q7_sex_exchange,-Q7_sex_exchange_where,-Q7_sex_exchange_where_other,
             -starts_with("R",ignore.case = FALSE)
      )
    }
             
  
  return(return_dailylog)
}

tidy_ema <- function(data_dirname, wockets_dirname, manual_dirname,
                     remove_sni = TRUE, skip_manual = FALSE, 
                     retain_names = FALSE, fast_mode = FALSE) {
  if(!fast_mode){
    pre_filtered_ema <- write_ema_responses(data_dirname = data_dirname,
                                            wockets_dirname = wockets_dirname,
                                            manual_dirname = manual_dirname,
                                            skip_manual = skip_manual)
  } else {
    pre_filtered_ema <- read_csv(paste(data_dirname,"ema_responses.csv",sep="/"))
  }

  filtered_ema <- pre_filtered_ema %>%
    mutate_all(funs(as_character)) %>%
    filter(!is.na(Subject_ID)) %>%
    mutate_all(funs(ifelse(.=="question is not displayed",NA_character_,.))) %>%
    mutate_all(funs(ifelse(.=="skipped",NA_character_,.))) %>%
    mutate_all(funs(as_character)) %>%
    mutate(subject = substr(str_split_fixed(Subject_ID,"/", n = 7)[,1],4,7)) %>%
    select(-Q0_welcome,-Q15_thankyou,-Q13_a8_drugs_type_other,-Subject_ID) %>%
    rename_at(vars(Q3_happy:Q10_hungry),funs(str_remove(.,"Q[0-9]*_"))) %>%
    mutate_at(vars(happy:hungry),funs(factor(.,levels = c("Slightly/not at all",
                                                                     "A little",
                                                                     "Moderately",
                                                                     "Quite a bit",
                                                                     "Extremely")))) %>%
    mutate(safe = factor(Q2_safe, levels = c("Very unsafe",
                                          "Somewhat unsafe",
                                          "Neither safe nor unsafe",
                                          "Somewhat safe",
                                          "Very safe"))) %>%
    mutate(Q10_stressevents = ifelse(Q10_stressevents == "0",NA_character_,Q10_stressevents),
      Q10_stressevents = ifelse(Q10_stressevents == "A tobacco product not listed here",NA_character_,Q10_stressevents),
      Q10_stressevents = ifelse(Q10_stressevents == "I have not used tobacco",NA_character_,Q10_stressevents),
      Q10_stressevents = ifelse(Q10_stressevents == "Paper cigarettes",NA_character_,Q10_stressevents) ,
      Q10_stressevents = ifelse(Q10_stressevents == "0",NA_character_,Q10_stressevents) ,
      Q11_tobacco = ifelse(Q11_tobacco == "0",NA_character_,Q11_tobacco),
      Q11_tobacco = ifelse(Q11_tobacco == "1",NA_character_,Q11_tobacco),
      Q12_alcohol = ifelse(Q12_alcohol == "My apartment/residence",NA_character_,Q12_alcohol),
      Q12_b_alcohol_where = ifelse(Q12_b_alcohol_where == "self",NA_character_,Q12_b_alcohol_where),
      Q12_d_alcohol_who_other = ifelse(Q12_d_alcohol_who_other == "No",NA_character_,Q12_d_alcohol_who_other)) %>%
    mutate(where = as_factor(Q2_where),
           what = as_factor(Q2_what),
           alcohol = as_factor(Q12_alcohol),
           alcohol_social_use = as_factor(Q12_e_alcohol_who_use),
           drugs = as_factor(Q13_drugs),
           drugs_social_use = as_factor(Q13_e_drugs_who_use),
           tempted = as_factor(Q14_tempted),
           tempted_social_use = as_factor(Q14_d_tempted_who_use)) %>%
    bind_cols(social_ema(.)) %>%
    #mutate(social_alone, ifelse(is.na(Q1_social),NA_integer_,
    #                            ifelse(Q1_social == "I have not interacted with anyone",1,0))) %>%
    bind_cols(stressevents_ema(.)) %>%
    bind_cols(tobacco_ema(.)) %>%
    bind_cols(alcohol_where_ema(.)) %>%
    bind_cols(alcohol_who_other_ema(.)) %>%
    bind_cols(drugs_type_ema(.)) %>%
    bind_cols(drugs_where_ema(.)) %>%
    bind_cols(drugs_who_other_ema(.)) %>%
    bind_cols(tempted_where_ema(.)) %>%
    bind_cols(tempted_who_other_ema(.))
  if(retain_names){
    return(filtered_ema)
  } else {
    filtered_ema <- filtered_ema %>%
      select(-Q2_safe,
              -Q2_where,
             -Q1_a_socialother,
             -Q2_what,
             -Q10_stressevents,
             -Q11_tobacco,
             -Q12_alcohol,-Q12_b_alcohol_where,-Q12_d_alcohol_who_other,-Q12_e_alcohol_who_use,
             -Q13_drugs,-Q13_a_drugs_type,-Q13_b_drugs_where,-Q13_d_drugs_who_other,-Q13_e_drugs_who_use,
             -Q14_tempted,-Q14_a_tempted_where,-Q14_c_tempted_who_other,-Q14_d_tempted_who_use)
  }

  return(filtered_ema)
}


