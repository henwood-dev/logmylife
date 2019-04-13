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


# Override
select <- dplyr::select

# Imports
source("LML_Tidy_Helpers.R", encoding = "UTF-8")
source("LML_Tidy_EMA.R", encoding = "UTF-8")
source("LML_Tidy_Daily.R", encoding = "UTF-8")
source("LML_Baseline.R", encoding = "UTF-8")
source("LML_GIS.R", encoding = "UTF-8")


# Keys
if(.Platform$OS.type == "windows"){
  data_dirname <- "C:/Users/dzubur/Desktop/LML Raw Data"
} else {
  data_dirname <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Prompt Level"
}

wockets_dirname <- "Wockets"
manual_dirname <- "Manual"
id_varstub <- "lml_com$"
filename_varstub <- "PromptResponses_EMA.csv$"

full_study_visualizer <- function() {
  ema_enroll <- ema %>%
    mutate(pid = as.numeric(pid)) %>%
    left_join(new_enroll, by = "pid") %>%
    filter(!is.na(housing_status)) %>%
    filter(ema_prompt_status == "Completed")
  ggplot(data = ema_enroll, aes(fill=housing_status, x=alcohol)) +
    geom_bar(position="dodge")
  
  ggplot(data = ema_enroll, aes(fill=housing_status, x=drugs)) +
    geom_bar(position="dodge")
  
  
}

full_study_adapter <- function() {
  v1_data <<- v1_fix_errors(main_filepath,v1_filepath,TRUE)
  v2_data <<- v2_fix_errors(main_filepath,v2_filepath,TRUE)
  v3_data <<- v3_fix_errors(main_filepath,v30_filepath,v31_filepath,TRUE)
  v4_data <<- v4_fix_errors(main_filepath,v4_filepath,TRUE)
  v5_data <<- v5_fix_errors(main_filepath,v5_filepath,TRUE)
  v6_data <<- v6_fix_errors(main_filepath,v6_filepath,TRUE)
  v7_data <<- v7_fix_errors(main_filepath,v7_filepath,TRUE)
  
  v1234_sni <<- sni_fix_errors(main_filepath,v1_sni_filepath,v2_sni_filepath,v3_sni_filepath,v4_sni_filepath) 
  
  baseline_data <<- version_adapter(v1_data = v1_data,
                                   v2_data = v2_data,
                                   v3_data = v3_data,
                                   v4_data = v4_data,
                                   v1234_sni = v1234_sni,
                                   v5_data = v5_data,
                                   v6_data = v6_data,
                                   v7_data = v7_data)
  
  v2q_data <<- followup_fix_errors(main_filepath,
                                  v1_v2q_filepath = v1_v2q_filepath,
                                  v2_v2q_filepath = v2_v2q_filepath,
                                  v3_v2q_filepath = v3_v2q_filepath,
                                  v4_v2q_filepath = v4_v2q_filepath,
                                  v5_v2q_filepath = v5_v2q_filepath,
                                  v6_v2q_filepath = v6_v2q_filepath,
                                  v7_v2q_filepath = v7_v2q_filepath)
  
  new_baseline <<- clean_baseline(baseline_data)
  tidy_baseline <<- tidy_name_adapter(main_filepath,new_baseline,"bl")
  new_followup <<- clean_followup(v2q_data)
  tidy_followup <<- tidy_name_adapter(main_filepath,new_followup,"v2q")
  new_enroll <<- clean_enrollment(enrollment_dirname)
  
  ema <<- tidy_ema(data_dirname, wockets_dirname,manual_dirname,new_baseline,new_enroll,FALSE,FALSE,TRUE)
  write_dta(ema,paste0(data_dirname,"/ema_sni.dta"))
  daily <<- tidy_daily(data_dirname, wockets_dirname,manual_dirname,new_baseline,new_enroll,FALSE,FALSE,TRUE)
  write_dta(daily,paste0(data_dirname,"/daily_sni.dta"))
  
  #housing_geo <- gps_geocode_address()
  housing_geo <<- read_csv("/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Geospatial/geocoded_housing.csv")
  gps <<- read_gps(data_dirname, wockets_dirname, manual_dirname, TRUE, TRUE)
  ema_gps <<- tidy_gps(data_dirname, ema, gps, new_enroll, housing_geo)
}


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
    select(PID = pid, SURVEYNUMBER = PromptID,PROMPT_REPROMPT = PromptType,
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

tidy_gps <- function(data_dirname, ema, gps, new_enroll, housing_geo){
  ema_dt <- ema %>%
    mutate(file_id = pid) %>%
    as.data.table
  ema_dt[, merge_row := .I]
  print("Successfully loaded EMA")
  
  gps_dt <- enhance_gps(gps = gps, new_enroll = new_enroll, housing_geo = housing_geo)
  gps_dt <- gps_dt[, c(append(names(gps),"athome")), with = FALSE]
  print("Successfully loaded GPS")
  
  ema_premerge <- ema_dt[, c("ema_prompt_time","file_id","merge_row")]
  ema_premerge[, ema_prompt_time := str_replace(ema_prompt_time,"PDT","")]
  ema_premerge[, ema_prompt_time := str_replace(ema_prompt_time,"PST","")]
  ema_premerge[, ema_time := as.POSIXct(ema_prompt_time, tz = "America/Los_Angeles", format = "%a %b %d %H:%M:%S %Y", origin = "1970-01-01")]

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
    temp_gps <- gps_dt[file_id == merge_id & fulltime > merge_time_low & fulltime < merge_time_high]
    collapsed_gps <- temp_gps[,lapply(.SD, mean, na.rm=TRUE),by = file_id]
    
    if(nrow(collapsed_gps) == 0) {
      empty_gps <- as.data.table(merge_id)
      empty_gps[,file_id := merge_id]
      empty_gps[,merge_id := NULL]
      empty_gps[,fulltime := merge_time]
      empty_gps[,latitude := NA_real_]
      empty_gps[,longitude := NA_real_]
      empty_gps[,accuracy := NA_real_]
      empty_gps[,athome := NA_real_]
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

export_person_level <- function(data_dirname, ema_gps, daily){

  ema_person_compliance <- ema_gps %>%
    mutate(participant = pid) %>%
    mutate(comply = ifelse(Status == "Completed",1,0),
           gps_bin = as.integer(!is.na(latitude))) %>%
    group_by(participant) %>%
    summarize_at(vars(ema_compliance = comply,
                      gps_availability = gps_bin),funs(mean(.,na.rm=TRUE)))
  
  ema_person <- ema_gps %>%
    mutate(participant = pid) %>%
    mutate(alcohol_bin = as.integer(alcohol) != 1,
           drugs_bin = as.integer(drugs) == 1,
           tobacco_bin = as.integer(tobacco_none) != 1,
           tempted_bin = as.integer(tempted) != 1,
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
  
  daily_person_compliance <- daily %>%
    mutate(participant = subject_id) %>%
    mutate(comply = ifelse(daily_prompt_status == "Completed",1,0)) %>%
    group_by(participant) %>%
    summarize_at(vars(daily_compliance = comply),funs(sum(.,na.rm=TRUE))) %>%
    mutate(daily_compliance = daily_compliance/7)
  
  daily_person <- daily %>%
    mutate(participant = subject_id) %>%
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


tidy_daily <- function(data_dirname, wockets_dirname, manual_dirname,
                       new_baseline = NULL, new_enroll = NULL,
                       skip_manual = FALSE, retain_names = FALSE, 
                       fast_mode = FALSE){
  if(!fast_mode){
    pre_filtered_dailylog <- write_daily_responses(data_dirname = data_dirname,
                                            wockets_dirname = wockets_dirname,
                                            manual_dirname = manual_dirname,
                                            skip_manual = skip_manual)
  } else {
    pre_filtered_dailylog <- fread(paste(data_dirname,"daily_responses.csv",sep="/")) %>%
      mutate_all(funs(as.character))
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
    rename(pid = system_file,
           daily_prompt_date = PromptDate,
           daily_prompt_time = PromptTime,
           daily_prompt_status = Status) %>%
    mutate(daily_prompt_date = as.Date(daily_prompt_date, format = "%Y-%m-%d", origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
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
    bind_cols(sex_where_daily(.)) %>%
    bind_cols(multiple_sex_partners(.)) %>%
    bind_cols(sleepy_time(.)) %>%
    bind_cols(sleepy_time(., lowrange = 0, highrange = 2)) %>%
    bind_cols(sleepy_time(., lowrange = 14, highrange = 24))
  if(!retain_names){
    filtered_dailylog <- filtered_dailylog %>%
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
  if(!is.null(new_baseline)){
    other_sni_label <- "Someone else not listed here"
    none_sni_label = "I did not"
    
    filtered_dailylog <- filtered_dailylog %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "core")) %>%
      pipe_print("Core Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "alcdaily")) %>%
      pipe_print("Alcohol Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "marijuana")) %>%
      pipe_print("MJ Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "syntheticmj")) %>%
      pipe_print("Synthetic MJ Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "meth")) %>%
      pipe_print("Meth Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "rxmisuse")) %>%
      pipe_print("RX Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "mdma")) %>%
      pipe_print("MDMA Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "hallucinogen")) %>%
      pipe_print("Hallucinogen Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "heroin")) %>%
      pipe_print("Heroin Complete") %>%
      bind_cols(daily_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "cocaine")) %>%
      pipe_print("SNI Complete") %>%
      select(-Q4_0_soccore,-Q4_2_b_alcohol_who, -Q4_3_b_marijuana_who, -Q4_6_b_synthmj_who,
             -Q4_4_b_meth_who, -Q4_8_b_prescription_who, -Q4_5_b_mdma_who, -Q4_7_b_halluc_who,
             -Q4_9_b_heroin_who, -Q4_10_c_coke_who)
  }
  if(!is.null(new_enroll)){
    enroll_convert <- new_enroll %>%
      mutate(pid = as.character(pid)) %>%
      select(pid, dates_start_app, dates_end_app)
    
    filtered_dailylog <- filtered_dailylog %>%
      left_join(enroll_convert, by = "pid") %>%
      filter(daily_prompt_date >= dates_start_app & daily_prompt_date <= dates_end_app) %>%
      select(-dates_start_app,-dates_end_app)
  }
  
  return(filtered_dailylog)
}

tidy_ema <- function(data_dirname, wockets_dirname, manual_dirname,
                     new_baseline = NULL, new_enroll = NULL,
                     skip_manual = FALSE, retain_names = FALSE, 
                     fast_mode = FALSE) {
  if(!fast_mode){
    pre_filtered_ema <- write_ema_responses(data_dirname = data_dirname,
                                            wockets_dirname = wockets_dirname,
                                            manual_dirname = manual_dirname,
                                            skip_manual = skip_manual)
  } else {
    pre_filtered_ema <- fread(paste(data_dirname,"ema_responses.csv",sep="/"))
  }

  filtered_ema <- pre_filtered_ema %>%
    mutate_all(funs(as_character)) %>%
    mutate_at(vars(PromptType),funs(factor(.,levels = c("Prompt","Reprompt")))) %>%
    rename(pid = system_file,
           prompt_id = PromptID,
           prompt_wasreprompt = PromptType,
           ema_prompt_status = Status,
           ema_prompt_date = PromptDate,
           ema_prompt_time = PromptTime) %>%
    filter(!is.na(Subject_ID)) %>%
    mutate_all(funs(ifelse(.=="question is not displayed",NA_character_,.))) %>%
    mutate_all(funs(ifelse(.=="skipped",NA_character_,.))) %>%
    mutate_all(funs(as_character)) %>%
    mutate(ema_prompt_date = as.Date(ema_prompt_date, format = "%Y-%m-%d", origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
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
  if(!retain_names){
    filtered_ema <- filtered_ema %>%
      select(-Q2_safe,
              -Q2_where,
             -Q1_a_socialother,
             -Q2_what,
             -Q10_stressevents,
             -Q11_tobacco,
             -Q12_alcohol,-Q12_b_alcohol_where,-Q12_d_alcohol_who_other,-Q12_e_alcohol_who_use,
             -Q13_drugs,-Q13_a_drugs_type,-Q13_b_drugs_where,-Q13_d_drugs_who_other,-Q13_e_drugs_who_use,
             -Q14_tempted,-Q14_a_tempted_where,-Q14_c_tempted_who_other,-Q14_d_tempted_who_use,
             -subject)
  }
  if(!is.null(new_baseline)){
    other_sni_label <- "Someone else not listed here"
    none_sni_label = "I have not interacted with anyone"
    
    filtered_ema <- filtered_ema %>%
      bind_cols(ema_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "who")) %>%
      bind_cols(ema_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "alcema")) %>%
      bind_cols(ema_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "drugs")) %>%
      bind_cols(ema_align_sni_alters(., new_baseline, other_sni_label = other_sni_label,none_sni_label = none_sni_label, social_type = "tempted")) %>%
      select(-Q1_social,-Q12_c_alcohol_who,-Q13_c_drugs_who,-Q14_b_tempted_who)
  }
  if(!is.null(new_enroll)){
    enroll_convert <- new_enroll %>%
      mutate(pid = as.character(pid)) %>%
      select(pid, dates_start_app, dates_end_app)
    
    filtered_ema <- filtered_ema %>%
      left_join(enroll_convert, by = "pid") %>%
      filter(ema_prompt_date >= dates_start_app & ema_prompt_date <= dates_end_app) %>%
      select(-dates_start_app,-dates_end_app)
  }
  
  return(filtered_ema)
}


