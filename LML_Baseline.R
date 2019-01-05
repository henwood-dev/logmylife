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
library(Hmisc)
#library(summarytools)

# Override
select <- dplyr::select

# Imports
source("LML_Tidy_Helpers.R", encoding = "UTF-8")


not_all_na <- function(x) any(!is.na(x))

main_filepath <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Person Level/_Raw Data/TSV"

v1_filepath <-"V1/Baseline V10.csv"
v2_filepath <-"V2/Baseline V20.csv"
v30_filepath <-"V3/Baseline V30.csv"
v31_filepath <- "V31/Baseline V31.csv"
v4_filepath <-"V4/Baseline V40.csv"
v5_filepath <-"V5/BaselineSNI V50.csv"
v6_filepath <- "V6/BaselineSNI V60.csv"
v7_filepath <- "V7/BaselineSNI V70.csv"

v1_sni_filepath <- "V1/SNI/SNI V10.csv"
v2_sni_filepath <- "V2/SNI/SNI V20.csv"
v3_sni_filepath <- "V3/SNI/SNI V30.csv"
v4_sni_filepath <- "V4/SNI/SNI V40.csv"

v1_v2q_filepath <- "V2Q V1.csv"
v2_v2q_filepath <- "V2Q V2.csv"
v3_v2q_filepath <- "V2Q V3.csv"
v4_v2q_filepath <- "V2Q V4.csv"
v5_v2q_filepath <- "V2Q V5.csv"
v6_v2q_filepath <- "V2Q V6.csv"
v7_v2q_filepath <- "V2Q V7.csv"

rename_drugs <- function(varname, suffix, length){
  drug_list <- c("alc","meth","mdma","synmj","halluc","pdm","heroin","coc","crack","inhal","ster","no2","ket","pcp")
  midvar <- drug_list[eval(parse(text = substr(varname,length+1,length+2)))]
  return(paste0("sni_su30_",midvar,"_",suffix))
}

rename_by_substract <- function(varname, orig_num, new_num){
 current_num <- as.integer(gsub("\\D","",varname))
 num_transform <- new_num - orig_num
 update_num <- current_num + num_transform
 update_var <- gsub(as.character(current_num),as.character(update_num),varname)
 return(update_var)
}

demographics <- function() {
  v1_data <- v1_fix_errors(main_filepath,v1_filepath,TRUE)
  v2_data <- v2_fix_errors(main_filepath,v2_filepath,TRUE)
  v3_data <- v3_fix_errors(main_filepath,v30_filepath,v31_filepath,TRUE)
  v4_data <- v4_fix_errors(main_filepath,v4_filepath,TRUE)
  v5_data <- v5_fix_errors(main_filepath,v5_filepath,TRUE)
  v6_data <- v6_fix_errors(main_filepath,v6_filepath,TRUE)
  v7_data <- v7_fix_errors(main_filepath,v7_filepath,TRUE)
  
  v1234_sni <- sni_fix_errors(main_filepath,v1_sni_filepath,v2_sni_filepath,v3_sni_filepath,v4_sni_filepath) 
  
  baseline_data <- version_adapter(v1_data = v1_data,
                                   v2_data = v2_data,
                                   v3_data = v3_data,
                                   v4_data = v4_data,
                                   v1234_sni = v1234_sni,
                                   v5_data = v5_data,
                                   v6_data = v6_data,
                                   v7_data = v7_data)
}

birace_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,demo_birace = birace)
  
  new_names <- c(
    `demo_birace_American Indian or Alaska Native` = "demo_birace_native",
    `demo_birace_Asian` = "demo_birace_asian",
    `demo_birace_Black or African-American` = "demo_birace_black",
    `demo_birace_Hispanic/Latino` = "demo_birace_hispanic",
    `demo_birace_Native Hawaiian or other Pacific Islander` = "demo_birace_islander",
    `demo_birace_Other (please specify)` = "demo_birace_other",
    `demo_birace_South Asian` = "demo_birace_soasian",
    `demo_birace_White` = "demo_birace_white"
  )
  new_labels <- list(
    demo_birace_native = "Multi-racial: American Indian or Alaska Native",
    demo_birace_asian = "Multi-racial: Asian",
    demo_birace_black = "Multi-racial: Black or African-American",
    demo_birace_hispanic = "Multi-racial: Hispanic or Latino",
    demo_birace_islander = "Multi-racial: Native Hawaiian or Pacific Islander",
    demo_birace_other = "Multi-racial: Other",
    demo_birace_soasian = "Multi-racial: South Asian",
    demo_birace_white = "Multi-racial: White"
  )
  
  return_data <- prebind_data(filtered_data, "demo_birace", new_names, new_labels, separator = ",")
  return(return_data)
}

factor_keep_rename <- function(var_column, level_vector){
  original_label_name <- attributes(var_column)$label
  factored_column <- factor(var_column, levels = level_vector)
  attr(factored_column,"label") <- original_label_name
  return(factored_column)
}

clean_baseline <- function(baseline_data){
  filtered_baseline <- baseline_data %>%
    mutate(dob = gsub("-","/",dob),
           dob = ifelse(pid == "1011",NA_character_,dob), ## RECONCILE AGE
           date = ifelse(pid == "1011","06/16/2017",date),
           dob = ifelse(pid == "1038",NA_character_,dob), ## RECONCILE AGE
           date = ifelse(pid == "1038","09/13/2017",date)) %>%
    select(-startdate,-enddate,-status,-ipaddress,-progress,-finished,-recordeddate,-starts_with("recipient"),
           -externalreference,-locationlatitude,-locationlongitude,-distributionchannel,-userlanguage,-intname,
           -q569,-sni_5_for_app,-q524,-q524_1_text) %>%
    mutate(baseline_duration = as.numeric(duration_in_seconds),
           baseline_date = as_date(date, format = "%m/%d/%Y", tz = "America/Los_Angeles"),
           baseline_survey = as_factor(survey_type),
           baseline_site_housed = ifelse(site_housed == "Other",site_housed_77_text,site_housed),
           baseline_site_unhoused = ifelse(site_unhoused == "Other", site_unhoused_77_text,site_unhoused),
           demo_dob = as_date(dob, format = "%m/%d/%Y", tz = "America/Los_Angeles"),
           demo_age = as.integer((baseline_date-demo_dob)/365.25)) %>%
    mutate(demo_hispanic = ifelse(race == "Hisp/Lat and no other race","Yes","No"),
           demo_hispanic = ifelse(hisp == "Yes","Yes",demo_hispanic),
           demo_hispanic = ifelse(hisp == "I don't know","Unknown",demo_hispanic),
           demo_hispanic = ifelse(is.na(hisp),"Unknown",demo_hispanic)) %>%
    mutate(demo_race_cat = ifelse(race == "American Indian or Alaska Native","American Indian or Alaska Native","Unknown"),
           demo_race_cat = ifelse(race == "Asian","Asian",demo_race_cat),
           demo_race_cat = ifelse(race == "Black or African-American","Black or African American",demo_race_cat),
           demo_race_cat = ifelse(race == "Native Hawaiian or other Pacific Islander","Native Hawaiian or other Pacific Islander",demo_race_cat),
           demo_race_cat = ifelse(race == "South Asian","Asian",demo_race_cat),
           demo_race_cat = ifelse(race == "White","White",demo_race_cat),
           demo_race_cat = ifelse(race == "Bi/Multi-racial or Ethnic","More than one race",demo_race_cat)) %>%
    rename(demo_race_min = demo_racemin,
           demo_housed = housed_yn,
           baseline_version = version) %>%
    mutate(demo_race_min = ifelse(demo_race_cat == "White" & is.na(demo_race_min),"No",demo_race_min),
           demo_race_min = ifelse(demo_race_cat != "White" & demo_race_cat != "Unknown" & is.na(demo_race_min),"Yes",demo_race_min),
           demo_race_min = ifelse(demo_race_cat == "Unknown" & is.na(demo_race_min),"Unknown",demo_race_min)) %>%
    mutate(demo_gendercis = ifelse(sex == "Male" & gender == "Male","Yes","No"),
           demo_gendercis = ifelse(sex == "Female" & gender == "Female","Yes",demo_gendercis),
           demo_gendercis = ifelse(gender == "Different identity (please state):" | sex == "Don't know" 
                                   | is.na(gender) | is.na(sex),"Unknown",demo_gendercis)) %>%
    mutate(demo_sex_min = ifelse(sexori == "Heterosexual or straight","No","Yes"),
           demo_sex_min = ifelse(sexori == "Another sexual orientation (please state):" | is.na(sexori),"Unknown",demo_sex_min)) %>%
    mutate(demo_sex_gender_min = ifelse(demo_sex_min == "Yes" | demo_gendercis == "No","Yes","No"),
           demo_sex_gender_min = ifelse((demo_sex_min == "Unknown" | demo_gendercis == "Unknown") & demo_sex_gender_min == "No","Unknown",demo_sex_gender_min)) %>%
    mutate_at(vars(starts_with("giscale_")), funs(factor_keep_rename(., level_vector = c("Strongly Agree (1)",
                                                                                         "Agree (2)",
                                                                                         "Mixed Feelings (3)",
                                                                                         "Disagree (4)",
                                                                                         "Strongly Disagree (5)")))) %>%
    mutate_at(vars(starts_with("stress_streets_")), funs(factor_keep_rename(., level_vector = c("None at all",
                                                                                                "A little",
                                                                                                "More than a little",
                                                                                                "A lot")))) %>%
    bind_cols(birace_baseline(.)) %>%
    select(-duration_in_seconds,-date,-survey_type,-site_unhoused_77_text,-site_housed_77_text,-consent,
           -dob,-hisp,-site_housed,-site_unhoused,-birace,-starts_with("research_")) %>%
    rename(baseline_id = responseid,
           demo_birace_other_text = birace_8_text)
 
  old_names<- c("race","race_8_text","inschool","inschool_type","inschool_type_4_text","educ","sex","gender","gender_6_text",
                "sexori","sexori_6_text","sexattr_formales",
                "giscale_1","giscale_2","giscale_3","giscale_4","giscale_5","giscale_6","giscale_7",
                "romrel_marr","romrel_curr","romrel_ptnrs","romrel_ptnrsgndr","romrel_dur","romrel_sex",
                "firsthomeless","reasonhomeless","reasonhomeless_17_text","housingprogs_ever","housingprogs_ever_20_text",
                "timehomeless_ever","timehoused","staybeforehoused","staybeforehoused_20_text","timehomeless_beforeh","timehomeless_recent",
                "uh_livsit_3mo","uh_livsit_3mo_20_text","uh_livsit_curr","uh_livsit_curr_20_text",
                "fc_ever","fc_placements","jjs_ever","jjs_ageout","arrest_ever","jail_ever",
                "stress_streets_1","stress_streets_2","stress_streets_3","stress_streets_4","stress_streets_5",                 
                "stress_streets_6","stress_streets_7","stress_streets_8","stress_streets_9","stress_streets_10",
                "stress_streets_11","stress_streets_12","stress_streets_13","stress_streets_14","stress_streets_15",
                "mse_talkto","mse_housingopts","mse_progengage","mse_progengage_10_text","mse_drop_inaccess",
                "mse_shelteraccess","mse_txaccess")
  new_names <- c("demo_race","demo_race_other","demo_school","demo_school_type","demo_school_other","demo_education","demo_sex",
                 "demo_gender","demo_gender_other","demo_sex_ori","demo_sex_ori_other","demo_sex_attr",
                 "scale_gay_identity_1","scale_gay_identity_2","scale_gay_identity_3","scale_gay_identity_4","scale_gay_identity_5",
                 "scale_gay_identity_6","scale_gay_identity_7",
                 "history_romance_married","history_romance_current","history_romance_partners","history_romance_partners_gender",
                 "history_romance_duration","history_romance_sex",
                 "history_homeless_first_time","history_homeless_reason","history_homeless_reason_other","history_housing_programs","history_housing_programs_other",
                 "history_homeless_time_ever","history_time_housed","history_stay_prehoused","history_stay_prehoused_other","history_homeless_time_prehoused",
                 "history_homeless_time_recent",
                 "unhoused_livsit_3mo","unhoused_livsit_3mo_other","unhoused_livsit_current","unhoused_livsit_current_other",
                 "history_foster_care_ever","history_foster_care_placement","history_jjs_ever","history_jjs_age_out",
                 "history_arrest_ever","history_jail_ever",
                 "scale_stress_streets_1","scale_stress_streets_2","scale_stress_streets_3","scale_stress_streets_4","scale_stress_streets_5",                 
                 "scale_stress_streets_6","scale_stress_streets_7","scale_stress_streets_8","scale_stress_streets_9","scale_stress_streets_10",
                 "scale_stress_streets_11","scale_stress_streets_12","scale_stress_streets_13","scale_stress_streets_14","scale_stress_streets_15",
                 "survey_mse_talking","survey_mse_housing","survey_mse_engage","survey_mse_engage_other","survey_mse_dropin",
                 "survey_mse_shelter","survey_mse_treatment")
  setnames(filtered_baseline,old_names,new_names)
  
  
  
  
  ## VARS PRESERVED
  # site_housed
  
  attr(filtered_baseline$baseline_duration,"label") <- attributes(baseline_data$duration_in_seconds)$label
  attr(filtered_baseline$baseline_date,"label") <- attributes(baseline_data$date)$label
  attr(filtered_baseline$baseline_survey,"label") <- attributes(baseline_data$survey_type)$label
  attr(filtered_baseline$baseline_site_housed,"label") <- attributes(baseline_data$site_housed)$label
  attr(filtered_baseline$baseline_site_unhoused,"label") <- attributes(baseline_data$site_unhoused)$label
  attr(filtered_baseline$demo_birace_other_text,"label") <- "Multi-Racial: Other Text Entry"
  attr(filtered_baseline$demo_sex_gender_min,"label") <- "Is the partcipant a sexual or gender minority?"
  attr(filtered_baseline$demo_sex_min,"label") <- "Is the partcipant a sexual identity minority? (Not Heterosexual)"
  attr(filtered_baseline$demo_gendercis,"label") <- "Is the participant cis-gendered?"
  attr(filtered_baseline$demo_race_min,"label") <- "Is the participant a racial minority? (Not White)"
  attr(filtered_baseline$demo_race_cat,"label") <- "Census tract recategorized racial category"
  attr(filtered_baseline$demo_hispanic,"label") <- "Does the participant identify as Hispanic or Latino?"
  attr(filtered_baseline$demo_dob,"label") <- "Date of birth, in date format"
  attr(filtered_baseline$demo_age,"label") <- "Age at baseline (in years)"
  attr(filtered_baseline$demo_housed,"label") <- "Is the participant housed or unhoused?"
  attr(filtered_baseline$baseline_version,"label") <- "Baseline survey version"
  attr(filtered_baseline$baseline_id,"label") <- "Baseline survery respone id"
  
  names(select(filtered_baseline,-starts_with("demo"),-starts_with("baseline"),-starts_with("scale"),-starts_with("history")))
}

followup_fix_errors <- function(main_filepath, v1_v2q_filepath,v2_v2q_filepath,v3_v2q_filepath,
                                v4_v2q_filepath,v5_v2q_filepath,v6_v2q_filepath,v7_v2q_filepath){
  v1_raw <- return_raw_baseline(main_filepath, v1_v2q_filepath) %>%
    filter(responseid != "R_OiLgOEXdq9zF0iJ" & exit_pid != "105" & exit_pid != "10100"
           & responseid != "R_1pyRsMTctbJuoK3" & responseid != "R_0NugAIF3VyvohKV"
           & exit_pid != "125231" & exit_pid != "7675667") %>%
    mutate(hes_date = ifelse(exit_pid == "1001","6/19/2017",hes_date),
           hes_date = ifelse(exit_pid == "1002","6/19/2017",hes_date),
           hes_date = ifelse(exit_pid == "1004","6/19/2017",hes_date),
           hes_date = ifelse(exit_pid == "1005","6/19/2017",hes_date)) %>%
    rename(v2q_pid = exit_pid,
           v2q_date = hes_date)
  #v2_raw <- return_raw_baseline(main_filepath, v2_v2q_filepath)
  v3_raw <- return_raw_baseline(main_filepath, v3_v2q_filepath) %>%
    filter(responseid != "R_3eNrv9wgEEw1DfH" & responseid != "R_2X66FVl0NeOhixn") %>%
    bind_rows(v1_raw)
  v4_raw <- return_raw_baseline(main_filepath, v4_v2q_filepath) %>%
    filter(v2q_pid != "1092" & responseid != "R_8ffh1QKZh1MjMHL") %>%
    mutate(v2q_pid = ifelse(responseid == "R_uyQRieSTpMom4jD","1099",v2q_pid),
           v2q_pid = ifelse(responseid == "R_1LeznYAMP8Ac2TV","1115",v2q_pid))
  
  v4_fix_1087 <- v4_raw %>%
    filter(v2q_pid == "1087") %>%
    summarize_all(funs(last(na.omit(.))))
  
  v4_fix <- v4_raw %>%
    filter(v2q_pid != "1087") %>%
    bind_rows(v4_fix_1087) %>%
    bind_rows(v3_raw)
  
  v5_raw <- return_raw_baseline(main_filepath, v5_v2q_filepath) %>%
    mutate(v2q_pid = ifelse(responseid == "R_338UYPxSPohpAg1","1123",v2q_pid),
           v2q_pid = ifelse(responseid == "R_10NlniZ8rWHuIV9","1129",v2q_pid),
           v2q_pid = ifelse(responseid == "R_WDnNcnnPz5lI7Ml","1130",v2q_pid),
           v2q_pid = ifelse(responseid == "R_3PmLgNuV3kHZMVt","2011",v2q_pid),
           v2q_pid = ifelse(responseid == "R_31Qt8FUpZ8f2Y2D","2019",v2q_pid)) %>%
    filter(v2q_pid != "101" & responseid != "R_3e3KHPHwy4TsSAa") %>%
    bind_rows(v4_fix) %>%
    rename(ei_extent_ei_interfe = ei_interfere,
           ei_extent_ei_stress = ei_stress,
           ei_extent_ei_battery = ei_batcharge,
           ei_agree_ei_behalter = ei_behalter,
           ei_agree_ei_honestly = ei_honestly,
           ei_agree_ei_doitagai = ei_doitagain,
           ei_agree_ei_judged = ei_judged) %>%
    select(-starts_with("v2q_work_schoolweek_"))
    
  v6_raw <- return_raw_baseline(main_filepath, v6_v2q_filepath) %>%
    mutate(v2q_pid = ifelse(v2q_pid == "2038","1132",v2q_pid)) %>%
    filter(responseid != "R_ykEWOsM5fBrha5X" & v2q_pid != "101" & responseid != "R_1iniSJLreXVWkXE")
  
  v6_fix_1132 <- v6_raw %>%
    filter(v2q_pid == "1132") %>%
    summarize_all(funs(first(na.omit(.))))
  
  v6_fix <- v6_raw %>%
    filter(v2q_pid != "1132") %>%
    bind_rows(v6_fix_1132) %>%
    bind_rows(v5_raw)
  
  v7_raw <- return_raw_baseline(main_filepath, v7_v2q_filepath) %>%
    bind_rows(v6_fix)
  
  return(v7_raw)
}

return_raw_baseline <- function(main_filepath,raw_filepath){
  raw_data <- read_csv(paste(main_filepath,raw_filepath, sep = "/"), col_names = FALSE)
  raw_names <- raw_data %>% 
    filter(row_number()==1) %>%
    mutate_all(funs(str_to_lower(.))) %>%
    mutate_all(funs(gsub("[^[:alnum:][:space:]]"," ",.))) %>%
    mutate_all(funs(str_squish(.))) %>%
    mutate_all(funs(gsub(" ","_",.)))
  raw_labels <- raw_data %>% 
    filter(row_number()==2)
  for(i in 1:ncol(raw_labels)){
    label(raw_data[i]) <- raw_labels[,i]
  }
  raw_names <- make.unique(t(raw_names))
  colnames(raw_data) <- raw_names
  return_data <- raw_data %>%
    filter(row_number() > 3)
  return(return_data)
}

version_adapter <- function(v1_data,v2_data,v3_data,v4_data,v1234_sni,v5_data,v6_data,v7_data){
  v1_premerge <- v1_data %>%
    rename(ders_sf_1 = ders_1,
           ders_sf_2 = ders_2,
           ders_sf_3 = ders_3,
           ders_sf_4 = ders_4,
           ders_sf_5 = ders_5,
           ders_sf_6 = ders_6,
           ders_sf_8 = ders_7,
           ders_sf_9 = ders_8,
           ders_sf_10 = ders_9,
           ders_sf_11 = ders_10,
           ders_sf_12 = ders_11,
           ders_sf_13 = der_12,
           ders_sf_14 = ders_13,
           ders_sf_15 = ders_14,
           ders_sf_16 = ders_15,
           ders_sf_17 = ders_16,
           ders_sf_18 = ders_17,
           ders_sf_19 = ders_18) %>%
    rename_at(vars(starts_with("stress_scale_")),funs(paste0("stress_streets_",substr(.,14,15)))) %>%
    # mutate(stress_streets_3 = NA_character_,
    #        stress_streets_8 = NA_character_,
    #        stress_streets_14 = NA_character_) %>%
    mutate(mh_current = NA_character_,
           site_unhoused_77_text = NA_character_,
           site_housed_77_text = NA_character_,
           heroin_30_roa = NA_character_,
           hivp_ptcounsel = NA_character_,
           site_housed = ifelse(housed_yn == "Yes",site,NA_character_),
           site_unhoused = ifelse(housed_yn == "No",site,NA_character_)) %>%
    rename(chronicdx_23_text = chronicdx_22_text) %>%
    select(-stress_streets,-site,-literacyscreen,-starts_with("mh_current_"),
           -lifesex_online,-`3mosex_online_types`,-`3mosex_online_prop`,-exchsex_ever,
           -exchsex_ever_forwhat,exchsex_ever_forced,-exchsex_3mo_app,-exchsex_ever_forced,
           -exchsex_ever_forwhat,-exchsex_ever_forwhat_17_text,
           -desrim_exps,-discrim_reasons,
           -sutypes_use_ever_17_text) %>%
    rename(marj_30_types_5_text = marj_30_types_2_text,
           marj_30_roa_10_text = marj_30_roa_11_text,
           cell_bill_7_text = cell_bill_6_text,
           cell_bill_8_text = cell_bill_7_text)
    
  v2_premerge <- v2_data %>%
    bind_rows(v1_premerge) %>%
    mutate(uh_livsit_3mo = NA_character_,
           uh_livsit_3mo_20_text = NA_character_,
           uh_livsit_curr = NA_character_,
           uh_livsit_curr_20_text = NA_character_,
           ttlincome_benefits = NA_character_)
  
  v3_premerge <- v3_data %>%
    bind_rows(v2_premerge) %>%
    select(-consentcomp) %>%
    mutate(q569 = NA_character_,
           hfias_1 = NA_character_,
           hfias_1_a = NA_character_,
           hfias_2 = NA_character_,
           hfias_2_a = NA_character_,
           hfias_3 = NA_character_,
           hfias_3_a = NA_character_,
           hfias_4 = NA_character_,
           hfias_4_a = NA_character_,
           hfias_5 = NA_character_,
           hfias_5_a = NA_character_,
           hfias_6 = NA_character_,
           hfias_6_a = NA_character_,
           hfias_7 = NA_character_,
           hfias_7_a = NA_character_,
           hfias_8 = NA_character_,
           hfias_8_a = NA_character_,
           hfias_9 = NA_character_,
           hfias_9_a = NA_character_,
           hivp_meds_whynot_6_text_topics = NA_character_)
    
    
  v4_premerge <- v4_data %>%
    bind_rows(v3_premerge) %>%
    left_join(v1234_sni, by = "pid") %>%
    rename_at(vars(starts_with("sni_id_")),funs(paste0("sni_inst_gen_",substr(.,8,8),"_text"))) %>%
    rename(sni_gen_1 = sni_gender_1,
           sni_gen_2 = sni_gender_2,
           sni_gen_3 = sni_gender_3,
           sni_gen_4 = sni_gender_4,
           sni_gen_5 = sni_gender_5,
           q523 = q525) %>%
    rename_at(vars(starts_with("sni_gender_")),funs(paste0("sni_gen_",substr(.,12,12),"_text"))) %>%
    select(-hivp_meds_whynot_6_text_topics,-sni_responseid)
  
  v5_premerge <- v5_data %>%
    mutate(sni_version = as.character(version)) %>%
    bind_rows(v4_premerge) %>%
    mutate(marj_access_9_text = NA_character_)
  
  sni_v6 <- select(v6_data,pid,starts_with("sni_"))
  sni_v6_nona <- select_if(sni_v6,not_all_na)
  v6_drops <- setdiff(names(sni_v6),names(sni_v6_nona))
  
  null_v6 <- select(v6_data,pid,starts_with("null_"))
  null_v6_nona <- select_if(null_v6,not_all_na)
  v6_null_drops <- setdiff(names(null_v6),names(null_v6_nona))
  
  v6_premerge_alpha <- v6_data %>%
    mutate(sni_version = as.character(version)) %>%
    select(-one_of(v6_drops),-one_of(v6_null_drops)) %>%
    bind_rows(v5_premerge)
  
  text_v6 <- select(v6_premerge_alpha,pid,contains("text"))
  text_v6_nona <- select_if(text_v6,not_all_na)
  v6_text_drops <- setdiff(names(text_v6),names(text_v6_nona))
  
  v6_premerge <- v6_premerge_alpha %>%
    select(-one_of(v6_text_drops)) %>%
    rename(ders_sf_1 = ders_sf_1,
           ders_sf_2 = ders_sf_2,
           ders_sf_3 = ders_sf_3,
           ders_sf_4 = ders_sf_4,
           ders_sf_5 = ders_sf_5,
           ders_sf_6 = ders_sf_6,
           ders_sf_7 = ders_sf_8,
           ders_sf_8 = ders_sf_9,
           ders_sf_9 = ders_sf_10,
           ders_sf_10 = ders_sf_11,
           ders_sf_11 = ders_sf_12,
           ders_sf_12 = ders_sf_13,
           ders_sf_13 = ders_sf_14,
           ders_sf_14 = ders_sf_15,
           ders_sf_15 = ders_sf_16,
           ders_sf_16 = ders_sf_17,
           ders_sf_17 = ders_sf_18,
           ders_sf_18 = ders_sf_19) %>%
    rename(sni_alter_ids_1 = sni_inst_gen_1_text,
           sni_alter_ids_2 = sni_inst_gen_2_text,
           sni_alter_ids_3 = sni_inst_gen_3_text,
           sni_alter_ids_4 = sni_inst_gen_4_text,
           sni_alter_ids_5 = sni_inst_gen_5_text,
           sni_su30_marj_none = q130_x1,
           sni_su30_marj_1 = q130_xx29,
           sni_su30_marj_2 = q130_xx30,
           sni_su30_marj_3 = q130_xx31,
           sni_su30_marj_4 = q130_xx32,
           sni_su30_marj_5 = q130_xx33) %>%
    rename_at(vars(contains("_xx1")),funs(gsub("xx1","1",.))) %>%
    rename_at(vars(contains("_xx2")),funs(gsub("xx2","2",.))) %>%
    rename_at(vars(contains("_xx3")),funs(gsub("xx3","3",.))) %>%
    rename_at(vars(contains("_xx4")),funs(gsub("xx4","4",.))) %>%
    rename_at(vars(contains("_xx5")),funs(gsub("xx5","5",.))) %>%
    rename_at(vars(contains("_x1")),funs(gsub("x1","1",.))) %>%
    rename_at(vars(contains("_x2")),funs(gsub("x2","2",.))) %>%
    rename_at(vars(contains("_x3")),funs(gsub("x3","3",.))) %>%
    rename_at(vars(contains("_x4")),funs(gsub("x4","4",.))) %>%
    rename_at(vars(contains("_x5")),funs(gsub("x5","5",.))) %>%
    rename_at(vars(contains("_a1")),funs(gsub("a1","1",.))) %>%
    rename_at(vars(contains("_a2")),funs(gsub("a2","2",.))) %>%
    rename_at(vars(contains("_a3")),funs(gsub("a3","3",.))) %>%
    rename_at(vars(contains("_a4")),funs(gsub("a4","4",.))) %>%
    rename_at(vars(contains("_a5")),funs(gsub("a5","5",.))) %>%
    rename_at(vars(starts_with("sni_gen_")),funs(gsub("gen","gender",.))) %>%
    rename_at(vars(starts_with("sni_su30_ket_")),funs(gsub("ket","ketamine",.))) %>%
    rename_at(vars(starts_with("sni_su30_no2_")),funs(gsub("no2","nitrous",.))) %>%
    rename_at(vars(starts_with("sni_su30_synmj_")),funs(gsub("synmj","synthmj",.))) %>%
    rename_at(vars(starts_with("sni_su30_coc_")),funs(gsub("coc","cocaine",.))) %>%
    rename_at(vars(starts_with("sni_su30_inhal_")),funs(gsub("inhal","inhalants",.))) %>%
    rename_at(vars(starts_with("sni_su30_ster_")),funs(gsub("ster","steroids",.))) %>%
    rename(sni_phone_3mo = sni_phone3mo,
           sni_roommate_h = sni_rmate,
           sni_disapp_use_meth = sni_dissu_meth,
           sni_disapp_use_mdma = sni_dissu_mdma,
           sni_disapp_use_synmj = sni_dissu_synthmj,
           sni_disapp_use_hallu = sni_dissu_halluc,
           sni_disapp_use_pdm = sni_dissu_pdm,
           sni_disapp_use_ivdu = sni_dissu_idu,
           sni_suever_marj = sni_suever_mj,
           sni_suever_synthmj = sni_suever_synmj,
           sni_suever_cocaine = sni_suever_coc,
           sni_suever_steroid = sni_suever_ster,
           sni_suever_nitrous = sni_suever_no2,
           sni_suever_ketamine = sni_suever_ket) %>%
    rename_at(vars(starts_with("sni_age_")),funs(map(.,rename_by_substract,29,1))) %>%
    rename_at(vars(starts_with("sni_rel_uh_")),funs(map(.,rename_by_substract,29,1))) %>%
    rename_at(vars(starts_with("sni_rel_h_")),funs(map(.,rename_by_substract,29,1))) %>%
    rename_at(vars(starts_with("sni_rel_h_oth")),funs(gsub("oth-","oth",.))) %>%
    rename_at(vars(starts_with("sni_rel_uh_oth")),funs(gsub("oth-","oth",.))) %>%
    rename_at(vars(starts_with("sni_rel_uh_oth")),funs(map(.,rename_by_substract,23,1))) %>%
    rename_at(vars(starts_with("sni_rel_h_oth")),funs(map(.,rename_by_substract,23,1))) %>%
    select(-ends_with("none")) %>%
    rename(sni_pctab_3mo = sni_pctab3mo,
           sni_homeless_ever = sni_hmlsever,
           sni_homeless_now = sni_hmlsnow,
           sni_gang_ever = sni_gang,
           # = sni_disapp_use_meth,
           sni_safesexsup3mo = sni_safesxsup3mo,
           sni_brthctrl30mo = sni_brthctrl3mo,
           sni_policesup = sni_police,
           sni_emosupp_3mo = sni_emsup3mo,
           sni_matsupp_3mo = sni_matsup3mo,
           sni_servsupp_3mo = sni_servsup3mo,
           sni_sameprog_h = sni_nor,
           sni_sameplace_uh = sni_nor_uh)
  
  v7_premerge_alpha <- v7_data %>%
    mutate(sni_version = as.character(version))

  text_v7 <- select(v7_premerge_alpha,pid,contains("text"))
  text_v7_nona <- select_if(text_v7,not_all_na)
  v7_text_drops <- setdiff(names(text_v7),names(text_v7_nona))
  
  v7_premerge <- v7_premerge_alpha %>%
    select(-one_of(v7_text_drops)) %>%
    select(-person1,-person2,-person3,-person4,-person5)
    
  hold_text <- setdiff(names(v6_premerge),names(v7_premerge))
  v7_new_text_drops <- setdiff(v7_text_drops,hold_text)
  
  v7_premerge <- v7_premerge_alpha %>%
    select(-one_of(v7_new_text_drops)) %>%
    select(-person1,-person2,-person3,-person4,-person5) %>%
    mutate(sni_disapp_use_meth = NA_character_)
  
  v7_bind_labels <- get_label(v7_premerge)
  
  baseline_data <- v7_premerge %>%
    bind_rows(v6_premerge) %>%
    set_label(unlist(v7_bind_labels))
  
  return(baseline_data)
}

sni_fix_errors <- function(main_filepath,v1_sni_filepath,v2_sni_filepath,v3_sni_filepath,v4_sni_filepath){
  sni_pid <- c("5001","5002") 
  sni_pid <- as.data.table(sni_pid)
  v1_raw <- return_raw_baseline(main_filepath,v1_sni_filepath) %>%
    mutate(sni_version = "1") %>%
    select(-starts_with("sni_cosu3mo"),-starts_with("sni_anysu")) %>%
    bind_cols(sni_pid) %>%
    mutate(sni_pid = ifelse(sni_pid == "5002","1002",sni_pid)) %>%  ## MUST DOUBLE CHECK ##
    filter(sni_pid != "5001")
  v2_raw <- return_raw_baseline(main_filepath,v2_sni_filepath) %>%
    mutate(sni_version = "2") %>%
    bind_rows(v1_raw) %>%
    mutate(sni_nor_uh = NA_character_,
           sni_nor_uh_31_text = NA_character_,
           sni_nor_uh_32_text = NA_character_,
           sni_nor_uh_33_text = NA_character_,
           sni_nor_uh_34_text = NA_character_,
           sni_nor_uh_35_text = NA_character_)
  v3_raw <- return_raw_baseline(main_filepath,v3_sni_filepath) %>%
    mutate(sni_version = "3") %>%
    bind_rows(v2_raw) %>%
    rename(sni_rel_h_x29 = sni_rel_1,
           sni_rel_h_x29_text = sni_rel_1_text,
           sni_rel_h_x30 = sni_rel_2,
           sni_rel_h_x30_text = sni_rel_2_text,
           sni_rel_h_x31 = sni_rel_3,
           sni_rel_h_x31_text = sni_rel_3_text,
           sni_rel_h_x32 = sni_rel_4,
           sni_rel_h_x32_text = sni_rel_4_text,
           sni_rel_h_x33 = sni_rel_5,
           sni_rel_h_x33_text = sni_rel_5_text) %>%
    mutate(sni_rel_uh_x29 = NA_character_,
           sni_rel_uh_x29_text = NA_character_,
           sni_rel_uh_x30 = NA_character_,
           sni_rel_uh_x30_text = NA_character_,
           sni_rel_uh_x31 = NA_character_,
           sni_rel_uh_x31_text = NA_character_,
           sni_rel_uh_x32 = NA_character_,
           sni_rel_uh_x32_text = NA_character_,
           sni_rel_uh_x33 = NA_character_,
           sni_rel_uh_x33_text = NA_character_,
           sni_rel_h_oth1 = NA_character_,
           sni_rel_h_oth2 = NA_character_,
           sni_rel_h_oth3 = NA_character_,
           sni_rel_h_oth4 = NA_character_,
           sni_rel_h_oth5 = NA_character_,
           sni_rel_uh_oth1 = NA_character_,
           sni_rel_uh_oth2 = NA_character_,
           sni_rel_uh_oth3 = NA_character_,
           sni_rel_uh_oth4 = NA_character_,
           sni_rel_uh_oth5 = NA_character_,
           sni_5_for_app = NA_character_)
  v4_raw <- return_raw_baseline(main_filepath,v4_sni_filepath) %>%
    mutate(sni_version = "4")
  
  pre_bind_labels <- get_label(v4_raw)
  
  sni_fix <- v4_raw %>%
    bind_rows(v3_raw) %>%
    mutate(sni_pid = ifelse(responseid == "R_10qtoRYWsUkoIzt","1001",sni_pid),
           sni_pid = ifelse(responseid == "R_21ieA681AseMilh","3003",sni_pid),
           sni_pid = ifelse(responseid == "R_1pxTWxuU54eFKlB","1005",sni_pid),
           sni_pid = ifelse(responseid == "R_2diIpwcMFW8hEwg","3007",sni_pid),
           sni_pid = ifelse(responseid == "R_300lktfUf1pY6P5","1010",sni_pid),
           sni_pid = ifelse(responseid == "R_3JeBtrOV0li5zX8","1011",sni_pid),
           sni_pid = ifelse(responseid == "R_3shy6kIs7KNdtHV","1014",sni_pid),
           sni_pid = ifelse(responseid == "R_1kNSGuHA9YCyU18","3025",sni_pid),
           sni_pid = ifelse(responseid == "R_3065aTmwnpPCYF7","3046",sni_pid),
           sni_pid = ifelse(responseid == "R_12bSr93YzKLRLbh","3078",sni_pid),
           sni_pid = ifelse(responseid == "R_3hioYmelXxNGYOl","3089",sni_pid),
           sni_pid = ifelse(sni_pid == "2028","1129",sni_pid),
           sni_pid = ifelse(sni_pid == "2037","1130",sni_pid),
           sni_pid = ifelse(sni_pid == "2058","1137",sni_pid),
           sni_pid = ifelse(responseid == "R_3OfbRQsgDj2Kevm","2001",sni_pid),
           sni_pid = ifelse(responseid == "R_3Rt5HgK5XpHLn2Y","2002",sni_pid),
           sni_pid = ifelse(responseid == "R_w0pYnI7VH9xrMlz","2003",sni_pid),
           sni_pid = ifelse(responseid == "R_1IWMkYe9k7Y45ZN","2004",sni_pid),
           sni_pid = ifelse(responseid == "R_3MG6UTQ2fig8Us4","4008",sni_pid),
           sni_pid = ifelse(responseid == "R_3pgs8knsRgeM2Zb","4016",sni_pid)) %>%
    filter(responseid != "R_3HU2nKY8iSPGrCj" & responseid != "R_3qaF6Nse4uVMoE8" 
           & sni_pid != "1092" & responseid != "R_30bJEBI1ibsDp0D" & sni_pid != "2024" &
             sni_pid != "0" & responseid != "R_2f73yMqqRmR7RmV" & responseid != "R_1CIKwLoXlp7JGzG" &
             sni_pid != "105" & responseid != "R_pniit4z74M4zpu1" & responseid != "R_eyrEw6BGnL8LbOx" &
             responseid != "R_26ausAH2Hrf6N7F" & responseid != "R_3nNLDwYMELWbHuB" & responseid != "R_W23nNeL5OU6dVLj" &
             responseid != "R_1kNZzNdyMdPAYQp" & responseid != "R_2zvTwwoNVvualpm" & responseid != "R_Oy63Cz1GCwCrWut" &
             responseid != "R_1rBylBL2D0Tck8L" & responseid != "R_2CwgiaGXIhRR3g9" & responseid != "R_XH5rOqpEI6hoCdj" &
             responseid != "R_b3YdNZJPghhc1Gx" & responseid != "R_esoglRG96MTvO4F" & responseid != "R_3rTDSfMlY25KE53" &
             sni_pid != "575668" & sni_pid != "9999") %>%
    rename(sni_responseid = responseid,
           pid = sni_pid) %>%
    set_label(unlist(pre_bind_labels)) %>%
    select(pid,starts_with("sni")) 
  
  return(sni_fix)
}

v1_fix_errors <- function(main_filepath,v1_filepath, keep_missing = FALSE) {
  # v1_fix_1002 OK
  v1_raw <- return_raw_baseline(main_filepath,v1_filepath)
  v1_fix_3007 <- tibble(pid ="3007", sex = "Female", race = "I don't know", hisp = "I don't know")  # Fixing 1007 > 3007
  v1_fix_1004 <- v1_raw %>%
    filter(responseid == "R_72GUjPfXIWKYOlP" | responseid == "R_1o0ursi841wl4tT") %>%
    summarize_all(funs(first(na.omit(.)))) %>%
    mutate(dob = ifelse(pid == "1004","09/05/1994",dob))
  pre_bind_labels <- get_label(v1_raw)
  v1_fix <- v1_raw %>%
    filter(responseid != "R_72GUjPfXIWKYOlP" & responseid != "R_1o0ursi841wl4tT") %>%
    mutate(
      race = ifelse(pid == "1010","I don't know",race),
      race = ifelse(pid == "1011","I don't know",race)
      ) %>% 
    filter(responseid != "R_eaPtOykJQhKLnOh") %>%
    mutate(
      race = ifelse(pid == "1014","I don't know",race),
      alc_30_binge_m = ifelse(pid == "2002","0",alc_30_binge_m),
      race = ifelse(pid == "2002","I don't know",race),
      race = ifelse(pid == "2003","I don't know",race),
      race = ifelse(pid == "2004","I don't know",race),
      sexori = ifelse(pid == "2004","Heterosexual or straight",sexori),
      sex = ifelse(pid == "2005","Male",sex),
      sucost_30d_marj = ifelse(pid == "2006","$51 - $250",sucost_30d_marj),
      sucost_30d_ildrug = ifelse(pid == "2006","Less than $50",sucost_30d_ildrug),
      race = ifelse(pid == "2006","I don't know",race),
      pid = ifelse(responseid == "R_wTrw4o4fQzrcgUh","3003",pid),
      dob = ifelse(pid == "1014","09/20/1993",dob),
      dob = ifelse(dob == "3-2-1999","03/02/1999",dob)) %>%
    filter(
      responseid != "R_1pyRsMTctbJuoK3" & 
      responseid != "R_1pAVgwmZ0NSDvrc" &
      responseid != "R_2cu51Ni0Lit4T0e" &
      responseid != "R_0NugAIF3VyvohKV" &
      responseid != "R_OiLgOEXdq9zF0iJ" &
      responseid != "R_1myHhk3o8mAi0GY"
      ) %>%
    bind_rows(v1_fix_1004) %>%
    bind_rows(v1_fix_3007) %>%
    set_label(unlist(pre_bind_labels)) %>%
    mutate(version = 1) %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No"),
           demo_racemin = NA_character_)
  
  if(keep_missing){
    return(v1_fix)
  } else {
    return(v1_fix %>% select_if(not_all_na))
  }

}

v2_fix_errors <- function(main_filepath,v2_filepath, keep_missing = FALSE) {
  v2_raw <- return_raw_baseline(main_filepath,v2_filepath)
  v2_fix <- v2_raw %>%
    mutate(
      demo_racemin = NA_character_, 
      race = ifelse(pid == "1008","Black or African-American",race),
      sexori = ifelse(pid == "1008","Heterosexual or straight",sexori),
      race = ifelse(pid == "1009","I don't know",race),
      race = ifelse(pid == "1015","I don't know",race),
      demo_racemin = ifelse(pid == "1015","Yes",demo_racemin),          #### Remember to exclude during demo_racemin
      survey_type = ifelse(pid == "1021","Housed",survey_type),
      race = ifelse(pid == "1032","White",race),
      pid = ifelse(responseid == "R_2PmUtgGazr5a8z7","1036",pid),
      hisp = ifelse(pid == "1037","Yes",hisp),
      sex = ifelse(pid == "1041","Male",sex),
      gender = ifelse(pid == "1041","Male",gender),
      site_housed = ifelse(pid == "1050", "Harbor Interfaith SPA8 Rapid Rehousing",site_housed),
      site_housed = ifelse(pid == "1051", "Harbor Interfaith SPA8 Rapid Rehousing",site_housed),
      site_housed = ifelse(pid == "1052", "Hathaway Sycamores TLC",site_housed),
      site_housed = ifelse(pid == "1054", "Harbor Interfaith SPA8 Rapid Rehousing",site_housed),
      race = ifelse(pid == "2009", "Bi/Multi-racial or Ethnic", race),
      pid = ifelse(responseid == "R_1GUHNGNcf1AYuJY", "2017", pid),
      site_housed = ifelse(pid == "2017","Jovenes Emergency Shelter",site_housed),
      pid = ifelse(responseid == "R_pFaeDxOuzvRPFkZ", "3025", pid),
      race = ifelse(pid == "3025", "I don't know", race),
      pid = ifelse(responseid == "R_2AQdJ7QXdBSRZDC", "3046", pid),
      pid = ifelse(responseid == "R_1exDzqYGNb34xQq", "4008", pid),
      sex = ifelse(pid == "4008","Female",sex),
      pid = ifelse(responseid == "R_1Fb1Tth45hmne1q", "4016", pid)
    ) %>%
    filter(
      pid != "101" & 
      pid != "0" &
      !is.na(pid)
    )  %>%
    mutate(version = 2) %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No"))
  
  if(keep_missing){
    return(v2_fix)
  } else {
    return(v2_fix %>% select_if(not_all_na))
  }
  
    
}

v3_fix_errors <- function(main_filepath,v30_filepath,v31_filepath, keep_missing = FALSE) {
  v30_raw <- return_raw_baseline(main_filepath,v30_filepath)
  v31_raw <- return_raw_baseline(main_filepath,v31_filepath)
  
  v30_new <- v30_raw %>%
    mutate(site_housed = ifelse(pid == "1058","Penny Lane - North Hills TLP",site_housed),
           incmgen_30day = ifelse(pid == "1058","From an agency or program, such as food stamps or welfare (e.g., food stamps/SNAP, TANF, SSI, GR)",incmgen_30day),
           ttlincome_benefits = ifelse(pid == "1058","$251 - $500",NA))
  
  v31_new <- v31_raw %>%
    mutate(race = ifelse(pid == "1056","I don't know",race)) %>%
    filter(pid != "1077")
  
  
  v3_fix <- bind_rows(v30_new,v31_new)  %>%
    mutate(demo_racemin = NA_character_) %>%
    filter(responseid != "R_28MuG4hl2PCLe0I" & responseid != "R_12R3h2qrWS8AkUi" & responseid != "R_3pgMyQjKSLDa3H9"
           & responseid != "R_2aDfQEATCqHYQHi") %>%
    mutate(version = 3) %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No"))
  
  if(keep_missing){
    return(v3_fix)
  } else {
    return(v3_fix %>% select_if(not_all_na))
  }
  
}

v4_fix_errors <- function(main_filepath,v4_filepath, keep_missing = FALSE) {
  v4_raw <- return_raw_baseline(main_filepath,v4_filepath)
  
  v4_fix <- v4_raw %>%
    mutate(demo_racemin = NA_character_,
           race = ifelse(pid == "1070","I don't know",race),
           demo_racemin = ifelse(pid == "1070","Yes",demo_racemin),          #### Remember to exclude during demo_racemin
           site_housed = ifelse(pid =="1102","David & Margaret THP",site_housed),
           race = ifelse(pid == "1087","I don't know",race),
           demo_racemin = ifelse(pid == "1087","Unknown",demo_racemin),          #### Remember to exclude during demo_racemin
           pid = ifelse(responseid == "R_1q8J5bPCH9so9ev","1123",pid),
           pid = ifelse(responseid == "R_pRUAf7AhL2RWtLb","1127",pid),
           pid = ifelse(pid == "1089","3089",pid),
           pid = ifelse(pid == "1078","3078",pid),
           race = ifelse(pid == "1127","I don't know",race),
           demo_racemin = ifelse(pid == "1127","Yes",demo_racemin)) %>%
    filter(responseid != "R_1FwVRmAmCCIpTr4" & pid != "1092")  %>%
    mutate(version = 4) %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No"))
  
  if(keep_missing){
    return(v4_fix)
  } else {
    return(v4_fix %>% select_if(not_all_na))
  }
}

v5_fix_errors <- function(main_filepath,v5_filepath, keep_missing = FALSE) {
  v5_raw <- return_raw_baseline(main_filepath,v5_filepath) %>%
    mutate(version = 5) %>%
    mutate(demo_racemin = NA_character_,
           pid = ifelse(responseid == "R_2xITaCY51RIVNsr","1129",pid),
           pid = ifelse(responseid == "R_1IB4Z1SffntGWQJ","1130",pid),
           race = ifelse(pid == "2007","I don't know",race),
           demo_racemin = ifelse(pid == "2007","Yes",demo_racemin),
           race = ifelse(pid == "2014","White",race),
           demo_racemin = ifelse(pid == "2014","No",demo_racemin),
           race = ifelse(pid == "2019","I don't know",race),
           uh_livsit_curr = ifelse(pid == "2036","Street, park, beach, or outside",uh_livsit_curr),
           race = ifelse(pid == "2036","Black",race),
           demo_racemin = ifelse(pid == "2036","Yes",demo_racemin)) %>%
    filter(pid != "2024" & pid != "9999" & pid != "9999999" & pid != "1100") %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No")) %>%
    rename_at(vars(starts_with("null_x1.")),funs(map(.,rename_drugs,"none",8))) %>%
    rename(sni_su30_tob_none = null_x1) %>%
    rename_at(vars(starts_with("null_xx29.")),funs(map(.,rename_drugs,"xx1",10))) %>%
    rename(sni_su30_tob_a1 = null_xx29) %>%
    rename_at(vars(starts_with("null_xx30.")),funs(map(.,rename_drugs,"xx2",10))) %>%
    rename(sni_su30_tob_a2 = null_xx30) %>%
    rename_at(vars(starts_with("null_xx31.")),funs(map(.,rename_drugs,"xx3",10))) %>%
    rename(sni_su30_tob_a3 = null_xx31) %>%
    rename_at(vars(starts_with("null_xx32.")),funs(map(.,rename_drugs,"xx4",10))) %>%
    rename(sni_su30_tob_a4 = null_xx32) %>%
    rename_at(vars(starts_with("null_xx33.")),funs(map(.,rename_drugs,"xx5",10))) %>%
    rename(sni_su30_tob_a5 = null_xx33) %>%
    rename_at(vars(starts_with("null_xx29_text.")),funs(map(.,rename_drugs,"xx1_text",15))) %>%
    rename(sni_su30_tob_a1_text = null_xx29_text) %>%
    rename_at(vars(starts_with("null_xx30_text.")),funs(map(.,rename_drugs,"xx2_text",15))) %>%
    rename(sni_su30_tob_a2_text = null_xx30_text) %>%
    rename_at(vars(starts_with("null_xx31_text.")),funs(map(.,rename_drugs,"xx3_text",15))) %>%
    rename(sni_su30_tob_a3_text = null_xx31_text) %>%
    rename_at(vars(starts_with("null_xx32_text.")),funs(map(.,rename_drugs,"xx4_text",15))) %>%
    rename(sni_su30_tob_a4_text = null_xx32_text) %>%
    rename_at(vars(starts_with("null_xx33_text.")),funs(map(.,rename_drugs,"xx5_text",15))) %>%
    rename(sni_su30_tob_a5_text = null_xx33_text)
  
  if(keep_missing){
    return(v5_raw)
  } else {
    return(v5_raw %>% select_if(not_all_na))
  }
}

v6_fix_errors <- function(main_filepath,v6_filepath, keep_missing = FALSE) {
  v6_raw <- return_raw_baseline(main_filepath,v6_filepath) %>%
    mutate(demo_racemin = NA_character_,
           pid = ifelse(pid == "2038","1132",pid),
           sexori = ifelse(pid == "1135","Questioning or unsure",sexori),
           sutypes_use_ever = ifelse(pid == "2040","Alcohol,Marijuana,Meth,Ecstasy / MDMA / “Molly”,Hallucinogens/psychedelics (LSD/acid, “shrooms,” etc),Cocaine (powdered)",sutypes_use_ever),
           race = ifelse(pid == "2044","Bi/Multi-racial or Ethnic",race),
           demo_racemin = ifelse(pid == "2044","Yes",demo_racemin),
           uh_livsit_curr = ifelse(pid == "2047","Adult emergency/temporary shelter (less than 30 days)",uh_livsit_curr),
           sexori = ifelse(pid == "2048","Bisexual",sexori)
           ) %>%
    mutate(version = 6) %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No"))
  
  if(keep_missing){
    return(v6_raw)
  } else {
    return(v6_raw %>% select_if(not_all_na))
  }

}

v7_fix_errors <- function(main_filepath,v7_filepath, keep_missing = FALSE) {
  v7_raw <- return_raw_baseline(main_filepath,v7_filepath) %>%
    mutate(demo_racemin = NA_character_)
  pre_bind_labels <- get_label(v7_raw)
  
  v7_fix_2053 <- v7_raw %>%
    filter(pid == "2053") %>%
    summarize_all(funs(first(na.omit(.))))
  
  v7_fix <- v7_raw %>%
    filter(!is.na(pid) & as.numeric(pid) > 100) %>%
    filter(pid != "2053") %>%
    mutate(pid = ifelse(pid == "2058","1137",pid),
           sexori = ifelse(pid == "2059","Bisexual",sexori),
           uh_livsit_curr = ifelse(pid == "2068","Street, park, beach, or outside",uh_livsit_curr),
           race = ifelse(pid == "2066","I don't know",race),
           demo_racemin = ifelse(pid == "2066","Yes",demo_racemin)) %>%
    bind_rows(v7_fix_2053) %>%
    set_label(unlist(pre_bind_labels)) %>%
    mutate(version = 7) %>%
    mutate(housed_yn = ifelse(as.integer(pid) < 2000 | (as.integer(pid) < 4000 & as.integer(pid) > 2999),"Yes","No"))
  
  if(keep_missing){
    return(v7_fix)
  } else {
    return(v7_fix %>% select_if(not_all_na))
  }
  
}



  

