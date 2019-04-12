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

sleepy_time <- function(filtered_dailylog){
  return_sleep <- filtered_dailylog %>%
    select(daily_prompt_date,daily_prompt_time,wake_hour,sleep_hour,sleep_minute,wake_minute,no_sleep) %>%
    mutate(date_now = daily_prompt_date,
           date_yesterday = date_now - days(),
           og_wake_hour = wake_hour,
           og_sleep_hour = sleep_hour,
           og_wake_min = wake_minute,
           og_sleep_min = sleep_minute) %>%
    mutate(daily_prompt_time = gsub("PDT ","",daily_prompt_time),
           daily_prompt_time = gsub("PST ","",daily_prompt_time),
           time_full = as.POSIXct(daily_prompt_time, format = "%a %b %d %T %Y", tz = "America/Los_Angeles", origin = "1970-01-01")) %>%
    select(-daily_prompt_time) %>%
    mutate(wake_hour = ifelse(wake_hour > 12, wake_hour - 12, wake_hour),
           sleep_hour = ifelse(sleep_hour > 12, sleep_hour - 12, sleep_hour),
           wake_hour = ifelse(wake_hour == 0, 12,wake_hour),
           sleep_hour = ifelse(sleep_hour == 0, 12, sleep_hour)) %>%
    mutate(wake_am_time = paste0(format(date_now,"%Y-%m-%d")," ",str_pad(wake_hour,2,pad="0"),":",str_pad(wake_minute,2,pad = "0")," AM"),
           wake_pm_time = paste0(format(date_now,"%Y-%m-%d")," ",str_pad(wake_hour,2,pad="0"),":",str_pad(wake_minute,2,pad = "0"), " PM"),
           sleep_am_time = paste0(format(date_now,"%Y-%m-%d")," ",str_pad(sleep_hour,2,pad="0"),":",str_pad(sleep_minute,2,pad = "0")," AM"),
           sleep_pm_time = paste0(format(date_yesterday,"%Y-%m-%d")," ",str_pad(sleep_hour,2,pad="0"),":",str_pad(sleep_minute,2,pad = "0"), " PM")) %>%
    mutate_at(vars(ends_with("time")),funs(as.POSIXct(., format = "%Y-%m-%d %I:%M %p", origin = "1970-01-01", tz = "America/Los_Angeles"))) %>%
    mutate(wake_am_time = ifelse(wake_am_time > time_full,0,wake_am_time),
           wake_pm_time = ifelse(wake_pm_time > time_full,0,wake_pm_time),
           sleep_am_time = ifelse(sleep_am_time > time_full,0,sleep_am_time)) %>%
    mutate_at(vars(wake_am_time,wake_pm_time,sleep_am_time),funs(as.POSIXct(., origin = "1970-01-01", tz = "America/Los_Angeles"))) %>%
    mutate(wakeam_sleepam = as.numeric(difftime(wake_am_time,sleep_am_time,units = "hours")),
           wakeam_sleeppm = as.numeric(difftime(wake_am_time,sleep_pm_time,units = "hours")),
           wakepm_sleepam = as.numeric(difftime(wake_pm_time,sleep_am_time,units = "hours")),
           wakepm_sleeppm = as.numeric(difftime(wake_pm_time,sleep_pm_time,units = "hours"))) %>%
    mutate(aa_bin = ifelse(wakeam_sleepam > 0 & wakeam_sleepam < 24,1,0),
           ap_bin = ifelse(wakeam_sleeppm > 0 & wakeam_sleeppm < 24,1,0),
           pa_bin = ifelse(wakepm_sleepam > 0 & wakepm_sleepam < 24,1,0),
           pp_bin = ifelse(wakepm_sleeppm > 0 & wakepm_sleeppm < 24,1,0)) %>%
    mutate(duped = ifelse(og_wake_hour == og_sleep_hour & og_wake_min == og_sleep_min,1,0)) %>%
    mutate(aa_bin = ifelse(wakeam_sleepam < 2,0,aa_bin),
           aa_bin = ifelse(wakeam_sleepam > 14,0,aa_bin),
           aa_bin = ifelse(wakeam_sleepam < 0,0,aa_bin)) %>%
    mutate(ap_bin = ifelse(wakeam_sleeppm < 2,0,ap_bin),
           ap_bin = ifelse(wakeam_sleeppm > 14,0,ap_bin),
           ap_bin = ifelse(wakeam_sleeppm < 0,0,ap_bin)) %>%
    mutate(pa_bin = ifelse(wakepm_sleepam < 2,0,pa_bin),
           pa_bin = ifelse(wakepm_sleepam > 14,0,pa_bin),
           pa_bin = ifelse(wakepm_sleepam < 0,0,pa_bin)) %>%
    mutate(pp_bin = ifelse(wakepm_sleeppm < 2,0,pp_bin),
           pp_bin = ifelse(wakepm_sleeppm > 14,0,pp_bin),
           pp_bin = ifelse(wakepm_sleeppm < 0,0,pp_bin)) %>%
    mutate(decision = rowSums(cbind(aa_bin,ap_bin,pa_bin,pp_bin), na.rm = TRUE)) %>%
    mutate(decision = ifelse(ap_bin == 1 & pa_bin == 1, decision - 1, decision),
           decision = ifelse(is.na(wake_am_time),3, decision),
           decision = ifelse(is.na(sleep_am_time),3, decision),
           decision = ifelse(duped == 1,0, decision)) %>%
    mutate(aa_binhold = ifelse(aa_bin == 1,wakeam_sleepam,NA_integer_),
           ap_binhold = ifelse(ap_bin == 1,wakeam_sleeppm,NA_integer_),
           pa_binhold = ifelse(pa_bin == 1,wakepm_sleepam,NA_integer_),
           pp_binhold = ifelse(pp_bin == 1,wakepm_sleeppm,NA_integer_)) %>%
    mutate(sleep_time = pmax(aa_binhold,ap_binhold,pa_binhold,pp_binhold, na.rm = TRUE)) %>%
    mutate(sleep_time = ifelse(decision == 1,sleep_time,NA_integer_)) %>%
    mutate(sleep_time = ifelse(no_sleep == 1,0,sleep_time)) %>%
    mutate(a_sleep_48 = ifelse(aa_bin == 1,24 + (sleep_hour+sleep_minute/60),NA_integer_),
           p_sleep_48 = ifelse(ap_bin == 1,(12+sleep_hour+sleep_minute/60),NA_integer_),
           a_sleep_48 = ifelse(pa_bin == 1,24 + (sleep_hour+sleep_minute/60),a_sleep_48),
           p_sleep_48 = ifelse(pp_bin == 1,(12+sleep_hour+sleep_minute/60),p_sleep_48),
           p_sleep_48 = p_sleep_48 - 12*(sleep_hour==12),
           a_sleep_48 = a_sleep_48 - 12*(sleep_hour==12)) %>%
    mutate(a_wake_48 = ifelse(aa_bin == 1,24 + (wake_hour+wake_minute/60),NA_integer_),
           a_wake_48 = ifelse(ap_bin == 1,24 + (wake_hour+wake_minute/60),a_wake_48),
           p_wake_48 = ifelse(pa_bin == 1,24 + (12+wake_hour+wake_minute/60),NA_integer_),
           p_wake_48 = ifelse(pp_bin == 1,24 + (12+wake_hour+wake_minute/60),p_wake_48),
           p_wake_48 = p_wake_48 - 12*(wake_hour==12),
           a_wake_48 = a_wake_48 - 12*(wake_hour==12)) %>%
    mutate(sleep_missing = is.na(a_wake_48)+is.na(p_wake_48)+is.na(a_sleep_48)+is.na(p_sleep_48)) %>%
    mutate(a_wake_48 = ifelse(is.na(p_sleep_48) & sleep_missing == 1,NA,a_wake_48),
           p_wake_48 = ifelse(is.na(a_sleep_48) & sleep_missing == 1,NA,p_wake_48),
           a_sleep_48 = ifelse(is.na(p_wake_48) & sleep_missing == 1,NA,a_sleep_48),
           p_sleep_48 = ifelse(is.na(a_wake_48) & sleep_missing == 1,NA,p_sleep_48)) %>%
    mutate(sleep_missing = is.na(a_wake_48)+is.na(p_wake_48)+is.na(a_sleep_48)+is.na(p_sleep_48)) %>%
    select(sleep_time,a_wake_48,p_wake_48,a_sleep_48,p_sleep_48,sleep_missing)
  
  return(return_sleep)
}


daily_align_sni_alters <- function(filtered_dailylog, new_baseline, other_sni_label = "Someone else not listed here",
                                 none_sni_label = "I have not interacted with anyone", social_type){
  if(social_type == "core"){
    new_daily <- select(filtered_dailylog, pid, sni_social = Q4_0_soccore)
  } else if(social_type == "alcdaily"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_2_b_alcohol_who)
  } else if(social_type == "marijuana"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_3_b_marijuana_who)
  } else if(social_type == "syntheticmj"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_6_b_synthmj_who)
  } else if(social_type == "meth"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_4_b_meth_who)
  } else if(social_type == "rxmisuse"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_8_b_prescription_who)
  } else if(social_type == "mdma"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_5_b_mdma_who)
  } else if(social_type == "hallucinogen"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_7_b_halluc_who)
  } else if(social_type == "heroin"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_9_b_heroin_who)
  } else if(social_type == "cocaine"){
    new_daily <- rename(filtered_dailylog, sni_social = Q4_10_c_coke_who)
  }
  
  sni_ids <- new_baseline %>%
    select(pid,starts_with("sni_alter_ids_")) %>%
    filter(!is.na(sni_alter_ids_1)) %>%
    mutate(sni_alter_ids_6 = other_sni_label) %>%
    mutate(sni_alter_ids_0 = none_sni_label) %>%
    mutate_at(vars(starts_with("sni_alter_ids_")),funs(simplify_sni_id(.))) %>%
    mutate_at(vars(starts_with("sni_alter_ids_")),funs(paste0("|",.,"|"))) 
  
  full_ids <- unique(new_daily$pid)
  
  not_in_sni <- setdiff(full_ids, sni_ids$pid)
  
  sni_ids <- sni_ids %>%
    add_row(pid = not_in_sni)
  
  sni_social_daily <- new_daily %>%
    select(pid,sni_social) %>%
    mutate(sni_social = tolower(sni_social)) %>%
    mutate(sni_social = str_replace_all(sni_social,"\\.","")) %>%
    mutate(sni_social = ifelse(sni_social == "question is not displayed",NA_character_,sni_social)) %>%
    mutate(sni_social = str_replace_all(sni_social," ","")) %>%
    mutate(sni_social = paste0("|",sni_social,"|")) %>%
    mutate(sni_social = str_replace_all(sni_social,",","")) %>%
    mutate(sni_social = str_replace_all(sni_social,"(?<=ididnot).*?(?=\\|)","")) %>%
    mutate(sni_social = ifelse(sni_social == "|NA|",NA_character_,sni_social)) %>%
    mutate(sni_social_a1 = 0,
           sni_social_a2 = 0,
           sni_social_a3 = 0,
           sni_social_a4 = 0,
           sni_social_a5 = 0,
           sni_social_a6 = 0,
           sni_social_a0 = 0)
  
  sni_test_df <- as_tibble(str_split(sni_social_daily$sni_social, fixed("|"), simplify = TRUE))
  sni_expand <- 8 - ncol(sni_test_df)
  sni_ogncol <- ncol(sni_test_df)
  if(sni_expand > 0){
    for(i in 1:sni_expand){
      sni_colname = paste0("V",i+sni_ogncol)
      sni_test_df <- mutate_(sni_test_df, .dots= setNames(list(NA_character_), sni_colname))
      }
  }
  sni_test_array <- sni_test_df %>%
    mutate_all(funs(ifelse(is.na(.),"",.))) %>%
    mutate(sni_test = 
             (V1 != "") + 
             (V2 != "") + 
             (V3 != "") + 
             (V4 != "") + 
             (V5 != "") + 
             (V6 != "") + 
             (V7 != "") + 
             (V8 != ""))
  
  sni_social_daily <- sni_social_daily %>%
    add_column("sni_test" = sni_test_array$sni_test)
  
  core_count <- detectCores() - 1L
  registerDoParallel(cores = core_count)
  
  new_social_daily <- foreach(i=1:nrow(sni_ids), .combine = rbind) %dopar% {
    sni_social_daily %>%
      filter(grepl(sni_ids[i,"pid"],pid)) %>%
      mutate(sni_social_a1 = ifelse(grepl(sni_ids[i,"sni_alter_ids_1"],sni_social, fixed = TRUE),1,sni_social_a1)) %>%
      mutate(sni_social_a2 = ifelse(grepl(sni_ids[i,"sni_alter_ids_2"],sni_social, fixed = TRUE),1,sni_social_a2)) %>%
      mutate(sni_social_a3 = ifelse(grepl(sni_ids[i,"sni_alter_ids_3"],sni_social, fixed = TRUE),1,sni_social_a3)) %>%
      mutate(sni_social_a4 = ifelse(grepl(sni_ids[i,"sni_alter_ids_4"],sni_social, fixed = TRUE),1,sni_social_a4)) %>%
      mutate(sni_social_a5 = ifelse(grepl(sni_ids[i,"sni_alter_ids_5"],sni_social, fixed = TRUE),1,sni_social_a5)) %>%
      mutate(sni_social_a6 = ifelse(grepl(sni_ids[i,"sni_alter_ids_6"],sni_social, fixed = TRUE),1,sni_social_a6)) %>%
      mutate(sni_social_a0 = ifelse(grepl(sni_ids[i,"sni_alter_ids_0"],sni_social, fixed = TRUE),1,sni_social_a0))
  }
  
  registerDoSEQ()
  
  prereturn_sociallog <- new_social_daily %>%
    mutate_at(vars(starts_with("sni_social_a")),funs(ifelse(is.na(sni_social),NA,.))) %>%
    #select(starts_with("sni_social_a"), sni_test, sni_social) %>%
    mutate(sni_new_test = sni_social_a0 + sni_social_a1 + sni_social_a2 + sni_social_a3 + 
             sni_social_a4 + sni_social_a5 + sni_social_a6) %>%
    mutate(sni_check = sni_test == sni_new_test)
  
  return_sociallog <- prereturn_sociallog %>%
    select(-sni_new_test,-sni_test,-pid,-sni_social) %>%
    select_all(.funs = funs(paste0(social_type,"_",.)))
  
  return(return_sociallog)
}

multiple_sex_partners <- function(filtered_dailylog){
  return_dl <- filtered_dailylog %>%
    select(starts_with("R",ignore.case = FALSE)) %>%
    rename_at(vars(starts_with("R",ignore.case = FALSE)), funs(paste0(substring(.,4),"_p",substring(.,2,2)))) %>%
    rename_at(vars(starts_with("Q5_sex_a_id_")), funs(paste0("sex_partner_id_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_b_parttype_")), funs(paste0("sex_partner_type_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_b2_partdur_")), funs(paste0("sex_partner_duration_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_c_identity_")), funs(paste0("sex_partner_gender_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_d_condom_")), funs(paste0("sex_partner_condomuse_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_d1_hiv_")), funs(paste0("sex_partner_hiv_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_d1_hiv1_")), funs(paste0("sex_partner_hiv2_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_e_substance_")), funs(paste0("sex_partner_druguse_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_f_where_")), funs(paste0("sex_partner_where_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_f_where1_")), funs(paste0("sex_partner_where2_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_g_where_other_")), funs(paste0("sex_partner_where_other_p",substring(.,nchar(.),nchar(.))))) %>%
    rename_at(vars(starts_with("Q5_sex_g_where_other1_")), funs(paste0("sex_partner_where_other2_p",substring(.,nchar(.),nchar(.))))) %>%
    mutate(sex_partner_where_p1 = ifelse(is.na(sex_partner_where_p1),sex_partner_where2_p1,sex_partner_where_p1),
           sex_partner_where_p2 = ifelse(is.na(sex_partner_where_p2),sex_partner_where2_p2,sex_partner_where_p2),
           sex_partner_where_p3 = ifelse(is.na(sex_partner_where_p3),sex_partner_where2_p3,sex_partner_where_p3),
           sex_partner_where_p4 = ifelse(is.na(sex_partner_where_p4),sex_partner_where2_p4,sex_partner_where_p4)) %>%
    mutate(sex_partner_where_other_p1 = ifelse(is.na(sex_partner_where_other_p1),sex_partner_where_other2_p1,sex_partner_where_other_p1),
           sex_partner_where_other_p2 = ifelse(is.na(sex_partner_where_other_p2),sex_partner_where_other2_p2,sex_partner_where_other_p2),
           sex_partner_where_other_p3 = ifelse(is.na(sex_partner_where_other_p3),sex_partner_where_other2_p3,sex_partner_where_other_p3),
           sex_partner_where_other_p4 = ifelse(is.na(sex_partner_where_other_p4),sex_partner_where_other2_p4,sex_partner_where_other_p4)) %>%
    mutate(sex_partner_where_p1 = ifelse(is.na(sex_partner_where_p1),sex_partner_where2_p1,sex_partner_where_p1),
           sex_partner_where_p2 = ifelse(is.na(sex_partner_where_p2),sex_partner_where2_p2,sex_partner_where_p2),
           sex_partner_where_p3 = ifelse(is.na(sex_partner_where_p3),sex_partner_where2_p3,sex_partner_where_p3),
           sex_partner_where_p4 = ifelse(is.na(sex_partner_where_p4),sex_partner_where2_p4,sex_partner_where_p4)) %>%
    select(-starts_with("sex_partner_where2"),-starts_with("sex_partner_where_other2"),-starts_with("sex_partner_hiv2"))
  return(return_dl)
}

social_daily <- function(filtered_dailylog, sni_stata_filename, social_type){
  sni_list <-  read_sni(sni_stata_filename)
  
  if(social_type == "core"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_0_soccore)
  }
  else if(social_type == "alcohol"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_2_b_alcohol_who)
  }
  else if(social_type == "marijuana"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_3_b_marijuana_who)
  }
  else if(social_type == "synthetic"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_6_b_synthmj_who)
  }
  else if(social_type == "meth"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_4_b_meth_who)
  }
  else if(social_type == "rx"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_8_b_prescription_who)
  }
  else if(social_type == "mdma"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_5_b_mdma_who)
  }
  else if(social_type == "hallucinogen"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_7_b_halluc_who)
  }
  else if(social_type == "heroin"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_9_b_heroin_who)
  }
  else if(social_type == "cocaine"){
    prefiltered_dailylog <- rename(filtered_dailylog, sni_social = Q4_10_c_coke_who)
  }

  new_dailylog <- prefiltered_dailylog %>%
    mutate(sni_social = tolower(sni_social)) %>%
    mutate(sni_social = str_replace_all(sni_social,"\\.","")) %>%
    mutate(sni_social = str_replace_all(sni_social," ","")) %>%
    mutate(sni_social = paste0("|",sni_social,"|")) %>%
    mutate(sni_social = str_replace_all(sni_social,",","")) %>%
    #mutate(sni_social = ifelse(sni_social == "|ididnotinteractwithanyofthesepeople<b>yesterday</b>|",NA_character_,sni_social)) %>%
    #mutate(sni_social = ifelse(sni_social_alone == "|ididnotinteractwithanyofthesepeople<b>yesterday</b>|",1,0)) %>%
    #mutate(sni_social = ifelse(sni_social == "|ididnotinteractwithanyofthesepeople&lt;b&gt;yesterday&lt;/b&gt;|",NA_character_,sni_social)) %>%
    #mutate(sni_social = ifelse(sni_social_alone == "|ididnotinteractwithanyofthesepeople&lt;b&gt;yesterday&lt;/b&gt;|",1,sni_social_alone)) %>%
    mutate(sni_social = ifelse(sni_social == "|NA|",NA_character_,sni_social)) %>%
    mutate(sni_social_a1 = 0,
           sni_social_a2 = 0,
           sni_social_a3 = 0,
           sni_social_a4 = 0,
           sni_social_a5 = 0,
           sni_social_a0 = 0)
  
  social_dailylog <- new_dailylog
  
  for(i in 1:nrow(sni_list)){
    social_dailylog <- social_dailylog %>%
      mutate(sni_social_a1 = ifelse(grepl(sni_list[i,"subject_id"],subject_id)
                                    & grepl(sni_list[i,"sni1"],sni_social),1,sni_social_a1)) %>%
      mutate(sni_social_a2 = ifelse(grepl(sni_list[i,"subject_id"],subject_id)
                                    & grepl(sni_list[i,"sni2"],sni_social),1,sni_social_a2)) %>%
      mutate(sni_social_a3 = ifelse(grepl(sni_list[i,"subject_id"],subject_id)
                                    & grepl(sni_list[i,"sni3"],sni_social),1,sni_social_a3)) %>%
      mutate(sni_social_a4 = ifelse(grepl(sni_list[i,"subject_id"],subject_id)
                                    & grepl(sni_list[i,"sni4"],sni_social),1,sni_social_a4)) %>%
      mutate(sni_social_a5 = ifelse(grepl(sni_list[i,"subject_id"],subject_id)
                                    & grepl(sni_list[i,"sni5"],sni_social),1,sni_social_a5)) %>%
      mutate(sni_social_a0 = ifelse(grepl(sni_list[i,"subject_id"],subject_id)
                                    & grepl("ididnotinteractwithanyofthesepeople",sni_social),1,sni_social_a0))
  }
  
  prereturn_sociallog <- social_dailylog %>%
    mutate_at(vars(starts_with("sni_social_a")),funs(ifelse(is.na(sni_social),NA_character_,.))) %>%
    select(sni_social_a0,sni_social_a1,sni_social_a2,sni_social_a3,sni_social_a4,sni_social_a5)
  
  return_sociallog <- prereturn_sociallog %>%
      select_all(.funs = funs(paste0(social_type,"_",.)))
  
  return(return_sociallog)
}

sex_where_daily <- function(filtered_dailylog){
  filtered_data <- mutate(filtered_dailylog,sex_exchange_where = Q7_sex_exchange_where)
  prebind_data(filtered_data, "sex_exchange_where", new_names, new_labels, return_name_columns = TRUE)
  
  new_names <- c(
    `sex_exchange_where_Hotel/motel` = "sex_exchange_where_hotel",
    `sex_exchange_where_My home` = "sex_exchange_where_myhome",
    `sex_exchange_where_Shelter or drop-in` = "sex_exchange_where_shelter",
    `sex_exchange_where_Outside (park, beach)` = "sex_exchange_where_outside",
    `sex_exchange_where_The home of someone I know` = "sex_exchange_where_knownhome",
    `sex_exchange_where_The home of someone I don&#8217;t know or barely know` = "sex_exchange_where_unknownhome",
    `sex_exchange_where_Tent or improvised shelter` = "sex_exchange_where_tent",
    `sex_exchange_where_Car, RV, or other vehicle` = "sex_exchange_where_vehicle",
    `sex_exchange_where_Abandoned building or squat` = "sex_exchange_where_squat",
    `sex_exchange_where_Other` = "sex_exchange_where_other"
    )
  new_labels <- c(
      sex_exchange_where_hotel = "Hotel/motel,",
      sex_exchange_where_myhome = "My home",
      sex_exchange_where_shelter = "Shelter or drop-in",
      sex_exchange_where_outside = "Outside (park, beach)",
      sex_exchange_where_knownhome = "The home of someone I know",
      sex_exchange_where_unknownhome = "The home of someone I don't know or barely know",
      sex_exchange_where_tent = "Tent or improvised shelter",
      sex_exchange_where_vehicle = "Car, RV, or other vehicle",
      sex_exchange_where_squat = "Abandoned building or squat",
      sex_exchange_where_other = "Other"
    )
  return_data <- prebind_data(filtered_data, "sex_exchange_where", new_names, new_labels)
  
  return(return_data)
}

drugs_type_daily <- function(filtered_dailylog){
  #filtered_dailylog <- mutate(filtered_dailylog,Q13_a_drugs_type = ifelse(Q13_a_drugs_type == "Ecstasy / MDMA / “Molly”","Ecstasy / MDMA / Molly",Q13_a_drugs_type))
  filtered_data <- mutate(filtered_dailylog,drugs_type = ifelse(Q4a_substances == "Other illicit drug" & !is.na(Q4b_substances)
                                                                  ,Q4b_substances,Q4a_substances))
  new_names <- c(
      `drugs_type_Alcohol` = "drugs_type_alcohol",
      `drugs_type_Cocaine or crack` = "drugs_type_cocaine",
      `drugs_type_Hallucinogens/psychedelics` = "drugs_type_hallucinogens",
      `drugs_type_I did not use any drugs yesterday` = "drugs_type_none",
      `drugs_type_Marijuana` = "drugs_type_marijuana",
      `drugs_type_Meth` = "drugs_type_meth",
      `drugs_type_Other illicit drug` = "drugs_type_other2",
      `drugs_type_Prescription drugs, not as prescribed (Rx cough syrup, Oxycontin, Xanax, etc.)` = "drugs_type_rx",
      `drugs_type_Something else not listed here` = "drugs_type_other",
      `drugs_type_Synthetic marijuana (K2, Spice, etc.)` = "drugs_type_spice"
      )
  new_labels <- c(
    drugs_type_alcohol = "Alcohol,",
    drugs_type_cocaine = "Cocaine or crack,",
    drugs_type_hallucinogens = "Hallucinogens/psychedelics,",
    drugs_type_none = "I did not use any drugs yesterday,",
    drugs_type_marijuana = "Marjuana,",
    drugs_type_meth = "Meth,",
    drugs_type_other = "Something else not listed here,",
    drugs_type_other2 = "Something else not listed here,",
    drugs_type_rx = "Prescription drugs, not as prescribed (Rx cough syrup, Oxycontin, Xanax, etc.),",
    drugs_type_spice = "Synthetic marijuana (K2, Spice, etc.)"
  )
  return_data <- prebind_data(filtered_data, "drugs_type", new_names, new_labels) %>%
    mutate(drugs_type_other = ifelse(drugs_type_other2 == 1,
                                     1,drugs_type_other)) %>%
    select(-drugs_type_other2) %>%
    rename_all(funs(paste0("daily_",.)))
    

  return(return_data)
}

sex_partners_daily <- function(filtered_dailylog){
  return_dailylog <- filtered_dailylog %>%
    select(starts_with("R", ignore.case = FALSE)) %>%
    rename_at(vars(ends_with("sex_a_id")),funs(paste0("sex_id_p",substr(.,2,2)))) %>%
    mutate_at(vars(ends_with("sex_b_parttype")),funs(as.character)) %>%
    mutate_at(vars(ends_with("sex_b_parttype")),funs(as_factor)) %>%
    rename_at(vars(ends_with("sex_b_parttype")),funs(paste0("sex_relation_p",substr(.,2,2)))) %>%
    mutate_at(vars(ends_with("b2_partdur")),funs(as.character)) %>%
    mutate_at(vars(ends_with("b2_partdur")),funs(as_factor)) %>%
    rename_at(vars(ends_with("b2_partdur")),funs(paste0("sex_duration_p",substr(.,2,2)))) %>%
    mutate_at(vars(ends_with("c_identity")),funs(as.character)) %>%
    mutate_at(vars(ends_with("c_identity")),funs(as_factor)) %>%
    rename_at(vars(ends_with("c_identity")),funs(paste0("sex_identity_p",substr(.,2,2)))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(
      ifelse(. == "Yes – vaginal without a condom <u>only</u>",
             "Yes, vaginal without a condom only",.))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(
      ifelse(. == "Yes – <u>both</u> anal and vaginal sex without a condom",
             "Yes, both anal and vaginal sex without a condom",.))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(
      ifelse(. == "Yes, vaginal without a condom &lt;u&gt;only&lt;/u&gt;",
             "Yes, vaginal without a condom only",.))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(
      ifelse(. == "Yes, anal without a condom &lt;u&gt;only&lt;/u&gt;",
             "Yes, anal without a condom only",.))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(
      ifelse(. == "Yes, &lt;u&gt;both&lt;/u&gt; anal and vaginal sex without a condom",
             "Yes, both anal and vaginal sex without a condom",.))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(
      ifelse(. == "Yes – anal without a condom <u>only</u>",
             "Yes, anal without a condom only",.))) %>%
    mutate_at(vars(ends_with("d_condom")),funs(as.character)) %>%
    mutate_at(vars(ends_with("d_condom")),funs(as_factor)) %>%
    rename_at(vars(ends_with("d_condom")),funs(paste0("sex_condom_p",substr(.,2,2)))) %>%
    mutate_at(vars(ends_with("e_substance")),funs(as.character)) %>%
    mutate_at(vars(ends_with("e_substance")),funs(as_factor)) %>%
    rename_at(vars(ends_with("e_substance")),funs(paste0("sex_drugs_p",substr(.,2,2))))
  
  return(return_dailylog)
}
