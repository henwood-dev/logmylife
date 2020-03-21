# Libraries
library(sjlabelled)
library(sjmisc)
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
source("LML_Baseline_Helpers.R", encoding = "UTF-8")


not_all_na <- function(x) any(!is.na(x))

main_filepath <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Person Level/_Raw Data/TSV"
main_export_filepath <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Person Level/Progress Reports"

enrollment_dirname <- main_filepath

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

v2q_redcap_filepath <- "exit scales_redcap.csv"

redcap_to_v1 <- function(main_filepath, v2q_redcap_filepath) {
  redcap_data <- read_csv(paste(main_filepath,v2q_redcap_filepath, sep = "/")) %>%
    filter(`Event Name` == "Exit Interview") %>%
    filter(`Complete?` == "Complete") %>%
    rename(selectoff = 8) %>%
    filter(is.na(selectoff)) %>%
    mutate(pid = as.character(`Participant ID`)) %>%
    select(pid, 9:26) %>%
    select(v2q_pid = pid,
           ei_phonetype = 2,
           ei_phonepref = 3,
           ei_typweek = 5,
           ei_typweek_2_text = 6,
           ei_genexp = 7,
           ei_interfere = 9,
           ei_stress = 10,
           ei_behalter = 12,
           ei_honestly = 13,
           ei_doitagain = 14,
           ei_judged = 15,
           ei_follow_up_yn = 19) %>%
    filter(!is.na(ei_follow_up_yn)) 
  
  return(redcap_data)
}

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

demographics <- function(main_filepath, v1_filepath, v2_filepath, v30_filepath, v31_filepath,
                         v4_filepath, v5_filepath, v6_filepath, v7_filepath) {
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
  
  v2q_data <- followup_fix_errors(main_filepath,
                                  v1_v2q_filepath = v1_v2q_filepath,
                                  v2_v2q_filepath = v2_v2q_filepath,
                                  v3_v2q_filepath = v3_v2q_filepath,
                                  v4_v2q_filepath = v4_v2q_filepath,
                                  v5_v2q_filepath = v5_v2q_filepath,
                                  v6_v2q_filepath = v6_v2q_filepath,
                                  v7_v2q_filepath = v7_v2q_filepath)
  
  new_baseline <- clean_baseline(baseline_data)
  tidy_baseline <- tidy_name_adapter(main_filepath,new_baseline,"bl")
  new_followup <- clean_followup(v2q_data)
  tidy_followup <- tidy_name_adapter(main_filepath,new_followup,"v2q")
  new_enroll <- clean_enrollment(enrollment_dirname)
}

process_personlevel_data <- function(data_dirname, new_enroll, tidy_baseline, tidy_followup, ema_gps, main_export_filepath){
  
  new_ethnicity <- read_csv(paste(main_export_filepath,"race-eth clean vars.csv",sep = "/"))
  new_ethnicity$race_clean <- set_label(new_ethnicity$race_clean, "Race, cleaned using all response options")
  new_ethnicity$hispanic_clean <- set_label(new_ethnicity$hispanic_clean, "Hispanic ethnicity, cleaned using all response options")
  
  copy_baseline <- copy(tidy_baseline) %>%
    mutate(pid = as.numeric(pid)) %>%
    left_join(new_ethnicity, by = "pid") %>%
    mutate(race_clean = factor_keep_rename(race_clean),
           hispanic_clean = factor_keep_rename(hispanic_clean, level_vector = c("No or don't know",
                                                                                "Yes"))) 
  
  copy_followup <- copy(tidy_followup) %>%
    mutate(pid = as.numeric(pid))
  
  copy_enroll <- copy(new_enroll) 
  
    
  ema_exists <- ema_gps %>%
    mutate(comply = ema_prompt_status == "Completed") %>%
    group_by(pid) %>%
    summarize_at(vars(comply), funs(mean)) %>%
    filter(comply > 0) %>%
    mutate(pid = as.numeric(pid)) %>%
    select(-comply) %>%
    mutate(ema_exists = 1)
  
  daily_exists <- daily %>%
    mutate(comply = daily_prompt_status == "Completed") %>%
    group_by(pid) %>%
    summarize_at(vars(comply), funs(mean)) %>%
    filter(comply > 0) %>%
    mutate(pid = as.numeric(pid)) %>%
    select(-comply) %>%
    mutate(daily_exists = 1)
  

  merged_data <- copy_enroll %>%
    inner_join(copy_baseline, by = "pid") %>%
    left_join(copy_followup, by = "pid") %>%
    left_join(ema_exists, by = "pid") %>%
    left_join(daily_exists, by = "pid")
  
  pluhcm_levels <- levels(merged_data$placeslived_uh_current_main)
  new_pluhcm_levels <- append(pluhcm_levels,"Place of business")
  
  hes_recode_vars <- c("hes_pq_2_privacy","hes_pq_3_4_5_6_unitprobs","hes_nq_2_crime_problem",
                       "hes_nq_4_healthcare_difficulty","hes_nq_7_family_friends_far","hes_nq_8_street_lighting_poor",
                       "hes_nq_10_noisy","hes_nq_12_traffic","hes_nq_14_outdoor_recreation","hes_nsc_2_unwelcome_ethnicity",
                       "hes_nsc_4_policing_disparity","hes_nsc_5_hassle_walking","hes_nsc_6_careful_talk_to",
                       "hes_s_1_freq_attack_nearby","hes_s_2_freq_drug_sales","hes_s_3_freq_drug_use",
                       "hes_s_4_freq_robbery_nearby","hes_s_5_freq_theft_from_units","hes_s_6_freq_property_damage",
                       "hes_s_7_freq_loitering","hes_s_8_freq_new_graffiti","hes_s_9_freq_weapons_used",
                       "hes_n_5_no_close_neighbors","hes_n_8_neighbors_argue","hes_n_12_neighbor_complains",
                       "hes_ll_6_only_cares_about_rent","hes_ll_7_doesnt_respond","hes_ll_11_contact_sp_issues_only",
                       "hes_ll_12_contact_ll_issues_only","hes_ll_15_complains_about_me","hes_rm_10_dont_get_along",
                       "hes_rm_11_argue_a_lot","hes_rm_14_takes_advantage","hes_rs_3_compare_prev_livsit",
                       "hes_rs_4_compare_prev_nhood")
  
  demo_changes <- merged_data %>%
    filter(!is.na(age_demo) & age_demo < 28) %>%
    mutate(placeslived_uh_current_main = case_when(
      housing_status == 1 ~ NA_character_,
      pid == 2001 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2002 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2003 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2004 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2005 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2006 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2016 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2017 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      
      pid == 2008 ~ "Family home",
      pid == 2012 ~ "Street, park, beach, or outside",
      pid == 2015 ~ "Street, park, beach, or outside",
      pid == 2024 ~ "Street, park, beach, or outside",
      pid == 2047 ~ "Street, park, beach, or outside",
      pid == 2059 ~ "Street, park, beach, or outside",
      pid == 2023 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2066 ~ "Place of business",
      pid == 2071 ~ "Place of business",
      pid == 2092 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2103 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2104 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2107 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2111 ~ "Youth-only emergency/temporary shelter (less than 30 days)",
      pid == 2018 ~ "Street, park, beach, or outside",
      pid == 2080 ~ NA_character_,
      pid == 2115 ~ NA_character_,
      pid == 2126 ~ NA_character_,
      pid == 2127 ~ NA_character_,
      pid == 2131 ~ NA_character_,
      pid == 2119 ~ NA_character_,
      TRUE ~ as.character(placeslived_uh_current_main)
    )) %>%
    mutate(placeslived_uh_current_main = factor_keep_rename(placeslived_uh_current_main, level_vector = new_pluhcm_levels)) %>%
    select(-living_situation_unhoused) %>%
    mutate(placelive_current_collapse = case_when(
      as.numeric(placeslived_uh_current_main) == 1 ~ "Explicit homelessness",
      as.numeric(placeslived_uh_current_main) == 4 ~ "Explicit homelessness",
      as.numeric(placeslived_uh_current_main) == 11 ~ "Explicit homelessness",
      as.numeric(placeslived_uh_current_main) == 15 ~ "Explicit homelessness",
      as.numeric(placeslived_uh_current_main) == 2 ~ "Shelter",
      as.numeric(placeslived_uh_current_main) == 3 ~ "Shelter",
      as.numeric(placeslived_uh_current_main) == 13 ~ "Shelter",
      as.numeric(placeslived_uh_current_main) == 14 ~ "Shelter",
      as.numeric(placeslived_uh_current_main) == 5 ~ "Someone's home or hotel/motel",
      as.numeric(placeslived_uh_current_main) == 6 ~ "Someone's home or hotel/motel",
      as.numeric(placeslived_uh_current_main) == 7 ~ "Someone's home or hotel/motel",
      as.numeric(placeslived_uh_current_main) == 8 ~ "Someone's home or hotel/motel",
      as.numeric(placeslived_uh_current_main) == 10 ~ "Someone's home or hotel/motel",
      TRUE ~ NA_character_)) %>%
    bind_cols(to_dummy(.,placelive_current_collapse, suffix = "label")) %>%
    rename(homeless_cat_homeorhotel = `placelive_current_collapse_Someone's home or hotel/motel`,
           homeless_cat_shelter = placelive_current_collapse_Shelter,
           homeless_cat_explicit = `placelive_current_collapse_Explicit homelessness`) %>%
    mutate(sexual_ori_id_collapse = case_when(
      as.character(sexual_orientation_id) == "Heterosexual or straight" ~ "Heterosexual or straight",
      as.character(sexual_orientation_id) == "Gay or lesbian" ~ "Gay or lesbian",
      as.character(sexual_orientation_id) == "Bisexual" ~ "Bisexual or pansexual",
      as.character(sexual_orientation_id) == "Another sexual orientation (please state):" ~ "Another sexual orientation",
      as.character(sexual_orientation_id) == "Asexual" ~ "Another sexual orientation",
      as.character(sexual_orientation_id) == "Questioning or unsure" ~ "Another sexual orientation",
      TRUE ~ NA_character_
    )) %>%
    mutate(ed_school_now_hsged = case_when(
      as.character(education_school_now_type) == "GED program" ~ 1,
      as.character(education_school_now_type) == "High School" ~ 1,
      as.character(education_school_now_type) != "High School" & as.character(education_school_now_type) != "GED program" & as.character(education_school_now) == "Yes" ~ 0,
      TRUE ~ NA_real_
    )) %>%
    mutate(ed_school_now_posths = case_when(
      as.character(education_school_now_type) == "College" ~ 1,
      as.character(education_school_now_type) == "Vocational/trade school" ~ 1,
      as.character(education_school_now_type) != "Vocational/trade school" & as.character(education_school_now_type) != "College" & as.character(education_school_now) == "Yes" ~ 0,
      TRUE ~ NA_real_
    )) %>%
    mutate(work_job_ftorptortemp = case_when(
      as.integer(income_30day_types_job_ft) == 1 ~ 1,
      as.integer(income_30day_types_job_pt_fthrs) == 1 ~ 1,
      as.integer(income_30day_types_job_pt_pthrs) == 1 ~ 1,
      as.integer(income_30day_types_temp_work) == 1 ~ 1,
      as.integer(income_30day_types_job_ft) == 0 & as.integer(income_30day_types_job_pt_fthrs) == 0 
            & as.integer(income_30day_types_job_pt_pthrs) == 0 
              & as.integer(income_30day_types_temp_work) == 0 ~ 0,
      TRUE ~ NA_real_
    )) %>%
    mutate(workschool_any_now = case_when(
      as.integer(work_job_ftorptortemp) == 1 ~ 1,
      as.character(education_school_now) == "Yes" ~ 1,
      as.integer(work_job_ftorptortemp) == 0 & as.character(education_school_now) == "No" ~ 0,
      TRUE ~ NA_real_
    )) %>%
    mutate_at(vars(one_of(hes_recode_vars)),funs(fct_rev(.))) %>%
    mutate(hes_rs_1_satisfy_housing = case_when(
      as.integer(hes_rs_1_satisfy_housing) == 1 ~ 1.0,
      as.integer(hes_rs_1_satisfy_housing) == 2 ~ 1.5,
      as.integer(hes_rs_1_satisfy_housing) == 3 ~ 2.0,
      as.integer(hes_rs_1_satisfy_housing) == 4 ~ 2.5,
      as.integer(hes_rs_1_satisfy_housing) == 5 ~ 3.0,
      TRUE ~ NA_real_
    )) %>%
    mutate(hes_rs_2_satisfy_neighborhood = case_when(
      as.integer(hes_rs_2_satisfy_neighborhood) == 1 ~ 1.0,
      as.integer(hes_rs_2_satisfy_neighborhood) == 2 ~ 1.5,
      as.integer(hes_rs_2_satisfy_neighborhood) == 3 ~ 2.0,
      as.integer(hes_rs_2_satisfy_neighborhood) == 4 ~ 2.5,
      as.integer(hes_rs_2_satisfy_neighborhood) == 5 ~ 3.0,
      TRUE ~ NA_real_
    )) %>%
    mutate(hes_sub_rs_satisfy = as.integer(hes_rs_1_satisfy_housing) + as.integer(hes_rs_2_satisfy_neighborhood),
           hes_sub_rs_compare = as.integer(hes_rs_3_compare_prev_livsit) + as.integer(hes_rs_4_compare_prev_nhood),
           hes_sub_rs_housing = as.integer(hes_rs_1_satisfy_housing) + as.integer(hes_rs_3_compare_prev_livsit),
           hes_sub_rs_nbrhood = as.integer(hes_rs_2_satisfy_neighborhood) + as.integer(hes_rs_4_compare_prev_nhood),
           hes_score_rs_4item = as.integer(hes_rs_1_satisfy_housing) + as.integer(hes_rs_3_compare_prev_livsit) + 
             as.integer(hes_rs_2_satisfy_neighborhood) + as.integer(hes_rs_4_compare_prev_nhood)) %>%
    mutate(hes_score_pq = rowSums(map_dfr(select(.,starts_with("hes_pq")),as.integer)),
         #  hes_score_nsc = rowSums(map_dfr(select(demo_changes,starts_with("hes_nsc"),-hes_nsc_13_samerace_howmany),as.integer)),
           hes_score_nq = rowSums(map_dfr(select(.,starts_with("hes_nq")),as.integer)),
           hes_score_s = rowSums(map_dfr(select(.,starts_with("hes_s_")),as.integer)),
           hes_score_n = rowSums(map_dfr(select(.,starts_with("hes_n_")),as.integer)),
           hes_score_ll = rowSums(map_dfr(select(.,starts_with("hes_ll_")),as.integer))) %>%
           #hes_score_rm = rowSums(map_dfr(select(demo_changes,starts_with("hes_rm"),-starts_with("hes_rm_6"),
            #                                     -starts_with("hes_rm_1"),-starts_with("hes_rm_2"),
             #                                    -starts_with("hes_rm_3"),-starts_with("hes_rm_4"),
              #                                   -starts_with("hes_rm_5")),as.integer))) %>%
    mutate(lifetimehmls_di = ifelse(as.integer(homeless_duration_lifetime) < 6,0,
                                    ifelse(as.integer(homeless_duration_lifetime) > 5,1,NA_integer_))) %>%
    mutate(housed_duration_collapse = case_when(
      as.integer(housed_duration_currentprogram) < 3 ~ "3 months or less",
      as.integer(housed_duration_currentprogram) == 3 ~ "4-12 months",
      as.integer(housed_duration_currentprogram) == 4 ~ "4-12 months",
      as.integer(housed_duration_currentprogram) == 5 ~ "4-12 months",
      as.integer(housed_duration_currentprogram) == 6 ~ "1-2 years",
      as.integer(housed_duration_currentprogram) > 6 ~ "3 or more years",
      TRUE ~ NA_character_
    ), housed_duration_collapse = factor_keep_rename(housed_duration_collapse, level_vector = c("3 months or less",
                                                                                                "4-12 months",
                                                                                                "1-2 years",
                                                                                                "3 or more years"))) %>%
    mutate(housed_model = case_when(
      as.integer(living_model) == 2 ~ "supportive housing",
      as.integer(living_model) == 3 ~ "TLP",
      TRUE ~ NA_character_
    ), housed_model = factor_keep_rename(housed_model, level_vector = c("supportive housing","TLP")),
    housed_psh = as.integer(as.integer(housed_model) == 1),
    housed_tlp = as.integer(as.integer(housed_model) == 2)) %>%
    copy_labels(select(tidy_baseline,-pid)) %>%
    copy_labels(select(tidy_followup,-pid)) %>%
    rename(lifetimehmls = homeless_duration_lifetime) %>%
    select(-sni_bl_suever_nitrous_a1,-sni_bl_suever_nitrous_a2,-sni_bl_suever_nitrous_a3,
           -sni_bl_suever_nitrous_a4,-sni_bl_suever_nitrous_a5,-sni_bl_suever_nitrous_a0) %>%
    select(-race_demo,-race_min_demo,-race_single,-starts_with("race_birace_"),-hispanic_latinx)

  write_dta(demo_changes,paste0(main_export_filepath,"/lml_personlevel.dta"))
  return(demo_changes)
}

other_text_adapter <- function(tidy_data, bl_or_v2q){
  var_split_stub_rename <- function(input_data,varname_stub, return_all_data = FALSE){
    return_col <- input_data %>%
      select(ends_with(varname_stub)) %>%
      cSplit_e(varname_stub,type = "character", fill = 0, sep = ",") %>%
      select(-ends_with(varname_stub)) %>%
      rename_all(funs(str_replace(.,paste0(varname_stub,"_"),"")))
    return_data <- input_data %>%
      select(-ends_with(varname_stub)) %>%
      bind_cols(return_col)
    if(return_all_data){
      return(return_data)
    } else{
      return(return_col)
    }
  }
  
  triple_mutate <- function(tidy_data,pidval,var1,var2,var3,val1 = 1, val2 = NA, val3 = 0){
    if(length(pidval)==1){
      return_data <- tidy_data %>%
        mutate_at(var1,funs(ifelse(pid == pidval,val1,.))) %>%
        mutate_at(var2,funs(ifelse(pid == pidval,val2,.))) %>%
        mutate_at(var3,funs(ifelse(pid == pidval,val3,.))) 
    } else {
      return_data <- tidy_data
      for(i in pidval){
        return_data <- return_data %>%
          mutate_at(var1,funs(ifelse(pid == i,val1,.))) %>%
          mutate_at(var2,funs(ifelse(pid == i,val2,.))) %>%
          mutate_at(var3,funs(ifelse(pid == i,val3,.))) 
      }
    }
    
    return(return_data)
  }
  
  mutate_and_refactor <- function(tidy_data,pidval,var1,var2,val1,val2 = NA,var_levels){
    if(length(pidval)==1){
      return_data <- tidy_data %>%
        mutate_at(var1,funs(ifelse(pid == pidval,as.character(val1),as.character(.)))) %>%
        mutate_at(var2,funs(ifelse(pid == pidval,val2,.))) %>%
        mutate_at(var1, funs(factor_keep_rename(.,level_vector = var_levels)))
    } else {
      return_data <- tidy_data
      for(i in pidval){
        return_data <- return_data %>%
          mutate_at(var1,funs(ifelse(pid == i,as.character(val1),as.character(.)))) %>%
          mutate_at(var2,funs(ifelse(pid == i,val2,.))) %>%
          mutate_at(var1, funs(factor_keep_rename(.,level_vector = var_levels)))
      }
    }
    return(return_data)
  }
  
  
  if(bl_or_v2q == "bl"){
    levels_placeslived_h_main_prehoused <- append(levels(tidy_data$placeslived_h_main_prehoused),"Sober living facility")
    govtbenefit_vector <- c(2056,2061,2064,2066,2074,2081,2091,1139,2095,2112,
                            2113,2041,1131,2049,2050,2031,1129,1083,1089,1099,
                            1107,1077,1125,1124,1061,2016,1048,1031,1054,1051,
                            1050,2001,1002,1004)
    healthcarenone_vector <- c(2071,2098,2019,2032,1066,1072,1105,1111,1117,1060,1016,1025)
    
    #levels_rxmisuse_last_use <- levels(tidy_data$rxmisuse_last_use)
    #levels_hiv_neg_lasttest_where
    
    return_data <- tidy_data %>%
      triple_mutate(2014,"prep_nottaking_barrier_lowrisk","prep_nottaking_barrier_other_txt","prep_nottaking_barrier_other") %>%
      triple_mutate(1106,"gender_id_gqnc_selected","gender_id_other_text","gender_id_other_selected") %>%
      triple_mutate(1112,"gender_id_gqnc_selected","gender_id_other_text","gender_id_other_selected") %>%
      triple_mutate(2062,"homeless_causes_famhmls_unstable","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(2072,"homeless_causes_famhmls_unstable","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(2083,"homeless_causes_kickout_famhome","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(2130,"homeless_causes_movedhere_nohome","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(1085,"homeless_causes_couldnt_pay_rent","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(2125,"homeless_causes_couldnt_pay_rent","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(2006,"homeless_causes_ranaway_famhome","homeless_causes_other_text","homeless_causes_other_selected") %>%
      triple_mutate(2084,"placeslived_sincehmls_street","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(1141,"placeslived_sincehmls_vehicmetro","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(1130,"placeslived_sincehmls_squat","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(2059,"placeslived_sincehmls_street","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(1067,"placeslived_sincehmls_street","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(1088,"placeslived_sincehmls_street","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(1022,"placeslived_sincehmls_vehicmetro","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      triple_mutate(2084,"placeslived_sincehmls_vehicmetro","placeslived_sincehmls_other_text","placeslived_sincehmls_other") %>%
      mutate_and_refactor(1140,"placeslived_h_main_prehoused","placeslived_h_main_prehousedtext","Abandoned building or squat",NA,levels_placeslived_h_main_prehoused) %>%
      mutate_and_refactor(1126,"placeslived_h_main_prehoused","placeslived_h_main_prehousedtext","Sober living facility",NA,levels_placeslived_h_main_prehoused) %>%
      mutate_and_refactor(1008,"placeslived_h_main_prehoused","placeslived_h_main_prehousedtext","Family home",NA,levels_placeslived_h_main_prehoused) %>%
      mutate_and_refactor(1048,"placeslived_h_main_prehoused","placeslived_h_main_prehousedtext","Street, park, beach, or outside",NA,levels_placeslived_h_main_prehoused) %>%
      triple_mutate(2084,"placeslived_uh_3mo_street","placeslived_uh_3mo_other_text","placeslived_uh_3mo_other") %>%
      triple_mutate(2039,"placeslived_uh_3mo_street","placeslived_uh_3mo_other_text","placeslived_uh_3mo_other") %>%
      triple_mutate(2059,"placeslived_uh_3mo_street","placeslived_uh_3mo_other_text","placeslived_uh_3mo_other") %>%
      triple_mutate(2098,"dx_chronic_none_selected","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(2124,"dx_chronic_none_selected","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(2026,"dx_chronic_kidney","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(1129,"dx_chronic_cirrhosis","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(1101,"dx_chronic_cardiovascular","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(1016,"dx_chronic_respiratory","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(1002,"dx_chronic_none_selected","dx_chronic_other_text","dx_chronic_other_selected") %>%
      triple_mutate(1140,"sti_posever_none","sti_posever_other_text","sti_posever_other_selected") %>%
      triple_mutate(2123,"sti_posever_none","sti_posever_other_text","sti_posever_other_selected") %>%
      triple_mutate(1119,"sti_posever_none","sti_posever_other_text","sti_posever_other_selected") %>%
      triple_mutate(1005,"sti_posever_none","sti_posever_other_text","sti_posever_other_selected") %>%
      triple_mutate(2080,"prep_where_learned_ad","prep_where_learned_other_text","prep_where_learned_other") %>%
      mutate(otherdrug_ever_text = ifelse(pid == 2048,NA,otherdrug_ever_text),
             otherdrug_last_use = ifelse(pid == 2048,NA,otherdrug_last_use),
             otherdrug_ever_selected = ifelse(pid == 2048,0,otherdrug_ever_selected),
             rxmisuse_ever = ifelse(pid == 2048,1,rxmisuse_ever),
             rxmisuse_last_use = ifelse(pid == 2048,"More than 3 months ago",rxmisuse_last_use)
             ) %>%
      triple_mutate(1025,"otherdrug_30day_roa_oral","otherdrug_30day_roa_other_text","otherdrug_30day_roa_other") %>%
      triple_mutate(1106,"tobacco_30day_types_smoked","tobacco_30day_types_other_text","tobacco_30day_types_other") %>%
      triple_mutate(2014,"tobacco_30day_types_smoked","tobacco_30day_types_other_text","tobacco_30day_types_other") %>%
      triple_mutate(2014,"tobacco_30day_types_vaped","tobacco_30day_types_other_text","tobacco_30day_types_other") %>%
      triple_mutate(1077,"tobacco_30day_types_smoked","tobacco_30day_types_other_text","tobacco_30day_types_other") %>%
      triple_mutate(2035,"marijuana_30day_type_plant","marijuana_30day_type_other_text","marijuana_30day_type_other") %>%
      triple_mutate(1106,"tech_access_where_borrow_friends","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(2100,"tech_access_where_borrow_friends","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(2117,"tech_access_where_work","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(2021,"tech_access_where_borrow_friends","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(1071,"tech_access_where_borrow_friends","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(1099,"tech_access_where_place_of_stay","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(1021,"tech_access_where_place_of_stay","tech_access_where_other_text","tech_access_where_other_selected") %>%
      triple_mutate(1027,"tech_access_where_family","tech_access_where_other_text","tech_access_where_other_selected") %>%
      mutate(phone_paybill_govtbenefit = ifelse(!is.na(phone_paybill_friend),0,NA)) %>%
      triple_mutate(govtbenefit_vector,"phone_paybill_govtbenefit","phone_paybill_other_text","phone_paybill_other") %>%
      mutate(phone_uses_games = ifelse(!is.na(phone_uses_textcall_family),0,NA)) %>%
      triple_mutate(2033,"phone_uses_textcall_family","phone_uses_other_text","phone_uses_other_selected") %>%
      triple_mutate(1049,"phone_uses_games","phone_uses_other_text","phone_uses_other_selected") %>%
      triple_mutate(1037,"phone_uses_games","phone_uses_other_text","phone_uses_other_selected") %>%
      triple_mutate(2004,"phone_uses_internet","phone_uses_other_text","phone_uses_other_selected") %>%
      triple_mutate(2021,"phone_benefits_intouch_family","phone_benefits_other_text","phone_benefits_other_selected") %>%
      triple_mutate(1021,"gang_affiliate_none","phone_benefits_other_text","phone_benefits_other_selected") %>%
      triple_mutate(2004,"gang_affiliate_none","gang_affiliate_other_text","gang_affiliate_other_selected") %>%
      triple_mutate(2066,"mse_activities_ever_2_jobtrain","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(1106,"mse_activities_ever_5_art_music","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(2030,"mse_activities_ever_8_counsl_grp","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(1129,"mse_activities_ever_8_counsl_grp","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(2059,"mse_activities_ever_5_art_music","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(1084,"mse_activities_ever_0_none","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(2004,"mse_activities_ever_8_counsl_grp","mse_activities_ever_10_other_txt","mse_activities_ever_10_other") %>%
      triple_mutate(healthcarenone_vector,"healthcare_access_needs_none","healthcare_access_needs_othertxt","healthcare_access_needs_other") %>%
      mutate(healthcare_access_needs_mental = ifelse(!is.na(healthcare_access_needs_primary),0,NA)) %>%
      triple_mutate(2074,"healthcare_access_needs_primary","healthcare_access_needs_othertxt","healthcare_access_needs_other") %>%
      triple_mutate(2077,"healthcare_access_needs_sexrepro","healthcare_access_needs_othertxt","healthcare_access_needs_other") %>%
      triple_mutate(2013,"healthcare_access_needs_mental","healthcare_access_needs_othertxt","healthcare_access_needs_other") %>%
      triple_mutate(2026,"healthcare_access_needs_vision","healthcare_access_needs_othertxt","healthcare_access_needs_other") %>%
      triple_mutate(1116,"healthcare_access_needs_mental","healthcare_access_needs_othertxt","healthcare_access_needs_other") %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 2117,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 2117,"(Other) Plasma center",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 2026,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 2026,"Jail or prison",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 2030,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 2030,"(Other) Plasma center",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 1130,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 1130,"Jail or prison",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 1078,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 1078,"Community clinic",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 1120,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 1120,"Jail or prison",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 1032,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 1032,"Community clinic",hiv_neg_lasttest_where)
      ) %>%
      mutate(hiv_neg_lasttest_where_txt = ifelse(pid == 2006,NA,hiv_neg_lasttest_where_txt),
             hiv_neg_lasttest_where = ifelse(pid == 2006,"(Other) Plasma center",hiv_neg_lasttest_where)
      )
  } else {
    sheltermate_vector <- c(2049,2050,2074,2083,2089,2092,2095,2108,2128,2030)
    levels_hes_uh_rm_3_roommate_rel_type <- append(levels(tidy_data$hes_uh_rm_3_roommate_rel_type),"Sheltermate")
    
    return_data <- tidy_data %>%
      mutate_and_refactor(sheltermate_vector,"hes_uh_rm_3_roommate_rel_type","hes_uh_rm_3_roommate_relothertxt",
                          "Sheltermate",NA,levels_hes_uh_rm_3_roommate_rel_type)
  }
  
  return_and_relabel <- return_data %>%
    copy_labels(tidy_data)
  
  return(return_and_relabel)
}

tidy_name_adapter <- function(main_filepath, clean_baseline_data, bl_or_v2q, main_export_filepath){
  new_data <- copy(clean_baseline_data)
  r_tidy_names <- fread(paste0(main_filepath,"/r_tidy.csv")) %>%
    filter(source == bl_or_v2q) %>%
    filter(!startsWith(varname_r,"sni")) %>%
    filter(varname_r != "" & varname_tidy != "")  %>%
    filter(varname_r != "pid" & varname_tidy != "pid") %>%
    filter(varname_r != "survey_hiv_postx_why" & varname_tidy != "survey_hiv_postx_why") %>%
   filter(varname_r != "survey_hiv_posmeds_whynot" & varname_tidy != "survey_hiv_posmeds_whynot") # %>%
   # filter(varname_r != "survey_prep_who_romantic" & varname_tidy != "survey_prep_who_romantic") %>%
    #filter(varname_r != "survey_prep_who_socialworker" & varname_tidy != "survey_prep_who_socialworker") %>%
    #filter(varname_r != "sni_version" & varname_tidy != "sni_version")
  setnames(new_data,r_tidy_names$varname_r,r_tidy_names$varname_tidy)
  if(bl_or_v2q == "v2q"){
    setnames(new_data,c("v2q_version"),c("surveyversion_v2q"))
    setnames(new_data,c("v2q_surveytype"),c("survey_type_h_uh_v2q"))
    new_data <- select(new_data,pid,surveyversion_v2q,one_of(r_tidy_names$varname_tidy),starts_with("sni_"))
  } else {
    new_data <- select(new_data,pid,one_of(r_tidy_names$varname_tidy),starts_with("sni_")) 
  }
  if(bl_or_v2q == "bl"){
    new_data <- as.data.table(new_data)[pid != "1115"]
    
    new_data <- new_data %>%
      mutate(age_18to25 = ifelse(!is.na(age_demo),ifelse(age_demo < 26,1,0),NA_integer_)) %>%
      mutate(race_eth_combine = case_when((race_single == "Black or African-American" | race_single == "Black") & hispanic_latinx != "Yes" ~ "Black",
                                          race_single == "White" & hispanic_latinx != "Yes" ~ "White",
                                          race_single == "Hisp/Lat and no other race" | (race_single == "I don't know" & hispanic_latinx == "Yes") ~ "Hisp/Lat ONLY",
                                          race_single ==  "Bi/Multi-racial or Ethnic" | hispanic_latinx == "Yes" & (race_single != "Hisp/Lat and no other race" & race_single != "I don't know") ~ "Bi/Multi-racial or Ethnic",
                                          race_single == "American Indian or Alaska Native" | race_single == "Asian" | race_single == "Native Hawaiian or other Pacific Islander" | race_single == "South Asian" | (race_single == "I don't know" & hispanic_latinx != "Yes") | race_single == "Other (please specify)" & hispanic_latinx != "Yes" ~ "Another race or ethnicity",
                                          race_single == "I don't know" & hispanic_latinx == "Unknown" ~ NA_character_,
                                          TRUE ~ NA_character_)) %>%
      mutate(gender_id_transother_selected = case_when(gender_id_transmale_selected == 1 | gender_id_transfemale_selected == 1 | gender_id_gqnc_selected == 1 | gender_id_other_selected == 1 ~ 1,
                                               gender_id_transmale_selected == 0 & gender_id_transfemale_selected == 0 & gender_id_gqnc_selected == 0 & gender_id_other_selected == 0 ~ 0,
                                               TRUE ~ NA_real_)) %>%
      mutate(education_highest_collapse = case_when(education_highest_grade_complete == "9th to 12th grade (no degree)"  | education_highest_grade_complete == "No formal education" | education_highest_grade_complete == "Kindergarten to 5th grade" ~ "Less than high school",
                                                    education_highest_grade_complete == "GED" | education_highest_grade_complete == "High school diploma" ~ "High school or GED",
                                                    education_highest_grade_complete == "Some vocational/trade school (no degree)" | education_highest_grade_complete == "Associates (AA) degree" |
                                                      education_highest_grade_complete == "Some graduate school (no degree)" | education_highest_grade_complete == "Bachelor's (BA/BS) degree" | 
                                                      education_highest_grade_complete == "Vocational/trade school degree" ~ "Post high school education",
                                                    TRUE ~ NA_character_),
             education_highest_collapse = factor_keep_rename(education_highest_collapse, level_vector = c("Less than high school","High school or GED","Post high school education"))) %>%
      other_text_adapter(bl_or_v2q) %>%
      mutate_at(vars(prep_currently_taking, prep_ever_rx, prep_alter_taking,
                     hcv_positive_current_tx, hiv_neg_lasttest_post_counsel,
                     hiv_neg_lasttest_got_result, hiv_pos_meds_now, hiv_pos_tx_current,
                     hiv_pos_posttest_counsel, hiv_pos_ever, sex_3mo_forced_attempt,
                     sex_3mo_forced_sex, sex_3mo_exchange_sex_forced, sex_3mo_exchange_sex,
                     sex_3mo_no_hiv_talk_before, justice_involve_juvenile_ageout),funs(
                       factor_keep_rename_yn(.))) %>%
      mutate_at(vars(sex_3mo_planb_use, sex_3mo_fam_app_use, mse_perceived_access_treatment,
                     mse_perceived_access_shelters, mse_perceived_access_dropins,
                     mse_know_housing_options, mse_provider_talk_systemssupport,
                     mse_provider_positive_relship),funs(factor_keep_rename(., level_vector = c(
                       "No","Yes","Not sure"
                     )))) %>%
      mutate_at(vars(race_min_demo, demo_sex_gender_min, demo_sex_min, demo_gendercis,hispanic_latinx),
                funs(factor_keep_rename(., level_vector = c(
                       "No","Yes","Unknown"
                     )))) %>%
      mutate_at(vars(race_demo, race_eth_combine, hiv_neg_lasttest_where,
                     sex_3mo_partner_biosex, sexual_orientation_id, sex_at_birth,
                     race_single,hiv_pos_tested_where),funs(factor_keep_rename(.))) %>%
      mutate_at(vars(children_current_livewith, children_ever_total, pregnancies_ever_unplanned),
                funs(factor_keep_rename(., level_vector = c(
                  "0","1","2","3","4 or more"
                )))) %>%
      mutate_at(vars(pregnancies_ever_total),
                funs(factor_keep_rename(., level_vector = c(
                  "0","1","2","3","4 or more times"
                )))) %>%
      mutate_at(vars(sex_3mo_intoxicated_freq,sex_3mo_condom_freq), funs(factor_keep_rename(., level_vector = c(
        "Never","Less than half the time","Half of the time","More than half the time","Every time")))) 
      
      
    
    
    
    
    
    
    qa_check_text <- new_data %>%
      mutate(pid = as.character(pid)) %>%
      select(pid,starts_with("race"), starts_with("prep_nottaking_barrier_"),starts_with("prep_nottaking_barrier_"),
             starts_with("race_birace_eth_"),starts_with("gender_id_"),
             starts_with("homeless_causes_"),starts_with("placeslived_sincehmls_"),
             starts_with("placeslived_h_main_"),starts_with("placeslived_uh_3mo_"),
             starts_with("dx_chronic_"),starts_with("sti_posever_"),
             starts_with("prep_where_learned_"),starts_with("otherdrug_"),
             starts_with("tobacco_30day_types_"),starts_with("marijuana_30day_type_"),
             starts_with("marijuana_30day_roa_"),starts_with("tech_access_where_"),
             starts_with("phone_paybill_"),starts_with("phone_uses_"),
             starts_with("phone_benefits_"),starts_with("gang_affiliate_"),
             starts_with("mse_activities_ever_10_"),starts_with("healthcare_access_needs_"),
             starts_with("sex_3mo_partner_gender_"),starts_with("sex_3mo_contraceptypes_"),
             starts_with("hiv_neg_lasttest_"),starts_with("prep_nottaking_barrier_"))
    
    write_csv(qa_check_text,paste(main_export_filepath,"baseline_qa_textdata.csv",sep = "/"))

    new_data <- new_data %>%
      select(-ends_with("_text")) %>%
      mutate(tobacco_last_use2 = as_character(tobacco_last_use),
             tobacco_last_use2 = ifelse(tobacco_ever == 0, "Never", tobacco_last_use2),
             tobacco_last_use = factor_add_new(tobacco_last_use,tobacco_last_use2,"Never")) %>%
      select(-tobacco_last_use2) %>%
      mutate_at(vars(tobacco_30day_types_chewing, tobacco_30day_types_other,
                     tobacco_30day_types_smoked, tobacco_30day_types_vaped), funs(carry_zero_forward(tobacco_last_use, ., use_var = 1, flip_sign = TRUE))) %>%
      mutate_at(vars(tobacco_freq_smoke), funs(carry_zero_forward(tobacco_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = "Not at all"))) %>%
      mutate_at(vars(starts_with("tobacco_quant_smoke_daily")), funs(carry_zero_forward(tobacco_freq_smoke, ., use_var = 3, flip_sign = TRUE, replace_var = 0))) %>%
      mutate_at(vars(starts_with("tobacco_quant_smoke_week")), funs(carry_zero_forward(tobacco_freq_smoke, ., use_var = 2, flip_sign = TRUE, replace_var = 0))) %>%
      mutate_at(vars(tobacco_freq_smokeless), funs(carry_zero_forward(tobacco_30day_types_chewing, ., use_var = 1, flip_sign = TRUE, replace_var = "Not at all"))) %>%
      mutate_at(vars(tobacco_freq_vape), funs(carry_zero_forward(tobacco_30day_types_vaped, ., use_var = 1, flip_sign = TRUE, replace_var = "Not at all"))) %>%
      mutate(marijuana_last_use2 = as_character(marijuana_last_use),
             marijuana_last_use2 = ifelse(marijuana_ever == 0, "Never", marijuana_last_use2),
             marijuana_last_use = factor_add_new(marijuana_last_use,marijuana_last_use2, 
                                                 c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      select(-marijuana_last_use2) %>%
      mutate_at(vars(marijuana_30day_freq), funs(carry_zero_forward(marijuana_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate_at(vars(starts_with("marijuana_30day_type"),starts_with("marijuana_30day_roa")),
                funs(carry_zero_forward(marijuana_30day_freq, ., use_var = 0, flip_sign = FALSE, replace_var = 0))) %>%
      mutate_at(vars(marijuana_freq_current), funs(carry_zero_forward(marijuana_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = "Use once a month or less"))) %>%
      mutate(alcohol_last_use2 = as_character(alcohol_last_use),
             alcohol_last_use2 = ifelse(alcohol_ever == 0, "Never", alcohol_last_use2),
             alcohol_last_use = factor_add_new(alcohol_last_use,alcohol_last_use2, 
                                                 c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      select(-alcohol_last_use2) %>%
      mutate_at(vars(alcohol_30day_freq), funs(carry_zero_forward(alcohol_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(cocaine_last_use = carry_factor_forward(cocaine_ever, cocaine_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(cocaine_30day_freq), funs(carry_zero_forward(cocaine_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(crack_last_use = carry_factor_forward(crack_ever, crack_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(crack_30day_freq), funs(carry_zero_forward(crack_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(hallucinogen_last_use = carry_factor_forward(hallucinogen_ever, hallucinogen_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(hallucinogen_30day_freq), funs(carry_zero_forward(hallucinogen_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(heroin_last_use = carry_factor_forward(heroin_ever, heroin_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(heroin_30day_freq), funs(carry_zero_forward(heroin_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(inhalants_last_use = carry_factor_forward(inhalants_ever, inhalants_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(inhalants_30day_freq), funs(carry_zero_forward(inhalants_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(ketamine_last_use = carry_factor_forward(ketamine_ever, ketamine_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(ketamine_30day_freq), funs(carry_zero_forward(ketamine_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(mdma_last_use = carry_factor_forward(mdma_ever, mdma_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(mdma_30day_freq), funs(carry_zero_forward(mdma_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(meth_last_use = carry_factor_forward(meth_ever, meth_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(meth_30day_freq), funs(carry_zero_forward(meth_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(nitrous_last_use = carry_factor_forward(nitrous_ever, nitrous_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(nitrous_30day_freq), funs(carry_zero_forward(nitrous_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(otherdrug_last_use = carry_factor_forward(otherdrug_ever_selected, otherdrug_last_use, parent_value = 0, new_value = "Never",
                                                      new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(otherdrug_30day_freq), funs(carry_zero_forward(otherdrug_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(pcp_last_use = carry_factor_forward(pcp_ever, pcp_last_use, parent_value = 0, new_value = "Never",
                                                  new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(pcp_30day_freq), funs(carry_zero_forward(pcp_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(rxmisuse_last_use = carry_factor_forward(rxmisuse_ever, rxmisuse_last_use, parent_value = 0, new_value = "Never",
                                                  new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(rxmisuse_30day_freq), funs(carry_zero_forward(rxmisuse_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(synthetic_mj_last_use = carry_factor_forward(synthetic_mj_ever, synthetic_mj_last_use, parent_value = 0, new_value = "Never",
                                                     new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(synthetic_mj_30day_freq), funs(carry_zero_forward(synthetic_mj_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      mutate(steroid_last_use = carry_factor_forward(steroid_ever, steroid_last_use, parent_value = 0, new_value = "Never",
                                                          new_label_pair = c("Within the past 30 days","Between 1-3 months ago","More than 3 months ago","Never"))) %>%
      mutate_at(vars(steroid_30day_freq), funs(carry_zero_forward(steroid_last_use, ., use_var = 1, flip_sign = TRUE, replace_var = 0))) %>%
      select(-qualtrics_response_id_bl)
    
    qual_data <- new_data %>%
      mutate(pid = as.character(pid)) %>%
      select_if(is.character) %>%
      filter_all(any_vars(!is.na(.))) %>%
      select(-starts_with("sni_"))
    write_dta(qual_data,paste(main_export_filepath,"baseline_qualitative.dta",sep = "/"))
       
    
    write_dta(new_data,paste(main_export_filepath,"baseline_tidy.dta",sep = "/"))
  } else {
    hold_names <- new_data
    
    new_data <- new_data %>%
      filter(pid != "1115") %>%
      copy_labels(hold_names) %>%
      other_text_adapter(bl_or_v2q) %>%
      select(-qualtrics_response_id_v2q)
      
    
    qual_data <- new_data %>%
      mutate(pid = as.character(pid)) %>%
      select_if(is.character) %>%
      filter_all(any_vars(!is.na(.))) %>%
      select(-starts_with("sni_"))
    
    write_dta(qual_data,paste(main_export_filepath,"followup_qualitative.dta",sep = "/"))
    
    qa_check_text <- new_data %>%
      mutate(pid = as.character(pid)) %>%
      select(pid,starts_with("exit_typicalweek_"), starts_with("hes_uh_rm_3_roommate_"))
    
    write_csv(qa_check_text,paste(main_export_filepath,"followup_qa_textdata.csv",sep = "/"))
    
    
    write_dta(new_data,paste(main_export_filepath,"followup_tidy.dta",sep = "/"))
  }
  
  return(new_data)
}

clean_enrollment <- function(enrollment_dirname, enrollment_filename = "enrollment.csv"){
  raw_enroll <- fread(paste(enrollment_dirname,enrollment_filename, sep = "/"))
  #raw_enroll <- fread(paste(enrollment_dirname,enrollment_filename, sep = "/"),colClasses = list(character=1:ncol(num_enroll)))
  fixed_enroll <- raw_enroll %>%
    rename(pid = PID_master,
           status = Status) %>%
    mutate(housing_status = ifelse(`H-UH` == "H","Housed",
                                   ifelse(`H-UH` == "UH","Unhoused",NA_character_)),
           dates_start_app = as.Date(appstartdate, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           dates_end_app = as.Date(appenddate, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           dates_duration_app = dates_end_app-dates_start_app,
           dates_screener = as.Date(Date_Screen, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           dates_enrollment = as.Date(Date_Enroll, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           dates_baselinesni = as.Date(`Date_BL-SNI`, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           dates_phonesetup = as.Date(Date_PhoneSetup, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           dates_followup = as.Date(Date_Visit2, format = "%m/%d/%y", origin = "1970-01-01", tz = "America/Los_Angeles"),
           enroll_sleep_hour = as.numeric(str_split_fixed(sleeptime,":",n=2)[,1]),
           enroll_wake_hour = as.numeric(str_split_fixed(waketime,":",n=2)[,1]),
           enroll_sleep_minute = as.numeric(str_split_fixed(sleeptime,":",n=2)[,2]),
           enroll_wake_minute = as.numeric(str_split_fixed(waketime,":",n=2)[,2]),
           enroll_sleep_duration = abs((enroll_wake_hour+enroll_wake_minute/60) - (enroll_sleep_hour+enroll_sleep_minute/60)),
           PhoneType = ifelse(PhoneType == "n/a" | PhoneType == "",NA_character_,PhoneType),
           phone_study_motog = ifelse(grepl("(MG3)",PhoneType),1,ifelse(!is.na(PhoneType),0,NA_integer_)),
           phone_study_motoe = ifelse(grepl("(ME4)",PhoneType),1,ifelse(!is.na(PhoneType),0,NA_integer_)),
           phone_study_personal = ifelse(grepl("(sona)",PhoneType),1,ifelse(!is.na(PhoneType),0,NA_integer_)),
           phone_app_version = ifelse(appver == "999" | appver == "n/a" | appver == "", NA_character_,appver)) %>%
    cSplit(., "status", sep = "_", type.convert = FALSE) %>%
    rename(status = status_1, status_partial = status_2, status_reason = status_3) %>%
    cSplit(., "live_program", sep = "_", type.convert = FALSE) %>%
    rename(housing_agency = live_program_1, housing_program_type = live_program_2, housing_building_name = live_program_3) %>%
    cSplit(., "live_type", sep = "_",type.convert = FALSE) %>%
    rename(living_model = live_type_1, living_situation_scattered = live_type_2, living_situation_agerange = live_type_3) %>%
    mutate(living_model = ifelse(living_model == "SH","Supportive Housing",
                                 ifelse(living_model == "TLP","Transitional Living Program",
                                        ifelse(living_model == "RR","Rapid Rehousing",NA_character_)))) %>%
    mutate(living_situation_unhoused = ifelse(housing_status == "Unhoused",living_situation_scattered,NA_character_),
           living_situation_scattered = ifelse(housing_status == "Housed", living_situation_scattered,NA_character_),
           unhoused_location = ifelse(housing_status == "Unhoused", housing_program_type,NA_character_),
           unhoused_program = ifelse(housing_status == "Unhoused", housing_building_name,NA_character_),
           housing_agency = ifelse(housing_status == "Housed", housing_agency,NA_character_),
           housing_building_name = ifelse(housing_status == "Housed", housing_building_name,NA_character_)) %>%
    mutate_at(vars(recruit_agency,recruit_site,housing_status,status,
                   housing_agency, housing_program_type, housing_building_name,
                   living_model, living_situation_scattered, living_situation_agerange,
                   phone_app_version,living_situation_unhoused,unhoused_location,unhoused_program),
              funs(factor_keep_rename(.))) %>%
    mutate_at(vars(pid),funs(numeric_keep_rename(.))) %>%
    select(pid, status, housing_status, recruit_agency, recruit_site,
           housing_agency, housing_building_name,
           living_model, living_situation_scattered,living_situation_unhoused,unhoused_program,
           starts_with("dates_"),starts_with("enroll_"),starts_with("phone_")) %>%
    filter(pid != "1115") %>%
    filter(pid != "1092") %>%
    filter(pid != "1050") %>%
    filter(pid != "1051") %>%
    filter(pid != "1054") %>%
    filter(pid != "1019") %>%
    filter(pid != "1061") %>%
    filter(pid != "1007") %>%
    filter(pid != "1038") %>%
    filter(pid != "1135") %>%
    filter(pid != "2024") %>%
    filter(pid != "2040") %>%
    filter(pid != "2008") %>%
    filter(pid != "2054")
    
  write_dta(fixed_enroll,paste(main_export_filepath,"enrollment.dta",sep = "/"))
}

clean_followup <- function(v2q_data, main_export_filepath){
  gad_vars <- paste0(rep_varnames("gad7_",1:7),"_v2")
  rm_vars <- append(rep_varnames("hes_rm_",7:15),rep_varnames("hes_uh_rm_",7:15))
  n_vars <- append(rep_varnames("hes_n_",1:9),rep_varnames("hes_uh_n_",1:9))
  nsc_vars <- append(rep_varnames("hes_nsc_",1:8),rep_varnames("hes_uh_nsc_",1:8))
  
  
  hes_d_vars <- paste0(rep_varnames("hes_d_",1:7))
  new_hes_d <- c("hes_d_1_household_size","hes_d_2_bedrooms_quant","hes_d_3_bathrooms_quant","hes_d_4_other_rooms_quant","hes_d_5_what_floor","hes_d_10_name_on_lease","hes_d_add_choice_in_livsit")
  
  filtered_followup <- v2q_data %>%
    select(-startdate,-enddate,-status,-ipaddress,-progress,-finished,-recordeddate,-starts_with("recipient"),
           -externalreference,-locationlatitude,-locationlongitude,-distributionchannel,-userlanguage) %>%
    mutate(followup_duration = as.numeric(duration_in_seconds),
           followup_date = as_date(v2q_date, format = "%m/%d/%Y", tz = "America/Los_Angeles"),
           followup_survey = as_factor(v2q_surveytype)) %>%
    rename(followup_id = responseid,
           pid = v2q_pid) %>%
    setnames(c("q165"),c("hes_uh_nes_8")) %>%
    setnames(c("casey_wsl_1"),c("casey_wls_1")) %>%
    mutate_at(vars(hes_rm_1,hes_uh_rm_1,hes_c_3,hes_uh_c_3,hes_uh_stay,
                   starts_with("smasi_"),ei_follow_up_yn), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(hes_rm_2,hes_uh_rm_2), funs(factor_keep_rename(., level_vector = c(
      "1","2","3","4","5 or more")))) %>%
    setnames(c("hes_uh_stay"),c("hes_uh_applicable")) %>%
    mutate_at(vars(starts_with("ders_sf_")), funs(gsub("\n","",.))) %>%
    mutate_at(vars(starts_with("ders_sf_")), funs(tolower(.))) %>%
    mutate_at(vars(starts_with("ders_sf_")), funs(factor_keep_rename(., level_vector = c("almost never(0-10%)",
                                                                                         "sometimes(11-35%)",
                                                                                         "about half of the time(36-65%)",
                                                                                         "most of the time(66-90%)",
                                                                                         "almost always(91-100%)")))) %>%
    mutate_at(vars(hes_rm_3,hes_uh_rm_3,ei_phonetype,ei_phonepref), funs(factor_keep_rename(.))) %>%
    mutate_at(vars(ei_genexp), funs(factor_keep_rename(., level_vector = c(
      "Very Negative","Somewhat Negative","Neutral","Somewhat Positive","Very Positive")))) %>%
    #mutate_at(vars(ei_genexp), funs(factor_keep_rename(., level_vector = c(
    #  "No, this week was not typical (explain):","Yes, this was a typical week")))) %>%
    mutate_at(vars(hes_rm_4,hes_uh_rm_4), funs(factor_keep_rename(., level_vector = c(
      "None","Little","Some","Much","Completely my choice")))) %>%
    mutate_at(vars(hes_rs_3,hes_rs_4,hes_uh_rs_3,hes_uh_rs_4), funs(factor_keep_rename(., level_vector = c(
      "Worse","Same","Better")))) %>%
    mutate_at(vars(hes_c_5,hes_uh_c_5), funs(factor_keep_rename(., level_vector = c(
      "None of the time","Some of the time","Half of the time","Most of the time","All of the time")))) %>%
    mutate_at(vars(hes_rm_5,hes_uh_rm_5), funs(factor_keep_rename(., level_vector = c(
      "Not at all","Slightly","Somewhat","Pretty well","Very well")))) %>%
    mutate_at(vars(hes_rm_6_1,hes_rm_6_2,hes_uh_rm_6_1,hes_uh_rm_6_2,
                   hes_c_2,hes_uh_c_2,hes_c_4,hes_uh_c_4), funs(numeric_keep_rename(.))) %>%
    mutate_at(vars(one_of(rm_vars)), funs(factor_keep_rename(., level_vector = c(
                "Strongly disagree","Disagree","Neither disagree or agree","Agree","Strongly agree")))) %>%
    mutate_at(vars(hes_rm_16,hes_uh_rm_16,hes_n_10,hes_uh_n_10), funs(factor_keep_rename(., level_vector = c(
      "Not at all important","Slightly important","Somewhat important","Very important","Extremely important")))) %>%
    mutate_at(vars(hes_rs_5,hes_uh_rs_5), funs(factor_keep_rename(., level_vector = c(
      "Not at all","A little","Moderately","Quite a bit","Extremely")))) %>%
    mutate_at(vars(starts_with("phq9_")), funs(factor_keep_rename(., level_vector = c("Not at all",
                                                                                      "Several days",
                                                                                      "More than half the days",
                                                                                      "Nearly every day")))) %>%
    mutate_at(vars(one_of(gad_vars)), funs(factor_keep_rename(., level_vector =c("Not at all",
                                                                                 "Several days",
                                                                                 "Over half the days",
                                                                                 "Nearly every day")))) %>%
    mutate_at(vars(gad7_8_v2), funs(factor_keep_rename(., level_vector = c("Not difficult at all",
                                                                        "Somewhat difficult",
                                                                        "Very difficult",
                                                                        "Extremely difficult")))) %>%
    mutate_at(vars(hes_rm_17,hes_uh_rm_17,hes_rs_1,hes_rs_2,
                   hes_uh_rs_1,hes_uh_rs_2), funs(factor_keep_rename(., level_vector = c(
      "Very dissatisfied","Dissatisfied","Neither dissatisfied or satisfied","Satisfied","Very satisfied")))) %>%
    mutate_at(vars(starts_with("hes_pq_"),starts_with("hes_nes_"),starts_with("hes_uh_nes"),
                   one_of(n_vars),one_of(nsc_vars),starts_with("hes_ll_"))
              , funs(factor_keep_rename(tolower(.), level_vector = c(
      "strongly disagree","disagree","neither disagree nor agree","agree","strongly agree")))) %>%
    mutate_at(vars(starts_with("ei_agree_"))
              , funs(factor_keep_rename(tolower(.), level_vector = c(
                "strongly disagree","disagree","neither agree nor disagree","agree","strongly agree")))) %>%
    mutate_at(vars(starts_with("casey_")), funs(factor_keep_rename(., level_vector = c("No","Mostly no",
                                                                                                "Somewhat",
                                                                                                "Mostly yes",
                                                                                                "Yes")))) %>%
    mutate_at(vars(hes_uh_nsc_9,hes_nsc_9), funs(factor_keep_rename(., level_vector = c(
      "No one","A few","About half","Most","Everybody")))) %>%
    mutate_at(vars(starts_with("hes_uh_pq_")), funs(factor_keep_rename(., level_vector = c(
      "Strongly disagree","Disagree","Neither disagree nor agree","Agree","Strongly agree",
      "No applicable for my living situation")))) %>%
    mutate_at(vars(starts_with("hes_uh_s_"),starts_with("hes_s_")), funs(factor_keep_rename(., level_vector = c(
      "Never","A few times a year","Once per month or less","2-3 times a month","Once a week",
      "2-3 times a week","Once a day or more")))) %>%
    mutate_at(vars(starts_with("ei_extent_")), funs(factor_keep_rename(., level_vector = c(
      "Not at all","A little bit","Somewhat","Quite a bit","Very much")))) %>%
    mutate_at(vars(ei_typweek,hes_d_5), funs(factor_keep_rename(.))) %>%
    mutate_at(vars(hes_uh_c_na, hes_d_6), funs(factor_keep_rename(., level_vector = c("No","Yes")))) %>%
    mutate_at(vars(hes_d_1,hes_d_2), funs(factor_keep_rename(., level_vector = c("1","2","3","4","5 or more")))) %>%
    mutate_at(vars(hes_d_3), funs(gsub("[^(1-9)]","",.))) %>%
    mutate_at(vars(hes_d_3,hes_rs_6_1,hes_uh_rs_6_1), funs(numeric_keep_rename(.))) %>%
    mutate_at(vars(hes_d_4), funs(factor_keep_rename(., level_vector = c("0","1","2","3","4","5 or more")))) %>%
    mutate_at(vars(hes_d_7), funs(factor_keep_rename(., level_vector = c("None at all",
                                                                         "A little",
                                                                         "A moderate amount",
                                                                         "A lot",
                                                                         "A great deal")))) %>%
    mutate_at(vars(v2q_version), funs(numeric_keep_rename(.))) %>%
    select(-duration_in_seconds,-v2q_date,-q276) %>%
    setnames(c("hes_rs_6_1","hes_uh_rs_6_1"),c("hes_rs_duration_want_to_stay","hes_uh_rs_duration_want_to_stay")) %>%
    setnames(c("smasi_sf_servacc","smasi_sf_servacc_30d","smasi_sf_servenv","smasi_sf_servenv_30d"
               ),c("smasi_sf_serviceaccess","smasi_sf_30day_serviceaccess","smasi_sf_servicefit","smasi_sf_30day_servicefit"))
  
  pq <- c("space","privacy","locks","unit_problems","building")
  s <- c("attack_nearby","drug_sales","drug_use","robbery_nearby",
         "theft_from_units","property_damage","loitering","new_graffiti","weapons_used")
  nes_nq <- c("transportation_easy","crime_problem","store_needs","healthcare_difficult",
              "fun_things_todo","police_available","family_friends_far","street_lighting_poor",
              "looks_nice","noisy","sidewalks_good","traffic","parks_nice","outdoor_recreation")
  pre_rm <- as.character(1:5)
  pre_rm <- append(pre_rm,c("6_1","6_2"))
  pre_rm <- append(pre_rm,as.character(7:17))
  rm <- c("roommates_any", "roommates_quant", "roommates_rel_types", "roommate_choice",
          "roommate_know", "roommate_howlong_years", "roommate_howlong_months", "get_along",
          "count_on", "close_relationship", "dont_get_along", "argue_a_lot", "emotional_support",
          "watchful", "takes_advantage", "invites_me", "rel_importance", "rel_satisfaction")
  n <- c("count_on_neighbor","no_close_neighbors","close_neighbor","ride_from_neighbor","neighbors_argue",
         "talk_about_problem","watchful_neighbor","neighbor_invites","neighbor_complains","neighbor_rel_imprtnt")
  ll <- c("knows_what_goes_on", "only_cares_about_rent", "doesnt_respond", "cares_about_me", 
          "encourages_involvement", "available_for_problems", "contact_sp_issues_only",
          "contact_ll_issues_only", "friendly_all_races", "complains_about_me")
  nsc <- c("safe","unwelcome_ethnicity","friendly_all_races","policing_disparity","hassle_walking",
           "careful_talk_to","easy_to_live","treated_equal","samerace_howmany")
  c <- c("rent_paid_by_sp", "pay_utilities", "pay_utilities_per_month", "freq_enough_for_expenses")
  rs <- c("satisfy_housing", "satisfy_neighborhood", "compare_prev_livsit", "compare_prev_nhood","choice_in_livsit")
  smasi <- c("joke","phase","unsafe","racial","school","self","family")
  smasi_30day <- paste0("30day_",smasi)
  casey_adl <- c("cls_daily_living_11_make_meals", "cls_daily_living_14_laundry",
                 "cls_daily_living_15_clean_home", "cls_daily_living_16_cleaningknow")
  casey_hmm <- c("cls_hmm_4_bank_account", "cls_hmm_8_payday_loan", "cls_hmm_11_rental_app",
                 "cls_hmm_12_utility_help", "cls_hmm_19_plan_expenses", "cls_hmm_24_public_transit")
  casey_wls <- c("cls_wsl_1_resume", "cls_wsl_2_jop_app", "cls_wsl_3_job_interview", "cls_wsl_9_get_work_documents",
                 "cls_wsl_10_get_system_records")
  casey_cep <- c("cls_wsl_2_jobtraining_info", "cls_wsl_5_educationforjobs","cls_wsl_7_financial_aid")
  ders <- c("awareness_1", "clarity_2", "clarity_3","awareness_4","clarity_5","awareness_6")
  
  old_names<- rep_varnames("hes_pq_",1:5)
  new_names <- rep_varnames("hes_pq_",pq)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_pq_",1:5)
  new_names <- rep_varnames("hes_uh_pq_",pq)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_s_",1:9)
  new_names <- rep_varnames("hes_s_",s)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_s_",1:9)
  new_names <- rep_varnames("hes_uh_s_",s)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_nes_",1:14)
  new_names <- rep_varnames("hes_nq_",nes_nq)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_nes_",1:14)
  new_names <- rep_varnames("hes_uh_nq_",nes_nq)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_rm_",pre_rm)
  new_names <- rep_varnames("hes_rm_",rm)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_rm_",pre_rm)
  new_names <- rep_varnames("hes_uh_rm_",rm)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_n_",1:10)
  new_names <- rep_varnames("hes_n_",n)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_n_",1:10)
  new_names <- rep_varnames("hes_uh_n_",n)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_ll_",1:10)
  new_names <- rep_varnames("hes_ll_",ll)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_nsc_",1:9)
  new_names <- rep_varnames("hes_nsc_",nsc)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_nsc_",1:9)
  new_names <- rep_varnames("hes_uh_nsc_",nsc)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_c_",2:5)
  new_names <- rep_varnames("hes_c_",c)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_c_",2:5)
  new_names <- rep_varnames("hes_uh_c_",c)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- hes_d_vars
  new_names <- new_hes_d
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("hes_rs_",1:5)
  new_names <- rep_varnames("hes_rs_",rs)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("hes_uh_rs_",1:5)
  new_names <- rep_varnames("hes_uh_rs_",rs)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("smasi_sf_",1:7)
  new_names <- rep_varnames("smasi_sf_",smasi)
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("smasi_sf_",paste0(as.character(1:7),"_30day"))
  new_names <- rep_varnames("smasi_sf_",smasi_30day)
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("phq9_",paste0(as.character(1:9),"_v2"))
  new_names <- rep_varnames("depression_followup_phq9_",paste0(as.character(1:9)))
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("gad7_",paste0(as.character(1:8),"_v2"))
  new_names <- rep_varnames("anxiety_followup_gad7_",paste0(as.character(1:8)))
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("ders_sf_v2q_",1:6)
  new_names <- rep_varnames("ders_sf_followup_",ders)
  setnames(filtered_followup,old_names,new_names)
  
  old_names <- c("hes_uh_rm_3_6_text")
  new_names <- c("hes_uh_rm_3_roommate_relothertxt")
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- rep_varnames("casey_adl_",1:4)
  new_names <- casey_adl
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("casey_hmm_",1:6)
  new_names <- casey_hmm
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("casey_wls_",1:5)
  new_names <- casey_wls
  setnames(filtered_followup,old_names,new_names)
  old_names<- rep_varnames("casey_cep_",1:3)
  new_names <- casey_cep
  setnames(filtered_followup,old_names,new_names)
  
  old_names<- c("ei_phonetype","ei_phonepref","ei_typweek","ei_typweek_2_text","ei_genexp",
                "ei_extent_ei_interfe","ei_extent_ei_stress","ei_extent_ei_battery",
                "ei_agree_ei_behalter","ei_agree_ei_honestly","ei_agree_ei_doitagai",
                "ei_agree_ei_judged","ei_follow_up_yn")
  new_names <- c("exit_phonetype","exit_phonepref_newstudy","exit_typicalweek","exit_typicalweek_text","exit_app_experience_gen",
                 "exit_extent_ema_interfere","exit_extent_ema_stress_anxiety","exit_extent_charging_difficulty","exit_agree_ema_behavior_change",
                 "exit_agree_ema_honest_open","exit_agree_participate_again","exit_agree_judged_responses","exit_followup_okay")
  setnames(filtered_followup,old_names,new_names)
  
  ## NAMING VARIABLES
  
  attr(filtered_followup$followup_duration,"label") <- attributes(v2q_data$duration_in_seconds)$label
  attr(filtered_followup$followup_date,"label") <- attributes(v2q_data$v2q_date)$label
  attr(filtered_followup$followup_survey,"label") <- attributes(v2q_data$v2q_surveytype)$label
  
  attr(filtered_followup$followup_id,"label") <- "Baseline survery respone id"
  attr(filtered_followup$pid,"label") <- "Participant ID:"
  
  incomplete_name_out <- capture.output(names(select(filtered_followup,-pid,-starts_with("followup_"),-starts_with("hes_"),
                                                     -starts_with("exit_"),-starts_with("smasi_"),-starts_with("depression_"),
                                                     -starts_with("anxiety_"),-starts_with("cls_"),-starts_with("ders_"))))
  print(incomplete_name_out)
  cat("List of variable names that are currently in progress", incomplete_name_out, file=paste(main_export_filepath,"v2q_incomplete_varnames.txt",sep = "/"), sep="\n")
  
  complete_name_out <- capture.output(names(select(filtered_followup,pid,starts_with("followup_"),starts_with("hes_"),
                                                   starts_with("exit_"),starts_with("smasi_"),starts_with("depression_"),
                                                   starts_with("anxiety_"),starts_with("cls_"),starts_with("ders_"))))
  complete_name_out
  cat("List of variable names that are complete + SNI", complete_name_out, file=paste(main_export_filepath,"v2q_complete_varnames.txt",sep = "/"), sep="\n")
  
  completedsofar <- select(filtered_followup,pid,starts_with("followup_"),starts_with("hes_"),starts_with("exit_"),
                           starts_with("smasi_"),starts_with("depression_"),starts_with("anxiety_"),
                           starts_with("cls_"),starts_with("ders_"))
  write_dta(completedsofar,paste(main_export_filepath,"followup.dta",sep = "/"))
  
  return(filtered_followup)
}

clean_baseline <- function(baseline_data, main_export_filepath){
  drug_30 <- c("alc_30","marj_30","meth_30","mdma_30","synthmj_30",
               "halluc_30","pdm_30","heroin_30","cocaine_30","crack_30",
               "inhal_30","steroid_30","nitrous_30","keta_30","pcp_30",
               "othersu_30")
  
  drugs_last <- c("tobacco_last","alc_last","marj_last","meth_last","mdma_last",
                  "synthmj_last","halluc_last","pdm_last","heroin_last",
                  "cocaine_last","crack_last","inhal_last","steroid_last",
                  "nitrous_last","keta_last","pcp_last","othersu_last")
  
  yn_vars <- c("ivdu_needleexh","ivdu_reuse","cage2","gang_fmr","streetfamily",
               "ivdu_needlepurch","ivdu_ever","cage1","gang_cur","cage3","cage4",
               "sytx_pastyear","ivdu_share","marj_card","vict_asslt","avoidpolice",
               "perp_asslt","vict_robbery","cell_charge","sutx_ever","exch_30days")
  
  numeric_varnames <- c("marj_hpd30d","alc_30_binge_m","gang_age_1","alc_30_binge_f")
  
  tobacco_daily <- rep_varnames("tobacco_dailyamt_",1:8)
  tobacco_weekly <- rep_varnames("tobacco_weeklyamt_",1:8)
  tobacco_types <- c("retailcigs","handcigs","clovecigs","pipes","cigars","hookah","text","other")
  new_tobacco_daily <- rep_varnames("tobacco_dailyamount_",tobacco_types)
  new_tobacco_weekly <- rep_varnames("tobacco_weeklyamount_",tobacco_types)
  tobacco_varnames <- c("tobacco_smokefreq","tobacco_smokelessfre","tobacco_vepfreq","tobacco_types_4_text")
  new_tobacco_varnames <- c("tobacco_frequency_smoke","tobacco_frequency_smokeless","tobacco_frequency_vape",
                            "tobacco_typeused_text")
  
  filtered_baseline <- baseline_data %>%
    mutate(dob = gsub("-","/",dob),
           dob = ifelse(pid == "1011","07/17/1998",dob), 
           date = ifelse(pid == "1011","06/16/2017",date),
           dob = ifelse(pid == "1038",NA_character_,dob), ## RECONCILE AGE
           date = ifelse(pid == "1038","09/13/2017",date)) %>%
    select(-startdate,-enddate,-status,-ipaddress,-progress,-finished,-recordeddate,-starts_with("recipient"),
           -externalreference,-locationlatitude,-locationlongitude,-distributionchannel,-userlanguage,-intname,
           -q569,-sni_5_for_app,-q524,-q524_1_text) %>%
    mutate(baseline_duration = as.numeric(duration_in_seconds),
           baseline_date = as_date(date, format = "%m/%d/%Y", tz = "America/Los_Angeles"),
           baseline_survey = as_factor(survey_type),
           #baseline_site_housed = ifelse(site_housed == "Other",site_housed_77_text,site_housed),
           #baseline_site_unhoused = ifelse(site_unhoused == "Other", site_unhoused_77_text,site_unhoused),
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
    select(-demo_housed) %>%
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
    mutate(demo_lgbq = ifelse(sexori == "Heterosexual or straight","No",ifelse(!is.na(sexori),"Yes",NA_character_))) %>%
    mutate(demo_education_cat = ifelse(educ == "9th to 12th grade (no degree)" | educ == "No formal education"
                                       | educ == "Kindergarten to 5th grade","No HS Degree",ifelse(
                                         educ == "GED" | educ == "High school diploma","HS Degree",ifelse(
                                           educ == "Some vocational/trade school (no degree)" 
                                           | educ ==  "Associates (AA) degree" | educ == "Some graduate school (no degree)"
                                           | educ == "Bachelor's (BA/BS) degree" | educ == "Vocational/trade school degree",
                                           "Post HS Education",NA_character_)))) %>%
    mutate_at(vars(starts_with("ucla_ptsd_")), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(starts_with("mh_dx_")), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(ptsd_1,ptsd_2,ptsd_3,ptsd_4,inschool), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(fc_ever,jjs_ever,arrest_ever,jail_ever), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(romrel_marr), funs(factor_keep_rename(., level_vector = c("No, not currently married","Yes, to a woman","Yes, to a man")))) %>%
    mutate_at(vars(romrel_curr), funs(factor_keep_rename(., level_vector = c("No","Yes","I don't know")))) %>%
    mutate_at(vars(romrel_ptnrs), funs(factor_keep_rename(., level_vector = c("1","2","More than 2")))) %>%
    mutate_at(vars(romrel_dur), funs(factor_keep_rename(., level_vector = c("1 day or less",
                                                                            "1-30 days",
                                                                            "1-3 months",
                                                                            "3-12 months",
                                                                            "1-3 years",
                                                                            "More than 3 years")))) %>%
    mutate_at(vars(firsthomeless), funs(numeric_keep_rename(.))) %>%
    mutate_at(vars(timehomeless_ever,timehoused,timehomeless_beforeh,
                   timehomeless_recent), funs(factor_keep_rename(., level_vector = c("Less than 1 month",
                                                                                   "1-3 months",
                                                                                   "4-6 months",
                                                                                   "7-9 months",
                                                                                   "10-11 months",
                                                                                   "1-2 years",
                                                                                   "3-4 years",
                                                                                   "5-6 years",
                                                                                   "7-8 years",
                                                                                   "9 or more years")))) %>%
    mutate_at(vars(staybeforehoused,uh_livsit_curr), funs(factor_keep_rename(.))) %>%
    mutate_at(vars(fc_placements), funs(factor_keep_rename(., level_vector = c("1-2",
                                                                               "3-4",
                                                                               "5-9",
                                                                               "10-20",
                                                                               "More than 20")))) %>%
    mutate_at(vars(mh_current,sleep_safe,sd_sf3,sd_sf4), funs(factor_keep_rename(., level_vector = c(
      "Not at all","A little bit","Somewhat","Quite a bit","Very much")))) %>%
    mutate_at(vars(sd_sf2), funs(factor_keep_rename(., level_vector = c(
      "Very much","Quite a bit","Somewhat","A little bit","Not at all")))) %>%
    mutate_at(vars(one_of(drug_30)), funs(numeric_keep_rename(.))) %>%
    mutate_at(vars(sd_sf1), funs(factor_keep_rename(., level_vector = c("Very good","Good",
                                                                        "Fair","Poor","Very poor")))) %>%
    mutate_at(vars(gad7_1,gad7_2,gad7_3,gad7_4,gad7_5,gad7_6,gad7_7), funs(factor_keep_rename(., level_vector = 
                                                                                                c("Not at all",
                                                                                                  "Several days",
                                                                                                  "Over half the days",
                                                                                                  "Nearly every day")))) %>%
    mutate_at(vars(gad7_8), funs(factor_keep_rename(., level_vector = c("Not difficult at all",
                                                                        "Somewhat difficult",
                                                                        "Very difficult",
                                                                        "Extremely difficult")))) %>%
    mutate_at(vars(starts_with("giscale_")), funs(factor_keep_rename(., level_vector = c("Strongly Agree (1)",
                                                                                         "Agree (2)",
                                                                                         "Mixed Feelings (3)",
                                                                                         "Disagree (4)",
                                                                                         "Strongly Disagree (5)")))) %>%
    mutate_at(vars(starts_with("pss_")), funs(factor_keep_rename(., level_vector = c("Never",
                                                                                     "Almost never",
                                                                                     "Sometimes",
                                                                                     "Fairly often",
                                                                                     "Very often")))) %>%
    mutate_at(vars(starts_with("stress_streets_")), funs(factor_keep_rename(., level_vector = c("None at all",
                                                                                                "A little",
                                                                                                "More than a little",
                                                                                                "A lot")))) %>%
    mutate_at(vars(starts_with("phq9_")), funs(factor_keep_rename(., level_vector = c("Not at all",
                                                                                      "Several days",
                                                                                      "More than half the days",
                                                                                      "Nearly every day")))) %>%
    mutate_at(vars(isi7_sri), funs(factor_keep_rename(., level_vector = c("Not at all interfering",
                                                                          "A little",
                                                                          "Somewhat",
                                                                          "Much",
                                                                          "Very much")))) %>%
    mutate_at(vars(tobacco_smokefreq,tobacco_smokelessfre,tobacco_vepfreq), funs(factor_keep_rename(., level_vector = c(
      "Not at all",
      "Less than daily",
      "Daily",
      "Don't know")))) %>%
    mutate_at(vars(tobacco_last), funs(factor_keep_rename(., level_vector = c(
      "Within the past 30 days",
      "Between 1-3 months ago",
      "More than 3 months ago")))) %>%
    mutate_at(vars(isi7_sri_since), funs(factor_keep_rename(., level_vector = c(
      "Less often, or to a lesser extent than before I was housed",
      "No difference",
      "More often, or to a greater extent than before I was housed")))) %>%
    mutate_at(vars(sd_since), funs(factor_keep_rename(., level_vector = c(
      "Worsened",
      "Stayed the same",
      "Improved")))) %>%
    mutate_at(vars(starts_with("ders_sf_")), funs(gsub("\n","",.))) %>%
    mutate_at(vars(starts_with("ders_sf_")), funs(tolower(.))) %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "00","0",.)
    )) %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "Once","1",.)
    )) %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "4-6","5",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "1-6","3",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "10-20","15",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "20+","20",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "1pack","1",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "None","0",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "N/A","0",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "No","0",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),funs(
      ifelse(. == "Nothing","0",.)
    ))  %>%
    mutate_at(vars(starts_with("tobacco_dailyamt_"),starts_with("tobacco_weeklyamt_")),
              funs(numeric_keep_rename(.))) %>%
    mutate_at(vars(starts_with("ders_sf_")), funs(factor_keep_rename(., level_vector = c("almost never(0-10%)",
                                                                                         "sometimes(11-35%)",
                                                                                         "about half of the time(36-65%)",
                                                                                         "most of the time(66-90%)",
                                                                                         "almost always(91-100%)")))) %>%
    mutate(scale_hfias_1 = ifelse(hfias_1 == "Yes",hfias_1_a,ifelse(!is.na(hfias_1),"No",NA_character_)),
           scale_hfias_2 = ifelse(hfias_2 == "Yes",hfias_2_a,ifelse(!is.na(hfias_2),"No",NA_character_)),
           scale_hfias_3 = ifelse(hfias_3 == "Yes",hfias_3_a,ifelse(!is.na(hfias_3),"No",NA_character_)),
           scale_hfias_4 = ifelse(hfias_4 == "Yes",hfias_4_a,ifelse(!is.na(hfias_4),"No",NA_character_)),
           scale_hfias_5 = ifelse(hfias_5 == "Yes",hfias_5_a,ifelse(!is.na(hfias_5),"No",NA_character_)),
           scale_hfias_6 = ifelse(hfias_6 == "Yes",hfias_6_a,ifelse(!is.na(hfias_6),"No",NA_character_)),
           scale_hfias_7 = ifelse(hfias_7 == "Yes",hfias_7_a,ifelse(!is.na(hfias_7),"No",NA_character_)),
           scale_hfias_8 = ifelse(hfias_8 == "Yes",hfias_8_a,ifelse(!is.na(hfias_8),"No",NA_character_)),
           scale_hfias_9 = ifelse(hfias_9 == "Yes",hfias_9_a,ifelse(!is.na(hfias_9),"No",NA_character_))) %>% 
    mutate_at(vars(starts_with("scale_hfias_")), funs(factor_keep_rename(., level_vector = c("No",
                                                                                       "Rarely (once or twice in the past 30 days)",
                                                                                       "Sometimes (three to ten times in the past 30 days)",
                                                                                       "Often (more than ten times in the past 30 days)")))) %>%
    mutate_at(vars(starts_with("casey_")), funs(factor_keep_rename(., level_vector = c("No","Mostly no",
                                                                                       "Somewhat",
                                                                                       "Mostly yes","Yes")))) %>%
    mutate_at(vars(employ_paidjobs), funs(factor_keep_rename(., level_vector = c(
      "0","1","2","3","4","5","6 or more")))) %>%
    mutate_at(vars(employ_ttlpaidhrs), funs(factor_keep_rename(., level_vector = c(
      '1-5','6-10','11-15','16-20','21-25','26-30',
      '31-35','36-40','41-45','46-50','51-55',
      '56-60','61-65','66-70','71-75','76-80',
      '81-85','86-90','91-95','96-100','101 or more')))) %>%
    mutate_at(vars(ttlincome_employ), funs(factor_keep_rename(., level_vector = c(
      'Less than $250','$251 - $500','$501 - $750','$751 - $1,000',
      '$1,001 - $1,250','$1,251 - $1,500','$1,501 - $1,750','$1,751 - $2,000',
      '$2,001 - $2,250','$2,251 - $2,500','$2,751 - $3,000','More than $3,000')))) %>%
    mutate_at(vars(ttlincome_benefits), funs(factor_keep_rename(., level_vector = c(
      'Less than $50','$50-$150','$150-$300','$300-$600','$600-$1000','$1000 or more')))) %>%
    mutate_at(vars(ttlincome_informal), funs(factor_keep_rename(., level_vector = c(
      '$0 - $250','$251 - $500','$501 - $750','$751 - $1,000',
      '$1,001 - $1,250','$1,251 - $1,500','$1,501 - $1,750','$1,751 - $2,000',
      '$2,001 - $2,250','$2,251 - $2,500','$2,751 - $3,000','More than $3,000')))) %>%
    mutate_at(vars(health), funs(factor_keep_rename(., level_vector = c(
      "Poor","Fair","Good","Very Good","Excellent")))) %>%
    mutate_at(vars(ends_with("_12mo"),suic_thought,suic_attempt), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(mhneed_perceive), funs(factor_keep_rename(., level_vector = c(
      "No","Yes","Not sure","I am currently receiving treatment")))) %>%
    mutate_at(vars(one_of(yn_vars)), funs(factor_keep_rename_yn(.))) %>%
    mutate_at(vars(one_of(numeric_varnames)), funs(numeric_keep_rename(.))) %>%
    mutate_at(vars(marj_amtwk_1), funs(factor_keep_rename(., level_vector = c(
      "None","Less than 1/8 of an ounce (3.5 g)","Between 1/8 and 1/2 of an ounce (3.5-14 g)",
      "Between 1/2 and 1 ounce (14-27 g)","More than 1 ounce (28 g)")))) %>%
    mutate_at(vars(marj_amtwk_2), funs(factor_keep_rename(., level_vector = c(
      "None","Less than 0.5 grams","Between 0.5 and 1 gram",
      "Between 1 and 2 grams","More than 2 grams")))) %>%
    mutate_at(vars(sucost_30d_alcohol), funs(ifelse(. == "$0","Less than $50",ifelse(. == "$1 - $50","Less than $50",.)))) %>%
    mutate_at(vars(sucost_30d_alcohol,sucost_30d_marj,sucost_30d_ildrug), funs(factor_keep_rename(., level_vector = c(
      "Less than $50","$51 - $250","$251 - $500","$501 - $750","$751 - $1,000",
      "$1,001 - $1,250","$1,251 - $1,500","$1,501 - $1,750","$1,751 - $2,000",
      "$2,001 - $2,250","$2,251 - $2,500","$2,501 - $2,750","$2,751 - $3,000","More than $3,000")))) %>%
    mutate_at(vars(policein_3mo_exp,policeintgen), funs(factor_keep_rename(., level_vector = c(
      "Very negative","Negative","Somewhat negative","Neither positive or negative","Somewhat positive","Positive","Very positive")))) %>%
    mutate_at(vars(policeint_3mo), funs(factor_keep_rename(., level_vector = c(
      "0 times","1-2 times","3-5 times","5-7 times","8-10 times","10 times or more")))) %>%
    mutate_at(vars(vict_ipv_perp,vict_ipv_vic), funs(factor_keep_rename(., level_vector = c(
      "No","Yes","I have not been in a relationship during this time period")))) %>%
    mutate_at(vars(cell_plan), funs(factor_keep_rename(.))) %>%
    mutate_at(vars(marj_freq_curr,marj_freqaccpt), funs(factor_keep_rename(., level_vector = c(
      "Use once a month or less","Use once a week","Use once every few days",
      "Use once a day, every day","Use multiple times a day, every day")))) %>%
    mutate_at(vars(cell_turnover), funs(factor_keep_rename(., level_vector = c(
      "1","2","3","4","5","More than 5")))) %>%
    mutate_at(vars(ivdu_30day), funs(factor_keep_rename(., level_vector = c(
      "0 times","1 time","2 to 3 times","4 to 5 times","More than 5 times")))) %>%
    mutate_at(vars(ivdu_reuse_quant), funs(factor_keep_rename(., level_vector = c(
      "2 times","3 to 5 times","6 or more times")))) %>%
    mutate_at(vars(lifesex_partnum), funs(factor_keep_rename(., level_vector = c(
      "0 (I have never had vaginal or anal sex)","1","2-5","6-10","11-20","21-30","31 or more")))) %>%
    mutate_at(vars(lifesex_beh), funs(factor_keep_rename(., level_vector = c(
      "Women only","Both men and women","Men only","I've never had sex")))) %>%
    mutate_at(vars(chlam_pos_last,gono_pos_last,syph_pos_last,herp_pos_last,
                   hpv_pos_last,hepb_pos_last,other_pos_last),funs(factor_keep_rename(., level_vector = c(
                     "Within the past 3 months","More than 3 months ago but less than 6 months ago",
                     "More than 6 months ago")))) %>%
    mutate_at(vars(sti_last),funs(factor_keep_rename(., level_vector = c(
      "Within the past 3 months","More than 3 months ago but less than 6 months ago",
      "More than 6 months ago","I have never been tested for an STI or STD")))) %>%
    mutate_at(vars(hcv_status),funs(factor_keep_rename(., level_vector = c(
      "No","Yes","I did not get my results")))) %>%
    mutate_at(vars(prep_know),funs(factor_keep_rename(., level_vector = c(
      "I have never heard of it","I have heard of it, but don't know what it is",
      "I know a little bit about it","I know a lot about it")))) %>%
    mutate_at(vars(prep_interest),funs(factor_keep_rename(., level_vector = c(
      "Not interested","A little interested","Somewhat interested",
      "Moderately interested","Very interested")))) %>%
    mutate_at(vars(hcv_lasttest),funs(factor_keep_rename(., level_vector = c(
      "Within the past 3 months","More than 3 months ago but less than 6 months ago",
      "More than 6 months ago","I have never been tested for hepatitis C")))) %>%
    mutate_at(vars(hivp_when),funs(factor_keep_rename(., level_vector = c(
      "Within the past 3 months","More than 3 months ago but less than 6 months ago",
      "More than 6 months but less than 1 year ago","More than 1 year ago")))) %>%
    mutate_at(vars(hivn_lasttest),funs(factor_keep_rename(., level_vector = c(
      "Within the past 3 months","More than 3 months ago but less than 6 months ago",
      "More than 6 months but less than 1 year ago","More than 1 year ago","I have never been tested for HIV/AIDS")))) %>%
    mutate_at(vars(inschool_type), funs(factor_keep_rename(., level_vector = c(
      "GED program","High School","Vocational/trade school","College","Other (please specify)")))) %>%
    mutate_at(vars(educ), funs(factor_keep_rename(., level_vector = c(
      "No formal education","Kindergarten to 5th grade","6th grade to 8th grade","9th to 12th grade (no degree)",
      "GED","High school diploma","Some vocational/trade school (no degree)","Vocational/trade school degree",
      "Associates (AA) degree", "Bachelor's (BA/BS) degree", "Some graduate school (no degree)")))) %>%
    mutate_at(vars(sexattr_formales), funs(factor_keep_rename(., level_vector = c(
      "Only attracted to males?","Mostly attracted to males?","Equally attracted to females and males?",
      "Mostly attracted to females?","Only attracted to females?","Not sure?")))) %>%
    mutate_at(vars(`3mosex_partnum`), funs(factor_keep_rename(., level_vector = c(
      "I have not had vaginal or anal sex with anyone in the past 3 months",
      "1","2-5","6-10","11-20","21-30","31 or more")))) %>%
    bind_cols(birace_baseline(.)) %>%
    bind_cols(gender_baseline(.)) %>%
    bind_cols(romance_partners_gender_baseline(.)) %>%
    bind_cols(romance_sex_baseline(.)) %>%
    bind_cols(homeless_why_baseline(.)) %>%
    bind_cols(house_progs_baseline(.)) %>%
    bind_cols(livsit_3mo_baseline(.)) %>%
    #bind_cols(livsit_current_baseline(.)) %>%
    bind_cols(prep_where_baseline(.)) %>%
    bind_cols(prep_social_baseline(.)) %>%
    bind_cols(prep_barrier_baseline(.)) %>%
    bind_cols(sex3mo_describe_baseline(.)) %>%
    bind_cols(sex3mo_gender_baseline(.)) %>%
    bind_cols(sex3mo_type_baseline(.)) %>%
    bind_cols(sex3mo_cntrcptv_baseline(.)) %>%
    bind_cols(sex3mo_extype_baseline(.)) %>%
    bind_cols(survey_testever(.)) %>%
    bind_cols(sti_pos_baseline(.)) %>%
    bind_cols(healthcare_access_needs(.)) %>%
    bind_cols(income_source_30day(.)) %>%
    bind_cols(scale_phealth(.)) %>%
    bind_cols(scale_coping(.)) %>%
    bind_cols(tobacco_typeused(.)) %>%
    bind_cols(mse_engage(.)) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "meth")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "mdma")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "rxmisuse")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "heroin")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "cocaine")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "crack")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "steroid")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "pcp")) %>%
    bind_cols(drugs_roa_abstracted(., drug_name = "other")) %>%
    bind_cols(drugs_everuse(.)) %>%
    bind_cols(gang_closest(.)) %>%
    bind_cols(needle_access(.)) %>%
    bind_cols(techuse_accessonly(.)) %>%
    bind_cols(techuse_cellbill(.)) %>%
    bind_cols(techuse_chargeaccess(.)) %>%
    bind_cols(techuse_celluse(.)) %>%
    bind_cols(techuse_barriers(.)) %>%
    bind_cols(police_reason(.)) %>%
    bind_cols(marijuana_type(.)) %>%
    bind_cols(hiv_posmeds_whynot(.)) %>%
    bind_cols(hiv_postx_why(.)) %>%
    bind_cols(techuse_locations(.)) %>%
    bind_cols(marijuana_access(.)) %>%
    bind_cols(hallucinogen_type(.)) %>%
    bind_cols(rxmisuse_type(.)) %>%
    bind_cols(roa_marijuana(.)) %>%
    bind_cols(techuse_techowned(.)) %>%
    bind_cols(sni_baseline(.)) %>%
    select(-gender,-duration_in_seconds,-date,-survey_type,-site_unhoused_77_text,-site_housed_77_text,-consent,
           -dob,-hisp,-site_housed,-site_unhoused,-birace,-starts_with("research_"),-starts_with("hfias_"),
           -prep_whereleard,-prep_social_who,-prep_barriers,-describe_3mopartner,-`3mosex_partgndr`,
           -`3mosex_types`,-`3mosex_cntrcptv`,-exchsex_types,-sti_pos,-q523,-romrel_sex,-romrel_ptnrsgndr,-reasonhomeless,
           -housingprogs_ever,-uh_livsit_3mo,-healthneeds,-incmgen_30day,-chronicdx,-coping,-tobacco_types,
           -meth_30_roa,-mdma_30_roa,-pdm_30_roa,-heroin_30_roa,-cocaine_30_roa,-crack_30_roa,-steroid_30_roa,-pcp_30_roa,-othersu_30_roa,
           -sutypes_use_ever,-gang_aff,
           -exch_where,-tech_access_noown,-cell_bill,-cell_charge_access,-cel_use,-tech_barriers,
           -policein_3mo_why,-marj_30_types,-tech_access_where,-marj_access,-halluc_30_types,
           -pdm_30_types,-marj_30_roa,-techaccess,-mse_progengage,-sni_testever,-hivp_meds_whynot,-hivp_tx_why) %>%
    rename(baseline_id = responseid,
           demo_birace_text = birace_8_text)
  old_names <- c("pdm_30_types_4_text")
  new_names <- c("drugs_rxmisuse_text")
  setnames(filtered_baseline,old_names,new_names)
  
  sni_baseline_names <- filtered_baseline %>%
    select(starts_with("sni_")) %>%
    select(-starts_with("sni_bl_"),-sni_version,-starts_with("sni_alter_ids_")) %>%
    names()
  
  filtered_baseline <- select(filtered_baseline,-one_of(sni_baseline_names))
 
  old_names<- c("race","race_8_text","inschool","inschool_type","inschool_type_4_text","educ","sex","gender_6_text",
                "sexori","sexori_6_text","sexattr_formales",
                "giscale_1","giscale_2","giscale_3","giscale_4","giscale_5","giscale_6","giscale_7",
                "romrel_marr","romrel_curr","romrel_ptnrs","romrel_dur",
                "firsthomeless","reasonhomeless_17_text","housingprogs_ever_20_text",
                "timehomeless_ever","timehoused","staybeforehoused","staybeforehoused_20_text","timehomeless_beforeh","timehomeless_recent",
                "uh_livsit_3mo_20_text","uh_livsit_curr","uh_livsit_curr_20_text",
                "fc_ever","fc_placements","jjs_ever","jjs_ageout","arrest_ever","jail_ever",
                "stress_streets_1","stress_streets_2","stress_streets_3","stress_streets_4","stress_streets_5",                 
                "stress_streets_6","stress_streets_7","stress_streets_8","stress_streets_9","stress_streets_10",
                "stress_streets_11","stress_streets_12","stress_streets_13","stress_streets_14","stress_streets_15",
                "mse_talkto","mse_housingopts","mse_progengage_10_text","mse_drop_inaccess",
                "mse_shelteraccess","mse_txaccess","mse_rel_prov",
                "phq9_1","phq9_2","phq9_3","phq9_4","phq9_5","phq9_6","phq9_7","phq9_8","phq9_9",
                "prep_whereleard_9_text","prep_know","prep_social","prep_rx_ever","prep_currentlytaking","prep_interest","prep_barriers_9_text",
                "lifesex_beh","lifesex_partnum","3mosex_partgndr_6_text",
                "3mosex_partnum","3mosex_partbiosex","3mosex_condomfreq",
                "3mosex_cntrcptv_8_text","3mosex_cntrcptv_fam","3mosex_cntrcptv_plan",
                "3mosex_suifreq","3mosex_hivconvo","exchsex_3mo",
                "exchsex_3mo_forced","forcedsex_3mo","forcedsexattempt_3mo",
                "hiv_st","hivp_when","hivp_testloc","hivp_ptcounsel","hivp_tx", 
                "hivp_meds","hivn_lasttest",
                "hivn_testloc","hivn_testloc_9_text","hivn_gotresult","hivn_ptcounsel",
                "sti_last",  "sti_pos_7_text","chlam_pos_last",
                "gono_pos_last","syph_pos_last","herp_pos_last","hpv_pos_last",
                "hepb_pos_last","other_pos_last","hcv_lasttest","hcv_status",
                "hcv_tx","preg_freq","preg_unplan","preg_child",
                "preg_live",
                "ucla_ptsd_disaster", "ucla_ptsd_accident", "ucla_ptsd_commvict",
                "ucla_ptsd_familyvict", "ucla_ptsd_war", "ucla_ptsd_commwit", "ucla_ptsd_injury",
                "ucla_ptsd_physabuse", "ucla_ptsd_deadbody", "ucla_ptsd_forcedsex", "ucla_ptsd_witness",
                "ucla_ptsd_sexabuse", "ucla_ptsd_death",
                "gad7_1", "gad7_2", "gad7_3", "gad7_4", "gad7_5", "gad7_6", "gad7_7", "gad7_8",
                "ders_sf_1", "ders_sf_2", "ders_sf_3", "ders_sf_4", "ders_sf_5", "ders_sf_6",
                "ders_sf_7", "ders_sf_8", "ders_sf_9", "ders_sf_10", "ders_sf_11", "ders_sf_12",
                "ders_sf_13", "ders_sf_14", "ders_sf_15", "ders_sf_16", "ders_sf_17", "ders_sf_18",
                "mh_dx_1","mh_dx_2","mh_dx_3","mh_dx_4","mh_dx_5","mh_dx_6","mh_dx_7",
                "mh_dx_8","mh_dx_9","mh_dx_other",
                "pss_1","pss_2","pss_3","pss_4",
                "ptsd_1","ptsd_2","ptsd_3","ptsd_4",
                "isi7_sri","isi7_sri_since","sleep_safe","sd_since","sd_sf1","sd_sf2","sd_sf3","sd_sf4",
                "healthneeds_10_text","chronicdx_23_text"
                )
  new_names <- c("demo_race","demo_race_other","demo_school","demo_school_type","demo_school_other","demo_education","demo_sex",
                 "demo_gender_text","demo_sex_ori","demo_sex_ori_other","demo_sex_attr",
                 "scale_gay_identity_1","scale_gay_identity_2","scale_gay_identity_3","scale_gay_identity_4","scale_gay_identity_5",
                 "scale_gay_identity_6","scale_gay_identity_7",
                 "history_romance_married","history_romance_current","history_romance_partners",
                 "history_romance_duration",
                 "history_homeless_first_time","history_homeless_why_text","history_house_progs_text",
                 "history_homeless_time_ever","history_time_housed","history_stay_prehoused","history_stay_prehoused_other","history_homeless_time_prehoused",
                 "history_homeless_time_recent",
                 "unhoused_livsit_3mo_text","unhoused_livsit_now","unhoused_livsit_now_text",
                 "history_foster_care_ever","history_foster_care_placement","history_jjs_ever","history_jjs_age_out",
                 "history_arrest_ever","history_jail_ever",
                 "scale_stress_streets_1","scale_stress_streets_2","scale_stress_streets_3","scale_stress_streets_4","scale_stress_streets_5",                 
                 "scale_stress_streets_6","scale_stress_streets_7","scale_stress_streets_8","scale_stress_streets_9","scale_stress_streets_10",
                 "scale_stress_streets_11","scale_stress_streets_12","scale_stress_streets_13","scale_stress_streets_14","scale_stress_streets_15",
                 "survey_mse_talking","survey_mse_housing","survey_mse_engage_text","survey_mse_dropin",
                 "survey_mse_shelter","survey_mse_treatment","survey_mse_relationship",
                 "scale_phq9_1","scale_phq9_2","scale_phq9_3","scale_phq9_4","scale_phq9_5",
                 "scale_phq9_6","scale_phq9_7","scale_phq9_8","scale_phq9_9",
                 "survey_prep_where_text","survey_prep_know","survey_prep_social","survey_prep_rx_ever","survey_prep_rx_current",
                 "survey_prep_interest","survey_prep_barrier_text",
                 "survey_sexlife_gender","survey_sexlife_count","survey_sex3mo_gender_text",
                 "survey_sex3mo_count","survey_sex3mo_biosex","survey_sex3mo_condom",
                 "survey_sex3mo_cntrcptv_text","survey_sex3mo_cntrcptv_app","survey_sex3mo_cntrcptv_plan",
                 "survey_sex3mo_suifreq","survey_sex3mo_hivtalk","survey_sex3mo_exchange",
                 "survey_sex3mo_exforced","survey_sex3mo_forced","survey_sex3mo_attemptforce",
                 "survey_hiv_posyes","survey_hiv_poswhen","survey_hiv_postestloc","survey_hiv_poscounsel","survey_hiv_postx", 
                 "survey_hiv_posmeds","survey_hiv_negwhen",
                 "survey_hiv_negtestloc","survey_hiv_negtestloc_text","survey_hiv_negresult","survey_hiv_negcounsel",
                 "survey_sti_when",  "survey_sti_pos_text","survey_sti_chlam_when",
                 "survey_sti_gono_when","survey_sti_syph_when","survey_sti_herp_when","survey_sti_hpv_when",
                 "survey_sti_hepb_when","survey_sti_other_when","survey_sti_hcv_when","survey_sti_hcv_status",
                 "survey_sti_hcv_tx","survey_preg_freq","survey_preg_unplan_freq","survey_preg_children",
                 "survey_preg_livewith",
                 "scale_ucla_ptsd_disaster", "scale_ucla_ptsd_accident", "scale_ucla_ptsd_commvict", 
                 "scale_ucla_ptsd_familyvict", "scale_ucla_ptsd_war", "scale_ucla_ptsd_commwit", 
                 "scale_ucla_ptsd_injury", "scale_ucla_ptsd_physabuse", "scale_ucla_ptsd_deadbody", 
                 "scale_ucla_ptsd_forcedsex", "scale_ucla_ptsd_witness", "scale_ucla_ptsd_sexabuse", 
                 "scale_ucla_ptsd_death",
                 "scale_gad7_1", "scale_gad7_2", "scale_gad7_3", "scale_gad7_4", "scale_gad7_5", 
                 "scale_gad7_6", "scale_gad7_7", "scale_gad7_8",
                 "scale_ders_sf_1", "scale_ders_sf_2", "scale_ders_sf_3", "scale_ders_sf_4",
                 "scale_ders_sf_5", "scale_ders_sf_6", "scale_ders_sf_7", "scale_ders_sf_8",
                 "scale_ders_sf_9", "scale_ders_sf_10", "scale_ders_sf_11", "scale_ders_sf_12",
                 "scale_ders_sf_13", "scale_ders_sf_14", "scale_ders_sf_15", "scale_ders_sf_16",
                 "scale_ders_sf_17", "scale_ders_sf_18",
                 "scale_mhealth_adhd","scale_mhealth_ptsd","scale_mhealth_odcd","scale_mhealth_depression",
                 "scale_mhealth_bd","scale_mhealth_schizo","scale_mhealth_gad","scale_mhealth_pd",
                 "scale_mhealth_other","scale_mhealth_text",
                 "scale_pss_1","scale_pss_2","scale_pss_3","scale_pss_4",
                 "scale_ptsd_1","scale_ptsd_2","scale_ptsd_3","scale_ptsd_4",
                 "sleep_daytimeimpair_current_isi7","sleep_since_housed_daytimeimpair","sleep_location_safe",
                 "sleep_since_housed_quality","sleep_disturbance_sf4a_1_quality","sleep_disturbance_sf4a_2_refresh",
                 "sleep_disturbance_sf4a_3_problem","sleep_disturbance_sf4a_4_fallslp",
                 "healthcare_access_needs_text","scale_phealth_text")
  setnames(filtered_baseline,old_names,new_names)
  
  old_names <- drug_30
  new_names <- c("drugs_30day_freq_alcohol","drugs_30day_freq_marijuana","drugs_30day_freq_meth",
                 "drugs_30day_freq_mdma","drugs_30day_freq_syntheticmj","drugs_30day_freq_hallucinogens",
                 "drugs_30day_freq_rxmisuse","drugs_30day_freq_heroin","drugs_30day_freq_cocaine",
                 "drugs_30day_freq_crack","drugs_30day_freq_inhalant","drugs_30day_freq_steroid",
                 "drugs_30day_freq_nitrous","drugs_30day_freq_ketamine","drugs_30day_freq_pcp",
                 "drugs_30day_freq_other")
  setnames(filtered_baseline,old_names,new_names)
  
  old_names <- drugs_last
  
  new_names <- c("drugs_lastuse_tobacco","drugs_lastuse_alcohol","drugs_lastuse_marijuana",
                 "drugs_lastuse_meth","drugs_lastuse_mdma","drugs_lastuse_syntheticmj",
                 "drugs_lastuse_hallucinogens","drugs_lastuse_rxmisuse","drugs_lastuse_heroin",
                 "drugs_lastuse_cocaine","drugs_lastuse_crack","drugs_lastuse_inhalant",
                 "drugs_lastuse_steroid","drugs_lastuse_nitrous","drugs_lastuse_ketamine",
                 "drugs_lastuse_pcp","drugs_lastuse_other")
  setnames(filtered_baseline,old_names,new_names)
  
  old_names <- c("employ_paidjobs","employ_ttlpaidhrs","ttlincome_employ",
                 "ttlincome_benefits","ttlincome_informal","health","mh_current",
                 "suic_thought","suic_attempt","med_12mo","ther_12mo","er_12mo",
                 "hospit_12mo","unmet_12mo","mhneed_perceive")
  new_names <- c("history_employment_paidjobs","history_employment_paidhours","history_income_employment",
                 "history_totalincome_benefits","history_totalincome_informal","history_phealth_current","history_mhealth_current",
                 "history_suicide_thoughts","history_suicide_attempts","history_treat12mo_medical","history_treat12mo_therapy",
                 "history_treat12mo_er","history_treat12mo_hospital","history_treat12mo_unmet","history_mhealth_needs")
  setnames(filtered_baseline,old_names,new_names)
  
  setnames(filtered_baseline,tobacco_daily,new_tobacco_daily)
  setnames(filtered_baseline,tobacco_weekly,new_tobacco_weekly)
  setnames(filtered_baseline,tobacco_varnames,new_tobacco_varnames)
  
  
  old_names <- c("casey_locuscontrol","casey_futurethink","casey_relationships","casey_modeling","casey_successrel","casey_transition","casey_pride","casey_selfefficacy")
  new_names <- rep_varnames("scale_",old_names)
  setnames(filtered_baseline,old_names,new_names)
  
  ## VARS PRESERVED
  # site_housed
  
  attr(filtered_baseline$baseline_duration,"label") <- attributes(baseline_data$duration_in_seconds)$label
  attr(filtered_baseline$baseline_date,"label") <- attributes(baseline_data$date)$label
  attr(filtered_baseline$baseline_survey,"label") <- attributes(baseline_data$survey_type)$label
  #attr(filtered_baseline$baseline_site_housed,"label") <- attributes(baseline_data$site_housed)$label
  #attr(filtered_baseline$baseline_site_unhoused,"label") <- attributes(baseline_data$site_unhoused)$label
  
  attr(filtered_baseline$scale_hfias_1,"label") <- attributes(baseline_data$hfias_1)$label
  attr(filtered_baseline$scale_hfias_2,"label") <- attributes(baseline_data$hfias_2)$label
  attr(filtered_baseline$scale_hfias_3,"label") <- attributes(baseline_data$hfias_3)$label
  attr(filtered_baseline$scale_hfias_4,"label") <- attributes(baseline_data$hfias_4)$label
  attr(filtered_baseline$scale_hfias_5,"label") <- attributes(baseline_data$hfias_5)$label
  attr(filtered_baseline$scale_hfias_6,"label") <- attributes(baseline_data$hfias_6)$label
  attr(filtered_baseline$scale_hfias_7,"label") <- attributes(baseline_data$hfias_7)$label
  attr(filtered_baseline$scale_hfias_8,"label") <- attributes(baseline_data$hfias_8)$label
  attr(filtered_baseline$scale_hfias_9,"label") <- attributes(baseline_data$hfias_9)$label
  
  for(i in 1:8){
    daytype <- paste0("filtered_baseline$tobacco_dailyamount_",tobacco_types[i])
    weektype <- paste0("filtered_baseline$tobacco_weeklyamount_",tobacco_types[i])
    olddaytype <- paste0("baseline_data$tobacco_dailyamt_",i)
    oldweektype <- paste0("baseline_data$tobacco_weeklyamt_",i)
    setattr(eval(parse(text = daytype)),"label",attributes(eval(parse(text = olddaytype)))$label)
    setattr(eval(parse(text = weektype)),"label",attributes(eval(parse(text = oldweektype)))$label)
  }
  
  attr(filtered_baseline$demo_birace_text,"label") <- "Multi-Racial: Other Text Entry"
  attr(filtered_baseline$demo_sex_gender_min,"label") <- "Is the partcipant a sexual or gender minority?"
  attr(filtered_baseline$demo_sex_min,"label") <- "Is the partcipant a sexual identity minority? (Not Heterosexual)"
  attr(filtered_baseline$demo_gendercis,"label") <- "Is the participant cis-gendered?"
  attr(filtered_baseline$demo_race_min,"label") <- "Is the participant a racial minority? (Not White)"
  attr(filtered_baseline$demo_race_cat,"label") <- "Census tract recategorized racial category"
  attr(filtered_baseline$demo_hispanic,"label") <- "Does the participant identify as Hispanic or Latino?"
  attr(filtered_baseline$demo_dob,"label") <- "Date of birth, in date format"
  attr(filtered_baseline$demo_age,"label") <- "Age at baseline (in years)"
  #attr(filtered_baseline$demo_housed,"label") <- "Is the participant housed or unhoused?"
  attr(filtered_baseline$baseline_version,"label") <- "Baseline survey version"
  attr(filtered_baseline$baseline_id,"label") <- "Baseline survery respone id"
  
  yn_newnames <- c("history_needle_exchange_ever","history_needle_reuse_30days","scale_cage_annoy",
                   "history_gang_current","history_street_family",
                   "history_needle_buy_ever","history_needle_use_ever","scale_cage_cutdown",
                   "history_gang_ever","scale_cage_guilty","scale_cage_morning",
                   "history_substancetx_pastyear","history_needle_share_30days",
                   "drugs_marijuana_card","history_assault_victim","history_avoid_police",
                   "history_assault_perp","history_robbery_victim","techuse_charging_difficulty",
                   "history_substancetx_ever","history_needle_exchange_30days")
  setnames(filtered_baseline,yn_vars,yn_newnames)
  
  text_varnames <- c("othersu_30_roa_5_text","cell_bill_8_text","tech_access_where_8_text",
                     "tech_barriers_10_text","cel_use_13_text","cell_bill_7_text",
                     "othersu_type","marj_30_roa_10_text","exch_where_7_text","marj_30_types_5_text",
                     "gang_aff_4_text")
  newtext_varnames <- c("drugs_other_roa_text","techuse_cellbill_textother","techuse_locations_text",
                        "techuse_barriers_text","techuse_celluse_text","techuse_cellbill_textfamily",
                        "drugs_everuse_text","drugs_roa_marijuana_text","history_needle_access_text",
                        "drugs_marijuana_type_text","history_gang_closest_text")
  setnames(filtered_baseline,text_varnames,newtext_varnames)
  
  numeric_newnames <- c("drugs_marijuana_30dayhits","drugs_alcohol_30day_binge_male","history_gang_age","drugs_alcohol_30day_binge_female")
  setnames(filtered_baseline,numeric_varnames,numeric_newnames)
  
  lastvarnames <- c("marj_amtwk_1","marj_amtwk_2","sucost_30d_alcohol","sucost_30d_marj","sucost_30d_ildrug",
                    "policein_3mo_exp","policeintgen","policeint_3mo","vict_ipv_vic","vict_ipv_perp","cell_plan",
                    "marj_freqaccpt","marj_freq_curr","cell_turnover","ivdu_reuse_quant","ivdu_30day")
  lastnewvarnames <- c("drugs_marijuana_amount_dry","drugs_marijuana_amount_wax",
                       "drugs_cost_alcohol","drugs_cost_marijuana","drugs_cost_others",
                       "history_police_experience_3mo","history_police_experience_ever","history_police_frequency_3mo",
                       "history_partnerviolence_victim","history_partnerviolence_perp","techuse_cell_plan",
                       "drugs_marijuana_acceptableuse","drugs_marijuana_currentuse","techuse_cell_turnover",
                       "history_needle_reuse_count","history_needle_30day_count"
                       )
  setnames(filtered_baseline,lastvarnames,lastnewvarnames)
  
  attr(filtered_baseline$drugs_cost_alcohol,"label") <- attributes(baseline_data$sucost_30d_alcohol)$label
  
  
  incomplete_name_out <- capture.output(names(select(filtered_baseline,-pid,-starts_with("demo"),-starts_with("baseline"),-starts_with("unhoused"),
               -starts_with("scale"),-starts_with("history"),-starts_with("survey"),-starts_with("sni"),-starts_with("sleep"),
               -starts_with("healthcare"),-starts_with("tobacco_"),-starts_with("drugs_"),-starts_with("techuse_"))))
  print(incomplete_name_out)
  cat("List of variable names that are currently in progress", incomplete_name_out, file=paste(main_export_filepath,"incomplete_varnames.txt",sep = "/"), sep="\n")
  
  complete_name_out <- capture.output(names(select(filtered_baseline,pid,starts_with("demo"),starts_with("baseline"),starts_with("unhoused"),
                                                     starts_with("scale"),starts_with("history"),starts_with("survey"),starts_with("sni"),
                                                   starts_with("sleep"),starts_with("healthcare"),starts_with("tobacco_"),starts_with("drugs_"),
                                                   starts_with("techuse_"))))
  complete_name_out
  cat("List of variable names that are complete + SNI", complete_name_out, file=paste(main_export_filepath,"complete_varnames.txt",sep = "/"), sep="\n")
  
  
  completedsofar <- select(filtered_baseline,pid,starts_with("demo"),starts_with("baseline"),starts_with("unhoused"),
                           starts_with("scale"),starts_with("history"),starts_with("survey"),starts_with("sni"),
                           starts_with("sleep"),starts_with("healthcare"),starts_with("tobacco_"),starts_with("drugs_"),
                           starts_with("techuse_"))
  write_dta(completedsofar,paste(main_export_filepath,"baseline.dta",sep = "/"))
  
  return(filtered_baseline)
  # test <- select(filtered_baseline,pid,starts_with("survey_prep_"),starts_with("demo_"),
  #                starts_with("survey_hiv_"),starts_with("survey_sti_"),starts_with("survey_sexlife_"),
  #                starts_with("survey_preg_"),starts_with("survey_sex3mo_"),starts_with("history_"),starts_with("unhoused"))
  # 
  # out <- capture.output(dfSummary(completedsofar))
  # cat("Baseline Data", out, file="/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Person Level/Progress Reports/current_baseline_summary.txt", sep="\n")
  # 
  # names(select(filtered_baseline,one_of(c("prep_know","prep_whereleard","prep_whereleard_9_text","prep_social","prep_social_who","prep_rx_ever","prep_currentlytaking","prep_interest","prep_barriers","prep_barriers_9_text"))))
}

followup_fix_errors <- function(main_filepath, v1_v2q_filepath,v2_v2q_filepath,v3_v2q_filepath,
                                v4_v2q_filepath,v5_v2q_filepath,v6_v2q_filepath,v7_v2q_filepath,
                                v2q_redcap_filepath = "exit scales_redcap.csv"){
  redcap_append <- redcap_to_v1(main_filepath, v2q_redcap_filepath)
  v1_raw <- return_raw_baseline(main_filepath, v1_v2q_filepath) %>%
    filter(responseid != "R_OiLgOEXdq9zF0iJ" & exit_pid != "105" & exit_pid != "10100"
           & responseid != "R_1pyRsMTctbJuoK3" & responseid != "R_0NugAIF3VyvohKV"
           & exit_pid != "125231" & exit_pid != "7675667") %>%
    mutate(hes_date = ifelse(exit_pid == "1001","6/19/2017",hes_date),
           hes_date = ifelse(exit_pid == "1002","6/19/2017",hes_date),
           hes_date = ifelse(exit_pid == "1004","6/19/2017",hes_date),
           hes_date = ifelse(exit_pid == "1005","6/19/2017",hes_date)) %>%
    rename(v2q_pid = exit_pid,
           v2q_date = hes_date) %>%
    mutate(v2q_version = "1") %>%
    left_join(redcap_append, by = "v2q_pid")
  #v2_raw <- return_raw_baseline(main_filepath, v2_v2q_filepath)
  v3_raw <- return_raw_baseline(main_filepath, v3_v2q_filepath) %>%
    filter(responseid != "R_3eNrv9wgEEw1DfH" & responseid != "R_2X66FVl0NeOhixn") %>%
    mutate(v2q_version = "3") %>%
    bind_rows(v1_raw)
  v4_raw <- return_raw_baseline(main_filepath, v4_v2q_filepath) %>%
    filter(v2q_pid != "1092" & responseid != "R_8ffh1QKZh1MjMHL") %>%
    mutate(v2q_pid = ifelse(responseid == "R_uyQRieSTpMom4jD","1099",v2q_pid),
           v2q_pid = ifelse(responseid == "R_1LeznYAMP8Ac2TV","1115",v2q_pid))
  
  v4_fix_1087 <- v4_raw %>%
    filter(v2q_pid == "1087") %>%
    select_if(~sum(!is.na(.)) > 0) %>%
    summarize_all(funs(last(na.omit(.))))
  
  v4_fix <- v4_raw %>%
    filter(v2q_pid != "1087") %>%
    bind_rows(v4_fix_1087) %>%
    mutate(v2q_version = "4") %>%
    bind_rows(v3_raw)
  
  v5_raw <- return_raw_baseline(main_filepath, v5_v2q_filepath) %>%
    mutate(v2q_pid = ifelse(responseid == "R_338UYPxSPohpAg1","1123",v2q_pid),
           v2q_pid = ifelse(responseid == "R_10NlniZ8rWHuIV9","1129",v2q_pid),
           v2q_pid = ifelse(responseid == "R_WDnNcnnPz5lI7Ml","1130",v2q_pid),
           v2q_pid = ifelse(responseid == "R_3PmLgNuV3kHZMVt","2011",v2q_pid),
           v2q_pid = ifelse(responseid == "R_31Qt8FUpZ8f2Y2D","2019",v2q_pid)) %>%
    filter(v2q_pid != "101" & responseid != "R_3e3KHPHwy4TsSAa") %>%
    mutate(v2q_version = "5") %>%
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
    select_if(~sum(!is.na(.)) > 0) %>%
    summarize_all(funs(first(na.omit(.))))
  
  v6_fix <- v6_raw %>%
    filter(v2q_pid != "1132") %>%
    bind_rows(v6_fix_1132) %>%
    mutate(v2q_version = "6") %>%
    bind_rows(v5_raw)
  
  v7_preraw <- return_raw_baseline(main_filepath, v7_v2q_filepath) %>%
    mutate(v2q_version = "7")
  
  v7_bind_labels <- get_label(v7_preraw)
  
  v7_raw <- return_raw_baseline(main_filepath, v7_v2q_filepath) %>%
    mutate(v2q_pid = ifelse(v2q_pid == "2038","1138",v2q_pid)) %>%
    mutate(v2q_version = "7") %>%
    bind_rows(v6_fix)
  
  followup_data <- v7_raw %>%
    set_label(unlist(v7_bind_labels))
  
  return(followup_data)
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
           sni_pid = ifelse(responseid == "R_21ieA681AseMilh","1003",sni_pid),
           sni_pid = ifelse(responseid == "R_1pxTWxuU54eFKlB","1005",sni_pid),
           sni_pid = ifelse(responseid == "R_2diIpwcMFW8hEwg","1007",sni_pid),
           sni_pid = ifelse(responseid == "R_300lktfUf1pY6P5","1010",sni_pid),
           sni_pid = ifelse(responseid == "R_3JeBtrOV0li5zX8","1011",sni_pid),
           sni_pid = ifelse(responseid == "R_3shy6kIs7KNdtHV","1014",sni_pid),
           sni_pid = ifelse(responseid == "R_1kNSGuHA9YCyU18","1025",sni_pid),
           sni_pid = ifelse(responseid == "R_3065aTmwnpPCYF7","1046",sni_pid),
           sni_pid = ifelse(responseid == "R_12bSr93YzKLRLbh","1078",sni_pid),
           sni_pid = ifelse(responseid == "R_3hioYmelXxNGYOl","1089",sni_pid),
           sni_pid = ifelse(sni_pid == "2028","1129",sni_pid),
           sni_pid = ifelse(sni_pid == "2037","1130",sni_pid),
           sni_pid = ifelse(sni_pid == "2058","1137",sni_pid),
           sni_pid = ifelse(responseid == "R_3OfbRQsgDj2Kevm","2001",sni_pid),
           sni_pid = ifelse(responseid == "R_3Rt5HgK5XpHLn2Y","2002",sni_pid),
           sni_pid = ifelse(responseid == "R_w0pYnI7VH9xrMlz","2003",sni_pid),
           sni_pid = ifelse(responseid == "R_1IWMkYe9k7Y45ZN","2004",sni_pid),
           sni_pid = ifelse(responseid == "R_3MG6UTQ2fig8Us4","2008",sni_pid),
           sni_pid = ifelse(responseid == "R_3pgs8knsRgeM2Zb","2016",sni_pid)) %>%
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
  v1_fix_3007 <- tibble(pid ="1007", sex = "Female", race = "I don't know", hisp = "I don't know")  # Fixing 1007 > 3007
  v1_fix_1004 <- v1_raw %>%
    filter(responseid == "R_72GUjPfXIWKYOlP" | responseid == "R_1o0ursi841wl4tT") %>%
    select_if(~sum(!is.na(.)) > 0) %>%
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
      pid = ifelse(responseid == "R_wTrw4o4fQzrcgUh","1003",pid),
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
    mutate(
      stress_scale_1 = ifelse(grepl("Finding enough food to eat",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_1),"None at all",stress_scale_1),
      stress_scale_2 = ifelse(grepl("Getting along with friends",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_2),"None at all",stress_scale_2),
      stress_scale_3 = ifelse(!is.na(stress_streets) & is.na(stress_scale_3),"None at all",stress_scale_3),
      stress_scale_4 = ifelse(grepl("Being unable to find work",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_4),"None at all",stress_scale_4),
      stress_scale_5 = ifelse(grepl("Being hit, kicked, or punched",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_5),"None at all",stress_scale_5),
      stress_scale_6 = ifelse(grepl("Finding a place to sleep",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_6),"None at all",stress_scale_6),
      stress_scale_7 = ifelse(grepl("Getting professional help for a health problem",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_7),"None at all",stress_scale_7),
      stress_scale_8 = ifelse(!is.na(stress_streets) & is.na(stress_scale_8),"None at all",stress_scale_8),
      stress_scale_9 = ifelse(grepl("Having a purpose for my life",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_9),"None at all",stress_scale_9),
      stress_scale_10 = ifelse(grepl("Getting more education",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_10),"None at all",stress_scale_10),
      stress_scale_11 = ifelse(grepl("Finding a place to take a bath or shower",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_11),"None at all",stress_scale_11),
      stress_scale_12 = ifelse(grepl("Finding a place to wash my clothes",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_12),"None at all",stress_scale_12),
      stress_scale_13 = ifelse(grepl("Finding other people to hang out with",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_13),"None at all",stress_scale_13),
      stress_scale_14 = ifelse(!is.na(stress_streets) & is.na(stress_scale_14),"None at all",stress_scale_14),
      stress_scale_15 = ifelse(grepl("Earning money",stress_streets) == FALSE & !is.na(stress_streets) & is.na(stress_scale_15),"None at all",stress_scale_15)
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
      pid = ifelse(responseid == "R_2AQdJ7QXdBSRZDC", "1046", pid),
      pid = ifelse(responseid == "R_1exDzqYGNb34xQq", "2008", pid),
      sex = ifelse(pid == "4008","Female",sex),
      pid = ifelse(responseid == "R_1Fb1Tth45hmne1q", "2016", pid),
      pid = ifelse(pid == "2009","1199",pid),
    ) %>%
    filter(
      responseid != "R_3dLBmeVaJN1KDsD" &
      responseid != "R_1Q9lqUXMprXCHUH" &
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
           pid = ifelse(pid == "1089","1089",pid),
           pid = ifelse(pid == "1078","1078",pid),
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
           sexori = ifelse(pid == "2044","Heterosexual or straight",sexori),
           demo_racemin = ifelse(pid == "2044","Yes",demo_racemin),
           uh_livsit_curr = ifelse(pid == "2047","Adult emergency/temporary shelter (less than 30 days)",uh_livsit_curr),
           sexori = ifelse(pid == "2048","Bisexual",sexori),
           
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
    mutate(demo_racemin = NA_character_) %>%
    mutate(demo_hispanic = NA_character_) %>%
    mutate(date = ifelse(pid == "2084", "01/10/2019",date))
  pre_bind_labels <- get_label(v7_raw)
  
  v7_fix_2053 <- v7_raw %>%
    filter(pid == "2053") %>%
    select_if(~sum(!is.na(.)) > 0) %>%
    summarize_all(funs(first(na.omit(.))))
  
  v7_fix <- v7_raw %>%
    filter(!is.na(pid) & as.numeric(pid) > 100) %>%
    filter(pid != "2053") %>%
    filter(responseid != "R_BLiD4SQ7oZfTFnj") %>%
    mutate(mh_dx_4 = ifelse(pid == "1019","Yes",mh_dx_4),
           uh_livsit_curr = ifelse(pid == "2100","Street, park, beach, or outside",uh_livsit_curr),
           race = ifelse(pid == "2099","Hisp/Lat and no other race",race),
           race = ifelse(pid == "1140","Hisp/Lat and no other race",race),
           race = ifelse(pid == "2083","Bi/Multi-racial or Ethnic",race),
           birace = ifelse(pid == "2083","Black or African-American,Hispanic/Latino",birace),
           demo_hispanic = ifelse(pid == "2083","Yes",demo_hispanic),
           demo_hispanic = ifelse(pid == "2099","Yes",demo_hispanic),
           sexori = ifelse(pid == "1140","Heterosexual or straight",sexori),
           sexori = ifelse(pid == "2043","Bisexual",sexori),
           pid = ifelse(pid == "2058","1137",pid),
           pid = ifelse(pid == "2038","1138",pid),
           pid = ifelse(responseid == "R_3Ny9MUnIwlRxQ9S","1142",pid),
           pid = ifelse(responseid == "R_1GHvO5JvZDoeyko","2121",pid),
           sexori = ifelse(pid == "2059","Bisexual",sexori),
           uh_livsit_curr = ifelse(pid == "2068","Street, park, beach, or outside",uh_livsit_curr),
           uh_livsit_curr = ifelse(pid == "2084","Street, park, beach, or outside",uh_livsit_curr),
           race = ifelse(pid == "2066","I don't know",race),
           demo_racemin = ifelse(pid == "2066","Yes",demo_racemin),
           birace = ifelse(pid == "2130","American Indian or Alaska Native,Black or African-American,White",birace),
           race = ifelse(pid == "2130","Bi/Multi-racial or Ethnic",race),
           sni_alter_ids_5 = ifelse(pid == "2079","Braids",sni_alter_ids_5),
           sni_alter_ids_1 = ifelse(pid == "2130","Bryan1",sni_alter_ids_1),
           sni_alter_ids_2 = ifelse(pid == "2130","V",sni_alter_ids_2),
           sni_alter_ids_2 = ifelse(pid == "2099","Brian",sni_alter_ids_2),
           sni_alter_ids_2 = ifelse(pid == "1141","Dad",sni_alter_ids_2),
           sni_alter_ids_3 = ifelse(pid == "2130","Jeff",sni_alter_ids_3),
           sni_alter_ids_3 = ifelse(pid == "2099","Annie",sni_alter_ids_3),
           sni_alter_ids_4 = ifelse(pid == "2130","Ug",sni_alter_ids_4),
           sni_alter_ids_5 = ifelse(pid == "2130","Bryan2",sni_alter_ids_5),
           sni_alter_ids_5 = ifelse(pid == "2099","Mohammed",sni_alter_ids_5)) %>%
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



  

