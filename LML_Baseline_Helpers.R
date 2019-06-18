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

simplify_baseline_sni_id <- function(sni_id){
  original_label_name <- attributes(sni_id)$label
  gsubbed_sni_id <- tolower(sni_id)
  repeat_gsub_blank <- c(" ", "\\.", "'", "\\$", "\\{","\\:","\\/","efield","\\}")
  for(i in repeat_gsub_blank){
    gsubbed_sni_id <- gsub(i,"",gsubbed_sni_id)
  }
  gsubbed_sni_id <- gsub("x29","person1",gsubbed_sni_id)
  gsubbed_sni_id <- gsub("x30","person2",gsubbed_sni_id)
  gsubbed_sni_id <- gsub("x31","person3",gsubbed_sni_id)
  gsubbed_sni_id <- gsub("x32","person4",gsubbed_sni_id)
  gsubbed_sni_id <- gsub("x33","person5",gsubbed_sni_id)
  attr(gsubbed_sni_id,"label") <- original_label_name
  return(gsubbed_sni_id)
}


baseline_align_sni_alters <- function(sni_column, new_name){
  social_daily <- as.tibble(sni_column)
  names(social_daily) <- c("sni_social")

  sni_ids <- c(",person1,",",person2,",",person3,",",person4,",",person5,",",noneoftheabove,")
  
  sni_social_daily <- social_daily %>%
    mutate(sni_social = paste0(",",sni_social,",")) %>%
    mutate(sni_social_a1 = 0,
           sni_social_a2 = 0,
           sni_social_a3 = 0,
           sni_social_a4 = 0,
           sni_social_a5 = 0,
           sni_social_a0 = 0)
  
  new_social_daily <- sni_social_daily %>%
    mutate(sni_social_a1 = ifelse(grepl(sni_ids[1],sni_social, fixed = TRUE),1,sni_social_a1)) %>%
    mutate(sni_social_a2 = ifelse(grepl(sni_ids[2],sni_social, fixed = TRUE),1,sni_social_a2)) %>%
    mutate(sni_social_a3 = ifelse(grepl(sni_ids[3],sni_social, fixed = TRUE),1,sni_social_a3)) %>%
    mutate(sni_social_a4 = ifelse(grepl(sni_ids[4],sni_social, fixed = TRUE),1,sni_social_a4)) %>%
    mutate(sni_social_a5 = ifelse(grepl(sni_ids[5],sni_social, fixed = TRUE),1,sni_social_a5)) %>%
    mutate(sni_social_a0 = ifelse(grepl(sni_ids[6],sni_social, fixed = TRUE),1,sni_social_a0))
  
  prereturn_sociallog <- new_social_daily %>%
    mutate_at(vars(starts_with("sni_social_a")),funs(ifelse(is.na(sni_social),NA,.)))

  bl_sni_rename <- function(string_varname){
    no_sni <- substr(string_varname,5)
  }
  
  return_sociallog <- prereturn_sociallog %>%
    select(-sni_social) %>%
    select_all(.funs = funs(paste0("sni_bl_",new_name,"_",str_sub(.,12))))
  
  return(return_sociallog)
}

sni_baseline <- function(filtered_baseline){
  sni_baseline_only <- filtered_baseline %>%
    select(starts_with("sni_")) %>%
    select(-sni_version,-starts_with("sni_alter_ids_")) %>%
    mutate_at(vars(starts_with("sni_gender_")),funs(case_when(
      . == "1" ~ "male",
      . == "2" ~ "female",
      . == "3" ~ "trans male/trans man",
      . == "4" ~ "trans female/trans woman",
      . == "5" ~ "genderqueer/gender non-conforming",
      . == "6" ~ "different identity",
      TRUE ~ tolower(.)
    ))) %>%
    mutate_at(vars(starts_with("sni_rel_h_")),funs(case_when(
      . == "Person you are romantically, intimately or sexually involved with" ~ "Person you are romantically, intimately, or sexually involved with",
      . == "Friend or other peer you know from the street or peers you interact with at a service agency"  ~ "Friend or other peer you know from the street or peers you interacted with at a homeless service agency, like a shelter or drop-in",
      TRUE ~ .
    )))
  
  sni_bl_names <- function(sni_baseline_only, endswithvar, newendswithvar){
    number_names <- names(select(sni_baseline_only,ends_with(endswithvar)))
    number_names_new <- gsub("sni_","sni_bl_",gsub(endswithvar,newendswithvar,number_names))
    setnames(sni_baseline_only,number_names,number_names_new)
  }
  
  sni_bl_names(sni_baseline_only, "_1","_a1")
  sni_bl_names(sni_baseline_only, "_2","_a2")
  sni_bl_names(sni_baseline_only, "_3","_a3")
  sni_bl_names(sni_baseline_only, "_4","_a4")
  sni_bl_names(sni_baseline_only, "_5","_a5")
  
  sni_bl_names(sni_baseline_only, "oth1","_a1")
  sni_bl_names(sni_baseline_only, "oth2","_a2")
  sni_bl_names(sni_baseline_only, "oth3","_a3")
  sni_bl_names(sni_baseline_only, "oth4","_a4")
  sni_bl_names(sni_baseline_only, "oth5","_a5")
 
  
  remaining_names <- names(select(sni_baseline_only,-ends_with("_a1"),-ends_with("_a2"),-ends_with("_a3")
                      ,-ends_with("_a4"),-ends_with("_a5")))
  
  new_sni_baseline <- sni_baseline_only %>%
    mutate_at(vars(one_of(remaining_names)),funs(simplify_baseline_sni_id(.))) %>%
    mutate_at(vars(starts_with("sni_bl_rel_h_a")),funs(factor_keep_rename(.,c("Case worker, social worker, agency staff or volunteer",
                                                                          "Person from work",
                                                                          "Person from school",
                                                                          "Friend or other peer you know from the street or peers you interacted with at a homeless service agency, like a shelter or drop-in",
                                                                          "Friend from home or from before you were homeless",
                                                                          "Person you are romantically, intimately, or sexually involved with",
                                                                          "Family (could include both biological and foster family)",
                                                                          "Other, please specify",
                                                                          "Friend or other resident you know from your current transitional or permanent housing program or another similar housing program"
                                                                          )))) %>%
    mutate_at(vars(starts_with("sni_bl_rel_uh_a")),funs(factor_keep_rename(.,c("Case worker, social worker, agency staff or volunteer",
                                                                          "Person from work",
                                                                          "Person from school",
                                                                          "Friend or other peer you know from the street or peers you interact with at a service agency, like a shelter or drop-in",
                                                                          "Friend from home or before you were homeless",
                                                                          "Person you are romantically, intimately, or sexually involved with",
                                                                          "Family (could include both biological and foster family)",
                                                                          "Other, please specify")))) 
  
  for(i in remaining_names){
    new_name <- str_sub(i,5)
    print(paste0(i," going to ",new_name))
    new_sni_baseline <- new_sni_baseline %>%
      bind_cols(baseline_align_sni_alters(select(new_sni_baseline,one_of(i)),new_name)) %>%
      select(-one_of(i))
  }
  
  return(new_sni_baseline)  
}

hiv_posmeds_whynot <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_hiv_posmeds_whynot = `hivp_meds_whynot`)
  variable_prefix <- "survey_hiv_posmeds_whynot"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_hiv_posmeds_whynot_Don't want anyone to know I am taking the medication/stigma associated with the condition` = "survey_hiv_posmeds_whynot_stigma",
    `survey_hiv_posmeds_whynot_I think it would be too expensive` = "survey_hiv_posmeds_whynot_price",
    `survey_hiv_posmeds_whynot_I'm concerned about side effects` = "survey_hiv_posmeds_whynot_sidefx",
    `survey_hiv_posmeds_whynot_It's too difficult to go to the doctor to get a prescription` = "survey_hiv_posmeds_whynot_rxhard",
    `survey_hiv_posmeds_whynot_It's too much effort to take a pill every day` = "survey_hiv_posmeds_whynot_effort",
    `survey_hiv_posmeds_whynot_Other (please specify)` = "survey_hiv_posmeds_whynot_other"
  )
  
  new_labels <- c(
    survey_hiv_posmeds_whynot_stigma = "What are some of the reasons you are not taking medications to help you manage your HIV?: Don't want anyone to know I am taking the medication/stigma associated with the condition",
    survey_hiv_posmeds_whynot_price = "What are some of the reasons you are not taking medications to help you manage your HIV?: I think it would be too expensive",
    survey_hiv_posmeds_whynot_sidefx = "What are some of the reasons you are not taking medications to help you manage your HIV?: I'm concerned about side effects",
    survey_hiv_posmeds_whynot_rxhard = "What are some of the reasons you are not taking medications to help you manage your HIV?: It's too difficult to go to the doctor to get a prescription",
    survey_hiv_posmeds_whynot_effort = "What are some of the reasons you are not taking medications to help you manage your HIV?: It's too much effort to take a pill every day",
    survey_hiv_posmeds_whynot_other = "What are some of the reasons you are not taking medications to help you manage your HIV?: Other"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

hiv_postx_why <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_hiv_postx_why = `hivp_tx_why`)
  variable_prefix <- "survey_hiv_postx_why"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_hiv_postx_why_Don't want anyone to know/stigma associated with the condition` = "survey_hiv_postx_why_stigma",
    `survey_hiv_postx_why_I can manage my HIV without a doctor's involvement` = "survey_hiv_postx_why_doctor",
    `survey_hiv_postx_why_I don't have health coverage` = "survey_hiv_postx_why_insurance",
    `survey_hiv_postx_why_It's too much effort to get to a clinic or doctor's office` = "survey_hiv_postx_why_effort"
  )
  
  new_labels <- c(
    survey_hiv_postx_why_stigma = "What are some of the reasons that you are not currently seeing a doctor or going to a clinic to help you manage your HIV?: Do not want anyone to know",
    survey_hiv_postx_why_doctor = "What are some of the reasons that you are not currently seeing a doctor or going to a clinic to help you manage your HIV?: Do not need doctors involvement",
    survey_hiv_postx_why_insurance = "What are some of the reasons that you are not currently seeing a doctor or going to a clinic to help you manage your HIV?: No health coverage",
    survey_hiv_postx_why_effort = "What are some of the reasons that you are not currently seeing a doctor or going to a clinic to help you manage your HIV?: Too much effort"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

template <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,scale_phealth = `chronicdx`) %>%
    mutate(scale_phealth = gsub(', such',' such',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', heart',' heart',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', arrhythmia',' arrhythmia',scale_phealth))
  variable_prefix <- "scale_phealth"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    
  )
  
  new_labels <- c(
    
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}


survey_testever <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_testever = `sni_testever`)
  variable_prefix <- "survey_testever"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_testever_Been tested for HIV` = "survey_testever_hiv",
    `survey_testever_Been tested for sexually transmitted diseases or infections (STD or STIs)` = "survey_testever_std",
    `survey_testever_None of the above` = "survey_testever_none"
  )
  
  new_labels <- c(
    survey_testever_hiv = "Which of the following have you done in your lifetime?: Been tested for HIV",
    survey_testever_std = "Which of the following have you done in your lifetime?: Been tested for STIs or STDs",
    survey_testever_none = "Which of the following have you done in your lifetime?: None of the above"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}


mse_engage <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_mse_engage = `mse_progengage`) %>%
    mutate(survey_mse_engage = gsub(', college',' college',survey_mse_engage))
  variable_prefix <- "survey_mse_engage"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_mse_engage_Art or music groups` = "survey_mse_engage_art",
    `survey_mse_engage_Counseling and support groups` = "survey_mse_engage_support",
    `survey_mse_engage_Education programs (GED college)` = "survey_mse_engage_school",
    `survey_mse_engage_I have not participated in any meaningful activities through youth services` = "survey_mse_engage_none",
    `survey_mse_engage_Job or college fair or tour` = "survey_mse_engage_jobfair",
    `survey_mse_engage_Job readiness training/employment services` = "survey_mse_engage_training",
    `survey_mse_engage_Legal clinic` = "survey_mse_engage_legal",
    `survey_mse_engage_Other (please specify)` = "survey_mse_engage_other",
    `survey_mse_engage_Paid internship/work experience` = "survey_mse_engage_work",
    `survey_mse_engage_Pregnancy and parenting classes` = "survey_mse_engage_parent",
    `survey_mse_engage_Yoga or meditation` = "survey_mse_engage_yoga"
  )
  
  new_labels <- c(
    survey_mse_engage_art = "During my time in youth services, I participated in one or more meaningful activities, such as: Art or music groups",
    survey_mse_engage_support = "During my time in youth services, I participated in one or more meaningful activities, such as: Counseling and support groups",
    survey_mse_engage_school = "During my time in youth services, I participated in one or more meaningful activities, such as: Education programs (GED college)",
    survey_mse_engage_none = "During my time in youth services, I participated in one or more meaningful activities, such as: I have not participated in any meaningful activities through youth services",
    survey_mse_engage_jobfair = "During my time in youth services, I participated in one or more meaningful activities, such as: Job or college fair or tour",
    survey_mse_engage_training = "During my time in youth services, I participated in one or more meaningful activities, such as: Job readiness training/employment services",
    survey_mse_engage_legal = "During my time in youth services, I participated in one or more meaningful activities, such as: Legal clinic",
    survey_mse_engage_other = "During my time in youth services, I participated in one or more meaningful activities, such as: Other (please specify)",
    survey_mse_engage_work = "During my time in youth services, I participated in one or more meaningful activities, such as: Paid internship/work experience",
    survey_mse_engage_parent = "During my time in youth services, I participated in one or more meaningful activities, such as: Pregnancy and parenting classes",
    survey_mse_engage_yoga = "During my time in youth services, I participated in one or more meaningful activities, such as: Yoga or meditation"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

  
needle_access <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_needle_access = `exch_where`)
  variable_prefix <- "history_needle_access"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_needle_access_A drop-in center like Safe Place for Youth` = "history_needle_dropin",
    `history_needle_access_A free health clinic` = "history_needle_freeclinic",
    `history_needle_access_A health center or appointment specific for treating HIV or Hepatitis C` = "history_needle_access_hivhepc",
    `history_needle_access_A mobile clinic` = "history_needle_mobileclinic",
    `history_needle_access_Hospital` = "history_needle_hospital",
    `history_needle_access_Other (please specify)` = "history_needle_other",
    `history_needle_access_Public health center` = "history_needle_publichealth"
  )
  
  new_labels <- c(
    history_needle_dropin = "Where have you ever accessed clean needles and/or exchanged used needles?: A drop-in center like Safe Place for Youth",
    history_needle_freeclinic = "Where have you ever accessed clean needles and/or exchanged used needles?: A free health clinic",
    history_needle_access_hivhepc = "Where have you ever accessed clean needles and/or exchanged used needles?: A health center or appointment specific for treating HIV or Hepatitis C",
    history_needle_mobileclinic = "Where have you ever accessed clean needles and/or exchanged used needles?: A mobile clinic",
    history_needle_hospital = "Where have you ever accessed clean needles and/or exchanged used needles?: Hospital",
    history_needle_other = "Where have you ever accessed clean needles and/or exchanged used needles?: Other (please specify)",
    history_needle_publichealth = "Where have you ever accessed clean needles and/or exchanged used needles?: Public health center"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_accessonly <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_accessonly = `tech_access_noown`) %>%
    mutate(techuse_accessonly = gsub(', or',' or',techuse_accessonly)) %>%
    mutate(techuse_accessonly = gsub(', Samsung',' Samsung',techuse_accessonly)) 
  variable_prefix <- "techuse_accessonly"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_accessonly_A cell phone that is not a smartphone` = "techuse_accessonly_basicphone",
    `techuse_accessonly_A desktop or laptop computer` = "techuse_accessonly_computer",
    `techuse_accessonly_A smartphone` = "techuse_accessonly_smartphone",
    `techuse_accessonly_A tablet computer like an iPad Samsung Galaxy Tablet or Kindle Fire` = "techuse_accessonly_tablet",
    `techuse_accessonly_None` = "techuse_accessonly_none"
  )
  
  new_labels <- c(
    techuse_accessonly_basicphone = "Which of the following items do you have access to, but do not personally own?: A cell phone that is not a smartphone",
    techuse_accessonly_computer = "Which of the following items do you have access to, but do not personally own?: A desktop or laptop computer",
    techuse_accessonly_smartphone = "Which of the following items do you have access to, but do not personally own?: A smartphone",
    techuse_accessonly_tablet = "Which of the following items do you have access to, but do not personally own?: A tablet computer like an iPad",
    techuse_accessonly_none = "Which of the following items do you have access to, but do not personally own?: None"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_cellbill <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_cellbill = `cell_bill`)
  variable_prefix <- "techuse_cellbill"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_cellbill_Another family member of mine (please list):` = "techuse_cellbill_otherfamily",
    `techuse_cellbill_I do` = "techuse_cellbill_self",
    `techuse_cellbill_My boss` = "techuse_cellbill_boss",
    `techuse_cellbill_My boyfriend/girlfriend/significant other` = "techuse_cellbill_partner",
    `techuse_cellbill_My brother/sister` = "techuse_cellbill_sibling",
    `techuse_cellbill_My friend` = "techuse_cellbill_friend",
    `techuse_cellbill_My parent` = "techuse_cellbill_parent",
    `techuse_cellbill_Other (please list):` = "techuse_cellbill_other"
  )
  
  new_labels <- c(
    techuse_cellbill_otherfamily = "Who pays your cell phone bill?: Another family member of mine (please list):",
    techuse_cellbill_self = "Who pays your cell phone bill?: I do",
    techuse_cellbill_boss = "Who pays your cell phone bill?: My boss",
    techuse_cellbill_partner = "Who pays your cell phone bill?: My boyfriend/girlfriend/significant other",
    techuse_cellbill_sibling = "Who pays your cell phone bill?: My brother/sister",
    techuse_cellbill_friend = "Who pays your cell phone bill?: My friend",
    techuse_cellbill_parent = "Who pays your cell phone bill?: My parent",
    techuse_cellbill_other = "Who pays your cell phone bill?: Other (please list):"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_chargeaccess <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_chargeaccess = `cell_charge_access`) %>%
    mutate(techuse_chargeaccess = gsub(', c',' c',techuse_chargeaccess)) %>%
    mutate(techuse_chargeaccess = gsub(', s',' s',techuse_chargeaccess)) 
  variable_prefix <- "techuse_chargeaccess"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_chargeaccess_Family member of relative’s apartment/house` = "techuse_chargeaccess_familyhouse",
    `techuse_chargeaccess_Friend or someone’s apartment/house` = "techuse_chargeaccess_friendhouse",
    `techuse_chargeaccess_I do not need electricity` = "techuse_chargeaccess_noneed",
    `techuse_chargeaccess_My place of stay` = "techuse_chargeaccess_myplace",
    `techuse_chargeaccess_Power outlet on the outside of a building` = "techuse_chargeaccess_outside",
    `techuse_chargeaccess_Public library` = "techuse_chargeaccess_library",
    `techuse_chargeaccess_Public space with WiFi access (restaurant store coffee shop)` = "techuse_chargeaccess_publicspace",
    `techuse_chargeaccess_School` = "techuse_chargeaccess_school",
    `techuse_chargeaccess_Work` = "techuse_chargeaccess_work",
    `techuse_chargeaccess_Youth service agency` = "techuse_chargeaccess_ysagency"
  )
  
  new_labels <- c(
    techuse_chargeaccess_familyhouse = "Where do you get access to electricity?: Family member of relative’s apartment/house",
    techuse_chargeaccess_friendhouse = "Where do you get access to electricity?: Friend or someone’s apartment/house",
    techuse_chargeaccess_noneed = "Where do you get access to electricity?: I do not need electricity",
    techuse_chargeaccess_myplace = "Where do you get access to electricity?: My place of stay",
    techuse_chargeaccess_outside = "Where do you get access to electricity?: Power outlet on the outside of a building",
    techuse_chargeaccess_library = "Where do you get access to electricity?: Public library",
    techuse_chargeaccess_publicspace = "Where do you get access to electricity?: Public space with WiFi access (restaurant store coffee shop)",
    techuse_chargeaccess_school = "Where do you get access to electricity?: School",
    techuse_chargeaccess_work = "Where do you get access to electricity?: Work",
    techuse_chargeaccess_ysagency = "Where do you get access to electricity?: Youth service agency"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_celluse <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_celluse = `cel_use`) %>%
    mutate(techuse_celluse = gsub(', drop',' drop',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', Apple',' Apple',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', etc',' etc',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', food',' food',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', Grindr',' Grindr',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', Instagram',' Instagram',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', OkCupid',' OkCupid',techuse_celluse)) %>%
    mutate(techuse_celluse = gsub(', Twitter',' Twitter',techuse_celluse)) 
  variable_prefix <- "techuse_celluse"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_celluse_Other (please specify):` = "techuse_celluse_other",
    `techuse_celluse_To browse the Internet` = "techuse_celluse_internet",
    `techuse_celluse_To call/text family` = "techuse_celluse_family",
    `techuse_celluse_To call/text friends` = "techuse_celluse_friends",
    `techuse_celluse_To check my email/write emails` = "techuse_celluse_email",
    `techuse_celluse_To check my social media accounts (Facebook Twitter Instagram etc.)` = "techuse_celluse_socialmedia",
    `techuse_celluse_To listen to music` = "techuse_celluse_music",
    `techuse_celluse_To look for employment` = "techuse_celluse_jobs",
    `techuse_celluse_To look for support services (shelters drop-in centers food pantries etc.)` = "techuse_celluse_services",
    `techuse_celluse_To look up directions/transportation options (Google Maps Apple Maps etc.)` = "techuse_celluse_directions",
    `techuse_celluse_To make emergency phone calls only` = "techuse_celluse_emergency",
    `techuse_celluse_To stream/watch videos` = "techuse_celluse_videos",
    `techuse_celluse_To use a dating app (Tinder Grindr OkCupid etc.)` = "techuse_celluse_datingapps"
  )
  
  new_labels <- c(
    techuse_celluse_other = "How do you use your phone?: Other (please specify):",
    techuse_celluse_internet = "How do you use your phone?: To browse the Internet",
    techuse_celluse_family = "How do you use your phone?: To call/text family",
    techuse_celluse_friends = "How do you use your phone?: To call/text friends",
    techuse_celluse_email = "How do you use your phone?: To check my email/write emails",
    techuse_celluse_socialmedia = "How do you use your phone?: To check my social media accounts (Facebook Twitter Instagram etc.)",
    techuse_celluse_music = "How do you use your phone?: To listen to music",
    techuse_celluse_jobs = "How do you use your phone?: To look for employment",
    techuse_celluse_services = "How do you use your phone?: To look for support services (shelters drop-in centers food pantries etc.)",
    techuse_celluse_directions = "How do you use your phone?: To look up directions/transportation options (Google Maps Apple Maps etc.)",
    techuse_celluse_emergency = "How do you use your phone?: To make emergency phone calls only",
    techuse_celluse_videos = "How do you use your phone?: To stream/watch videos",
    techuse_celluse_datingapps = "How do you use your phone?: To use a dating app (Tinder Grindr OkCupid etc.)"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_barriers <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_barriers = `tech_barriers`) %>%
    mutate(techuse_barriers = gsub(', food',' food',techuse_barriers)) %>%
    mutate(techuse_barriers = gsub(', etc',' etc',techuse_barriers)) %>%
    mutate(techuse_barriers = gsub(', drop',' drop',techuse_barriers))
  variable_prefix <- "techuse_barriers"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_barriers_It would be easier to access emergency services` = "techuse_barriers_emergency",
    `techuse_barriers_It would be easier to date/meet people` = "techuse_barriers_date",
    `techuse_barriers_It would be easier to find employment` = "techuse_barriers_jobs",
    `techuse_barriers_It would be easier to find my way around and look up transportation options/times` = "techuse_barriers_transport",
    `techuse_barriers_It would be easier to find support services (shelters drop-in centers food pantries etc.)` = "techuse_barriers_services",
    `techuse_barriers_It would be easier to keep in touch with family` = "techuse_barriers_family",
    `techuse_barriers_It would be easier to keep in touch with friends` = "techuse_barriers_friends",
    `techuse_barriers_It would be easier to listen to music` = "techuse_barriers_music",
    `techuse_barriers_It would be easier to watch videos` = "techuse_barriers_videos",
    `techuse_barriers_Other (please specify):` = "techuse_barriers_other"
  )
  
  new_labels <- c(
    techuse_barriers_emergency = "In what ways do you think owning a phone would make life easier?: It would be easier to access emergency services",
    techuse_barriers_date = "In what ways do you think owning a phone would make life easier?: It would be easier to date/meet people",
    techuse_barriers_jobs = "In what ways do you think owning a phone would make life easier?: It would be easier to find employment",
    techuse_barriers_transport = "In what ways do you think owning a phone would make life easier?: It would be easier to find my way around and look up transportation options/times",
    techuse_barriers_services = "In what ways do you think owning a phone would make life easier?: It would be easier to find support services (shelters drop-in centers food pantries etc.)",
    techuse_barriers_family = "In what ways do you think owning a phone would make life easier?: It would be easier to keep in touch with family",
    techuse_barriers_friends = "In what ways do you think owning a phone would make life easier?: It would be easier to keep in touch with friends",
    techuse_barriers_music = "In what ways do you think owning a phone would make life easier?: It would be easier to listen to music",
    techuse_barriers_videos = "In what ways do you think owning a phone would make life easier?: It would be easier to watch videos",
    techuse_barriers_other = "In what ways do you think owning a phone would make life easier?: Other (please specify):"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

police_reason <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_police_reason = `policein_3mo_why`)
  variable_prefix <- "history_police_reason"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_police_reason_I called them for myself` = "history_police_callself",
    `history_police_reason_I called them on behalf of someone else` = "history_police_callother",
    `history_police_reason_I was stopped because of who I am` = "history_police_stopwhoiam",
    `history_police_reason_I was stopped for a suspected crime` = "history_police_stopcrime",
    `history_police_reason_I was stopped for no apparent reason` = "history_police_stopnoreason",
    `history_police_reason_Other` = "history_police_other"
  )
  
  new_labels <- c(
    history_police_callself = "In what context(s) have you interacted with police in the past 3 months?: I called them for myself",
    history_police_callother = "In what context(s) have you interacted with police in the past 3 months?: I called them on behalf of someone else",
    history_police_stopwhoiam = "In what context(s) have you interacted with police in the past 3 months?: I was stopped because of who I am",
    history_police_stopcrime = "In what context(s) have you interacted with police in the past 3 months?: I was stopped for a suspected crime",
    history_police_stopnoreason = "In what context(s) have you interacted with police in the past 3 months?: I was stopped for no apparent reason",
    history_police_other = "In what context(s) have you interacted with police in the past 3 months?: Other"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

marijuana_type <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,drugs_marijuana_type = `marj_30_types`) %>%
    mutate(drugs_marijuana_type = gsub('\"','',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub('”','',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub('“','',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', b',' b',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', d',' d',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', e',' e',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', f',' f',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', h',' h',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', s',' s',drugs_marijuana_type)) %>%
    mutate(drugs_marijuana_type = gsub(', w',' w',drugs_marijuana_type)) 
  variable_prefix <- "drugs_marijuana_type"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `drugs_marijuana_type_Concentrates (i.e. hashish hash oil wax shatter dabs etc.)` = "drugs_marijuana_type_wax",
    `drugs_marijuana_type_Dried plant material (i.e. bud flowers herbs etc.)` = "drugs_marijuana_type_dry",
    `drugs_marijuana_type_Edibles/drinkables (homemade or bought at a dispensary/store)` = "drugs_marijuana_type_edibles",
    `drugs_marijuana_type_Other type(s) (please specify)` = "drugs_marijuana_type_other",
    `drugs_marijuana_type_Spray/drops` = "drugs_marijuana_type_spray"
  )
  
  new_labels <- c(
    drugs_marijuana_type_wax = "What are the different forms of marijuana you used in the past 30 days?: Concentrates (i.e. hashish hash oil wax shatter dabs etc.)",
    drugs_marijuana_type_dry = "What are the different forms of marijuana you used in the past 30 days?: Dried plant material (i.e. bud flowers herbs etc.)",
    drugs_marijuana_type_edibles = "What are the different forms of marijuana you used in the past 30 days?: Edibles/drinkables (homemade or bought at a dispensary/store)",
    drugs_marijuana_type_other = "What are the different forms of marijuana you used in the past 30 days?: Other type(s) (please specify)",
    drugs_marijuana_type_spray = "What are the different forms of marijuana you used in the past 30 days?: Spray/drops"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_locations <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_locations = `tech_access_where`)
  variable_prefix <- "techuse_locations"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_locations_Borrowed from a friend` = "techuse_locations_friend",
    `techuse_locations_Internet café` = "techuse_locations_café",
    `techuse_locations_Public library` = "techuse_locations_library",
    `techuse_locations_Where I’m staying` = "techuse_locations_home",
    `techuse_locations_Youth service agency` = "techuse_locations_ysagency",
    `techuse_locations_Family (biological or foster)` = "techuse_locations_family",
    `techuse_locations_Other (please specify)` = "techuse_locations_other",
    `techuse_locations_School` = "techuse_locations_school",
    `techuse_locations_Work` = "techuse_locations_work"
  )
  
  new_labels <- c(
    techuse_locations_friend = "Where do you get access to those items selected above?: Borrowed from a friend",
    techuse_locations_café = "Where do you get access to those items selected above?: Internet café",
    techuse_locations_library = "Where do you get access to those items selected above?: Public library",
    techuse_locations_home = "Where do you get access to those items selected above?: Where I’m staying",
    techuse_locations_ysagency = "Where do you get access to those items selected above?: Youth service agency",
    techuse_locations_family = "Where do you get access to those items selected above?: Family (biological or foster)",
    techuse_locations_other = "Where do you get access to those items selected above?: Other (please specify)",
    techuse_locations_school = "Where do you get access to those items selected above?: School",
    techuse_locations_work = "Where do you get access to those items selected above?: Work"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

marijuana_access <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,drugs_marijuana_access = `marj_access`) %>%
    mutate(drugs_marijuana_access = gsub(', or',' or',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(', friend',' friend',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(', family',' family',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(',,',',',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(', ,',',',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(';','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub('/delivery service','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub('/collective storefront','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub('/collective','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub('/dispensary','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(' or delivery service','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(' or recreational','',drugs_marijuana_access)) %>%
    mutate(drugs_marijuana_access = gsub(' medical','',drugs_marijuana_access)) 
  variable_prefix <- "drugs_marijuana_access"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `drugs_marijuana_access_A drug dealer friend family member or someone else obtained it from a source other than a marijuana dispensary` = "drugs_marijuana_access_othersourcerec",
    #`drugs_marijuana_access_A drug dealer friend family member or someone else obtained it from a source other than a medical marijuana dispensary/delivery service` = "drugs_marijuana_access_othersourcemed",
    `drugs_marijuana_access_A friend gave it to me for free` = "drugs_marijuana_access_free",
    `drugs_marijuana_access_Click to write Choice 6` = "drugs_marijuana_access_choice6",
    `drugs_marijuana_access_Click to write Choice 7` = "drugs_marijuana_access_choice7",
    `drugs_marijuana_access_Click to write Choice 8` = "drugs_marijuana_access_choice8",
    `drugs_marijuana_access_Dealer (bought informally)` = "drugs_marijuana_access_dealerinformal",
    `drugs_marijuana_access_Drug dealer bought it for me from a marijuana dispensary` = "drugs_marijuana_access_dealerdispensemedonly",
    #`drugs_marijuana_access_Drug dealer bought it for me from a medical or recreational marijuana dispensary/delivery service` = "drugs_marijuana_access_dealerdispensemedrec",
    `drugs_marijuana_access_Family member obtained it for me from a marijuana dispensary` = "drugs_marijuana_access_familymedonly",
    #`drugs_marijuana_access_Family member obtained it for me from a medical or recreational marijuana dispensary/delivery service` = "drugs_marijuana_access_familymedorrec",
    `drugs_marijuana_access_Friend obtained it for me from a marijuana dispensary` = "drugs_marijuana_access_friendmedonly",
    #`drugs_marijuana_access_Friend obtained it for me from a medical or recreational marijuana dispensary/delivery service` = "drugs_marijuana_access_friendmedorrec",
    `drugs_marijuana_access_I bought from a marijuana delivery service` = "drugs_marijuana_access_selfmedicaldelivery",
    `drugs_marijuana_access_I bought from a marijuana dispensary` = "drugs_marijuana_access_selfmedicalbm",
    `drugs_marijuana_access_I bought from a recreational marijuana shop` = "drugs_marijuana_access_selfrecreational",
    #`drugs_marijuana_access_I bought from a recreational marijuana shop/dispensary/collective or delivery service` = "drugs_marijuana_access_selfrecreationaldelivery",
    `drugs_marijuana_access_I have grown my own plants` = "drugs_marijuana_access_grown",
    `drugs_marijuana_access_Other (please specify)` = "drugs_marijuana_access_other"
  )
  
  new_labels <- c(
    drugs_marijuana_access_othersourcerec = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: A drug dealer friend family member or someone else obtained it from a source other than a marijuana dispensary/delivery service",
    #drugs_marijuana_access_othersourcemed = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: A drug dealer friend family member or someone else obtained it from a source other than a medical marijuana dispensary/delivery service",
    drugs_marijuana_access_free = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: A friend gave it to me for free",
    drugs_marijuana_access_choice6 = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Click to write Choice 6",
    drugs_marijuana_access_choice7 = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Click to write Choice 7",
    drugs_marijuana_access_choice8 = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Click to write Choice 8",
    drugs_marijuana_access_dealerinformal = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Dealer (bought informally)",
    drugs_marijuana_access_dealerdispensemedonly = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Drug dealer bought it for me from a medical marijuana dispensary/delivery service",
    #drugs_marijuana_access_dealerdispensemedrec = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Drug dealer bought it for me from a medical or recreational marijuana dispensary/delivery service",
    drugs_marijuana_access_familymedonly = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Family member obtained it for me from a medical marijuana dispensary/delivery service",
    #drugs_marijuana_access_familymedorrec = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Family member obtained it for me from a medical or recreational marijuana dispensary/delivery service",
    drugs_marijuana_access_friendmedonly = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Friend obtained it for me from a medical marijuana dispensary/delivery service",
    #drugs_marijuana_access_friendmedorrec = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Friend obtained it for me from a medical or recreational marijuana dispensary/delivery service",
    drugs_marijuana_access_selfmedicaldelivery = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: I bought from a medical marijuana delivery service",
    drugs_marijuana_access_selfmedicalbm = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: I bought from a medical marijuana dispensary/collective storefront",
    drugs_marijuana_access_selfrecreational = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: I bought from a recreational marijuana shop/dispensary/collective",
    #drugs_marijuana_access_selfrecreationaldelivery = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: I bought from a recreational marijuana shop/dispensary/collective or delivery service",
    drugs_marijuana_access_grown = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: I have grown my own plants",
    drugs_marijuana_access_other = "Which of the following persons or places have you bought or received marijuana from in the past 30 days?: Other (please specify)"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  
  new_return <- return_data %>%
    select(-drugs_marijuana_access_choice6,-drugs_marijuana_access_choice7,-drugs_marijuana_access_choice8,
           -drugs_marijuana_access_free,-drugs_marijuana_access_dealerinformal)
  
  return(new_return)
}

hallucinogen_type <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,drugs_halluc_type = `halluc_30_types`) %>%
    mutate(drugs_halluc_type = gsub('”','',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub('“','',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', acid',' acid',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', shrooms',' shrooms',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', dim',' dim',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', Dimitri',' Dimitri',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', magi',' magi',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', yakee',' yakee',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', yopo',' yopo',drugs_halluc_type)) %>%
    mutate(drugs_halluc_type = gsub(', lys',' lys',drugs_halluc_type)) 
  variable_prefix <- "drugs_halluc_type"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `drugs_halluc_type_DMT (aka dimethyltryptamine Dimitri yakee yopo)` = "drugs_halluc_type_dmt",
    `drugs_halluc_type_LSD (aka acid lysergic acid diethylamide)` = "drugs_halluc_type_lsd",
    `drugs_halluc_type_Psilocybin mushrooms (aka magical mushrooms shrooms)` = "drugs_halluc_type_mushrooms"
  )
  
  new_labels <- c(
    drugs_halluc_type_dmt = "During the past 30 days, what types of psychedelics/hallucinogens did you use?: DMT",
    drugs_halluc_type_lsd = "During the past 30 days, what types of psychedelics/hallucinogens did you use?: LSD",
    drugs_halluc_type_mushrooms = "During the past 30 days, what types of psychedelics/hallucinogens did you use?: Mushrooms"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

rxmisuse_type <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,drugs_rxmisuse_type = `pdm_30_types`) %>%
    mutate(drugs_rxmisuse_type = gsub(', bu',' bu',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', etc',' etc',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', fent',' fent',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', hydro',' hydro',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', meth',' meth',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', oxy',' oxy',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', oxy',' oxy',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', p',' p',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', A',' A',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', C',' C',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', D',' D',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', G',' G',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', L',' L',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', Per',' Per',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', R',' R',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', S',' S',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', V',' V',drugs_rxmisuse_type)) %>%
    mutate(drugs_rxmisuse_type = gsub(', X',' X',drugs_rxmisuse_type)) 
  variable_prefix <- "drugs_rxmisuse_type"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `drugs_rxmisuse_type_Prescription opioids (for example fentanyl oxycodone [OxyContin Percocet] hydrocodone [Vicodin] methadone buprenorphine etc.)` = "drugs_rxmisuse_opioids",
    `drugs_rxmisuse_type_Prescription sedatives or sleeping pills (for example Valium Serepax Ativan Xanax Librium Rohypnol GHB etc.)`= "drugs_rxmisuse_sedatives",              
    `drugs_rxmisuse_type_Prescription stimulants (for example Ritalin Concerta Dexedrine/dextroamphetamine Adderall/amphetamine salts prescription diet pills etc.)`= "drugs_rxmisuse_stimulants",
    `drugs_rxmisuse_type_Something else not listed here (please specify):` = "drugs_rxmisuse_other"
  )
  
  new_labels <- c(
    drugs_rxmisuse_opioids = "In the past 30 days, what types of prescription drugs have you used that were not prescribed, or in higher doses than prescribed?: Opioids",
    drugs_rxmisuse_sedatives = "In the past 30 days, what types of prescription drugs have you used that were not prescribed, or in higher doses than prescribed?: Sedatives or sleeping pills",
    drugs_rxmisuse_stimulants = "In the past 30 days, what types of prescription drugs have you used that were not prescribed, or in higher doses than prescribed?: Stimulants ",
    drugs_rxmisuse_other = "In the past 30 days, what types of prescription drugs have you used that were not prescribed, or in higher doses than prescribed?: Other "
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

roa_marijuana <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,drugs_roa_marijuana = `marj_30_roa`) %>%
    mutate(drugs_roa_marijuana = gsub('”','',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub('“','',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub('"','',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', b',' b',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', G Pen',' G Pen',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', c',' c',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', e',' e',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', Kandy',' Kandy',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', o',' o',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', t',' t',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', Volcano',' Volcano',drugs_roa_marijuana)) %>%
    mutate(drugs_roa_marijuana = gsub(', w',' w',drugs_roa_marijuana)) 
  variable_prefix <- "drugs_roa_marijuana"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `drugs_roa_marijuana_Other (please specify):` = "drugs_roa_marijuana_other",
    `drugs_roa_marijuana_Smoked in a blunt (dried marijuana rolled in a cigarillo or cigar)` = "drugs_roa_marijuana_blunt",
    `drugs_roa_marijuana_Smoked in a rolled joint (not combined with tobacco)` = "drugs_roa_marijuana_joint",
    `drugs_roa_marijuana_Smoked in a skiff/spliff (marijuana and tobacco mixed together)` = "drugs_roa_marijuana_mixed",
    `drugs_roa_marijuana_Smoked using a bong water pipe bubbler etc.` = "drugs_roa_marijuana_bong",
    `drugs_roa_marijuana_Smoked using a pipe bowl chillum one-hitter etc.` = "drugs_roa_marijuana_bowl",
    `drugs_roa_marijuana_Swallowed an edible or drinkable` = "drugs_roa_marijuana_edible",
    `drugs_roa_marijuana_Vaped dried plant material using a vaporizer (e.g. e-joint Volcano)` = "drugs_roa_marijuana_vaporizer",
    `drugs_roa_marijuana_Vaped hash oil or other concentrates using a vape pen (e.g. G Pen KandyPen)` = "drugs_roa_marijuana_vapepen",
    `drugs_roa_marijuana_Vaped hash oil or other concentrates using an oil rig or nail (e.g. took dabs)` = "drugs_roa_marijuana_oilrig"
  )
  
  new_labels <- c(
    drugs_roa_marijuana_other = "What are the different ways that you have used marijuana in the past 30 days?: Other (please specify):",
    drugs_roa_marijuana_blunt = "What are the different ways that you have used marijuana in the past 30 days?: Smoked in a blunt (dried marijuana rolled in a cigarillo or cigar)",
    drugs_roa_marijuana_joint = "What are the different ways that you have used marijuana in the past 30 days?: Smoked in a rolled joint (not combined with tobacco)",
    drugs_roa_marijuana_mixed = "What are the different ways that you have used marijuana in the past 30 days?: Smoked in a skiff/spliff (marijuana and tobacco mixed together)",
    drugs_roa_marijuana_bong = "What are the different ways that you have used marijuana in the past 30 days?: Smoked using a bong water pipe bubbler etc.",
    drugs_roa_marijuana_bowl = "What are the different ways that you have used marijuana in the past 30 days?: Smoked using a pipe bowl chillum one-hitter etc.",
    drugs_roa_marijuana_edible = "What are the different ways that you have used marijuana in the past 30 days?: Swallowed an edible or drinkable",
    drugs_roa_marijuana_vaporizer = "What are the different ways that you have used marijuana in the past 30 days?: Vaped dried plant material using a vaporizer (e.g. e-joint Volcano)",
    drugs_roa_marijuana_vapepen = "What are the different ways that you have used marijuana in the past 30 days?: Vaped hash oil or other concentrates using a vape pen (e.g. G Pen KandyPen)",
    drugs_roa_marijuana_oilrig = "What are the different ways that you have used marijuana in the past 30 days?: Vaped hash oil or other concentrates using an oil rig or nail (e.g. took dabs)" 
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

techuse_techowned <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,techuse_techowned = `techaccess`) %>%
    mutate(techuse_techowned = gsub(', or',' or',techuse_techowned)) %>%
    mutate(techuse_techowned = gsub(', Samsung',' Samsung',techuse_techowned))
  variable_prefix <- "techuse_techowned"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `techuse_techowned_A cell phone that is not a smartphone` = "techuse_techowned_basicphone",
    `techuse_techowned_A desktop or laptop computer` = "techuse_techowned_computer",
    `techuse_techowned_A smartphone` = "techuse_techowned_smartphone",
    `techuse_techowned_A tablet computer like an iPad Samsung Galaxy Tablet or Kindle Fire` = "techuse_techowned_tablet",
    `techuse_techowned_None` = "techuse_techowned_none"
  )
  
  new_labels <- c(
    techuse_techowned_basicphone = "Do you, personally, own any of the following items currently?: A cell phone that is not a smartphone",
    techuse_techowned_computer = "Do you, personally, own any of the following items currently?: A desktop or laptop computer",
    techuse_techowned_smartphone = "Do you, personally, own any of the following items currently?: A smartphone",
    techuse_techowned_tablet = "Do you, personally, own any of the following items currently?: A tablet computer like an iPad",
    techuse_techowned_none = "Do you, personally, own any of the following items currently?: None"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

gang_closest <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_gang_closest = `gang_aff`) %>%
    mutate(history_gang_closest = gsub(', etc',' etc',history_gang_closest)) %>%
    mutate(history_gang_closest = gsub(', fia',' fia',history_gang_closest)) %>%
    mutate(history_gang_closest = gsub(', hook',' hook',history_gang_closest)) %>%
    mutate(history_gang_closest = gsub(', husband',' husband',history_gang_closest))
  variable_prefix <- "history_gang_closest"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_gang_closest_Close friend` = "history_gang_closest_friend",
    `history_gang_closest_Family member` = "history_gang_closest_family",                                   
    `history_gang_closest_No one important in my life is a gang member` = "history_gang_closest_none",
    `history_gang_closest_Other (specify):` = "history_gang_closest_other",
    `history_gang_closest_Romantic/intimate partner (boyfriend/girlfriend husband/wife fiancé hookup etc.)` = "history_gang_closest_partner"
  )
  
  new_labels <- c(
    history_gang_closest_friend = "Do you have someone important in your life that is currently a member of a gang?: Close friend",
    history_gang_closest_family = "Do you have someone important in your life that is currently a member of a gang?: Family member",
    history_gang_closest_none = "Do you have someone important in your life that is currently a member of a gang?: No one important in my life is a gang member",
    history_gang_closest_other = "Do you have someone important in your life that is currently a member of a gang?: Other (specify)",
    history_gang_closest_partner = "Do you have someone important in your life that is currently a member of a gang?: Romantic or intimate partner"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

drugs_everuse <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,drugs_everuse = `sutypes_use_ever`) %>%
    mutate(drugs_everuse = gsub('“','',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub('”','',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', etc',' etc',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', shrooms',' shrooms',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', fake',' fake',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', markers',' markers',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', or',' or',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', paint',' paint',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', spice',' spice',drugs_everuse)) %>%
    mutate(drugs_everuse = gsub(', vaped',' vaped',drugs_everuse))
  variable_prefix <- "drugs_everuse"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `drugs_everuse_Alcohol` = "drugs_everuse_alcohol",
    `drugs_everuse_Cocaine (powdered)` = "drugs_everuse_cocaine",
    `drugs_everuse_Crack / Freebase Cocaine` = "drugs_everuse_crack",
    `drugs_everuse_Ecstasy / MDMA / Molly` = "drugs_everuse_mdma",
    `drugs_everuse_Hallucinogens/psychedelics (LSD/acid shrooms etc)` = "drugs_everuse_hallucinogens",
    `drugs_everuse_Heroin` = "drugs_everuse_heroin",
    `drugs_everuse_I have not used any substances in my lifetime.` = "drugs_everuse_none",
    `drugs_everuse_Inhalants (sniffed/huffed glue paint markers etc.)` = "drugs_everuse_inhalant",
    `drugs_everuse_Ketamine` = "drugs_everuse_ketamine",
    `drugs_everuse_Marijuana` = "drugs_everuse_marijuana",
    `drugs_everuse_Meth` = "drugs_everuse_meth",
    `drugs_everuse_Nitrous Oxide (whippets/laughing gas/Hippy Crack/etc.)` = "drugs_everuse_nitrous",
    `drugs_everuse_PCP` = "drugs_everuse_pcp",
    `drugs_everuse_Prescription drugs that were not prescribed or in higher doses than prescribed` = "drugs_everuse_rxmisuse",
    `drugs_everuse_Something else not listed here` = "drugs_everuse_other",
    `drugs_everuse_Steroid pills or shots without a prescription` = "drugs_everuse_steroids",
    `drugs_everuse_Synthetic marijuana (K2 spice fake weed)` = "drugs_everuse_syntheticmj",
    `drugs_everuse_Tobacco (smoked vaped or chewing tobacco)` = "drugs_everuse_tobacco"
  )
  
  new_labels <- c(
    drugs_everuse_alcohol = "Which of the following substances have you used in your lifetime?: Alcohol",
    drugs_everuse_cocaine = "Which of the following substances have you used in your lifetime?: Cocaine (powdered)",
    drugs_everuse_crack = "Which of the following substances have you used in your lifetime?: Crack / Freebase Cocaine",
    drugs_everuse_mdma = "Which of the following substances have you used in your lifetime?: Ecstasy / MDMA / Molly",
    drugs_everuse_hallucinogens = "Which of the following substances have you used in your lifetime?: Hallucinogens/psychedelics (LSD/acid shrooms etc)",
    drugs_everuse_heroin = "Which of the following substances have you used in your lifetime?: Heroin",
    drugs_everuse_none = "Which of the following substances have you used in your lifetime?: I have not used any substances in my lifetime.",
    drugs_everuse_inhalant = "Which of the following substances have you used in your lifetime?: Inhalants (sniffed/huffed glue paint markers etc.)",
    drugs_everuse_ketamine = "Which of the following substances have you used in your lifetime?: Ketamine",
    drugs_everuse_marijuana = "Which of the following substances have you used in your lifetime?: Marijuana",
    drugs_everuse_meth = "Which of the following substances have you used in your lifetime?: Meth",
    drugs_everuse_nitrous = "Which of the following substances have you used in your lifetime?: Nitrous Oxide (whippets/laughing gas/Hippy Crack/etc.)",
    drugs_everuse_pcp = "Which of the following substances have you used in your lifetime?: PCP",
    drugs_everuse_rxmisuse = "Which of the following substances have you used in your lifetime?: Prescription drugs that were not prescribed or in higher doses than prescribed",
    drugs_everuse_other = "Which of the following substances have you used in your lifetime?: Something else not listed here",
    drugs_everuse_steroids = "Which of the following substances have you used in your lifetime?: Steroid pills or shots without rx",
    drugs_everuse_syntheticmj = "Which of the following substances have you used in your lifetime?: Synthetic marijuana (K2 spice fake weed)",
    drugs_everuse_tobacco = "Which of the following substances have you used in your lifetime?: Tobacco (smoked vaped or chewing tobacco)"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

drugs_roa_abstracted <- function(filtered_baseline,drug_name){
  filtered_data <- filtered_baseline %>%
    rename(drugs_roa_meth = `meth_30_roa`,
           drugs_roa_mdma = `mdma_30_roa`,
           drugs_roa_rxmisuse = `pdm_30_roa`,
           drugs_roa_heroin = `heroin_30_roa`,
           drugs_roa_cocaine = `cocaine_30_roa`,
           drugs_roa_crack = `crack_30_roa`,
           drugs_roa_steroid = `steroid_30_roa`,
           drugs_roa_pcp = `pcp_30_roa`,
           drugs_roa_other = `othersu_30_roa`) %>%
    mutate(drugs_roa_other = gsub(' \\(please specify\\)\\:','',drugs_roa_other)) %>%
    mutate(drugs_roa_cocaine = gsub('Orally \\(swallowed, chewed, rubbed on gums, etc\\.\\)','Swallowed',drugs_roa_cocaine))
  variable_prefix <- paste0("drugs_roa_",drug_name)
  separator <- ","
  if(!all(is.na(eval(parse(text = paste0("filtered_data$drugs_roa_",drug_name)))))){
    prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  }
  
  pre_new_names <- c(
    paste0(variable_prefix,"_Injected"),
    paste0(variable_prefix,"_Smoked"),
    paste0(variable_prefix,"_Snorted"),
    paste0(variable_prefix,"_Swallowed"),
    paste0(variable_prefix,"_Other")
  )
  new_names <- tolower(pre_new_names)
  names(new_names) <- pre_new_names
  
  new_labels <- c(
    paste0("In the past 30 days, how have you used ",drug_name,"?: Injected"),
    paste0("In the past 30 days, how have you used ",drug_name,"?: Smoked"),
    paste0("In the past 30 days, how have you used ",drug_name,"?: Snorted"),
    paste0("In the past 30 days, how have you used ",drug_name,"?: Swallowed"),
    paste0("In the past 30 days, how have you used ",drug_name,"?: Other")
  )
  names(new_labels) <- new_names
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  
  if(drug_name == "crack"){
    return_data <- select(return_data,-drugs_roa_crack_swallowed)
  }
  if(drug_name == "heroin"){
    return_data <- select(return_data,-drugs_roa_heroin_swallowed)
  }
  if(drug_name == "mdma"){
    return_data <- select(return_data,-drugs_roa_mdma_smoked)
  }
  if(drug_name == "steroid"){
    return_data <- select(return_data,-drugs_roa_steroid_smoked)
  }
  if(drug_name == "steroid"){
    return_data <- select(return_data,-drugs_roa_steroid_snorted)
  }
  
  return(return_data)
}

tobacco_typeused <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,tobacco_typeused = `tobacco_types`) %>%
    mutate(tobacco_typeused = gsub(', hookah',' hookah',tobacco_typeused)) %>%
    mutate(tobacco_typeused = gsub(', etc',' etc',tobacco_typeused)) %>%
    mutate(tobacco_typeused = gsub(', pipes',' pipes',tobacco_typeused)) %>%
    mutate(tobacco_typeused = gsub(', cigars',' cigars',tobacco_typeused))
  variable_prefix <- "tobacco_typeused"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `tobacco_typeused_Chewing tobacco/dip` = "tobacco_typeused_smokeless",
    `tobacco_typeused_E-cigarettes/vaped tobacco` = "tobacco_typeused_vape",
    `tobacco_typeused_Smoked tobacco (cigarettes cigars/cigarillos pipes hookah etc.)` = "tobacco_typeused_smoke",
    `tobacco_typeused_Something else (please specify):` = "tobacco_typeused_other"
  )
  
  new_labels <- c(
    tobacco_typeused_smokeless = "What type(s) of tobacco have you used in the past 30 days?: Chewing tobacco/dip" ,
    tobacco_typeused_vape = "What type(s) of tobacco have you used in the past 30 days?: E-cigarettes/vaped tobacco" ,
    tobacco_typeused_smoke = "What type(s) of tobacco have you used in the past 30 days?: Smoked tobacco (cigarettes cigars/cigarillos pipes hookah etc.)",
    tobacco_typeused_other = "What type(s) of tobacco have you used in the past 30 days?: Something else (please specify):"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

scale_coping <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,scale_coping = `coping`) %>%
    mutate(scale_coping = gsub(', draw',' draw',scale_coping)) 
  variable_prefix <- "scale_coping"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `scale_coping_Concentrated on what to do and how to solve the problem` = "scale_coping_problemsolve",
    `scale_coping_Do a hobby (e.g. read draw)` = "scale_coping_hobby",
    `scale_coping_Go off by myself to think` = "scale_coping_alone",
    `scale_coping_Go to sleep` = "scale_coping_sleep",
    `scale_coping_Go to someone I trust for support` = "scale_coping_support",
    `scale_coping_I have not used any of these ways to deal with problems in the past 3 months` = "scale_coping_none",
    `scale_coping_Realize that I am strong and can deal with whatever is bothering me` = "scale_coping_realize",
    `scale_coping_Think about how things will get better in the future` = "scale_coping_thinkfuture",
    `scale_coping_Think about what happened and try to sort it out in my head` = "scale_coping_thinkpresent",
    `scale_coping_Try not to think about it` = "scale_coping_ignore",
    `scale_coping_Try to learn from the bad experience` = "scale_coping_learn",
    `scale_coping_Try to value myself and not think so much about other people's opinions` = "scale_coping_valueself",
    `scale_coping_Use drugs or alcohol` = "scale_coping_drugs",
    `scale_coping_Use my anger to get me through it` = "scale_coping_anger",
    `scale_coping_Use my spiritual beliefs/belief in a higher power` = "scale_coping_spiritual"
  )
  
  new_labels <- c(
    scale_coping_problemsolve = "Which of the following ways have you dealt with problems in the past 3 months?: Concentrated on what to do and how to solve the problem",
    scale_coping_hobby = "Which of the following ways have you dealt with problems in the past 3 months?: Do a hobby (e.g. read draw)",
    scale_coping_alone = "Which of the following ways have you dealt with problems in the past 3 months?: Go off by myself to think",
    scale_coping_sleep = "Which of the following ways have you dealt with problems in the past 3 months?: Go to sleep",
    scale_coping_support = "Which of the following ways have you dealt with problems in the past 3 months?: Go to someone I trust for support",
    scale_coping_none = "Which of the following ways have you dealt with problems in the past 3 months?: I have not used any of these ways to deal with problems in the past 3 months",
    scale_coping_realize = "Which of the following ways have you dealt with problems in the past 3 months?: Realize that I am strong and can deal with whatever is bothering me",
    scale_coping_thinkfuture = "Which of the following ways have you dealt with problems in the past 3 months?: Think about how things will get better in the future",
    scale_coping_thinkpresent = "Which of the following ways have you dealt with problems in the past 3 months?: Think about what happened and try to sort it out in my head",
    scale_coping_ignore = "Which of the following ways have you dealt with problems in the past 3 months?: Try not to think about it",
    scale_coping_learn = "Which of the following ways have you dealt with problems in the past 3 months?: Try to learn from the bad experience",
    scale_coping_valueself = "Which of the following ways have you dealt with problems in the past 3 months?: Try to value myself and not think so much about other people's opinions",
    scale_coping_drugs = "Which of the following ways have you dealt with problems in the past 3 months?: Use drugs or alcohol",
    scale_coping_anger = "Which of the following ways have you dealt with problems in the past 3 months?: Use my anger to get me through it",
    scale_coping_spiritual = "Which of the following ways have you dealt with problems in the past 3 months?: Use my spiritual beliefs/belief in a higher power"
  )
  
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

scale_phealth <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,scale_phealth = `chronicdx`) %>%
    mutate(scale_phealth = gsub(', Chronic',' Chronic',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', Chronic Bronchitis',' chronic bronchitis',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', Emphysema',' emphysema',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', COPD',' COPD',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', or',' or',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', such',' such',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', heart',' heart',scale_phealth)) %>%
    mutate(scale_phealth = gsub(', arrhythmia',' arrhythmia',scale_phealth))
  variable_prefix <- "scale_phealth"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `scale_phealth_Anemia` = "scale_phealth_anemia",
    `scale_phealth_Arthritis` = "scale_phealth_arthritis",
    `scale_phealth_Asthma Chronic Bronchitis emphysema COPD or other respiratory conditions.` = "scale_phealth_asthma",
    `scale_phealth_Blind in one or both eyes (condition not specified)` = "scale_phealth_blindness",
    `scale_phealth_Cancer` = "scale_phealth_cancer",
    `scale_phealth_Cirrhosis of the liver or pancreatitis` = "scale_phealth_cirrhosis",
    `scale_phealth_Diabetes or high blood sugar` = "scale_phealth_diabetes",
    `scale_phealth_Hearing impairment or deafness` = "scale_phealth_hearing",
    `scale_phealth_Heart murmur` = "scale_phealth_heartmurmur",
    `scale_phealth_High blood pressure or hypertension` = "scale_phealth_hypertension",
    `scale_phealth_High/low cholesterol` = "scale_phealth_cholesterol",
    `scale_phealth_I have never been told that I have any chronic health conditions` = "scale_phealth_none",
    `scale_phealth_Insomnia` = "scale_phealth_insomnia",
    `scale_phealth_Kidney disease or weak/failing kidneys` = "scale_phealth_kidneys",
    `scale_phealth_Migraines` = "scale_phealth_migraines",
    `scale_phealth_Obesity` = "scale_phealth_obesity",
    `scale_phealth_Other cardiovascular disease (coronary artery disease/atherosclerosis heart disease heart attack arrhythmia)` = "scale_phealth_cvdother",
    `scale_phealth_Respiratory conditions such as Asthma Chronic Bronchitis emphysema or COPD` = "scale_phealth_asthma2",
    `scale_phealth_Sciatica/sciatic pain` = "scale_phealth_sciatica",
    `scale_phealth_Scoliosis` = "scale_phealth_scoliosis",
    `scale_phealth_Seizures` = "scale_phealth_seizures",
    `scale_phealth_Sleep apnea` = "scale_phealth_sleepapnea",
    `scale_phealth_Something else not listed here (please specify)` = "scale_phealth_other",
    `scale_phealth_Thyroid disorder` = "scale_phealth_thyroid",
    `scale_phealth_Tuberculosis` = "scale_phealth_tuberculosis"
  )
  
  new_labels <- c(
    scale_phealth_anemia = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Anemia",
    scale_phealth_arthritis = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Arthritis",
    scale_phealth_asthma = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Asthma Chronic Bronchitis emphysema COPD or other respiratory conditions.",
    scale_phealth_blindness = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Blind in one or both eyes (condition not specified)",
    scale_phealth_cancer = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Cancer",
    scale_phealth_cirrhosis = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Cirrhosis of the liver or pancreatitis",
    scale_phealth_diabetes = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Diabetes or high blood sugar",
    scale_phealth_hearing = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Hearing impairment or deafness",
    scale_phealth_heartmurmur = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Heart murmur",
    scale_phealth_hypertension = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?High blood pressure or hypertension",
    scale_phealth_cholesterol = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?High/low cholesterol",
    scale_phealth_none = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?I have never been told that I have any chronic health conditions",
    scale_phealth_insomnia = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Insomnia",
    scale_phealth_kidneys = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Kidney disease or weak/failing kidneys",
    scale_phealth_migraines = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Migraines",
    scale_phealth_obesity = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Obesity",
    scale_phealth_cvdother = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Other cardiovascular disease (coronary artery disease/atherosclerosis heart disease heart attack arrhythmia)",
    scale_phealth_asthma2 = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Respiratory conditions such as Asthma Chronic Bronchitis emphysema or COPD",
    scale_phealth_sciatica = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Sciatica/sciatic pain",
    scale_phealth_scoliosis = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Scoliosis",
    scale_phealth_seizures = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Seizures",
    scale_phealth_sleepapnea = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Sleep apnea",
    scale_phealth_other = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Something else not listed here (please specify)",
    scale_phealth_thyroid = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Thyroid disorder",
    scale_phealth_tuberculosis = "Has a doctor or other health professional ever told you that you had any of the following conditions or other chronic health conditions not listed here?Tuberculosis"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator) %>%
    mutate(scale_phealth_asthma = ifelse(scale_phealth_asthma != 1,scale_phealth_asthma2,scale_phealth_asthma)) %>%
    select(-scale_phealth_asthma2)
  
  attr(return_data$scale_phealth_asthma,"label") <- new_labels[3]
  
  return(return_data)
}

income_source_30day <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_income_source_30day = `incmgen_30day`) %>%
    mutate(history_income_source_30day = gsub(', seasonal',' seasonal',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', minimum',' minimum',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', GR',' GR',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', food',' food',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', TANF',' TANF',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', SSI',' SSI',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', such',' such',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', drugs',' drugs',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', shelter',' shelter',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub(', or',' or',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub('\"','',history_income_source_30day)) %>%
    mutate(history_income_source_30day = gsub('From an agency or program such as food stamps or welfare',
                                              'Public benefits such as food stamps or welfare',history_income_source_30day))
  variable_prefix <- "history_income_source_30day"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_income_source_30day_Clothing or other personal possessions that you sold` = "history_income_source_30day_clothes",
    `history_income_source_30day_Collecting and selling bottles/cans` = "history_income_source_30day_bottles",
    `history_income_source_30day_Dealing drugs` = "history_income_source_30day_drugs",
    `history_income_source_30day_Doing any kind of paid temporary work (day labor seasonal work minimum wage or pick-up work)` = "history_income_source_30day_tempwork",
    `history_income_source_30day_Doing any work that you were paid under the table for` = "history_income_source_30day_underwork",
    `history_income_source_30day_Friends` = "history_income_source_30day_friends",
    #`history_income_source_30day_From an agency or program such as food stamps or welfare (e.g. food stamps/SNAP TANF SSI GR)` = "history_income_source_30day_agency",
    `history_income_source_30day_Gambling` = "history_income_source_30day_gambling",
    `history_income_source_30day_Panhandling/people just giving you money` = "history_income_source_30day_panhandle",
    `history_income_source_30day_Public benefits such as food stamps or welfare (e.g. food stamps/SNAP TANF SSI GR)` = "history_income_source_30day_publicbenefits",
    `history_income_source_30day_Relatives` = "history_income_source_30day_relatives",
    `history_income_source_30day_Selling self-made items` = "history_income_source_30day_sellmade",
    `history_income_source_30day_Selling stolen goods` = "history_income_source_30day_sellstolen",
    `history_income_source_30day_Stealing` = "history_income_source_30day_stealing",
    `history_income_source_30day_The sale of your blood/plasma` = "history_income_source_30day_blood",
    `history_income_source_30day_Trading sexual favors in exchange for money drugs shelter food or anything else of value` = "history_income_source_30day_tradesex",
    `history_income_source_30day_Working a full-time job (38+ hours per week)` = "history_income_source_30day_ftjob",
    `history_income_source_30day_Working multiple part-time jobs that add up to 38+ hours per week in total` = "history_income_source_30day_multijob",
    `history_income_source_30day_Working one or more part-time jobs that add up to less than 38 hours per week` = "history_income_source_30day_ptjobs"
  )
  
  new_labels <- c(
    history_income_source_30day_clothes = "During the past 30 days, did you get any money or resources to meet your basic needs from: Clothing or other personal possessions that you sold",
    history_income_source_30day_bottles = "During the past 30 days, did you get any money or resources to meet your basic needs from: Collecting and selling bottles/cans",
    history_income_source_30day_drugs = "During the past 30 days, did you get any money or resources to meet your basic needs from: Dealing drugs",
    history_income_source_30day_tempwork = "During the past 30 days, did you get any money or resources to meet your basic needs from: Doing any kind of paid temporary work (day labor seasonal work minimum wage or pick-up work)",
    history_income_source_30day_underwork = "During the past 30 days, did you get any money or resources to meet your basic needs from: Doing any work that you were paid under the table for",
    history_income_source_30day_friends = "During the past 30 days, did you get any money or resources to meet your basic needs from: Friends",
    #history_income_source_30day_agency = "During the past 30 days, did you get any money or resources to meet your basic needs from: From an agency or program such as food stamps or welfare (e.g. food stamps/SNAP TANF SSI GR)",
    history_income_source_30day_gambling = "During the past 30 days, did you get any money or resources to meet your basic needs from: Gambling",
    history_income_source_30day_panhandle = "During the past 30 days, did you get any money or resources to meet your basic needs from: Panhandling/people just giving you money",
    history_income_source_30day_publicbenefits = "During the past 30 days, did you get any money or resources to meet your basic needs from: Public benefits such as food stamps or welfare (e.g. food stamps/SNAP TANF SSI GR)",
    history_income_source_30day_relatives = "During the past 30 days, did you get any money or resources to meet your basic needs from: Relatives",
    history_income_source_30day_sellmade = "During the past 30 days, did you get any money or resources to meet your basic needs from: Selling self-made items",
    history_income_source_30day_sellstolen = "During the past 30 days, did you get any money or resources to meet your basic needs from: Selling stolen goods",
    history_income_source_30day_stealing = "During the past 30 days, did you get any money or resources to meet your basic needs from: Stealing",
    history_income_source_30day_blood = "During the past 30 days, did you get any money or resources to meet your basic needs from: The sale of your blood/plasma",
    history_income_source_30day_tradesex = "During the past 30 days, did you get any money or resources to meet your basic needs from: Trading sexual favors in exchange for money drugs shelter food or anything else of value",
    history_income_source_30day_ftjob = "During the past 30 days, did you get any money or resources to meet your basic needs from: Working a full-time job (38+ hours per week)",
    history_income_source_30day_multijob = "During the past 30 days, did you get any money or resources to meet your basic needs from: Working multiple part-time jobs that add up to 38+ hours per week in total",
    history_income_source_30day_ptjobs = "During the past 30 days, did you get any money or resources to meet your basic needs from: Working one or more part-time jobs that add up to less than 38 hours per week"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}


healthcare_access_needs <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,healthcare_access_needs = `healthneeds`) 
  variable_prefix <- "healthcare_access_needs"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `healthcare_access_needs_Chronic pain management/care` = "healthcare_access_needs_pain",
    `healthcare_access_needs_Hearing/auditory healthcare` = "healthcare_access_needs_hearing",
    `healthcare_access_needs_I've had access to all types of healthcare that I needed in the past year` = "healthcare_access_needs_none",
    `healthcare_access_needs_Oral/dental` = "healthcare_access_needs_dental",
    `healthcare_access_needs_Other (please specify)` = "healthcare_access_needs_other",
    `healthcare_access_needs_Physical therapy or rehabilitation` = "healthcare_access_needs_ptrehab",
    `healthcare_access_needs_Preventative healthcare/primary care` = "healthcare_access_needs_primary",
    `healthcare_access_needs_Sexual/reproductive health care` = "healthcare_access_needs_sexrepro",
    `healthcare_access_needs_Sleep health care` = "healthcare_access_needs_sleep",
    `healthcare_access_needs_Vision` = "healthcare_access_needs_vision",
    `healthcare_access_needs_Weight management` = "healthcare_access_needs_weight"
  )
  new_labels <- list(
    healthcare_access_needs_pain = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Chronic pain management or care",
    healthcare_access_needs_hearing = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Hearing or auditory healthcare",
    healthcare_access_needs_none = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: I've had access to all types of healthcare that I needed",
    healthcare_access_needs_dental = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Oral or dental",
    healthcare_access_needs_other = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Other",
    healthcare_access_needs_ptrehab = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Physical therapy or rehab",
    healthcare_access_needs_primary = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Preventative healthcare or primary care",
    healthcare_access_needs_sexrepro = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Sexual or reproductive health care",
    healthcare_access_needs_sleep = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Sleep health care",
    healthcare_access_needs_vision = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Vision",
    healthcare_access_needs_weight = "In the past year, have you felt that you needed to but were unable to access any of the following types of health care?: Weight management"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

livsit_current_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,unhoused_livsit_now = `uh_livsit_curr`) %>%
    mutate(unhoused_livsit_now = gsub(', park',' park',unhoused_livsit_now)) %>%
    mutate(unhoused_livsit_now = gsub(', beach',' beach',unhoused_livsit_now)) %>%
    mutate(unhoused_livsit_now = gsub(', or',' or',unhoused_livsit_now)) %>%
    mutate(unhoused_livsit_now = gsub(', bus',' bus',unhoused_livsit_now)) %>%
    mutate(unhoused_livsit_now = gsub(', light',' light',unhoused_livsit_now))
  variable_prefix <- "unhoused_livsit_now"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `unhoused_livsit_now_Abandoned building or squat` = "unhoused_livsit_now_squat",
    `unhoused_livsit_now_Adult emergency/temporary shelter (less than 30 days)` = "unhoused_livsit_now_shortshelter",
    `unhoused_livsit_now_Adult longer-term shelter (more than 30 days)` = "unhoused_livsit_now_longshelter",
    `unhoused_livsit_now_Car bus light rail or metro` = "unhoused_livsit_now_transit",
    `unhoused_livsit_now_Family home` = "unhoused_livsit_now_familyhome",
    `unhoused_livsit_now_Foster family home` = "unhoused_livsit_now_fosterhome",
    `unhoused_livsit_now_Friend's home` = "unhoused_livsit_now_friendhome",
    `unhoused_livsit_now_Group home` = "unhoused_livsit_now_grouphome",
    `unhoused_livsit_now_Home of boyfriend/girlfriend/person I'm having sex with` = "unhoused_livsit_now_relationhome",
    `unhoused_livsit_now_Home of someone I do not know or barely know` = "unhoused_livsit_now_strangerhome",
    `unhoused_livsit_now_Hotel/motel` = "unhoused_livsit_now_hotel",
    `unhoused_livsit_now_Juvenile detention center or jail` = "unhoused_livsit_now_jail",
    `unhoused_livsit_now_Other (please specify)` = "unhoused_livsit_now_other",
    `unhoused_livsit_now_Own apartment` = "unhoused_livsit_now_ownapt",
    `unhoused_livsit_now_Permanent supportive housing/supportive housing program` = "unhoused_livsit_now_psh",
    `unhoused_livsit_now_Relative's home` = "unhoused_livsit_now_relativehome",
    `unhoused_livsit_now_Sober living facility` = "unhoused_livsit_now_sober",
    `unhoused_livsit_now_Street park beach or outside` = "unhoused_livsit_now_outdoors",
    `unhoused_livsit_now_Transitional living program (TLP)` = "unhoused_livsit_now_tlp",
    `unhoused_livsit_now_Youth-only emergency/temporary shelter (less than 30 days)` = "unhoused_livsit_now_youthshortshelter",
    `unhoused_livsit_now_Youth-only longer-term shelter (more than 30 days)` = "unhoused_livsit_now_youthlongshelter"
  )
  new_labels <- list(
    unhoused_livsit_now_squat = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Abandoned building or squat",
    unhoused_livsit_now_shortshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Adult emergency shelter (less than 30 days)",
    unhoused_livsit_now_longshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Adult longer term shelter (more than 30 days)",
    unhoused_livsit_now_transit = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Car bus light rail or metro",
    unhoused_livsit_now_familyhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Family home",
    unhoused_livsit_now_fosterhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : foster family home",
    unhoused_livsit_now_friendhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : friends home",
    unhoused_livsit_now_grouphome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : group home",
    unhoused_livsit_now_relationhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : home of boyfriend/girlfriend/person I am having sex with",
    unhoused_livsit_now_strangerhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : home of someone I do not know",
    unhoused_livsit_now_hotel = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : hotel or motel",
    unhoused_livsit_now_jail = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : juvenile detention center or jail",
    unhoused_livsit_now_other = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : other",
    unhoused_livsit_now_ownapt = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : own apartment",
    unhoused_livsit_now_psh = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : PSH housing program",
    unhoused_livsit_now_relativehome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : relatives home",
    unhoused_livsit_now_sober = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : sober living facility",
    unhoused_livsit_now_outdoors = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : street park beach or outside",
    unhoused_livsit_now_tlp = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : TLP program",
    unhoused_livsit_now_youthshortshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Youth emergency shelter (less than 30 days)",
    unhoused_livsit_now_youthlongshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Youth longer-term shelter (more than 30 days)"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

livsit_3mo_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,unhoused_livsit_3mo = `uh_livsit_3mo`) %>%
    mutate(unhoused_livsit_3mo = gsub(', park',' park',unhoused_livsit_3mo)) %>%
    mutate(unhoused_livsit_3mo = gsub(', beach',' beach',unhoused_livsit_3mo)) %>%
    mutate(unhoused_livsit_3mo = gsub(', or',' or',unhoused_livsit_3mo)) %>%
    mutate(unhoused_livsit_3mo = gsub(', bus',' bus',unhoused_livsit_3mo)) %>%
    mutate(unhoused_livsit_3mo = gsub(', light',' light',unhoused_livsit_3mo))
  variable_prefix <- "unhoused_livsit_3mo"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `unhoused_livsit_3mo_Abandoned building or squat` = "unhoused_livsit_3mo_squat",
    `unhoused_livsit_3mo_Adult emergency/temporary shelter (less than 30 days)` = "unhoused_livsit_3mo_shortshelter",
    `unhoused_livsit_3mo_Adult longer-term shelter (more than 30 days)` = "unhoused_livsit_3mo_longshelter",
    `unhoused_livsit_3mo_Car bus light rail or metro` = "unhoused_livsit_3mo_transit",
    `unhoused_livsit_3mo_Family home` = "unhoused_livsit_3mo_familyhome",
    `unhoused_livsit_3mo_Foster family home` = "unhoused_livsit_3mo_fosterhome",
    `unhoused_livsit_3mo_Friend's home` = "unhoused_livsit_3mo_friendhome",
    `unhoused_livsit_3mo_Group home` = "unhoused_livsit_3mo_grouphome",
    `unhoused_livsit_3mo_Home of boyfriend/girlfriend/person I'm having sex with` = "unhoused_livsit_3mo_relationhome",
    `unhoused_livsit_3mo_Home of someone I do not know or barely know` = "unhoused_livsit_3mo_strangerhome",
    `unhoused_livsit_3mo_Hotel/motel` = "unhoused_livsit_3mo_hotel",
    `unhoused_livsit_3mo_Juvenile detention center or jail` = "unhoused_livsit_3mo_jail",
    `unhoused_livsit_3mo_Other (please specify)` = "unhoused_livsit_3mo_other",
    `unhoused_livsit_3mo_Own apartment` = "unhoused_livsit_3mo_ownapt",
    `unhoused_livsit_3mo_Permanent supportive housing/supportive housing program` = "unhoused_livsit_3mo_psh",
    `unhoused_livsit_3mo_Relative's home` = "unhoused_livsit_3mo_relativehome",
    `unhoused_livsit_3mo_Sober living facility` = "unhoused_livsit_3mo_sober",
    `unhoused_livsit_3mo_Street park beach or outside` = "unhoused_livsit_3mo_outdoors",
    `unhoused_livsit_3mo_Transitional living program (TLP)` = "unhoused_livsit_3mo_tlp",
    `unhoused_livsit_3mo_Youth-only emergency/temporary shelter (less than 30 days)` = "unhoused_livsit_3mo_youthshortshelter",
    `unhoused_livsit_3mo_Youth-only longer-term shelter (more than 30 days)` = "unhoused_livsit_3mo_youthlongshelter"
  )
  new_labels <- list(
    unhoused_livsit_3mo_squat = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Abandoned building or squat",
    unhoused_livsit_3mo_shortshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Adult emergency shelter (less than 30 days)",
    unhoused_livsit_3mo_longshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Adult longer term shelter (more than 30 days)",
    unhoused_livsit_3mo_transit = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Car bus light rail or metro",
    unhoused_livsit_3mo_familyhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Family home",
    unhoused_livsit_3mo_fosterhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : foster family home",
    unhoused_livsit_3mo_friendhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : friends home",
    unhoused_livsit_3mo_grouphome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : group home",
    unhoused_livsit_3mo_relationhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : home of boyfriend/girlfriend/person I am having sex with",
    unhoused_livsit_3mo_strangerhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : home of someone I do not know",
    unhoused_livsit_3mo_hotel = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : hotel or motel",
    unhoused_livsit_3mo_jail = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : juvenile detention center or jail",
    unhoused_livsit_3mo_other = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : other",
    unhoused_livsit_3mo_ownapt = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : own apartment",
    unhoused_livsit_3mo_psh = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : PSH housing program",
    unhoused_livsit_3mo_relativehome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : relatives home",
    unhoused_livsit_3mo_sober = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : sober living facility",
    unhoused_livsit_3mo_outdoors = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : street park beach or outside",
    unhoused_livsit_3mo_tlp = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : TLP program",
    unhoused_livsit_3mo_youthshortshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Youth emergency shelter (less than 30 days)",
    unhoused_livsit_3mo_youthlongshelter = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : Youth longer-term shelter (more than 30 days)"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

house_progs_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_house_progs = `housingprogs_ever`) %>%
    mutate(history_house_progs = gsub(', park',' park',history_house_progs)) %>%
    mutate(history_house_progs = gsub(', beach',' beach',history_house_progs)) %>%
    mutate(history_house_progs = gsub(', or',' or',history_house_progs)) %>%
    mutate(history_house_progs = gsub(', bus',' bus',history_house_progs)) %>%
    mutate(history_house_progs = gsub(', light',' light',history_house_progs))
  variable_prefix <- "history_house_progs"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_house_progs_Abandoned building or squat` = "history_house_progs_squat",
    `history_house_progs_Adult emergency/temporary shelter (less than 30 days)` = "history_house_progs_shortshelter",
    `history_house_progs_Adult longer-term shelter (more than 30 days)` = "history_house_progs_longshelter",
    `history_house_progs_Car bus light rail or metro` = "history_house_progs_transit",
    `history_house_progs_Family home` = "history_house_progs_familyhome",
    `history_house_progs_Foster family home` = "history_house_progs_fosterhome",
    `history_house_progs_Friend's home` = "history_house_progs_friendhome",
    `history_house_progs_Group home` = "history_house_progs_grouphome",
    `history_house_progs_Home of boyfriend/girlfriend/person I'm having sex with` = "history_house_progs_relationhome",
    `history_house_progs_Home of someone I do not know or barely know` = "history_house_progs_strangerhome",
    `history_house_progs_Hotel/motel` = "history_house_progs_hotel",
    `history_house_progs_Juvenile detention center or jail` = "history_house_progs_jail",
    `history_house_progs_Other (please specify)` = "history_house_progs_other",
    `history_house_progs_Own apartment` = "history_house_progs_ownapt",
    `history_house_progs_Permanent supportive housing/supportive housing program` = "history_house_progs_psh",
    `history_house_progs_Relative's home` = "history_house_progs_relativehome",
    `history_house_progs_Sober living facility` = "history_house_progs_sober",
    `history_house_progs_Street park beach or outside` = "history_house_progs_outdoors",
    `history_house_progs_Transitional living program (TLP)` = "history_house_progs_tlp",
    `history_house_progs_Youth-only emergency/temporary shelter (less than 30 days)` = "history_house_progs_youthshortshelter",
    `history_house_progs_Youth-only longer-term shelter (more than 30 days)` = "history_house_progs_youthlongshelter"
  )
  new_labels <- list(
    history_house_progs_squat = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Abandoned building or squat",
    history_house_progs_shortshelter = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Adult emergency shelter (less than 30 days)",
    history_house_progs_longshelter = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Adult longer term shelter (more than 30 days)",
    history_house_progs_transit = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Car bus light rail or metro",
    history_house_progs_familyhome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Family home",
    history_house_progs_fosterhome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: foster family home",
    history_house_progs_friendhome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: friends home",
    history_house_progs_grouphome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: group home",
    history_house_progs_relationhome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: home of boyfriend/girlfriend/person I am having sex with",
    history_house_progs_strangerhome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: home of someone I do not know",
    history_house_progs_hotel = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: hotel or motel",
    history_house_progs_jail = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: juvenile detention center or jail",
    history_house_progs_other = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: other",
    history_house_progs_ownapt = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: own apartment",
    history_house_progs_psh = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: PSH housing program",
    history_house_progs_relativehome = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: relatives home",
    history_house_progs_sober = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: sober living facility",
    history_house_progs_outdoors = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: street park beach or outside",
    history_house_progs_tlp = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: TLP program",
    history_house_progs_youthshortshelter = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Youth emergency shelter (less than 30 days)",
    history_house_progs_youthlongshelter = "Since the first time you became unstably housed/homeless, which of the following types of living situations/housing programs have you lived in?: Youth longer-term shelter (more than 30 days)"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

homeless_why_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_homeless_why = `reasonhomeless`)
  variable_prefix <- "history_homeless_why"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_homeless_why_I aged out of the foster care system` = "history_homeless_why_aged",
    `history_homeless_why_I couldn't pay rent` = "history_homeless_why_rent",
    `history_homeless_why_I had no place to go when I got out of jail/prison` = "history_homeless_why_jail",
    `history_homeless_why_I had no place to go when I got out of the hospital` = "history_homeless_why_hospital",
    `history_homeless_why_I had no place to stay when I moved here` = "history_homeless_why_transient",
    `history_homeless_why_I left a gang or a neighborhood with gang violence` = "history_homeless_why_gang",
    `history_homeless_why_I left a situation of domestic violence` = "history_homeless_why_violence",
    `history_homeless_why_I ran away from my family home` = "history_homeless_why_ranfamily",
    `history_homeless_why_I ran away from my foster family home` = "history_homeless_why_ranfoster",
    `history_homeless_why_I ran away from my group home` = "history_homeless_why_rangroup",
    `history_homeless_why_I ran away from my relative's home` = "history_homeless_why_ranrelative",
    `history_homeless_why_I was kicked out/asked to leave my family home` = "history_homeless_why_kickfamily",
    `history_homeless_why_I was kicked out/asked to leave my foster home` = "history_homeless_why_kickfoster",
    `history_homeless_why_I was kicked out/asked to leave my group home` = "history_homeless_why_kickgroup",
    `history_homeless_why_I was kicked out/asked to leave my relative's home` = "history_homeless_why_kickrelative",
    `history_homeless_why_My family does not have a stable place to stay` = "history_homeless_why_nofamilyhome",
    `history_homeless_why_Other (please specify)` = "history_homeless_why_other"
  )
  new_labels <- list(
    history_homeless_why_aged = "How did you become unstably housed or homeless?: I aged out of the foster care system",
    history_homeless_why_rent = "How did you become unstably housed or homeless?: I couldn't pay rent",
    history_homeless_why_jail = "How did you become unstably housed or homeless?: I had no place to go when I got out of jail",
    history_homeless_why_hospital = "How did you become unstably housed or homeless?: I had no place to go when I got out of the hospital",
    history_homeless_why_transient = "How did you become unstably housed or homeless?: I had no place to stay when I moved here",
    history_homeless_why_gang = "How did you become unstably housed or homeless?: I left a gang or neighborhoood with gang violence",
    history_homeless_why_violence = "How did you become unstably housed or homeless?: I left a situation of domestic violence",
    history_homeless_why_ranfamily = "How did you become unstably housed or homeless?: I ran away from my family home",
    history_homeless_why_ranfoster = "How did you become unstably housed or homeless?: I ran away from my foster family home",
    history_homeless_why_rangroup = "How did you become unstably housed or homeless?: I ran away from my group home",
    history_homeless_why_ranrelative = "How did you become unstably housed or homeless?: I ran away from my relatives home",
    history_homeless_why_kickfamily = "How did you become unstably housed or homeless?: I was kicked out or asked to leave family home",
    history_homeless_why_kickfoster = "How did you become unstably housed or homeless?: I was kicked out or asked to leave foster home",
    history_homeless_why_kickgroup = "How did you become unstably housed or homeless?: I was kicked out or asked to leave group home",
    history_homeless_why_kickrelative = "How did you become unstably housed or homeless?: I was kicked out or asked ot leave relatives home",
    history_homeless_why_nofamilyhome = "How did you become unstably housed or homeless?: My family does not have a stable place to stay",
    history_homeless_why_other = "How did you become unstably housed or homeless?: Other"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}


romance_sex_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_romance_sex = `romrel_sex`) %>%
    mutate(history_romance_sex = gsub('"','',history_romance_sex)) %>%
    mutate(history_romance_sex = gsub(', 3',' 3',history_romance_sex)) %>%
    mutate(history_romance_sex = gsub(", don't tell"," don't tell",history_romance_sex))
  variable_prefix <- "history_romance_sex"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_romance_sex_I have played around or cheated on my partner(s)` = "history_romance_sex_cheated",
    `history_romance_sex_I have sex with other people` = "history_romance_sex_others",
    `history_romance_sex_My partner(s) has played around or cheated on me` = "history_romance_sex_pcheated",
    `history_romance_sex_My partner(s) has sex with other people` = "history_romance_sex_pothers",
    `history_romance_sex_We do not have a sexual relationship` = "history_romance_sex_nosex",
    `history_romance_sex_We don't ask don't tell about having sex with others` = "history_romance_sex_dontask",
    `history_romance_sex_We had a period (or periods) of monogamy (sex only with each other)` = "history_romance_sex_partmono",
    `history_romance_sex_We have had sex with other people together (for example 3-ways)` = "history_romance_sex_together",
    `history_romance_sex_We have only had sex with each other and no one else since we started our relationship` = "history_romance_sex_fullmono"
  )
  new_labels <- list(
    history_romance_sex_cheated = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: I have played around or cheated",
    history_romance_sex_others = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: I have sex with other people",
    history_romance_sex_pcheated = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: My partners have played around or cheated",
    history_romance_sex_pothers = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: My partners have sex with other people",
    history_romance_sex_nosex = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We do not have a sexual relationship",
    history_romance_sex_dontask = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We don't ask don't tell about having sex with others",
    history_romance_sex_partmono = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We had a period of monogamy",
    history_romance_sex_together = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We only had sex with others together (for example 3-ways)",
    history_romance_sex_fullmono = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We have only had sex with each other"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

romance_sex_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_romance_sex = `romrel_sex`) %>%
    mutate(history_romance_sex = gsub('"','',history_romance_sex)) %>%
    mutate(history_romance_sex = gsub(', 3',' 3',history_romance_sex)) %>%
    mutate(history_romance_sex = gsub(", don't tell"," don't tell",history_romance_sex))
  variable_prefix <- "history_romance_sex"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_romance_sex_I have played around or cheated on my partner(s)` = "history_romance_sex_cheated",
    `history_romance_sex_I have sex with other people` = "history_romance_sex_others",
    `history_romance_sex_My partner(s) has played around or cheated on me` = "history_romance_sex_pcheated",
    `history_romance_sex_My partner(s) has sex with other people` = "history_romance_sex_pothers",
    `history_romance_sex_We do not have a sexual relationship` = "history_romance_sex_nosex",
    `history_romance_sex_We don't ask don't tell about having sex with others` = "history_romance_sex_dontask",
    `history_romance_sex_We had a period (or periods) of monogamy (sex only with each other)` = "history_romance_sex_partmono",
    `history_romance_sex_We have had sex with other people together (for example 3-ways)` = "history_romance_sex_together",
    `history_romance_sex_We have only had sex with each other and no one else since we started our relationship` = "history_romance_sex_fullmono"
  )
  new_labels <- list(
    history_romance_sex_cheated = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: I have played around or cheated",
    history_romance_sex_others = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: I have sex with other people",
    history_romance_sex_pcheated = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: My partners have played around or cheated",
    history_romance_sex_pothers = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: My partners have sex with other people",
    history_romance_sex_nosex = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We do not have a sexual relationship",
    history_romance_sex_dontask = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We don't ask don't tell about having sex with others",
    history_romance_sex_partmono = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We had a period of monogamy",
    history_romance_sex_together = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We only had sex with others together (for example 3-ways)",
    history_romance_sex_fullmono = "Which of the following describe how you and your current partner(s) have had sex during the course of your relationship?: We have only had sex with each other"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

romance_partners_gender_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_romance_pgen = `romrel_ptnrsgndr`)
  variable_prefix <- "history_romance_pgen"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `history_romance_pgen_Different identity (please state):` = "history_romance_pgen_other",
    `history_romance_pgen_Female` = "history_romance_pgen_female",
    `history_romance_pgen_Genderqueer/Gender non-conforming` = "history_romance_pgen_queer",
    `history_romance_pgen_Male` = "history_romance_pgen_male",
    `history_romance_pgen_Trans female/Trans woman` = "history_romance_pgen_transwoman",
    `history_romance_pgen_Trans male/Trans man` = "history_romance_pgen_transman"
  )
  new_labels <- list(
    history_romance_pgen_other = "What is the gender(s) of your partner(s)?: Other (please specify)",
    history_romance_pgen_female = "What is the gender(s) of your partner(s)?: Female",
    history_romance_pgen_queer = "What is the gender(s) of your partner(s)?: Genderqueer or nonconforming",
    history_romance_pgen_male = "What is the gender(s) of your partner(s)?: Male",
    history_romance_pgen_transwoman = "What is the gender(s) of your partner(s)?: Transwoman",
    history_romance_pgen_transman = "What is the gender(s) of your partner(s)?: Transman"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

gender_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,demo_gender = `gender`)
  variable_prefix <- "demo_gender"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `demo_gender_Different identity (please state):` = "demo_gender_other",
    `demo_gender_Female` = "demo_gender_female",
    `demo_gender_Genderqueer/Gender non-conforming` = "demo_gender_queer",
    `demo_gender_Male` = "demo_gender_male",
    `demo_gender_Trans female/Trans woman` = "demo_gender_transwoman",
    `demo_gender_Trans male/Trans man` = "demo_gender_transman"
  )
  new_labels <- list(
    demo_gender_other = "What is your current gender identity?: Other",
    demo_gender_female = "What is your current gender identity?: Female",
    demo_gender_queer = "What is your current gender identity?: Genderqueer or nonconforming",
    demo_gender_male = "What is your current gender identity?: Male",
    demo_gender_transwoman = "What is your current gender identity?: Transwoman",
    demo_gender_transman = "What is your current gender identity?: Transman"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
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

prep_where_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,prep_where = prep_whereleard)
  
  new_names <- c(
    `prep_where_Advertisement` = "survey_prep_where_advert",
    `prep_where_Counselor/social worker/case worker` = "survey_prep_where_counselor",
    `prep_where_Doctor` = "survey_prep_where_doctor",
    `prep_where_Family Member` = "survey_prep_where_family",
    `prep_where_Friend` = "survey_prep_where_friend",
    `prep_where_Internet` = "survey_prep_where_internet",
    `prep_where_Other (specify)` = "survey_prep_where_other",
    `prep_where_Research study or intervention` = "survey_prep_where_research",
    `prep_where_Sexual/romantic partner` = "survey_prep_where_partner"
  )
  new_labels <- list(
    survey_prep_where_advert = "Where did you hear about PrEP?: Advertisement",
    survey_prep_where_counselor = "Where did you hear about PrEP?: Counselor/social worker/case worker",
    survey_prep_where_doctor = "Where did you hear about PrEP?: Doctor",
    survey_prep_where_family = "Where did you hear about PrEP?: Family member",
    survey_prep_where_friend = "Where did you hear about PrEP?: Friend",
    survey_prep_where_internet = "Where did you hear about PrEP?: Internet",
    survey_prep_where_other = "Where did you hear about PrEP?: Other",
    survey_prep_where_research = "Where did you hear about PrEP?: Research study or intervention",
    survey_prep_where_partner = "Where did you hear about PrEP?: Romantic or sexual partner"
  )
  
  return_data <- prebind_data(filtered_data, "prep_where", new_names, new_labels, separator = ",")
  return(return_data)
}

prep_social_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_prep_social = prep_social_who) %>%
    mutate(survey_prep_social = gsub(', agency',' agency',survey_prep_social)) %>%
    mutate(survey_prep_social = gsub(', inti',' inti',survey_prep_social)) %>%
    mutate(survey_prep_social = gsub(', social',' social',survey_prep_social))
  variable_prefix <- "survey_prep_social"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  
  new_names <- c(
    `survey_prep_social_Case worker social worker agency staff or volunteer` = "survey_prep_who_provider",
    `survey_prep_social_Family (could include both biological and foster family)` = "survey_prep_who_family",
    `survey_prep_social_Friends from home or from before you were homeless` = "survey_prep_who_pastfriends",
    `survey_prep_social_Friends or other peers you know from the street or peers you interact with at a service agency` = "survey_prep_who_stfriends",
    `survey_prep_social_People from school` = "survey_prep_who_school",
    `survey_prep_social_People from work` = "survey_prep_who_work",
    `survey_prep_social_Person you are romantically intimately or sexually involved with` = "survey_prep_who_sexpartner"
  )
  new_labels <- list(
    survey_prep_who_provider = "Who do you know that is currently taking PrEP?: Case worker social worker agency staff or volunteer",
    survey_prep_who_family = "Who do you know that is currently taking PrEP?: Family",
    survey_prep_who_pastfriends = "Who do you know that is currently taking PrEP?: Friends from home",
    survey_prep_who_stfriends = "Who do you know that is currently taking PrEP?: From or peers from street or agency",
    survey_prep_who_school = "Who do you know that is currently taking PrEP?: People from school",
    survey_prep_who_work = "Who do you know that is currently taking PrEP?: People from work",
    survey_prep_who_sexpartner = "Who do you know that is currently taking PrEP?: Person you are romantically involved with"
  )
  
  return_data <- prebind_data(filtered_data, "survey_prep_social", new_names, new_labels, separator = ",")
  return(return_data)
}

prep_barrier_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_prep_barrier = prep_barriers)
  
  new_names <- c(
    `survey_prep_barrier_I don't want anyone to know that I'm taking PrEP/stigma associated with PrEP/HIV` = "survey_prep_barrier_hide",
    `survey_prep_barrier_I don’t really understand PrEP/know enough about it` = "survey_prep_barrier_know",
    `survey_prep_barrier_I don’t want to take PrEP because I don’t like taking any type of pharmaceutical medication` = "survey_prep_barrier_want",
    `survey_prep_barrier_I think it would be too expensive` = "survey_prep_barrier_cost",
    `survey_prep_barrier_I use other methods for protecting myself` = "survey_prep_barrier_diff",
    `survey_prep_barrier_I’m concerned about side effects` = "survey_prep_barrier_sidefx",
    `survey_prep_barrier_I’m not at risk for HIV` = "survey_prep_barrier_risk",
    `survey_prep_barrier_It’s too difficult to go to the doctor to get a prescription` = "survey_prep_barrier_doctor",
    `survey_prep_barrier_It’s too much effort to take a pill every day` = "survey_prep_barrier_pill",
    `survey_prep_barrier_None of the above` = "survey_prep_barrier_none",
    `survey_prep_barrier_Something else (please specify)` = "survey_prep_barrier_other"
  )
  new_labels <- list(
    survey_prep_barrier_hide = "I am not sure I would take PrEP because: I don't want anyone to know that I'm taking PrEP/stigma associated with PrEP/HIV",
    survey_prep_barrier_know = "I am not sure I would take PrEP because: I don’t really understand PrEP/know enough about it",
    survey_prep_barrier_want = "I am not sure I would take PrEP because: I don’t want to take PrEP because I don’t like taking any type of pharmaceutical medication",
    survey_prep_barrier_cost = "I am not sure I would take PrEP because: I think it would be too expensive",
    survey_prep_barrier_diff = "I am not sure I would take PrEP because: I use other methods for protecting myself",
    survey_prep_barrier_sidefx = "I am not sure I would take PrEP because: I’m concerned about side effects",
    survey_prep_barrier_risk = "I am not sure I would take PrEP because: I’m not at risk for HIV",
    survey_prep_barrier_doctor = "I am not sure I would take PrEP because: It’s too difficult to go to the doctor to get a prescription",
    survey_prep_barrier_pill = "I am not sure I would take PrEP because: It’s too much effort to take a pill every day",
    survey_prep_barrier_none = "I am not sure I would take PrEP because: None of the above",
    survey_prep_barrier_other = "I am not sure I would take PrEP because: Something else"
  )
  
  return_data <- prebind_data(filtered_data, "survey_prep_barrier", new_names, new_labels, separator = ",")
  return(return_data)
}

sex3mo_describe_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_relation = describe_3mopartner) %>%
    mutate(survey_sex3mo_relation = gsub(', like',' like',survey_sex3mo_relation))
  variable_prefix <- "survey_sex3mo_relation"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  
  new_names <- c(
    `survey_sex3mo_relation_Casual partner (someone I'm seeing/hooking up with/dating)` = "survey_sex3mo_relation_casual",
    `survey_sex3mo_relation_I have not had sex with anyone in the past 3 months` = "survey_sex3mo_relation_none",
    `survey_sex3mo_relation_Other` = "survey_sex3mo_relation_other",
    `survey_sex3mo_relation_Serious partner (husband/wife/life partner/girlfriend/boyfriend)` = "survey_sex3mo_relation_serious",
    `survey_sex3mo_relation_Someone I just met like a one-time hookup` = "survey_sex3mo_relation_hookup"
  )
  new_labels <- list(
    survey_sex3mo_relation_casual = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Casual partner",
    survey_sex3mo_relation_none = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): I have not had sex with anyone",
    survey_sex3mo_relation_other = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Other",
    survey_sex3mo_relation_serious = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Serious partner",
    survey_sex3mo_relation_hookup = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Someone I just met"
  )
  
  return_data <- prebind_data(filtered_data, "survey_sex3mo_relation", new_names, new_labels, separator = ",")
  return(return_data)
}

sex3mo_gender_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_gender = `3mosex_partgndr`)
  variable_prefix <- "survey_sex3mo_gender"
  
  new_names <- c(
    `survey_sex3mo_gender_Different identity (please state):` = "survey_sex3mo_gender_other",
    `survey_sex3mo_gender_Genderqueer/Gender non-conforming` = "survey_sex3mo_gender_queer",
    `survey_sex3mo_gender_Trans female/Trans woman` = "survey_sex3mo_gender_transfem",
    `survey_sex3mo_gender_Female` = "survey_sex3mo_gender_female",
    `survey_sex3mo_gender_Male` = "survey_sex3mo_gender_male",
    `survey_sex3mo_gender_Trans male/Trans man` = "survey_sex3mo_gender_transman"
  )
  new_labels <- list(
    survey_sex3mo_gender_other = "What are the gender(s) of the person(s) you've had sex (oral, anal, vaginal) with in the past 3 months?: Different identity",
    survey_sex3mo_gender_queer = "What are the gender(s) of the person(s) you've had sex (oral, anal, vaginal) with in the past 3 months?: Genderqueer or non-conforming",
    survey_sex3mo_gender_transfem = "What are the gender(s) of the person(s) you've had sex (oral, anal, vaginal) with in the past 3 months?: Trans female/trans woman",
    survey_sex3mo_gender_female = "What are the gender(s) of the person(s) you've had sex (oral, anal, vaginal) with in the past 3 months?: Female",
    survey_sex3mo_gender_male = "What are the gender(s) of the person(s) you've had sex (oral, anal, vaginal) with in the past 3 months?: Male",
    survey_sex3mo_gender_transman = "What are the gender(s) of the person(s) you've had sex (oral, anal, vaginal) with in the past 3 months?: Trans male/trans man"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, separator = ",")
  return(return_data)
}

sex3mo_type_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_type = `3mosex_types`) %>%
    mutate(survey_sex3mo_type = gsub(', with',' with',survey_sex3mo_type)) %>%
    mutate(survey_sex3mo_type = gsub(', no',' no',survey_sex3mo_type)) 
  variable_prefix <- "survey_sex3mo_type"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_sex3mo_type_Anal sex no condom/bareback` = "survey_sex3mo_type_anal_nocondom",
    `survey_sex3mo_type_Anal sex with a condom` = "survey_sex3mo_type_anal_condom",
    `survey_sex3mo_type_Oral sex no condom/dental dam` = "survey_sex3mo_type_oral_nocondom",
    `survey_sex3mo_type_Oral sex with a condom/dental dam` = "survey_sex3mo_type_oral_condom",
    `survey_sex3mo_type_Vaginal sex no condom` = "survey_sex3mo_type_vgnl_nocondom",
    `survey_sex3mo_type_Vaginal sex with a condom` = "survey_sex3mo_type_vgnl_condom"
  )
  new_labels <- list(
    survey_sex3mo_type_anal_nocondom = "What types of sex did you have in the past 3 months?: Anal sex no condom",
    survey_sex3mo_type_anal_condom = "What types of sex did you have in the past 3 months?: Anal sex with a condom",
    survey_sex3mo_type_oral_nocondom = "What types of sex did you have in the past 3 months?: Oral sex no condom",
    survey_sex3mo_type_oral_condom = "What types of sex did you have in the past 3 months?: Oral sex with a condom",
    survey_sex3mo_type_vgnl_nocondom = "What types of sex did you have in the past 3 months?: Vaginal sex no condom",
    survey_sex3mo_type_vgnl_condom = "What types of sex did you have in the past 3 months?: Vaginal sex with a condom"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, separator = ",")
  return(return_data)
}

sex3mo_cntrcptv_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_cntrcptv = `3mosex_cntrcptv`) %>%
    mutate(survey_sex3mo_cntrcptv = gsub(', or',' or',survey_sex3mo_cntrcptv)) %>%
    mutate(survey_sex3mo_cntrcptv = gsub(', r',' r',survey_sex3mo_cntrcptv)) %>%
    mutate(survey_sex3mo_cntrcptv = gsub(', temp',' temp',survey_sex3mo_cntrcptv)) 
  variable_prefix <- "survey_sex3mo_cntrcptv"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)

  new_names <- c(
    `survey_sex3mo_cntrcptv_An IUD or implant (e.g. Implanon)` = "survey_sex3mo_cntrcptv_iud",
    `survey_sex3mo_cntrcptv_Birth control patch (e.g. Ortho Evra) ring (e.g. NuvaRing) or shot (e.g. Depo-Provera)` = "survey_sex3mo_cntrcptv_patch",
    `survey_sex3mo_cntrcptv_Birth control pills` = "survey_sex3mo_cntrcptv_pill",
    `survey_sex3mo_cntrcptv_Condoms (male or female)` = "survey_sex3mo_cntrcptv_condom",
    `survey_sex3mo_cntrcptv_Fertility awareness methods (like cycle tracking rhythm method temperature)` = "survey_sex3mo_cntrcptv_fam",
    `survey_sex3mo_cntrcptv_I have not had vaginal sex in the past 3 months` = "survey_sex3mo_cntrcptv_nosex",
    `survey_sex3mo_cntrcptv_No method was used to prevent pregnancy` = "survey_sex3mo_cntrcptv_none",
    `survey_sex3mo_cntrcptv_Not sure` = "survey_sex3mo_cntrcptv_unsure",
    `survey_sex3mo_cntrcptv_Pulling out/withdrawal` = "survey_sex3mo_cntrcptv_pullout",
    `survey_sex3mo_cntrcptv_Some other method (please specify):` = "survey_sex3mo_cntrcptv_other"
  )
  new_labels <- list(
    survey_sex3mo_cntrcptv_iud = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: An IUD or implant",
    survey_sex3mo_cntrcptv_patch = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Patch shot or ring",
    survey_sex3mo_cntrcptv_pill = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Birth control pills",
    survey_sex3mo_cntrcptv_condom = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Condoms",
    survey_sex3mo_cntrcptv_fam = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Fertility awareness method",
    survey_sex3mo_cntrcptv_nosex = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: I have not had vaginal sex",
    survey_sex3mo_cntrcptv_none = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: No method was used",
    survey_sex3mo_cntrcptv_unsure = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Not sure",
    survey_sex3mo_cntrcptv_pullout = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Pulling out/withdrawal",
    survey_sex3mo_cntrcptv_other = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Some other method"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

sex3mo_extype_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_extype = `exchsex_types`) %>%
    mutate(survey_sex3mo_extype = gsub(', with',' with',survey_sex3mo_extype)) %>%
    mutate(survey_sex3mo_extype = gsub(', no',' no',survey_sex3mo_extype)) 
  variable_prefix <- "survey_sex3mo_extype"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_sex3mo_extype_Anal sex no condoms/bareback` = "survey_sex3mo_extype_anal_nocondom",
    `survey_sex3mo_extype_Anal sex with a condom` = "survey_sex3mo_extype_anal_condom",
    `survey_sex3mo_extype_Oral sex no condom/dental dam` = "survey_sex3mo_extype_oral_nocondom",
    `survey_sex3mo_extype_Oral sex with a condom/dental dam` = "survey_sex3mo_extype_oral_condom",
    `survey_sex3mo_extype_Vaginal sex no condom` = "survey_sex3mo_extype_vgnl_nocondom",
    `survey_sex3mo_extype_Vaginal sex with a condom` = "survey_sex3mo_extype_vgnl_condom"
  )
  new_labels <- list(
    survey_sex3mo_extype_anal_nocondom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Anal sex no condom",
    survey_sex3mo_extype_anal_condom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Anal sex with a condom",
    survey_sex3mo_extype_oral_nocondom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Oral sex no condom",
    survey_sex3mo_extype_oral_condom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Oral sex with a condom",
    survey_sex3mo_extype_vgnl_nocondom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Vaginal sex no condom",
    survey_sex3mo_extype_vgnl_condom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Vaginal sex with a condom"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

sti_pos_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sti_pos = `sti_pos`)
  variable_prefix <- "survey_sti_pos"
  separator <- ","
  prebind_data(filtered_data, variable_prefix, new_names, new_labels, TRUE, separator)
  
  new_names <- c(
    `survey_sti_pos_Chlamydia` = "survey_sti_pos_chla",
    `survey_sti_pos_Genital warts/HPV` = "survey_sti_pos_hpv",
    `survey_sti_pos_Gonorrhea` = "survey_sti_pos_gono",
    `survey_sti_pos_Hepatitis B` = "survey_sti_pos_hepb",
    `survey_sti_pos_Herpes` = "survey_sti_pos_herpes",
    `survey_sti_pos_I didn't get my results` = "survey_sti_pos_noresult",
    `survey_sti_pos_I have never tested positive for any STIs or STDs` = "survey_sti_pos_none",
    `survey_sti_pos_Other (please specify)` = "survey_sti_pos_other",
    `survey_sti_pos_Syphilis` = "survey_sti_pos_syph"
  )
  new_labels <- list(
    survey_sti_pos_chla = "Have you ever tested positive for any of the following STIs or STDs?: Chlamydia",
    survey_sti_pos_hpv = "Have you ever tested positive for any of the following STIs or STDs?: Genit wars or HPV",
    survey_sti_pos_gono = "Have you ever tested positive for any of the following STIs or STDs?: Gonorrhea",
    survey_sti_pos_hepb = "Have you ever tested positive for any of the following STIs or STDs?: Hepatitis B",
    survey_sti_pos_herpes = "Have you ever tested positive for any of the following STIs or STDs?: Herpes",
    survey_sti_pos_noresult = "Have you ever tested positive for any of the following STIs or STDs?: I did not get results",
    survey_sti_pos_none = "Have you ever tested positive for any of the following STIs or STDs?: I have never tested positive for STI",
    survey_sti_pos_other = "Have you ever tested positive for any of the following STIs or STDs?: Other",
    survey_sti_pos_syph = "Have you ever tested positive for any of the following STIs or STDs?: Syphilis"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

numeric_keep_rename <- function(var_column){
  original_label_name <- attributes(var_column)$label
  numeric_column <- as.numeric(var_column)
  attr(numeric_column,"label") <- original_label_name
  return(numeric_column)
}

carry_zero_forward <- function(ref_col, change_col, use_var = 0, flip_sign = FALSE, replace_var = 0){
  
  original_label_name <- attributes(change_col)$label
  if(is.character(replace_var)){
    original_level_name <- attributes(change_col)$levels
  }
  
  if(is.character(replace_var)){
    if(flip_sign){
      new_col <- ifelse(as.integer(ref_col) != use_var & !is.na(ref_col), replace_var, as.character(change_col))
    } else {
      new_col <- ifelse(as.integer(ref_col) == use_var, replace_var, as.character(change_col)) 
    }
  } else {
    if(flip_sign){
      new_col <- ifelse(as.integer(ref_col) != use_var & !is.na(ref_col), replace_var, change_col)
    } else {
      new_col <- ifelse(as.integer(ref_col) == use_var, replace_var, change_col) 
    }
  }
  
  
  if(is.character(replace_var)){
    new_col <- factor(new_col, levels = original_level_name)
  }
  attr(new_col,"label") <- original_label_name
  
  
  return(new_col)
}

carry_factor_forward <- function(parent_column, child_column, parent_value = 0, new_value,
                                 new_label_pair = new_value, first = FALSE){
  hold_column <- as_character(child_column)
  hold_column <- ifelse(parent_column == parent_value, new_value, hold_column)
  return_column <- factor_add_new(child_column,hold_column, new_label_pair, first = first) 
  return(return_column)
}

factor_add_new <- function(old_var_column, new_var_column, new_value, first = FALSE){
  original_label_name <- attributes(old_var_column)$label
  original_level_name <- attributes(old_var_column)$levels
  
  if(first){
    original_level_name <- append(original_level_name, new_value, after = 0)
  } else {
    original_level_name <- append(original_level_name, new_value)
  }
  
  factored_column <- factor(new_var_column, levels = original_level_name)

  attr(factored_column,"label") <- original_label_name
  
  return(factored_column)
}

factor_keep_rename <- function(var_column, level_vector = NULL){
  original_label_name <- attributes(var_column)$label
  if(is.null(level_vector)){
    factored_column <- factor(var_column)  
  } else {
    factored_column <- factor(var_column, levels = level_vector)
  }
  attr(factored_column,"label") <- original_label_name
  return(factored_column)
}

factor_keep_rename_yn <- function(var_column){
  level_vector <- c("No","Yes")
  original_label_name <- attributes(var_column)$label
  factored_column <- factor(var_column, levels = level_vector)
  attr(factored_column,"label") <- original_label_name
  return(factored_column)
}

rep_varnames <- function(prefix_name,name_seq){
  new_list <- NULL
  for(i in name_seq){
    j <- as.character(i)
    new_list <- append(new_list,paste0(prefix_name,j))
  }
  return(new_list)
}