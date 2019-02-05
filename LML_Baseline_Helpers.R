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
    #`unhoused_livsit_now_Foster family home` = "unhoused_livsit_now_fosterhome",
    `unhoused_livsit_now_Friend's home` = "unhoused_livsit_now_friendhome",
    #`unhoused_livsit_now_Group home` = "unhoused_livsit_now_grouphome",
    `unhoused_livsit_now_Home of boyfriend/girlfriend/person I'm having sex with` = "unhoused_livsit_now_relationhome",
    #`unhoused_livsit_now_Home of someone I do not know or barely know` = "unhoused_livsit_now_strangerhome",
    #`unhoused_livsit_now_Hotel/motel` = "unhoused_livsit_now_hotel",
    #`unhoused_livsit_now_Juvenile detention center or jail` = "unhoused_livsit_now_jail",
    `unhoused_livsit_now_Other (please specify)` = "unhoused_livsit_now_other",
    #`unhoused_livsit_now_Own apartment` = "unhoused_livsit_now_ownapt",
    #`unhoused_livsit_now_Permanent supportive housing/supportive housing program` = "unhoused_livsit_now_psh",
    #`unhoused_livsit_now_Relative's home` = "unhoused_livsit_now_relativehome",
    #`unhoused_livsit_now_Sober living facility` = "unhoused_livsit_now_sober",
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
    #unhoused_livsit_now_fosterhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : foster family home",
    unhoused_livsit_now_friendhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : friends home",
    #unhoused_livsit_now_grouphome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : group home",
    unhoused_livsit_now_relationhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : home of boyfriend/girlfriend/person I am having sex with",
    #unhoused_livsit_now_strangerhome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : home of someone I do not know",
    #unhoused_livsit_now_hotel = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : hotel or motel",
    #unhoused_livsit_now_jail = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : juvenile detention center or jail",
    unhoused_livsit_now_other = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : other",
    #unhoused_livsit_now_ownapt = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : own apartment",
    #unhoused_livsit_now_psh = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : PSH housing program",
    #unhoused_livsit_now_relativehome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : relatives home",
    #unhoused_livsit_now_sober = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : sober living facility",
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
    #`unhoused_livsit_3mo_Group home` = "unhoused_livsit_3mo_grouphome",
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
    #unhoused_livsit_3mo_grouphome = "In the past 3 months, which of the following types of living situations/housing programs have you lived in? : group home",
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
    `history_romance_pgen_Female` = "history_romance_pgen_female",
    `history_romance_pgen_Genderqueer/Gender non-conforming` = "history_romance_pgen_queer",
    `history_romance_pgen_Male` = "history_romance_pgen_male",
    `history_romance_pgen_Trans female/Trans woman` = "history_romance_pgen_transwoman",
    `history_romance_pgen_Trans male/Trans man` = "history_romance_pgen_transman"
  )
  new_labels <- list(
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
  filtered_data <- rename(filtered_baseline,survey_prep_social = prep_social_who)
  
  new_names <- c(
    `survey_prep_social_agency staff or volunteer` = "survey_prep_who_staff",
    `survey_prep_social_Case worker` = "survey_prep_who_caseworker",
    `survey_prep_social_Family (could include both biological and foster family)` = "survey_prep_who_family",
    `survey_prep_social_Friends from home or from before you were homeless` = "survey_prep_who_pastfriends",
    `survey_prep_social_Friends or other peers you know from the street or peers you interact with at a service agency` = "survey_prep_who_stfriends",
    `survey_prep_social_intimately or sexually involved with` = "survey_prep_who_sexual",
    `survey_prep_social_People from school` = "survey_prep_who_school",
    `survey_prep_social_People from work` = "survey_prep_who_work",
    `survey_prep_social_Person you are romantically` = "survey_prep_who_romantic",
    `survey_prep_social_social worker` = "survey_prep_who_socialworker"
  )
  new_labels <- list(
    survey_prep_who_staff = "Who do you know that is currently taking PrEP?: Agency staff or volunteer",
    survey_prep_who_caseworker = "Who do you know that is currently taking PrEP?: Case worker",
    survey_prep_who_family = "Who do you know that is currently taking PrEP?: Family",
    survey_prep_who_pastfriends = "Who do you know that is currently taking PrEP?: Friends from home",
    survey_prep_who_stfriends = "Who do you know that is currently taking PrEP?: From or peers from street or agency",
    survey_prep_who_sexual = "Who do you know that is currently taking PrEP?: Intimately or sexually involved",
    survey_prep_who_school = "Who do you know that is currently taking PrEP?: People from school",
    survey_prep_who_work = "Who do you know that is currently taking PrEP?: People from work",
    survey_prep_who_romantic = "Who do you know that is currently taking PrEP?: Person you are romantically involved with",
    survey_prep_who_socialworker = "Who do you know that is currently taking PrEP?: social worker"
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
  filtered_data <- rename(filtered_baseline,survey_sex3mo_relation = describe_3mopartner)
  
  new_names <- c(
    `survey_sex3mo_relation_Casual partner (someone I'm seeing/hooking up with/dating)` = "survey_sex3mo_relation_casual",
    `survey_sex3mo_relation_I have not had sex with anyone in the past 3 months` = "survey_sex3mo_relation_none",
    `survey_sex3mo_relation_like a one-time hookup` = "survey_sex3mo_relation_hookup",
    `survey_sex3mo_relation_Other` = "survey_sex3mo_relation_other",
    `survey_sex3mo_relation_Serious partner (husband/wife/life partner/girlfriend/boyfriend)` = "survey_sex3mo_relation_serious",
    `survey_sex3mo_relation_Someone I just met` = "survey_sex3mo_relation_stranger"
  )
  new_labels <- list(
    survey_sex3mo_relation_casual = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Casual partner",
    survey_sex3mo_relation_none = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): I have not had sex with anyone",
    survey_sex3mo_relation_hookup = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): like a one-time hookup",
    survey_sex3mo_relation_other = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Other",
    survey_sex3mo_relation_serious = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Serious partner",
    survey_sex3mo_relation_stranger = "Think about the person(s) that you have had sex (oral, anal, vaginal) with over the past 3 months. How would you describe this person(s): Someone I just met"
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
  filtered_data <- rename(filtered_baseline,survey_sex3mo_type = `3mosex_types`)
  variable_prefix <- "survey_sex3mo_type"
  
  new_names <- c(
    `survey_sex3mo_type_Anal sex` = "survey_sex3mo_type_anal",
    `survey_sex3mo_type_no condom/bareback` = "survey_sex3mo_type_bareback",
    `survey_sex3mo_type_Oral sex` = "survey_sex3mo_type_oral",
    `survey_sex3mo_type_with a condom` = "survey_sex3mo_type_condom",
    `survey_sex3mo_type_no condom` = "survey_sex3mo_type_nocondom",
    `survey_sex3mo_type_no condom/dental dam` = "survey_sex3mo_type_nodam",
    `survey_sex3mo_type_Vaginal sex` = "survey_sex3mo_type_vaginal",
    `survey_sex3mo_type_with a condom/dental dam` = "survey_sex3mo_dam"
  )
  new_labels <- list(
    survey_sex3mo_type_anal = "What types of sex did you have in the past 3 months?: Anal sex",
    survey_sex3mo_type_bareback = "What types of sex did you have in the past 3 months?: no condom/bareback",
    survey_sex3mo_type_oral = "What types of sex did you have in the past 3 months?: Oral sex",
    survey_sex3mo_type_condom = "What types of sex did you have in the past 3 months?: with a condom",
    survey_sex3mo_type_nocondom = "What types of sex did you have in the past 3 months?: no condom/bareback",
    survey_sex3mo_type_nodam = "What types of sex did you have in the past 3 months?: no condom/dental dam",
    survey_sex3mo_type_vaginal = "What types of sex did you have in the past 3 months?: Vaginal sex",
    survey_sex3mo_dam = "What types of sex did you have in the past 3 months?: with a condom/dental dam"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, separator = ",")
  return(return_data)
}

sex3mo_cntrcptv_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_cntrcptv = `3mosex_cntrcptv`)
  variable_prefix <- "survey_sex3mo_cntrcptv"
  separator <- ","
  
  new_names <- c(
    `survey_sex3mo_cntrcptv_An IUD or implant (e.g. Implanon)` = "survey_sex3mo_cntrcptv_iud",
    `survey_sex3mo_cntrcptv_Birth control patch (e.g. Ortho Evra)` = "survey_sex3mo_cntrcptv_patch",
    `survey_sex3mo_cntrcptv_Birth control pills` = "survey_sex3mo_cntrcptv_pill",
    `survey_sex3mo_cntrcptv_Condoms (male or female)` = "survey_sex3mo_cntrcptv_condom",
    `survey_sex3mo_cntrcptv_Fertility awareness methods (like cycle tracking` = "survey_sex3mo_cntrcptv_fam",
    `survey_sex3mo_cntrcptv_I have not had vaginal sex in the past 3 months` = "survey_sex3mo_cntrcptv_nosex",
    `survey_sex3mo_cntrcptv_No method was used to prevent pregnancy` = "survey_sex3mo_cntrcptv_none",
    `survey_sex3mo_cntrcptv_Not sure` = "survey_sex3mo_cntrcptv_unsure",
    `survey_sex3mo_cntrcptv_or shot (e.g. Depo-Provera)` = "survey_sex3mo_cntrcptv_shot",
    `survey_sex3mo_cntrcptv_Pulling out/withdrawal` = "survey_sex3mo_cntrcptv_pullout",
    `survey_sex3mo_cntrcptv_rhythm method` = "survey_sex3mo_cntrcptv_rhythm",
    `survey_sex3mo_cntrcptv_ring (e.g. NuvaRing)` = "survey_sex3mo_cntrcptv_ring",
    `survey_sex3mo_cntrcptv_Some other method (please specify):` = "survey_sex3mo_cntrcptv_other",
    `survey_sex3mo_cntrcptv_temperature)` = "survey_sex3mo_cntrcptv_temp"
  )
  new_labels <- list(
    survey_sex3mo_cntrcptv_iud = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: An IUD or implant",
    survey_sex3mo_cntrcptv_patch = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Birth control patch",
    survey_sex3mo_cntrcptv_pill = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Birth control pills",
    survey_sex3mo_cntrcptv_condom = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Condoms",
    survey_sex3mo_cntrcptv_fam = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Fertility awareness method",
    survey_sex3mo_cntrcptv_nosex = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: I have not had vaginal sex",
    survey_sex3mo_cntrcptv_none = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: No method was used",
    survey_sex3mo_cntrcptv_unsure = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Not sure",
    survey_sex3mo_cntrcptv_shot = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: or shot (Depo-Provera)",
    survey_sex3mo_cntrcptv_pullout = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Pulling out/withdrawal",
    survey_sex3mo_cntrcptv_rhythm = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: rhythm method",
    survey_sex3mo_cntrcptv_ring = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: ring (Nuvaring)",
    survey_sex3mo_cntrcptv_other = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: Some other method",
    survey_sex3mo_cntrcptv_temp = "In the past 3 months, which of the following methods did you or your partner use to prevent pregnancy during vaginal sex?: temperature"
  )
  
  return_data <- prebind_data(filtered_data, variable_prefix, new_names, new_labels, FALSE, separator)
  return(return_data)
}

sex3mo_extype_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,survey_sex3mo_extype = `exchsex_types`)
  variable_prefix <- "survey_sex3mo_extype"
  separator <- ","
  
  new_names <- c(
    `survey_sex3mo_extype_Anal sex` = "survey_sex3mo_extype_anal",
    `survey_sex3mo_extype_no condoms/bareback` = "survey_sex3mo_extype_bareback",
    `survey_sex3mo_extype_Oral sex` = "survey_sex3mo_extype_oral",
    `survey_sex3mo_extype_with a condom` = "survey_sex3mo_extype_condom",
    `survey_sex3mo_extype_no condom` = "survey_sex3mo_extype_nocondom",
    `survey_sex3mo_extype_no condom/dental dam` = "survey_sex3mo_extype_nodam",
    `survey_sex3mo_extype_Vaginal sex` = "survey_sex3mo_extype_vaginal",
    `survey_sex3mo_extype_with a condom/dental dam` = "survey_sex3mo_dam"
  )
  new_labels <- list(
    survey_sex3mo_extype_anal = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Anal sex",
    survey_sex3mo_extype_bareback = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: no condom/bareback",
    survey_sex3mo_extype_oral = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Oral sex",
    survey_sex3mo_extype_condom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: with a condom",
    survey_sex3mo_extype_nocondom = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: no condom/bareback",
    survey_sex3mo_extype_nodam = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: no condom/dental dam",
    survey_sex3mo_extype_vaginal = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: Vaginal sex",
    survey_sex3mo_extype_dam = "What types of sex did you trade in the past 3 months for money, drugs, a place to stay, food or meals, or anything else?: with a condom/dental dam"
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