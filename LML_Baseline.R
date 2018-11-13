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

# Override
select <- dplyr::select


main_dir <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Person Level/_Raw Data/TSV"
v0_filepath <-"V0/LogMyLife Baseline Questionnaire_ver0_November 5, 2018_13.23.csv"
v1_filepath <-"V1/LogMyLife+Baseline+Questionnaire_ver1_Copy+%28edits+2017-05-30%29_November+6%2C+2018_12.53.csv"
v2_filepath <-"V2/LogMyLife+Baseline+Questionnaire_ver2_%28edits+2017-07-11%29_November+6%2C+2018_12.40.csv"
v3_filepath <-"V3/LogMyLife+Baseline+Questionnaire_ver3+%28Jan2018%29_November+6%2C+2018_12.48.csv"
v31_filepath <- "V31/LogMyLife+Baseline+Questionnaire_ver3.1+%28Feb2018%29_November+6%2C+2018_12.49.csv"
v4_filepath <-"V4/LogMyLife+Baseline+Questionnaire_ver4.0+%28Mar2018%29_November+6%2C+2018_12.51.csv"
v5_filepath <-"V5/LogMyLife+Baseline-SNI+Questionnaire_ver5.0+%28May2018%29_November+6%2C+2018_12.54.csv"
v6_filepath <- "V6/LogMyLife+Baseline-SNI+Questionnaire_ver6.0+%28July2018%29_November+6%2C+2018_12.55.csv"
v7_filepath <- "V7/LogMyLife+Baseline-SNI+Questionnaire_ver7.0+%28Oct2018%29_November+6%2C+2018_12.55.csv"

v1_sni_filepath <- "V1/SNI/LogMyLife+SNI+Questionnaire_ver1_November+6%2C+2018_13.01.csv"
v2_sni_filepath <- "V2/SNI/LogMyLife+SNI+Questionnaire_ver2_Copy+%28Revisions+2017-05-31%29_November+6%2C+2018_13.02.csv"
v3_sni_filepath <- "V3/SNI/LogMyLife+SNI+Questionnaire_ver3+%28Jan2018%29_November+6%2C+2018_13.03.csv"
v4_sni_filepath <- "V4/SNI/LogMyLife+SNI+Questionnaire_ver4.0+%28Feb2018_formerly3.1%29_November+6%2C+2018_13.04.csv"

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
  colnames(raw_data) <- raw_names
  return_data <- raw_data %>%
    filter(row_number() > 3)
  return(return_data)
  # 
  # sni_data <- read_csv(paste(main_dir,v1_sni_filepath, sep = "/"), col_names = FALSE)
  # sni_names <- sni_data %>% 
  #   filter(row_number()==1) %>%
  #   mutate_all(funs(str_to_lower(.))) %>%
  #   mutate_all(funs(gsub("[^[:alnum:][:space:]]"," ",.))) %>%
  #   mutate_all(funs(str_squish(.))) %>%
  #   mutate_all(funs(gsub(" ","_",.)))
  # sni_labels <- sni_data %>% 
  #   filter(row_number()==2)
  # for(i in 1:ncol(sni_labels)){
  #   label(sni_data[i]) <- sni_labels[,i]
  # }
  # colnames(sni_data) <- sni_names
  # return_sni <- sni_data %>%
  #   filter(row_number() > 3)
}
v1_fix_errors <- function(main_filepath,v1_filepath) {
  # v1_fix_1002 OK
  v1_raw <- return_raw_baseline(main_filepath,v1_filepath)
  v1_fix_1004 <- v1_raw %>%
    filter(responseid == "R_72GUjPfXIWKYOlP" | responseid == "R_1o0ursi841wl4tT") %>%
    summarize_all(funs(first(na.omit(.))))
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
      alc_30_binge_m = ifelse(pid == "2002",0,alc_30_binge_m),
      race = ifelse(pid == "2002","I don't know",race),
      race = ifelse(pid == "2003","I don't know",race),
      race = ifelse(pid == "2004","I don't know",race),
      sexori = ifelse(pid == "2004","Heterosexual or straight",sexori),
      sex = ifelse(pid == "2005","Male",sex),
      sucost_30d_marj = ifelse(pid == "2006","$51 - $250",sucost_30d_marj),
      sucost_30d_ildrug = ifelse(pid == "2006","Less than $50",sucost_30d_ildrug),
      race = ifelse(pid == "2006","I don't know",race),
      pid = ifelse(responseid == "R_wTrw4o4fQzrcgUh","3003",pid)) %>%
    filter(
      responseid != "R_1pyRsMTctbJuoK3" & 
      responseid != "R_1pAVgwmZ0NSDvrc" &
      responseid != "R_2cu51Ni0Lit4T0e" &
      responseid != "R_0NugAIF3VyvohKV" &
      responseid != "R_OiLgOEXdq9zF0iJ" &
      responseid != "R_1myHhk3o8mAi0GY"
      ) %>%
    bind_rows(v1_fix_1004) %>%
    set_label(unlist(pre_bind_labels))
}
v2_fix_errors <- function() {
  v2_raw <- return_raw_baseline(main_dir,v2_filepath)
  v2_fix <- v2_raw %>%
    mutate(
      race = ifelse(pid == "1008","Black or African-American",race),
      sexori = ifelse(pid == "1008","Heterosexual or straight",sexori),
      race = ifelse(pid == "1009","I don't know",race),
      race = ifelse(pid == "1015","I don't know",race),
      demo_racemin = ifelse(pid == "1015","Yes",NA)
      
    )
    
    
    #replace RACE = 3 if PID == 1008
  #replace SEXORI = 3 if PID == 1008
  #replace RACE = 99 if PID == 1009
  #replace RACE = 999 if PID == 1015
  replace DEMO_RACEMIN = 1 if PID == 1015
  was changed in Baseline Cleaning do file under header /*ruth's place TLP */
  was changed in Baseline Cleaning do file under header /*ruth's place TLP */
    replace RACE = 6 if PID == 1032
  recoded PID
  replace HISP = 1 if PID == 1037
  replace SEX = 1 if PID == 1041
  replace GENDER_F=. If PID==1041; replace GENDER_M=1 if PID==1041
  
  re-labled SITE_HOUSED=10 to represent "Harbor Interfaith SPA8 Rapid Rehousing"
  re-labled SITE_HOUSED=10 to represent "Harbor Interfaith SPA8 Rapid Rehousing"
  replace SITE_HOUSED = -10 if PID == 1052
  re-labled SITE_HOUSED=10 to represent "Harbor Interfaith SPA8 Rapid Rehousing"
  replace RACE = 7 if PID == 2006
  recoded PID
  replace SITE_UNHOUSED=1 if PID==2017
  "replace PID = 3025 if RESPONSEID == ""R_pFaeDxOuzvRPFkZ""
  "
  "replace RACE = 999 if PID == 1025
  
  (updated to if PID == 3025 on 10/27/27)"
  replace PID = 3046 if RESPONSEID == "R_2AQdJ7QXdBSRZDC"
  Marked as missing (999) for now
  recoded PID
  replace SEX = 2 if PID == 4008 /*NEED TO REVISIT THIS: do we want to keep as recoded to female? or change back to seen but not answered since we don't know for 100% sure? or analyze as female but with * in methods section explaining?*/
  
  recoded PID
}


  

