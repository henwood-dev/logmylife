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

romance_partners_gender_baseline <- function(filtered_baseline){
  filtered_data <- rename(filtered_baseline,history_romance_pgen = `romrel_ptnrsgndr`)
  variable_prefix <- "history_romance_pgen"
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

factor_keep_rename <- function(var_column, level_vector){
  original_label_name <- attributes(var_column)$label
  factored_column <- factor(var_column, levels = level_vector)
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