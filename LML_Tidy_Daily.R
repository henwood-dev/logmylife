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
  new_names <- c(
    `sex_exchange_where_Hotel/motel` = "sex_exchange_where_hotel",
    `sex_exchange_where_Shelter or drop-in` = "sex_exchange_where_shelter",
    `sex_exchange_where_Outside (park, beach)` = "sex_exchange_where_outside",
    `sex_exchange_where_The home of someone I know` = "sex_exchange_where_knownhome",
    `sex_exchange_where_The home of someone I don’t know or barely know` = "sex_exchange_where_unknownhome",
    `sex_exchange_where_Tent or improvised shelter` = "sex_exchange_where_tent",
    `sex_exchange_where_Car, RV, or other vehicle` = "sex_exchange_where_vehicle",
    `sex_exchange_where_Abandoned building or squat` = "sex_exchange_where_squat",
    `sex_exchange_where_Other` = "sex_exchange_where_other"
    )
  new_labels <- c(
      sex_exchange_where_hotel = "Hotel/motel,",
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
    select(-drugs_type_other2)

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
