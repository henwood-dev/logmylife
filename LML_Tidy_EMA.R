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

social_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,social_other = Q1_a_socialother)
  return_ema <- cSplit_e(return_ema, "social_other",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("social_other_")),funs(ifelse(is.na(social_other),NA,.))) %>%
    select(starts_with("social_other_")) %>%
    rename(social_other_staff = `social_other_Case worker or agency staff/volunteer`,
           social_other_family = `social_other_Family (biological or foster)`,
           social_other_homefriends = `social_other_Friends from home / before you were homeless`,
           social_other_streetfriends = `social_other_Friends or peers from the street or an agency`,
           social_other_police = `social_other_Law enforcement (police, security, etc.)`,
           social_other_coworker = `social_other_People from work or school`,
           social_other_partner = `social_other_Romantic/sexual partner`,
           social_other_stranger = `social_other_Someone I don't know well/random person`) %>%
    set_label(c("Case worker or agency staff/volunteer",
                "Family (biological or foster)",
                "Friends from home / before you were homeless",
                "Friends or peers from the street or an agency",
                "Law enforcement (police, security, etc.)",
                "People from work or school",
                "Romantic/sexual partner",
                "Someone I don't know well/random person"))
  return(return_ema)
}

stressevents_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,stress_events = Q10_stressevents)
  return_ema <- cSplit_e(return_ema, "stress_events",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("stress_events_")),funs(ifelse(is.na(stress_events),NA,.))) %>%
    select(starts_with("stress_events_")) %>%
    # select(-stress_events_0,
    #        -`stress_events_A tobacco product not listed here`,
    #        -`stress_events_I have not used tobacco`,
    #        -`stress_events_Paper cigarettes`) %>%
    rename(stress_events_threat = `stress_events_I felt threatened or harrassed`,
           stress_events_injured = `stress_events_I got injured or became ill`,
           stress_events_law = `stress_events_Interaction with security/law enforcement`,
           stress_events_none = `stress_events_None of the above`,
           stress_events_physicalfight = `stress_events_Physical fight`,
           stress_events_badnews = `stress_events_Received bad news about something important`,
           stress_events_goodnews = `stress_events_Received good news about something important`,
           stress_events_verbalfight = `stress_events_Verbal fight or argument`) %>%
    set_label(c("I felt threatened or harrassed",
                "I got injured or became ill",
                "Interaction with security/law enforcement",
                "None of the above",
                "Physical fight",
                "Received bad news about something important",
                "Received good news about something important",
                "Verbal fight or argument"))
  return(return_ema)
}

tobacco_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,tobacco = Q11_tobacco)
  return_ema <- cSplit_e(return_ema, "tobacco",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("tobacco_")),funs(ifelse(is.na(tobacco),NA,.))) %>%
    select(starts_with("tobacco_")) %>%
    # select(-tobacco_0,
    #        -tobacco_1) %>%
    rename(tobacco_other = `tobacco_A tobacco product not listed here`,
           tobacco_chew = `tobacco_Chewing tobacco/dip`,
           tobacco_vape = `tobacco_E-cigarettes/vaped tobacco`,
           tobacco_none = `tobacco_I have not used tobacco`,
           tobacco_paper = `tobacco_Paper cigarettes`) %>%
    set_label(c("A tobacco product not listed here",
                "Chewing tobacco/dip",
                "E-cigarettes/vaped tobacco",
                "I have not used tobacco",
                "Paper cigarettes"))
  return(return_ema)
}

alcohol_where_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,alcohol_where = Q12_b_alcohol_where)
  return_ema <- cSplit_e(return_ema, "alcohol_where",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("alcohol_where_")),funs(ifelse(is.na(alcohol_where),NA,.))) %>%
    select(starts_with("alcohol_where_")) %>%
    #select(-`alcohol_where_question is not displayed`,
    #      -alcohol_where_skipped) %>%
    rename(alcohol_where_transit = `alcohol_where_In transit (bus, car, etc.)`,
           alcohol_where_apartment = `alcohol_where_My apartment/residence`,
           alcohol_where_other = `alcohol_where_Other`,
           alcohol_where_outdoors = `alcohol_where_Outdoors (park, beach, sidewalk, etc.)`,
           alcohol_where_business = `alcohol_where_Place of business (restaurant, bar, mall, etc.)`,
           alcohol_where_otherresidence = `alcohol_where_Residence of someone else`,
           alcohol_where_schoolwork = `alcohol_where_School or work`,
           alcohol_where_service = `alcohol_where_Social service agency (drop-in, shelter, DPSS, etc.)`,
           alcohol_where_otherresidence2 = `alcohol_where_Someone else’s residence`) %>%
    mutate(alcohol_where_otherresidence = ifelse(alcohol_where_otherresidence2 == 1,
                                                 1,alcohol_where_otherresidence)) %>%
    select(-alcohol_where_otherresidence2) %>%
    set_label(c("In transit (bus,car,etc)",
                "My apartment/residence",
                "Other",
                "Outdoors (park,beach,sidewalk,etc)",
                "Place of business (restaurant, bar, mall, etc)",
                "Someone else's residence",
                "School or work",
                "Social service agency (drop in, shelter, DPSS, etc)"))
  
  return(return_ema)
}

alcohol_who_other_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,alcohol_social_other = Q12_d_alcohol_who_other)
  return_ema <- cSplit_e(return_ema, "alcohol_social_other",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("alcohol_social_other_")),funs(ifelse(is.na(alcohol_social_other),NA,.))) %>%
    select(starts_with("alcohol_social_other_")) %>%
    rename(
      alcohol_social_other_family = `alcohol_social_other_Family (biological or foster)`,
      alcohol_social_other_homefriends = `alcohol_social_other_Friends from home / before you were homeless`,
      alcohol_social_other_streetfriends = `alcohol_social_other_Friends or peers from the street or an agency`,
      alcohol_social_other_coworker = `alcohol_social_other_People from work or school`,
      alcohol_social_other_partner = `alcohol_social_other_Romantic/sexual partner`,
      alcohol_social_other_stranger = `alcohol_social_other_Someone I don't know well/random person`) %>%
    set_label(c("Family (biological or foster)",
                "Friends from home / before you were homeless",
                "Friends or peers from the street or an agency",
                "People from work or school",
                "Romantic/sexual partner",
                "Someone I don't know well/random person"))
  
  return(return_ema)
}

drugs_type_ema <- function(filtered_ema){
  filtered_ema_new <- mutate(filtered_ema,Q13_a_drugs_type = ifelse(Q13_a_drugs_type == "Ecstasy / MDMA / “Molly”","Ecstasy / MDMA / Molly",Q13_a_drugs_type))
  return_ema <- rename(filtered_ema_new,drugs_type = Q13_a_drugs_type)
  return_ema <- cSplit_e(return_ema, "drugs_type",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("drugs_type_")),funs(ifelse(is.na(drugs_type),NA,.))) %>%
    select(starts_with("drugs_type_")) %>%
    rename(
      drugs_type_ecstasy = `drugs_type_Ecstasy / MDMA / Molly`,
      drugs_type_hallucinogens = `drugs_type_Hallucinogens/psychedelics`,
      drugs_type_marijuana = `drugs_type_Marijuana`,
      drugs_type_meth = `drugs_type_Meth`,
      drugs_type_other = `drugs_type_Other`,
      drugs_type_rx = `drugs_type_Prescription drugs, not as prescribed (Rx cough syrup, Oxycontin, Xanax, etc.)`,
      drugs_type_spice = `drugs_type_Synthetic marijuana (K2, Spice, etc.)`) %>%
    set_label(c("Ecstasy / MDMA / Molly",
                "Hallucinogens/psychadelics",
                "Marjuana",
                "Meth",
                "Other",
                "Prescription drugs, not as prescribed (Rx cough syrup, Oxycontin, Xanax, etc.)",
                "Synthetic marijuana (K2, Spice, etc.)"))
  return(return_ema)
}

drugs_where_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,drugs_where = Q13_b_drugs_where)
  return_ema <- cSplit_e(return_ema, "drugs_where",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("drugs_where_")),funs(ifelse(is.na(drugs_where),NA,.))) %>%
    select(starts_with("drugs_where_")) %>%
    # select(-tobacco_0,
    #        -tobacco_1) %>%
    rename(drugs_where_transit = `drugs_where_In transit (bus, car, etc.)`,
           drugs_where_apartment = `drugs_where_My apartment/residence`,
           drugs_where_other = `drugs_where_Other`,
           drugs_where_outdoors = `drugs_where_Outdoors (park, beach, sidewalk, etc.)`,
           drugs_where_business = `drugs_where_Place of business (restaurant, bar, mall, etc.)`,
           drugs_where_otherresidence = `drugs_where_Residence of someone else`,
           drugs_where_schoolwork = `drugs_where_School or work`,
           drugs_where_service = `drugs_where_Social service agency (drop-in, shelter, DPSS, etc.)`,
           drugs_where_otherresidence2 = `drugs_where_Someone else’s residence`) %>%
    mutate(drugs_where_otherresidence = ifelse(drugs_where_otherresidence2 == 1,
                                               1,drugs_where_otherresidence)) %>%
    select(-drugs_where_otherresidence2) %>%
    set_label(c("In transit (bus,car,etc)",
                "My apartment/residence",
                "Other",
                "Outdoors (park,beach,sidewalk,etc)",
                "Place of business (restaurant, bar, mall, etc)",
                "Someone else's residence",
                "School or work",
                "Social service agency (drop in, shelter, DPSS, etc)"))
  
  return(return_ema)
}

drugs_who_other_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,drugs_social_other = Q13_d_drugs_who_other)
  return_ema <- cSplit_e(return_ema, "drugs_social_other",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("drugs_social_other_")),funs(ifelse(is.na(drugs_social_other),NA,.))) %>%
    select(starts_with("drugs_social_other_")) %>%
    rename(
      drugs_social_other_family = `drugs_social_other_Family (biological or foster)`,
      drugs_social_other_homefriends = `drugs_social_other_Friends from home / before you were homeless`,
      drugs_social_other_streetfriends = `drugs_social_other_Friends or peers from the street or an agency`,
      drugs_social_other_coworker = `drugs_social_other_People from work or school`,
      drugs_social_other_partner = `drugs_social_other_Romantic/sexual partner`,
      drugs_social_other_stranger = `drugs_social_other_Someone I don't know well/random person`) %>%
    set_label(c("Family (biological or foster)",
                "Friends from home / before you were homeless",
                "Friends or peers from the street or an agency",
                "People from work or school",
                "Romantic/sexual partner",
                "Someone I don't know well/random person"))
  return(return_ema)
}

tempted_where_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,tempted_where = Q14_a_tempted_where)
  return_ema <- cSplit_e(return_ema, "tempted_where",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    rename(tempted_where_renamethis1 = `tempted_where_Social service agency (drop-in, shelter, DPSS， etc.)`,
           tempted_where_renamethis2 = `tempted_where_Social service agency (drop-in, shelter, DPSS&#65292; etc.)`) %>%
    mutate_at(vars(starts_with("tempted_where_")),funs(ifelse(is.na(tempted_where),NA,.))) %>%
    select(starts_with("tempted_where_")) %>%
    # select(-tobacco_0,
    #        -tobacco_1) %>%
    rename(tempted_where_transit = `tempted_where_In transit (bus, car, etc.)`,
           tempted_where_apartment = `tempted_where_My apartment/residence`,
           tempted_where_other = `tempted_where_Other`,
           tempted_where_outdoors = `tempted_where_Outdoors (park, beach, sidewalk, etc.)`,
           tempted_where_business = `tempted_where_Place of business (restaurant, bar, mall, etc.)`,
           tempted_where_otherresidence = `tempted_where_Residence of someone else`,
           tempted_where_schoolwork = `tempted_where_School or work`,
           tempted_where_service = tempted_where_renamethis1,
           tempted_where_service2 = tempted_where_renamethis2,
           tempted_where_otherresidence2 = `tempted_where_Someone else’s residence`) %>%
    mutate(tempted_where_otherresidence = ifelse(tempted_where_otherresidence2 == 1,
                                                 1,tempted_where_otherresidence)) %>%
    mutate(tempted_where_service = ifelse(tempted_where_service2 == 1,
                                          1,tempted_where_service)) %>%
    select(-tempted_where_otherresidence2,-tempted_where_service2) %>%
    set_label(c("In transit (bus,car,etc)",
                "My apartment/residence",
                "Other",
                "Outdoors (park,beach,sidewalk,etc)",
                "Place of business (restaurant, bar, mall, etc)",
                "Someone else's residence",
                "School or work",
                "Social service agency (drop in, shelter, DPSS, etc)"))
  
  return(return_ema)
}

tempted_who_other_ema <- function(filtered_ema){
  return_ema <- rename(filtered_ema,tempted_social_other = Q14_c_tempted_who_other)
  return_ema <- cSplit_e(return_ema, "tempted_social_other",type = "character", fill = 0, sep = ",|")
  return_ema <- return_ema %>%
    mutate_at(vars(starts_with("tempted_social_other_")),funs(ifelse(is.na(tempted_social_other),NA,.))) %>%
    select(starts_with("tempted_social_other_")) %>%
    rename(
      tempted_social_other_family = `tempted_social_other_Family (biological or foster)`,
      tempted_social_other_homefriends = `tempted_social_other_Friends from home / before you were homeless`,
      tempted_social_other_streetfriends = `tempted_social_other_Friends or peers from the street or an agency`,
      tempted_social_other_coworker = `tempted_social_other_People from work or school`,
      tempted_social_other_partner = `tempted_social_other_Romantic/sexual partner`,
      tempted_social_other_stranger = `tempted_social_other_Someone I don't know well/random person`) %>%
    set_label(c("Family (biological or foster)",
                "Friends from home / before you were homeless",
                "Friends or peers from the street or an agency",
                "People from work or school",
                "Romantic/sexual partner",
                "Someone I don't know well/random person"))
  
  return(return_ema)
}
