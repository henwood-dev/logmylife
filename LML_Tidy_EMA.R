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
  filtered_data <- rename(filtered_ema,social_other = Q1_a_socialother)
  new_names <-c(
    `social_other_Case worker or agency staff/volunteer` = "social_other_staff" ,
    `social_other_Family (biological or foster)` = "social_other_family",
    `social_other_Friends from home / before you were homeless` = "social_other_homefriends",
    `social_other_Friends or peers from the street or an agency` = "social_other_streetfriends",
    `social_other_Law enforcement (police, security, etc.)` = "social_other_police",
    `social_other_People from work or school` = "social_other_coworker",
    `social_other_Romantic/sexual partner` = "social_other_partner",
    `social_other_Someone I don't know well/random person` = "social_other_stranger")
  new_labels <- c(
    social_other_staff = "Case worker or agency staff/volunteer",
    social_other_family =  "Family (biological or foster)",
    social_other_homefriends = "Friends from home / before you were homeless",
    social_other_streetfriends = "Friends or peers from the street or an agency",
    social_other_police = "Law enforcement (police, security, etc.)",
    social_other_coworker = "People from work or school",
    social_other_partner = "Romantic/sexual partner",
    social_other_stranger = "Someone I don't know well/random person"
    )
  return_data <- prebind_data(filtered_data, "social_other", new_names, new_labels)
  return(return_data)
}

stressevents_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,stress_events = Q10_stressevents)
  new_names <- c(
    `stress_events_I felt threatened or harrassed` = "stress_events_threat",
    `stress_events_I got injured or became ill` = "stress_events_injured",
    `stress_events_Interaction with security/law enforcement` = "stress_events_law",
    `stress_events_None of the above` = "stress_events_none",
    `stress_events_Physical fight` = "stress_events_physicalfight",
    `stress_events_Received bad news about something important` = "stress_events_badnews",
    `stress_events_Received good news about something important` = "stress_events_goodnews",
    `stress_events_Verbal fight or argument` = "stress_events_verbalfight"
    )
  new_labels <-  c(
    stress_events_threat = "I felt threatened or harrassed",
    stress_events_injured = "I got injured or became ill",
    stress_events_law = "Interaction with security/law enforcement",
    stress_events_none = "None of the above",
    stress_events_physicalfight = "Physical fight",
    stress_events_badnews = "Received bad news about something important",
    stress_events_goodnews = "Received good news about something important",
    stress_events_verbalfight = "Verbal fight or argument"
    )
  return_data <- prebind_data(filtered_data, "stress_events", new_names, new_labels)
  return(return_data)
}

tobacco_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,tobacco = Q11_tobacco)
  new_names <- c(
    `tobacco_A tobacco product not listed here` = "tobacco_other",
    `tobacco_Chewing tobacco/dip` = "tobacco_chew",
    `tobacco_E-cigarettes/vaped tobacco` = "tobacco_vape",
    `tobacco_I have not used tobacco` = "tobacco_none",
    `tobacco_Paper cigarettes` = "tobacco_paper"
     )
  new_labels <- c(
    tobacco_other = "A tobacco product not listed here,",
    tobacco_chew = "Chewing tobacco/dip,",
    tobacco_vape = "E-cigarettes/vaped tobacco,",
    tobacco_none = "I have not used tobacco,",
    tobacco_paper = "Paper cigarettes"
    )
  return_data <- prebind_data(filtered_data, "tobacco", new_names, new_labels)
  return(return_data)
}

alcohol_where_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,alcohol_where = Q12_b_alcohol_where)
  new_names <- c(
    `alcohol_where_In transit (bus, car, etc.)` = "alcohol_where_transit",
    `alcohol_where_My apartment/residence` = "alcohol_where_apartment",
    `alcohol_where_Other` = "alcohol_where_other",
    `alcohol_where_Outdoors (park, beach, sidewalk, etc.)` = "alcohol_where_outdoors",
    `alcohol_where_Place of business (restaurant, bar, mall, etc.)` = "alcohol_where_business",
    `alcohol_where_Residence of someone else` = "alcohol_where_otherresidence",
    `alcohol_where_School or work` = "alcohol_where_schoolwork",
    `alcohol_where_Social service agency (drop-in, shelter, DPSS, etc.)` = "alcohol_where_service",
    `alcohol_where_Someone else’s residence` = "alcohol_where_otherresidence2"
     ) 
  new_labels <- c(
    alcohol_where_transit = "In transit (bus,car,etc),",
    alcohol_where_apartment = "My apartment/residence,",
    alcohol_where_other = "Other,",
    alcohol_where_outdoors = "Outdoors (park,beach,sidewalk,etc),",
    alcohol_where_business = "Place of business (restaurant, bar, mall, etc),",
    alcohol_where_otherresidence = "Someone else's residence,",
    alcohol_where_schoolwork = "School or work,",
    alcohol_where_service = "Social service agency (drop in, shelter, DPSS, etc)",
    alcohol_where_otherresidence2 = "Delete"
  )
  return_data <- prebind_data(filtered_data, "alcohol_where", new_names, new_labels)

  return_data <- return_data %>% 
    mutate(alcohol_where_otherresidence = ifelse(alcohol_where_otherresidence2 == 1,
                                                 1,alcohol_where_otherresidence)) %>%
    select(-alcohol_where_otherresidence2) 
  
  return(return_data)
}

alcohol_who_other_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,alcohol_social_other = Q12_d_alcohol_who_other)
  new_names <- c(
    `alcohol_social_other_Family (biological or foster)` = "alcohol_social_other_family",
    `alcohol_social_other_Friends from home / before you were homeless` = "alcohol_social_other_homefriends",
    `alcohol_social_other_Friends or peers from the street or an agency` = "alcohol_social_other_streetfriends",
    `alcohol_social_other_People from work or school` = "alcohol_social_other_coworker",
    `alcohol_social_other_Romantic/sexual partner` = "alcohol_social_other_partner",
    `alcohol_social_other_Someone I don't know well/random person` = "alcohol_social_other_stranger"
    )

  new_labels <- c(
    alcohol_social_other_family = "Family (biological or foster),",
    alcohol_social_other_homefriends = "Friends from home / before you were homeless,",
    alcohol_social_other_streetfriends = "Friends or peers from the street or an agency,",
    alcohol_social_other_coworker = "People from work or school,",
    alcohol_social_other_partner = "Romantic/sexual partner,",
    alcohol_social_other_stranger = "Someone I don't know well/random person"
    )
  return_data <- prebind_data(filtered_data, "alcohol_social_other", new_names, new_labels)
  
  return(return_data)
}

drugs_type_ema <- function(filtered_ema){
  filtered_ema_new <- mutate(filtered_ema,Q13_a_drugs_type = ifelse(Q13_a_drugs_type == "Ecstasy / MDMA / “Molly”","Ecstasy / MDMA / Molly",Q13_a_drugs_type))
  filtered_data <- rename(filtered_ema_new,drugs_type = Q13_a_drugs_type)
  new_names <- c(
    `drugs_type_Ecstasy / MDMA / Molly` = "drugs_type_ecstasy",
    `drugs_type_Hallucinogens/psychedelics` = "drugs_type_hallucinogens",
    `drugs_type_Marijuana` = "drugs_type_marijuana",
    `drugs_type_Meth` = "drugs_type_meth",
    `drugs_type_Other` = "drugs_type_other",
    `drugs_type_Prescription drugs, not as prescribed (Rx cough syrup, Oxycontin, Xanax, etc.)` = "drugs_type_rx",
    `drugs_type_Synthetic marijuana (K2, Spice, etc.)` = "drugs_type_spice"
    )
  new_labels <- c(
    drugs_type_ecstasy = "Ecstasy / MDMA / Molly,",
    drugs_type_hallucinogens = "Hallucinogens/psychadelics,",
    drugs_type_marijuana = "Marjuana,",
    drugs_type_meth = "Meth,",
    drugs_type_other = "Other,",
    drugs_type_rx = "Prescription drugs, not as prescribed (Rx cough syrup, Oxycontin, Xanax, etc.),",
    drugs_type_spice = "Synthetic marijuana (K2, Spice, etc.)"
    )
  return_data <- prebind_data(filtered_data, "drugs_type", new_names, new_labels)
    
  return(return_data)
}

drugs_where_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,drugs_where = Q13_b_drugs_where)
  new_names <- c(
    `drugs_where_In transit (bus, car, etc.)` = "drugs_where_transit",
    `drugs_where_My apartment/residence` = "drugs_where_apartment",
    `drugs_where_Other` = "drugs_where_other",
    `drugs_where_Outdoors (park, beach, sidewalk, etc.)` = "drugs_where_outdoors",
    `drugs_where_Place of business (restaurant, bar, mall, etc.)` = "drugs_where_business",
    `drugs_where_Residence of someone else` = "drugs_where_otherresidence",
    `drugs_where_School or work` = "drugs_where_schoolwork",
    `drugs_where_Social service agency (drop-in, shelter, DPSS, etc.)` = "drugs_where_service",
    `drugs_where_Someone else’s residence` = "drugs_where_otherresidence2"
    )
   new_labels <- c(
      drugs_where_transit = "In transit (bus,car,etc),",
      drugs_where_apartment = "My apartment/residence,",
      drugs_where_other = "Other,",
      drugs_where_outdoors = "Outdoors (park,beach,sidewalk,etc),",
      drugs_where_business = "Place of business (restaurant, bar, mall, etc),",
      drugs_where_otherresidence = "Someone else's residence,",
      drugs_where_schoolwork = "School or work,",
      drugs_where_service = "Social service agency (drop in, shelter, DPSS, etc)",
      drugs_where_otherresidence2 = "Someone else's residence,"
      )
   
  return_data <- prebind_data(filtered_data, "drugs_where", new_names, new_labels) %>%
    mutate(drugs_where_otherresidence = ifelse(drugs_where_otherresidence2 == 1,
                                                 1,drugs_where_otherresidence)) %>%
    select(-drugs_where_otherresidence2)
  
  return(return_data)
}

drugs_who_other_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,drugs_social_other = Q13_d_drugs_who_other)
  new_names <- c(
    `drugs_social_other_Family (biological or foster)` = "drugs_social_other_family",
    `drugs_social_other_Friends from home / before you were homeless` = "drugs_social_other_homefriends",
    `drugs_social_other_Friends or peers from the street or an agency` = "drugs_social_other_streetfriends",
    `drugs_social_other_People from work or school` = "drugs_social_other_coworker",
    `drugs_social_other_Romantic/sexual partner` = "drugs_social_other_partner",
    `drugs_social_other_Someone I don't know well/random person` = "drugs_social_other_stranger"
    )
  new_labels <- c(
        drugs_social_other_family = "Family (biological or foster),",
        drugs_social_other_homefriends = "Friends from home / before you were homeless,",
        drugs_social_other_streetfriends = "Friends or peers from the street or an agency,",
        drugs_social_other_coworker = "People from work or school,",
        drugs_social_other_partner = "Romantic/sexual partner,",
        drugs_social_other_stranger = "Someone I don't know well/random person"
      )

  return_data <- prebind_data(filtered_data, "drugs_social_other", new_names, new_labels)
    
  return(return_data)
}

tempted_where_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,tempted_where = Q14_a_tempted_where)
  new_names <- c(
    `tempted_where_In transit (bus, car, etc.)` = "tempted_where_transit",
    `tempted_where_My apartment/residence` = "tempted_where_apartment",
    `tempted_where_Other` = "tempted_where_other",
    `tempted_where_Outdoors (park, beach, sidewalk, etc.)` = "tempted_where_outdoors",
    `tempted_where_Place of business (restaurant, bar, mall, etc.)` = "tempted_where_business",
    `tempted_where_Residence of someone else` = "tempted_where_otherresidence",
    `tempted_where_School or work` = "tempted_where_schoolwork",
    `tempted_where_Social service agency (drop-in, shelter, DPSS, etc.)` = "tempted_where_service",
    `tempted_where_Social service agency (drop-in, shelter, DPSS&#65292; etc.)` = "tempted_where_service2",
    `tempted_where_Someone else’s residence` = "tempted_where_otherresidence2"
    )
  new_labels <- c(
    tempted_where_transit = "In transit (bus,car,etc),",
    tempted_where_apartment = "My apartment/residence,",
    tempted_where_other = "Other,",
    tempted_where_outdoors = "Outdoors (park,beach,sidewalk,etc),",
    tempted_where_business = "Place of business (restaurant, bar, mall, etc),",
    tempted_where_otherresidence = "Someone else's residence,",
    tempted_where_otherresidence2 = "Someone else's residence,",
    tempted_where_schoolwork = "School or work,",
    tempted_where_service = "Social service agency (drop in, shelter, DPSS, etc)",
    tempted_where_service2 = "Social service agency (drop in, shelter, DPSS, etc)"
  )
  
  return_data <- prebind_data(filtered_data, "tempted_where", new_names, new_labels) %>%
    mutate(tempted_where_otherresidence = ifelse(tempted_where_otherresidence2 == 1,
                                                 1,tempted_where_otherresidence)) %>%
    mutate(tempted_where_service = ifelse(tempted_where_service2 == 1,
                                          1,tempted_where_service)) %>%
    select(-tempted_where_otherresidence2,-tempted_where_service2)
    

  return(return_data)
}

tempted_who_other_ema <- function(filtered_ema){
  filtered_data <- rename(filtered_ema,tempted_social_other = Q14_c_tempted_who_other)
  new_names <- c(
    `tempted_social_other_Family (biological or foster)` = "tempted_social_other_family",
    `tempted_social_other_Friends from home / before you were homeless` = "tempted_social_other_homefriends",
    `tempted_social_other_Friends or peers from the street or an agency` = "tempted_social_other_streetfriends",
    `tempted_social_other_People from work or school` = "tempted_social_other_coworker",
    `tempted_social_other_Romantic/sexual partner` = "tempted_social_other_partner",
    `tempted_social_other_Someone I don't know well/random person` = "tempted_social_other_stranger"
      )
  new_labels <- c(
        tempted_social_other_family = "Family (biological or foster),",
        tempted_social_other_homefriends = "Friends from home / before you were homeless,",
        tempted_social_other_streetfriends = "Friends or peers from the street or an agency,",
        tempted_social_other_coworker = "People from work or school,",
        tempted_social_other_partner = "Romantic/sexual partner,",
        tempted_social_other_stranger = "Someone I don't know well/random person"
      )
    
  return_data <- prebind_data(filtered_data, "tempted_social_other", new_names, new_labels)
  
  return(return_data)
}
