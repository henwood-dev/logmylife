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

# Override
select <- dplyr::select


# Imports
source("LML_Tidy_Helpers.R", encoding = "UTF-8")
source("LML_Tidy_EMA.R", encoding = "UTF-8")
source("LML_Tidy_Daily.R", encoding = "UTF-8")
source("LML_Tidy.R", encoding = "UTF-8")

# Keys
if(.Platform$OS.type == "windows"){
  data_dirname <- "C:/Users/dzubur/Desktop/LML Raw Data"
} else {
  data_dirname <- "/Users/Eldin/Downloads/Data"
}
if(.Platform$OS.type == "windows"){
  sni_stata_filename <- "C:/Users/dzubur/SharePoint/T/Team/Eldin/LML Data Management/Data/SNI.dta"
} else {
  sni_stata_filename <- "/Users/Eldin/Downloads/Data/SNI.dta"
}
wockets_dirname <- "Wockets"
manual_dirname <- "Manual"
filename_varstub <- "PromptResponses_Youth.csv$"
id_varstub <- "mobile_$"
wockets_dirname <- "TAY SFTP"
prepend_surveys <- ".MobileTAY"

