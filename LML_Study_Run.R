rm(list=ls())

source("LML_Tidy.R", encoding = "UTF-8")

# Are you Eldin?
eldin <- TRUE

## Who are you?
person <- "lmldata"

if(eldin){
  ## Write the main data directory where stuff is stored
  data_dirname <- "C:/Users/dzubur/Desktop"
  ## Write the FILEPATH of the Fusion directory
  export_dirname <- "C:/Users/dzubur/Desktop/Fusion"
  ## Write the name, not path, to your wockets downloads folder here:
  wockets_dirname <-"Wockets"
  ## Write the name, not path, to your manual download folder here, otherwise leave as ""
  manual_dirname <- ""  
} else {
  ## Write the main data directory where stuff is stored
  data_dirname <- paste0("C:/Users/",person,"/Documents/Log My Life Study/Wockets Desktop App/Wockets-win32-v1.0.18/resources/app/src/srv")
  ## Write the FILEPATH of the Fusion directory
  export_dirname <- paste0("C:/Users/",person,"/Documents/Log My Life Study/Fusion Tables")
  ## Write the name, not path, to your wockets downloads folder here:
  wockets_dirname <-"LML"
  ## Write the name, not path, to your manual download folder here, otherwise leave as ""
  manual_dirname <- ""
}

## Do you want to skip manual data?
skip_manual <- TRUE
## Do you want to skip SNI data? (usually yes)
skip_sni = TRUE
## Location of SNI filename (usually default, doesn't matter if you're skipping)
sni_stata_filename <- "SNI.dta"

did_export <- export_person_level(data_dirname = data_dirname,
                    wockets_dirname = wockets_dirname,
                    manual_dirname = manual_dirname,
                    skip_manual = skip_manual,
                    skip_sni = skip_sni,
                    sni_stata_filename = sni_stata_filename,
                    export_dirname = export_dirname)

gps_ema <- tidy_gps(data_dirname = data_dirname,
                    wockets_dirname = wockets_dirname,
                    manual_dirname = manual_dirname,
                    skip_manual = skip_manual,
                    retain_names = TRUE)

fusion_ema <- gps_fusion_adapter(new_gps = gps_ema, export_dirname = export_dirname)

if(did_export){
  print("Successfully saved EMA person-level risk file.")
} else {
  print("Something went wrong with EMA person-level risk file, please contact Eldin.")
}

if(!is.null(fusion_ema)){
  print("Successfully saved fusion tables.")
} else {
  print("Something went wrong with fusion tables, please contact Eldin.")
}
