library("tidyverse")
library("haven")
library("readr")
library("janitor")
library("lubridate")

lifedata_file <- "/Users/eldin/Downloads/RAND_USC_V_NIS_Wide20200128_17_54_15.csv"

# Import wide Lifedata csv
wide_csv <- read_csv(lifedata_file)
checkin <- wide_csv %>%
  filter(`Session Name` == "Daytime Check in") %>%
  mutate(ema_comply = as.integer(Responded == "1"),
         date = as_date(`Notification Time`)) %>%
  rename(pid = `Participant ID`,
         ema_completed = `Completed Session`,
         ema_response_lapse = `Session Instance Response Lapse`,
         ema_response_length = `Session Length`,
         ema_reminders = `Reminders Delivered`,
         ema_notification_time = `Notification Time`,
         ema_index = `Notification No`,
         ema_gps_lat_start = `GPS Latitude Start`,
         ema_gps_lon_start = `GPS Longitude Start`,
         ema_gps_lat_end = `GPS Latitude Finish`,
         ema_gps_lon_end = `GPS Longitude Finish`) %>%
  rename(tired = `hungry (2)`,
         concentrate = concentrat,
         who_type_good = `who_type_I had GOOD or POSITIVE interaction(s) with others`,
         who_type_bad = `who_type_I had BAD or NEGATIVE interaction(s) with others`,
         who_type_neutral = `who_type_I had interactions that were not good or bad`) %>%
  select(-`Device ID`,-`Session Name`,-`Session Instance`,-Responded) %>%
  mutate_at(8:16,funs(recode_factor(., `1` = "Not at all", `2` = "A little", `3` = "Moderately",
                                    `4` = "Quite a bit", `5` = "Extremely"))) %>%
  mutate(safe = recode_factor(safe, `1` = "Very unsafe", `2` = "Somewhat unsafe", `3` = "Neither safe nor unsafe",
                               `4` = "Somewhat safe", `5` = "Very safe"),
         concentrate = recode_factor(concentrate, `1` = "None", `2` = "A little", `3` = "Somewhat",
                              `4` = "Quite a bit", `5` = "Very much"),
         thoughts = recode_factor(thoughts, `1` = "None", `2` = "A little", `3` = "Somewhat",
                              `4` = "Quite a bit", `5` = "Very much"),
         where = as_factor(where),
         who = recode_factor(who, `1` = "Yes", `2` = "No"),
         service = recode_factor(who, `1` = "Yes", `2` = "No"),
         bathroom = recode_factor(who, `1` = "Yes", `2` = "No", `3` = "I did not need a bathroom in the past two hours")) %>%
  remove_empty()

morning <- wide_csv %>%
  filter(`Session Name` == "Morning check in") %>%
  mutate(morning_comply = as.integer(Responded == "1"),
         date = as_date(`Notification Time`)) %>%
  rename(pid = `Participant ID`,
         sleep_location = sleep_loca,
         sleep_safe = safety,
         alcohol_tobacco = alctobyn,
         morning_completed = `Completed Session`,
         morning_response_lapse = `Session Instance Response Lapse`,
         morning_response_length = `Session Length`,
         morning_reminders = `Reminders Delivered`,
         morning_notification_time = `Notification Time`,
         morning_index = `Notification No`,
         morning_gps_lat_start = `GPS Latitude Start`,
         morning_gps_lon_start = `GPS Longitude Start`,
         morning_gps_lat_end = `GPS Latitude Finish`,
         morning_gps_lon_end = `GPS Longitude Finish`) %>%
  select(-`Device ID`,-`Session Name`,-`Session Instance`,-Responded,-`safety (2)`) %>%
  mutate(sleep_location = as_factor(sleep_location),
         sleep_safe = recode_factor(sleep_safe, `1` = "Very unsafe", `2` = "Somewhat unsafe", `3` = "Neither safe nor unsafe",
                              `4` = "Somewhat safe", `5` = "Very safe"),
         alcohol_tobacco = recode_factor(alcohol_tobacco, `1` = "Yes, alcohol", `2` = "Yes, tobacco",
                                         `3` = "Yes, both alcohol and tobacco", `4` = "No"),
         hard_drugs = recode_factor(hard_drugs, `1` = "Yes", `2` = "No")) %>%
  remove_empty()
evening <- wide_csv %>%
  filter(`Session Name` == "Evening check in") %>%
  mutate(evening_comply = as.integer(Responded == "1"),
         date = as_date(`Notification Time`)) %>%
  rename(pid = `Participant ID`,
         evening_completed = `Completed Session`,
         evening_response_lapse = `Session Instance Response Lapse`,
         evening_response_length = `Session Length`,
         evening_reminders = `Reminders Delivered`,
         evening_notification_time = `Notification Time`,
         evening_index = `Notification No`,
         evening_gps_lat_start = `GPS Latitude Start`,
         evening_gps_lon_start = `GPS Longitude Start`,
         evening_gps_lat_end = `GPS Latitude Finish`,
         evening_gps_lon_end = `GPS Longitude Finish`) %>%
  select(-`Device ID`,-`Session Name`,-`Session Instance`,-Responded) %>%
  remove_empty()

full_data <- checkin %>%
  left_join(morning, by = c("pid","date")) %>%
  left_join(evening, by = c("pid","date"))
  