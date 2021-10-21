# Get Netatmo data through API:
library(httr)
library(xml2)
library(jsonlite)

library(tidyverse)
library(lubridate)

library(keyring)
library(pins)

# NB: Gives error when access tokens expires: refresh_token does not work??
# Temporary solution: remove .httr-oauth file and re-authorize

# Query parameters for GET request:
# scale = c(max, 30min, 1hour, 3hours, 1day, 1week, 1month)
# type = c("Temperature", "Humidity", "CO2", "Pressure", "Noise")

# 1) function to get token: ----
get.netatmo.token <- function(){
  # create an OAuth application:
  app <- oauth_app("netatmo", 
                   key = key_get("client_id", keyring= "netatmo"), 
                   secret = key_get("secret", keyring= "netatmo"))
  # describe an OAuth endpoint:
  endpoint <- oauth_endpoint(
    request = NULL,
    authorize = "authorize",
    access = "token",
    base_url = "https://api.netatmo.com/oauth2"
  )
  # get token:
  token <- oauth2.0_token(endpoint, app, as_header = FALSE, 
                          scope = "read_homecoach")
}

# 2) function to get data from start time to stop, iterating over 1024 limit ----
get.netatmo.data <- function(type = c("Temperature", "Humidity", "CO2", "Pressure", "Noise"), 
                             date_begin, date_end, tz="CET"){
    
  date_begin <- as_datetime(date_begin, tz = tz)
  begin_timestamp <- as.numeric(date_begin)
  date_end <- as_datetime(date_end, tz = tz)
  
    if(date_end > ymd(Sys.Date(), tz = tz)){
      date_end <- ymd(Sys.Date(), tz = tz)
    }
    end_timestamp <- as.numeric(date_end)
    
    type_as_string <- paste(type, collapse = ",")
    
    url <- "https://api.netatmo.com/api/getmeasure"
    
    df_list <- list()
    i <- 1
    done <- FALSE
    
    while (!done){
      get_data <- GET(url, config=get.netatmo.token(), 
                      query=list(device_id = key_get("device_id", keyring="netatmo"), 
                                 scale = "max",
                                 type = type_as_string,
                                 limit = 1024,
                                 date_begin = begin_timestamp,
                                 date_end = end_timestamp))
      
      df_list[[i]] <- fromJSON(content(get_data, "text"), 
                                      flatten = FALSE)[["body"]] %>%
        mutate(across(where(is.list), 
                      ~ map(.x, ~ as.data.frame(.x) %>% 
                              setNames(type)))) %>%
        unnest(cols = value) %>%
        group_by(beg_time) %>%
        mutate(time = ifelse(row_number() == 1, 
                             beg_time, beg_time + cumsum(step_time) - step_time)) %>%
        ungroup() 
      
      time_to_end <- end_timestamp - rev(df_list[[i]]$time)[1]
      
      if(time_to_end < quantile(df_list[[i]]$step_time, 0.95, na.rm = TRUE) |
         nrow(df_list[[i]]) < 1000){
        done <- TRUE
      } else{
        begin_timestamp <- rev(df_list[[i]]$time)[1] + 1
        i <- i + 1
      }
    }
    
    df <- rbind_pages(df_list) %>%
      select(-beg_time, -step_time) %>%
      mutate(time = as_datetime(time, tz = tz))
}

# 3) to avoid loading everything create a function that loads the existing Netatmo data and then adds data till today (with max of 3 months to load at once)
update.netatmo.data <- function(tz="CET"){
  Netatmo <- pin_get("Netatmo")
  
  last_timestamp <- as.numeric(rev(Netatmo$time)[1])
  last_datetime <- as_datetime(last_timestamp, tz=tz)
  three_months_later <- ymd_hms(paste(ym(paste(year(last_datetime), month(last_datetime))) + months(4),
                                      "00:00:00"), tz = tz)
  
  if(three_months_later < Sys.Date()){
    date_end <- three_months_later
  } else{
    date_end <- Sys.Date()
  }
  
  nextdata <- get.netatmo.data(type = c("Temperature", "Humidity", "CO2", "Pressure", "Noise"),
                                date_begin = last_timestamp + 1, date_end = date_end)
  
  Netatmo <- bind_rows(Netatmo, nextdata)
  pin(Netatmo, "Netatmo")
}
update.netatmo.data()
