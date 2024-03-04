library(lubridate)
library(dplyr)
library(data.table)
library(future)
library(furrr)
library(tidyverse)
library(arrow)
library(readxl)

# Read in birth and temp files

births <- read.csv("S:/Projects/External_PI/BirthsToxics/Data/births/final_birth_data/DBR_2001_2019_v1.csv")%>%
  mutate(Date = as.Date(dob, format="%m/%d/%y"))%>%
  rename(location = tractfips10)%>%
  filter(year(Date) >= 2011 & year(Date) <= 2019)

birth1 <- births
birth1$GEST <- as.numeric(birth1$gest)
birth1$Pre <- as.Date(birth1$Date) - ((birth1$GEST * 7) + (13*7))
birth1$Tr1 <- as.Date(birth1$Date) - (birth1$GEST * 7)
birth1$Tr2 <- as.Date(birth1$Date) - ((birth1$GEST * 7) - (13*7))
birth1$Tr3 <- as.Date(birth1$Date) - ((birth1$GEST * 7) - (26*7))
birth1$Last_Gest_Week <- as.Date(birth1$Date) - 7
birth1$Tr3_end <- birth1$Date
birth1 <- birth1 %>%
  select(location, dob_yyyy, Date, dbr_id, GEST, Date, Pre, Tr1, Tr2, Tr3, Last_Gest_Week, Tr3_end)
#birth1 <- arrange(birth1, Date)

temp <- read.csv("S:/Projects/External_PI/BirthsToxics/Data/temperature/heatwaves climate region/climate_region_heatwaves_may_sept_2011_2019_w_absolute.csv")%>%
  mutate(Date=as.Date(Date),
         location=geoid10)

Exposure <- temp %>% # Filter for exposure variables of interest
  rename(TAVG = Temperature)%>%
  select(-region, -geoid10)

# Create days function

create_days <- function(birth_data){
  
  birth_data <- birth_data %>%
    data.frame()
  
  # Calculate number of days between start and end date
  birth_data$duration <- as.numeric(birth_data$Tr3_end - birth_data$Pre) + 1
  
  start_day <- birth_data$Pre
  end_day <- birth_data$Tr3_end
  num_days <- birth_data$duration
  column_names <- paste0("Day_", 1:max(num_days))
  
  # Create a list of date ranges, padding shorter ranges with NA values
  date_ranges <- mapply(function(start, end, days) {
    seq(start, end, by = "day")[1:days]
  }, start_day, end_day, num_days)
  
  # Pad shorter date ranges with NA values
  max_length <- max(num_days)
  date_ranges <- lapply(date_ranges, function(x) {
    if (length(x) < max_length) {
      c(x, rep(NA, max_length - length(x)))
    } else {
      x
    }
  })
  
  birth_data[column_names] <- as.data.frame(do.call(rbind, date_ranges))
  
  # Find columns starting with "Day_"
  day_columns <- grep("^Day_", names(birth_data), value = TRUE)
  
  # Convert numeric columns to date format
  birth_data[, day_columns] <- lapply(birth_data[, day_columns], function(x) as.Date(as.numeric(x), format="%Y-%m-%d"))
  
  birth_data <- birth_data %>%
    select(-duration)
  
  return(birth_data)
}

# Filter for Tr3 days

birth_Tr3 <- birth2[,-c(12:284)]

####2019####

birth2019 <- birth_Tr3 %>%
  filter(dob_yyyy=='2019')

Exposure_filter <- Exposure %>%
  filter(Date >= "2018-01-01" & Date <= "2019-12-31")

birth3 <- birth2019 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  mutate(row = row_number())%>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
  )

write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2019_hw_exposed_Tr3.csv")

birth_2019_hw <- birth3_grp 

####2018####
birth2018 <- birth_Tr3 %>%
  filter(dob_yyyy=='2018')

Exposure_filter <- Exposure %>%
  filter(Date >= "2017-01-01" & Date <= "2018-12-31")

birth3 <- birth2018 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
            
  )


write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2018_hw_exposed_Tr3.csv")

birth_2018_hw <- birth3_grp

####2017####
birth2017 <- birth_Tr3 %>%
  filter(dob_yyyy=='2017')

Exposure_filter <- Exposure %>%
  filter(Date >= "2016-01-01" & Date <= "2017-12-31")

birth3 <- birth2017 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
            
  )

write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2017_hw_exposed_Tr3.csv")

birth_2017_hw <- birth3_grp

####2016####
birth2016 <- birth_Tr3 %>%
  filter(dob_yyyy=='2016')

Exposure_filter <- Exposure %>%
  filter(Date >= "2015-01-01" & Date <= "2016-12-31")

birth3 <- birth2016 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
            
  )

write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2016_hw_exposed_Tr3.csv")

birth_2016_hw <- birth3_grp

####2015####
birth2015 <- birth_Tr3 %>%
  filter(dob_yyyy=='2015')

Exposure_filter <- Exposure %>%
  filter(Date >= "2014-01-01" & Date <= "2015-12-31")

birth3 <- birth2015 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
  )


write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2015_hw_exposed_Tr3.csv")

birth_2015_hw <- birth3_grp

####2014####
birth2014 <- birth_Tr3 %>%
  filter(dob_yyyy=='2014')

Exposure_filter <- Exposure %>%
  filter(Date >= "2013-01-01" & Date <= "2014-12-31")

birth3 <- birth2014 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
  )

write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2014_hw_exposed_Tr3.csv")

birth_2014_hw <- birth3_grp

####2013####
birth2013 <- birth_Tr3 %>%
  filter(dob_yyyy=='2013')

Exposure_filter <- Exposure %>%
  filter(Date >= "2012-01-01" & Date <= "2013-12-31")

birth3 <- birth2013 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
  )

write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2013_hw_exposed_Tr3.csv")

birth_2013_hw <- birth3_grp

####2012####
birth2012 <- birth_Tr3 %>%
  filter(dob_yyyy=='2012')

Exposure_filter <- Exposure %>%
  filter(Date >= "2011-01-01" & Date <= "2012-12-31")

birth3 <- birth2012 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
  )

write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2012_hw_exposed_Tr3.csv")

birth_2012_hw <- birth3_grp

####2011####
birth2011 <- birth_Tr3 %>%
  filter(dob_yyyy=='2011')

Exposure_filter <- Exposure %>%
  filter(Date >= "2010-01-01" & Date <= "2011-12-31")

birth3 <- birth2011 %>%
  mutate(row = row_number())

birth3 <- birth3 %>%
  select(starts_with("Day"), location, row, dbr_id) %>%
  tidyr::pivot_longer(c(-"location", -"row", -"dbr_id")) %>%
  group_by(location) %>%
  rename(Date=value)%>%
  left_join(Exposure_filter, by = c('location', 'Date'))%>% 
  pivot_wider(names_from = name)%>%
  data.frame() 

birth3_grp <- na.omit(birth3)

birth3_grp <- birth3_grp %>%
  group_by(dbr_id)%>%
  summarize(count90=sum(above_pct_90),
            count902=sum(two_days_90),
            count903=sum(three_days_90), 
            count904=sum(four_days_90),
            count95=sum(above_pct_95),
            count952=sum(two_days_95),
            count953=sum(three_days_95), 
            count954=sum(four_days_95),
            count975=sum(above_pct_975),
            count9752=sum(two_days_975),
            count9753=sum(three_days_975), 
            count9754=sum(four_days_975),
            count99=sum(above_pct_99),
            count992=sum(two_days_99),
            count993=sum(three_days_99), 
            count994=sum(four_days_99),
            state90ct=sum(state_90),
            state95ct=sum(state_95),
            state99ct=sum(state_99)
  )


write.csv(birth3_grp, "S:/Projects/External_PI/Births_HeatWavs/Data/Exposed/Tr3/births_2011_hw_exposed_Tr3.csv")

birth_2011_hw <- birth3_grp

all_births_hw <- rbind(birth_2011_hw, birth_2012_hw, birth_2013_hw, birth_2014_hw, birth_2015_hw, birth_2016_hw, birth_2017_hw, birth_2018_hw, birth_2019_hw)

births_hw <- births %>%
  left_join(all_births_hw, by=c("dbr_id"))

births_hw <- arrange(births_hw, Date)

births_hw <- births_hw %>%
  group_by(dbr_id)%>%
  mutate(exposed90=ifelse(count90 > 1, 1, 0),
         exposed902=ifelse(count902 > 1, 1, 0),
         exposed903=ifelse(count903 > 1, 1, 0), 
         exposed904=ifelse(count904 > 1, 1, 0),
         exposed95=ifelse(count95> 1, 1, 0),
         exposed952=ifelse(count952> 1, 1, 0),
         exposed953=ifelse(count953> 1, 1, 0), 
         exposed954=ifelse(count954> 1, 1, 0),
         exposed975=ifelse(count975> 1, 1, 0),
         exposed9752=ifelse(count9752> 1, 1, 0),
         exposed9753=ifelse(count9753> 1, 1, 0), 
         exposed9754=ifelse(count9754> 1, 1, 0),
         exposed99=ifelse(count99> 1, 1, 0),
         exposed992=ifelse(count992> 1, 1, 0),
         exposed993=ifelse(count993> 1, 1, 0), 
         exposed994=ifelse(count994> 1, 1, 0),
         expstate90=ifelse(state90ct> 1, 1, 0),
         expstate95=ifelse(state95ct> 1, 1, 0),
         expstate99=ifelse(state99ct> 1, 1, 0)
  )

write.csv(births_hw, "S:/Projects/External_PI/Births_HeatWaves/Data/Exposed/Tr3/births_Tr3_hw_exposed_2011_2019.csv")

#### Create table one ####

library(data.table)
library(dplyr)
library(lubridate)
library(tableone)

# Read in datasets

birth <- read.csv("S:/Projects/External_PI/Births_HeatWaves/Data/Exposed/Tr3/births_Tr3_hw_exposed_2011_2019.csv")

birth$race_cat <- fifelse(birth$dblack=="1", "Black", 
                          fifelse(birth$dwhite=="1", "White", 
                                  fifelse(birth$dhisp=="1", "Hispanic", 
                                          fifelse(birth$draceother=="1", "Other",
                                                  "Unknown"))))

birth_table<-birth%>%select("dbr_id", "lbw", "ptb", "pih", "nicuadmit", "gestdiab", "race_cat", "m_agegroup",
                            "mwic", "pay", "lowinc", "exposed90", "exposed902", "exposed903", "exposed904",
                            "exposed95", "exposed952", "exposed953", "exposed954", "exposed975",
                            "exposed9752", "exposed9753", "exposed9754", "exposed99", "exposed992",
                            "exposed993", "exposed994", "expstate90", "expstate95","expstate99"
                            )


Table<-CreateTableOne(data=birth_table, vars=(c("lbw", "ptb", "pih", "nicuadmit", "gestdiab", "race_cat", "m_agegroup",
                                                "mwic", "pay", "lowinc", "exposed90", "exposed902", "exposed903", "exposed904",
                                                "exposed95", "exposed952", "exposed953", "exposed954", "exposed975",
                                                "exposed9752", "exposed9753", "exposed9754", "exposed99", "exposed992",
                                                "exposed993", "exposed994", "expstate90", "expstate95","expstate99")),
                      factorVars=c("lbw", "ptb", "pih", "nicuadmit", "gestdiab", "race_cat", "m_agegroup",
                                   "mwic", "pay", "lowinc", "exposed90", "exposed902", "exposed903", "exposed904",
                                   "exposed95", "exposed952", "exposed953", "exposed954", "exposed975",
                                   "exposed9752", "exposed9753", "exposed9754", "exposed99", "exposed992",
                                   "exposed993", "exposed994", "expstate90", "expstate95","expstate99"))

tab<-print(Table, showAllLevels = TRUE, exact="stage", smd=TRUE)
write.csv(tab, "S:/Projects/External_PI/Births_HeatWaves/Data/Exposed/Tr3/Births_Tr3_w_Heatwaves_Table1_2011_2019_Sarah.csv")

