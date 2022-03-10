#creating a family level new entry for tolerance values
#Keleigh Reynolds
#6/25/2021

library(dplyr)
#read in the raw from the TEams location

master<-read.csv("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Macro_ITS/20210617_S_MSTR_MACRO_SPECIES.csv",stringsAsFactors = FALSE)

#now pick the order
order="odonata"

#filter the master table to get an average or worst case scenario
#

filtered<-master %>% 
  filter(MMS_ORDER==order)

filtered.2<-filtered %>% 
  group_by(MMS_TOLERANCE_NAME) %>% 
  mutate(new_tol=mean(MMS_TOLERANCE,na.rm=TRUE),
         new_nbi_p=mean(MMS_NBI_PHOSPHORUS_TOL,na.rm=TRUE),
         new_nbi_n=mean(MMS_NBI_NITRATE_TOL,na.rm = TRUE)) %>% 
  select(MMS_TOLERANCE_NAME,new_tol,new_nbi_p,new_nbi_n) %>% 
  distinct()

filtered.3<-filtered %>% 
  group_by(MMS_GENUS) %>% 
  mutate(new_tol=mean(MMS_TOLERANCE,na.rm=TRUE),
         new_nbi_p=mean(MMS_NBI_PHOSPHORUS_TOL,na.rm=TRUE),
         new_nbi_n=mean(MMS_NBI_NITRATE_TOL,na.rm = TRUE)) %>% 
  select(MMS_TOLERANCE_NAME,new_tol,new_nbi_p,new_nbi_n) %>% 
  distinct()


#select the worst case scenario? 
