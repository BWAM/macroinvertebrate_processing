#looking @ its flagged data
#6/17/21

library(dplyr)

its_master<-read.csv("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Macro_ITS/20210114_S_MSTR_MACRO_SPECIES.csv")

k_version<-read.csv("C:/Users/kareynol/Desktop/R_SMAS/macro_processing_in_house/data/20210114_S_MSTR_MACRO_SPECIES.csv")

its_species<-read.csv("data/STREAMS_missing genspecies.csv")

nomatch<-merge(its_species,its_master,by.x="MSDH_GENSPECIES",by.y="MMS_MACRO_GENSPECIES")

missing<-unique(its_species$MSDH_GENSPECIES)

species<-its_version %>% 
  subset((MMS_MACRO_GENSPECIES %in% missing))

#fixed manally
#now let's see if they match
#
fixd_master_table<-read.csv("data/adjusted_master_6_17_21.csv")

fixd_master<-read.csv("data/Copy of STREAMS_MASTER_genspecies_kar_adjust_6_17.csv")

fixd_species<-read.csv("data/STREAMS_missing genspecies_fixed.csv")  

nomatch<-merge(fixd_species,fixd_master,by.x="MSDH_GENSPECIES",by.y="MMS_MACRO_GENSPECIES") 

nomatch.l<-unique(nomatch$MSDH_GENSPECIES)

needs<-unique(fixd_species$MSDH_GENSPECIES)
master<-fixd_master %>% 
  filter(MMS_MACRO_GENSPECIES %in% needs )

still<-merge(needs, nomatch.l,by="MSDH_GENSPECIES")

write.csv(still,"outputs/check.species.csv")

#fix the masters of each
k_version<-read.csv("C:/Users/kareynol/Desktop/R_SMAS/macro_processing_in_house/data/20210114_S_MSTR_MACRO_SPECIES.csv")

old_species<-read.csv("data/20210602_S_MACRO_SPECIES_DATA_HISTORY.csv")

#table of the changes
adjustment<-read.csv("data/adjusments_kar_6_17_21.csv")
change<-adjustment %>% 
  select(MSDH_GENSPECIES,changed_to_match_master) %>% 
  filter(changed_to_match_master!="")

change.l<-unique(change$MSDH_GENSPECIES)

old_species<-merge(old_species,change,by.y="MSDH_GENSPECIES",by.x = "MSTR_MACRO_SPECIES_ID",all.x = TRUE)

old_species<-old_species %>% 
  mutate(new_species=case_when(is.na(changed_to_match_master)~paste(MSTR_MACRO_SPECIES_ID),
                               TRUE~paste(changed_to_match_master)))

old_species<-old_species %>% 
  select(MSDH_LINKED_ID_VALIDATOR,MSDH_INDIVIDUAL_SPECIES_CNT,new_species) %>% 
  rename(MSTR_MACRO_SPECIES_ID=new_species)

write.csv(old_species,"outputs/20210617_S_MACRO_SPECIES_DATA_HISTORY.csv",row.names = FALSE)
