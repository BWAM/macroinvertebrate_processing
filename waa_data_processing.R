#process new WAA data from template
#Keleigh Reynolds
#1/24/2022
#adjusting the data template to get the new data format to work
#
raw<-read.csv("data/Copy of 2020_NYSSBU_MacroTaxaOutput_1_24_22.csv")

template<-read.csv("lkp/template_BAP_raw.csv")

waa_lkup<-tibble::tribble(
                          ~colname,       ~type,
                          "WAA ID", "character",
     "MSSIH_EVENT_SMAS_HISTORY_ID", "character",
    "MSSIH_EVENT_SMAS_SAMPLE_DATE", "character",
                 "MSSIH_REPLICATE",   "integer",
  "MSSIH_BIOSAMPLE_COLLECT_METHOD", "character",
                "MSSIH_TAXONOMIST", "character",
                 "MSDH_GENSPECIES", "character",
     "MSDH_INDIVIDUAL_SPECIES_CNT",   "integer",
       "MSSIH_TOTAL_INDIV_CNT_IND",   "integer",
      "MSSIH_TOTAL_INDIV_CNT_NOTE", "character",
                 "PercentWTSorted",   "numeric",
              "Presence/Absence_%",   "numeric",
        "PDE_(precision counts)_%",   "numeric",
  "PTD_(taxonomic_disagreement)_%",   "numeric",
             "Client_Project_Name", "character"
  ) #want to figure out what to do to check the incoming file for types/headers etc

raw_1<-as.data.frame(sapply(raw,class))
waa_raw<-dplyr::left_join(raw,waa_lkup)

############after that's done

waa_data_prep<-function(df){
  prepped_df<-df %>% 
    select(MSSIH_EVENT_SMAS_HISTORY_ID,
           MSSIH_BIOSAMPLE_COLLECT_METHOD,
           MSSIH_REPLICATE,
           MSSIH_EVENT_SMAS_SAMPLE_DATE,
           MSDH_GENSPECIES,
           MSDH_INDIVIDUAL_SPECIES_CNT,
           MSSIH_TOTAL_INDIV_CNT_IND
           )
  prepped_df<-stringr::str_split_fixed(prepped_df$MSSIH_EVENT_SMAS_HISTORY_ID,"-",3)
  #rename columns and create other ones that exist in the original BAP format
  prepped_df<-prepped_df %>% 
    mutate(BASIN=substr(MSSIH_EVENT_SMAS_HISTORY_ID,start=1,stop=2),
           LOCATION=stringr::str_split(MSSIH_EVENT_SMA),
           RIVMILE=,
           COLL_DATE)
    
    
}
## just to get it to work
raw2<-read.csv("data/2020_NYSSBU_MacroTaxaOutput_1_24_22_kar_split.csv")
df<-raw2
df<-df %>% 
  select(MSSIH_EVENT_SMAS_HISTORY_ID,
                 MSSIH_BIOSAMPLE_COLLECT_METHOD,
                 MSSIH_REPLICATE,
                 MSSIH_EVENT_SMAS_SAMPLE_DATE,
                 MSDH_GENSPECIES,
                 MSDH_INDIVIDUAL_SPECIES_CNT,
                 MSSIH_TOTAL_INDIV_CNT_IND,
         BASIN,
         LOCATION,
         RIVMILE) %>% 
  rename(COLL_DATE=MSSIH_EVENT_SMAS_SAMPLE_DATE,
         Replicate=MSSIH_REPLICATE,
         MACRO_GENSPECIES=MSDH_GENSPECIES,
         INDIV=MSDH_INDIVIDUAL_SPECIES_CNT,
         site_id=MSSIH_EVENT_SMAS_HISTORY_ID
         )

collect<-read.csv("lkp/20211102_S_BIOSAMPLE_COLLECT_METHOD_its_kar.csv")

prepped_df<-merge(df,collect,
          by.x = "MSSIH_BIOSAMPLE_COLLECT_METHOD",
          by.y="BIOSAMPLE_COLLECT_METHOD")
prepped_df$COLLECT<-paste(prepped_df$BIOSAMPLE_COLLECT_METHOD_ID)
