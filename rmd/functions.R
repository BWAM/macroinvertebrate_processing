#thinking about functions for the processing of macro data

waa_data_prep<-function(df){
  library(dplyr)
  
  prepped_df<-df %>% 
    select(MSSIH_EVENT_SMAS_HISTORY_ID,
           MSSIH_BIOSAMPLE_COLLECT_METHOD,
           MSSIH_REPLICATE,
           MSSIH_EVENT_SMAS_SAMPLE_DATE,
           MSDH_GENSPECIES,
           MSDH_INDIVIDUAL_SPECIES_CNT,
           MSSIH_TOTAL_INDIV_CNT_IND
           )
 prepped_df2 <- cbind(prepped_df, stringr::str_split_fixed(prepped_df$MSSIH_EVENT_SMAS_HISTORY_ID,"-",3))

  #rename columns and create other ones that exist in the original BAP format
  prepped_df2<-prepped_df2 %>% 
    dplyr::rename(BASIN=`1`,
                  LOCATION=`2`,
                  RIVMILE=`3`)
    
  prepped_df2<-prepped_df2 %>% 
    mutate(COLL_DATE=MSSIH_EVENT_SMAS_SAMPLE_DATE,
         Replicate=MSSIH_REPLICATE,
         MACRO_GENSPECIES=MSDH_GENSPECIES,
         INDIV=MSDH_INDIVIDUAL_SPECIES_CNT,
         site_id=MSSIH_EVENT_SMAS_HISTORY_ID
         )

collect<-read.csv(here::here("lkp/20211102_S_BIOSAMPLE_COLLECT_METHOD_its_kar.csv"))

prepped_df3<-merge(prepped_df2,collect,
          by.x = "MSSIH_BIOSAMPLE_COLLECT_METHOD",
          by.y="BIOSAMPLE_COLLECT_METHOD")
prepped_df3$COLLECT<-paste(prepped_df3$BIOSAMPLE_COLLECT_METHOD_ID)

.GlobalEnv$prepped_df3 <- prepped_df3
}

#######################################################################

m_data_prep<-function(df){
  prepped_df<-df
  #adjust names of columns so they can be run through BAP
  prepped_df$RIVMILE<-paste(prepped_df$STATION)
  prepped_df$RIVMILE<-as.numeric(prepped_df$RIVMILE)
  prepped_df$BASIN<-substr(prepped_df$LOCATION,start=1,stop=2)
  prepped_df<-prepped_df %>% 
    rename(Replicate=REPLICATE)
  #fix location
  prepped_df$LOCATION_good<-substr(prepped_df$LOCATION,start=4,stop=max(length(prepped_df$LOCATION)))
  
  #correct the site_id
  prepped_df$site_id<-paste(prepped_df$BASIN,prepped_df$LOCATION_good,prepped_df$RIVMILE,sep="-")
  
  prepped_df<-prepped_df %>% 
    rename(ref=LOCATION,
           LOCATION=LOCATION_good,
           COLL_DATE=DATE,
           MACRO_GENSPECIES=GENSPECIES)
  
  prepped_df$Replicate<-prepped_df$MSSIH_REPLICATE
  prepped_df$INDIV<-(as.numeric(prepped_df$MSDH_INDIVIDUAL_SPECIES_CNT))
  
 .GlobalEnv$prepped_df <- prepped_df
}

#################################################################################
#species check script

species_check<-function(df){
  
  #read in master file
  #read in the master table
  master<-read.csv(paste("C:/Users/",params$user,"/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Macro_ITS/20210617_S_MSTR_MACRO_SPECIES.csv",sep = ""),stringsAsFactors = FALSE)
  
  #Make all characters lowercase and remove unwanted characters in both files.
  
  pos_new_species <- df %>% 
    mutate(MACRO_GENSPECIES = tolower(MACRO_GENSPECIES),
           MACRO_GENSPECIES = stringr::str_replace_all(MACRO_GENSPECIES, " ", "_"),
           MACRO_GENSPECIES = trimws(MACRO_GENSPECIES)
    )
  
  existing_species <- master %>% 
    rename(MACRO_GENSPECIES=MMS_MACRO_GENSPECIES) %>% 
    mutate_if(is.character, tolower)
  
  
  #Creating a variable to hold new species by subtracting any species already on the master species list from the new data, by looking through the MACRO_GENSPECIES column. Then displaying the new species in a table.
  
  new_species <- anti_join(pos_new_species, existing_species,
                           by = "MACRO_GENSPECIES")
  
  if (nrow(new_species>0)){
  DT::datatable(new_species)}
  else{
    print("There are no new species in this file.")
  }
  
  
  #Removing duplicate records of new species to display each new species name only once and printing the results.
  
  not.found.vec <- unique(new_species$MACRO_GENSPECIES)
  not.found.vec
  
  
  #Creating a list of column names to look through in the existing species data to double check that the "new species" aren't just poorly classified or not classified to the fullest extent. Printing out the results without duplicates in a table.
  
  col.vec <- c("MMS_PHYLUM", "MMS_CLASS", "MMS_ORDER", "MMS_FAMILY",
               "MMS_SUBFAMILY", "MMS_GENUS", "MMS_SPECIES", 
               "MACRO_GENSPECIES")
  not.found.vec <- gsub(" .*$", "", not.found.vec)
  double_check <- purrr::map_df(col.vec, function(col.i) {
    sub.df <- existing_species[grepl(paste(not.found.vec, collapse = "|"), existing_species[, col.i]), ]
    
  }) %>% 
    unique()
  
  if (nrow(new_species>0)){
    DT::datatable(double_check)}
  
  
  #Creating a csv output for the species found that aren't in the master list.
  
  if (nrow(new_species>0)){ write.csv(new_species,
            "outputs/newly_found_species.csv",
            row.names = FALSE)
  }
  
  #Creating a csv output for the double check, looking through all taxonomic levels for the classifications not found in the master list.
  
  if (nrow(new_species>0)){write.csv(double_check,
            "output/double_check.csv",
            row.names = FALSE)}
  
  
}
##################################################################################
#data mod prep for internal data
tables_1_2<-function(df){
tables<-df

tables$COLL_DATE<-as.Date(tables$COLL_DATE, "%m/%d/%Y")
#create species event table
tables$date<-format(tables$COLL_DATE,"%Y%m%d")
tables<-tables %>% 
  mutate(MSSIH_LINKED_ID_VALIDATOR=paste(site_id,date,COLLECT,Replicate,sep = "_")) %>% 
  mutate(MSSIH_EVENT_SMAS_SAMPLE_DATE=format(COLL_DATE,"%m/%d/%Y")) %>% 
  mutate(MSSIH_TOTAL_INDIV_CNT_NOTE="") %>% 
  mutate(Taxonomist_name=paste("SBU")) %>% 
  rename(MSSIH_TAXONOMIST=Taxonomist_name,
         MSSIH_REPLICATE=Replicate,
         MSSIH_EVENT_SMAS_HISTORY_ID=site_id,
         BIOSAMPLE_COLLECT_METHOD_NUM=COLLECT
         )


#create sum of samples
sum<-tables %>% 
  select(MSSIH_LINKED_ID_VALIDATOR,INDIV) %>% 
  group_by(MSSIH_LINKED_ID_VALIDATOR) %>% 
  summarise(MSSIH_TOTAL_INDIV_CNT=sum(INDIV))

#merge back
tables<-merge(tables,sum, by="MSSIH_LINKED_ID_VALIDATOR")

#merge wtih kick methods to get the validator

kick_method<-readxl::read_excel(paste(here::here(),
"/lkp/20201028_S_BIOSAMPLE_COLLECT_METHOD_all_fields.xlsx",sep = ""))

kick<-kick_method %>% 
  select(BIOSAMPLE_COLLECT_METHOD_NUM,BIOSAMPLE_COLLECT_METHOD)

tables<-merge(tables,kick, by="BIOSAMPLE_COLLECT_METHOD_NUM")


samp_event<-tables %>% 
  rename(MSSIH_BIOSAMPLE_COLLECT_METHOD_NUM=BIOSAMPLE_COLLECT_METHOD_NUM) %>% 
  select(MSSIH_LINKED_ID_VALIDATOR,
         MSSIH_BIOSAMPLE_COLLECT_METHOD_NUM,
         MSSIH_EVENT_SMAS_HISTORY_ID,
         MSSIH_EVENT_SMAS_SAMPLE_DATE,
         MSSIH_REPLICATE,
         MSSIH_TAXONOMIST,
         MSSIH_TOTAL_INDIV_CNT_NOTE,
         BIOSAMPLE_COLLECT_METHOD,
         MSSIH_TOTAL_INDIV_CNT) %>% 
  distinct()

species_history<-tables %>% 
  select(MSSIH_LINKED_ID_VALIDATOR,INDIV,MACRO_GENSPECIES) %>% 
  rename(MSDH_INDIVIDUAL_SPECIES_CNT=INDIV,
         MSTR_MACRO_SPECIES_ID=MACRO_GENSPECIES,
         MSDH_LINKED_ID_VALIDATOR=MSSIH_LINKED_ID_VALIDATOR) %>% 
  mutate(MSTR_MACRO_SPECIES_ID=tolower(MSTR_MACRO_SPECIES_ID)) %>% 
  mutate(MSTR_MACRO_SPECIES_ID=gsub(" ","_",MSTR_MACRO_SPECIES_ID))

#write them to csv
time=Sys.Date()
time.t=format(time,"%Y%m%d")

write.csv(species_history,paste(here::here(),"/outputs/",
                                time.t,"_S_MACRO_SPECIES_DATA_HISTORY.csv",sep = ""))
write.csv(samp_event,paste(here::here(),"/outputs/",
                           time.t,"_S_MACRO_SPECIES_SAMP_INF_HIST.csv",sep = ""))

print(paste("Tables",time.t,"S_MACRO_SPECIES_DATA_HISTORY", "and", time.t, 
      "S_MACRO_SPECIES_SAMP_INF_HIST","have been saved to the outputs folder.",sep=" "))

.GlobalEnv$species_history <- species_history
.GlobalEnv$samp_event <- samp_event

}
############################################################################################
#BAP function to run the correct BAP based on the collection method

apply_bap<-function(df){
  kick.standard<-df %>% 
  subset(COLLECT==1)

mp.nav<-df %>% 
  subset(COLLECT==2)

mp.nn<-df %>% 
  subset(COLLECT==5)

sandy.jab<-df %>% 
  subset(COLLECT==6)

ponar<-df %>% 
  subset(COLLECT==3)

riff_df<-if(nrow(kick.standard!=0)){BAP::bap_riffle(Long = kick.standard)}
mp.nav_df<-if(nrow(mp.nav!=0)){BAP::bap_mp_nav_waters(Long = mp.nav)}
mp.nn_df<-if(nrow(mp.nn!=0)){BAP::bap_mp_non_nav_water(Long=mp.nn)}
sandy.jab_df<-if(nrow(sandy.jab!=0)){BAP::bap_jab(Long = sandy.jab)}
ponar_df<-if(nrow(ponar!=0)){BAP::bap_ponar(long = ponar)}

bap.final<-bind_rows(riff_df,mp.nav_df,mp.nn_df,sandy.jab_df,ponar_df)

.GlobalEnv$bap.final <- bap.final
}
########################################################################################
#create right function
right = function (string, char) {
  substr(string,nchar(string)-(char-1),nchar(string))
}
########################################################################

#make metric's table with correct headers

metric_table<-function(df){
  metrics<-df
  
  #get collection method in here
  collect<-bap.prepped %>% 
    select(MSSIH_EVENT_SMAS_HISTORY_ID,COLLECT) %>% 
    distinct() %>% 
    dplyr::rename(site_id=MSSIH_EVENT_SMAS_HISTORY_ID)
  
  metrics$DATE<-as.Date(metrics$DATE,"%Y-%m-%d")
  
  metrics<-metrics %>% #make the validation id
    mutate(BASIN=stringr::str_pad(BASIN,2,side = c("left"),pad = "0")) %>% 
    mutate(RIVMILE=as.numeric(RIVMILE,digits=2)) %>% 
    mutate(site_id=paste(BASIN,LOCATION,sprintf("%.1f",RIVMILE),sep = "-")) %>% 
    mutate(date=format(DATE,"%Y%m%d"))#format date for tehe linked validator
  
  metrics<-merge(metrics,collect,by="site_id",
                 all.x = TRUE) #merge to get the collection method in there
  
  
  #rename columns and create validator ID
  metrics<-metrics %>% 
    mutate(REP=right(EVENT_ID,1),
      MMDH_LINKED_ID_VALIDATOR=paste(site_id,date,COLLECT,REP,sep="_")
)
  #need to make a vec for columns and name changes
  col.vec<-read.csv(here::here("lkp","col_names.csv"),stringsAsFactors = FALSE)
  
  col.from<-colnames(metrics)
  
  col.vec.short<- col.vec %>% 
    subset(old %in% col.from)
  
  names(metrics)[match(col.vec.short[,"old"], names(metrics))] = col.vec.short[,"new"]
   
  for.table.vec<-unique(col.vec.short$new)
  
  metrics.subset <- metrics[, for.table.vec]
  
  metrics.subset<-metrics.subset %>% 
    mutate_if(is.numeric, round, 3)

  .GlobalEnv$metrics.subset <- metrics.subset 
  
  #write them to csv
  time=Sys.Date()
  time.t=format(time,"%Y%m%d")
  
  write.csv(metrics.subset,paste(here::here(),"/outputs/",
                                  time.t,"_S_MACRO_METRICS_DATA_HISTORY.csv",sep = ""))
}
#################################################################################################
#tables 1-2 for waa data
tables_1_2.waa<-function(df){
    tables<-df
    
    tables$COLL_DATE<-as.Date(tables$COLL_DATE, "%m/%d/%Y")
    #create species event table
    tables$date<-format(tables$COLL_DATE,"%Y%m%d")
    tables<-tables %>% 
      mutate(MSSIH_LINKED_ID_VALIDATOR=paste(MSSIH_EVENT_SMAS_HISTORY_ID,date,BIOSAMPLE_COLLECT_METHOD_ID,MSSIH_REPLICATE,sep = "_")) %>% 
      mutate(MSSIH_EVENT_SMAS_SAMPLE_DATE=format(COLL_DATE,"%m/%d/%Y")) %>% 
      mutate(MSSIH_TOTAL_INDIV_CNT_NOTE="") %>% 
      mutate(Taxonomist_name=paste("WAA")) %>% 
      dplyr::rename(MSSIH_TAXONOMIST=Taxonomist_name
      )
    
    
    #create sum of samples
    sum<-tables %>% 
      select(MSSIH_LINKED_ID_VALIDATOR,MSSIH_TOTAL_INDIV_CNT_IND) %>% 
      group_by(MSSIH_LINKED_ID_VALIDATOR) %>% 
      summarise(MSSIH_TOTAL_INDIV_CNT=max(MSSIH_TOTAL_INDIV_CNT_IND))
    
    #merge back
    tables<-merge(tables,sum, by="MSSIH_LINKED_ID_VALIDATOR")
    
    
    samp_event<-tables %>% 
      rename(MSSIH_BIOSAMPLE_COLLECT_METHOD_NUM=BIOSAMPLE_COLLECT_METHOD_ID) %>% 
      select(MSSIH_LINKED_ID_VALIDATOR,
             MSSIH_BIOSAMPLE_COLLECT_METHOD_NUM,
             MSSIH_EVENT_SMAS_HISTORY_ID,
             MSSIH_EVENT_SMAS_SAMPLE_DATE,
             MSSIH_REPLICATE,
             MSSIH_TAXONOMIST,
             MSSIH_TOTAL_INDIV_CNT_NOTE,
             MSSIH_BIOSAMPLE_COLLECT_METHOD,
             MSSIH_TOTAL_INDIV_CNT) %>% 
      distinct()
    
    species_history<-tables %>% 
      select(MSSIH_LINKED_ID_VALIDATOR,MSSIH_TOTAL_INDIV_CNT,MACRO_GENSPECIES) %>% 
      rename(MSTR_MACRO_SPECIES_ID=MACRO_GENSPECIES,
             MSDH_LINKED_ID_VALIDATOR=MSSIH_LINKED_ID_VALIDATOR) %>% 
      mutate(MSTR_MACRO_SPECIES_ID=tolower(MSTR_MACRO_SPECIES_ID)) %>% 
      mutate(MSTR_MACRO_SPECIES_ID=gsub(" ","_",MSTR_MACRO_SPECIES_ID))
    
    #write them to csv
    time=Sys.Date()
    time.t=format(time,"%Y%m%d")
    
    write.csv(species_history,paste(here::here(),"/outputs/",
                                    time.t,"_S_MACRO_SPECIES_DATA_HISTORY.csv",sep = ""))
    write.csv(samp_event,paste(here::here(),"/outputs/",
                               time.t,"_S_MACRO_SPECIES_SAMP_INF_HIST.csv",sep = ""))
    
    print(paste("Tables",time.t,"S_MACRO_SPECIES_DATA_HISTORY", "and", time.t, 
                "S_MACRO_SPECIES_SAMP_INF_HIST","have been saved to the outputs folder.",sep=" "))
    
    .GlobalEnv$species_history <- species_history
    .GlobalEnv$samp_event <- samp_event
    
}
###################################################################################
site_check<-function(df){
  #checks to see if there are site id's that need to be changed
  sites<-df
  if(params$file_type=="waa"){
    sites$site_id<-paste(sites$MSSIH_EVENT_SMAS_HISTORY_ID)
  }
  #read in the site check file
  site_check.f<-readxl::read_excel(paste("C:/Users/",params$user,
                               "/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Sites_ITS/c2021_Sites_crosswalk_summary_v4_created_20211116.xlsx",
                               sep = ""))
  site_check.f<-site_check.f %>% 
    select(SMAS_HISTORY_ID,ORIGINAL_SITE_ID) %>% 
    distinct() %>% 
    filter(SMAS_HISTORY_ID != ORIGINAL_SITE_ID)#get a list of the sites to change
  
  sites<-merge(sites,site_check.f,
                   by.x="site_id",
                   by.y="ORIGINAL_SITE_ID",
               all.x = TRUE)
  #then replace the ones that need replacing
  sites.l<-unique(sites$ORIGINAL_SITE_ID)
  
  #does this if it needs to
  if(!is.null(sites.l)) {
    site.1<-sites %>% 
      filter(!site_id %in% sites.l)
    
    site.2<-sites %>% 
      filter(site_id %in% sites.l) %>% 
      mutate(site_id=paste(SMAS_HISTORY_ID))
    
    sites<-rbind(site.1,site.2)
    
    print("These sites were changed based on the lookup table :")
    print(paste(sites.l))
  }
  prepped_df<-sites
  .GlobalEnv$prepped_df <- prepped_df
  
  
}

event_check<-function(df){
  #checks to see if there are events associated with each record
  
  
}



