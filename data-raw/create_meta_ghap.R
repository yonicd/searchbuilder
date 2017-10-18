library(ghap)
library(reshape2)
library(plyr)
library(dbplyr)
library(dplyr)
library(XLConnect)


#set up file paths for meta data
git_base <- normalizePath(get_git_base_path(),winslash = '/')

dataDirIDX <- file.path(git_base,"HBGD/studyaccounting")

dataDirCommon <- file.path(git_base,"HBGD/common/meta")

# location of sqlite files that contain alllongitudinal and allcrosssectional raw data
sqlDir <- "H:/GitHub/shinyghap/Miscellaneous/data"

#checkout relevant files for IDX data
  if(!dir.exists(dataDirIDX))
    sparse_ghap('https://jonathan.sidi@git.ghap.io/stash/scm/hbgd/studyaccounting.git','HBGD/studyaccounting',queries = c('StudyExplorer/jobs/*.csv'))

#read in the IDX csv files 
f <- data.frame(f=list.files(file.path(dataDirIDX,'StudyExplorer/jobs'),pattern = 'IDX',full.names = TRUE),stringsAsFactors = FALSE)
  
IDX <- plyr::ddply(f,c('f'),function(x) read.csv(x$f,stringsAsFactors = FALSE)%>%reshape2::melt(.,'STUDYID')%>%select(-value)%>%unique,.progress='text')

#checkout relevant files for common data
  if(!dir.exists(dataDirCommon))
    sparse_ghap('https://jonathan.sidi@git.ghap.io/stash/scm/hbgd/common.git','HBGD/common',queries = c('meta/HBGD-codelists.xlsx'))

#read in the code lists from common directory
w <- loadWorkbook(file.path(dataDirCommon,'HBGD-codelists.xlsx'))

code.list <- readWorksheet(w,getSheets(w))

#rearrange into list containing data by type

  code.list.super <- vector('list',3)
  
  names(code.list.super) <- c('continuous','discrete','geo')
  
  plyr::l_ply(names(code.list),function(x){
    
    df <- code.list[[x]]
    
    names(df) <- toupper(names(df))
    
    if(any(grepl('CODEN',names(df)))){
      
      code.list.super$continuous[[x]] <<- df 
      
    }else{
      
      if(any(grepl('RMAP',names(df)))){
        
        code.list.super$geo[[x]] <<- df  
        
      }else{
        
        code.list.super$discrete[[x]] <<- df
        
      } 
    }
    })
  
  code.list.super <- plyr::llply(code.list.super,function(x) plyr::ldply(x,.id = 'DOMAIN'))

#get study info
study.info <- read.csv(file.path(dataDirIDX,"StudyExplorer/jobs/studyinfo.csv"),stringsAsFactors = FALSE)

w <- loadWorkbook(file.path(dataDirCommon,"HBGD-dataspecs.xlsx"))

data.specs <- readWorksheet(w,getSheets(w))

#connect to ghap db's (they are just sqlite dbs of data.frames from each domain)
ghap_long <- src_sqlite(path = file.path(sqlDir, "ghap_longitudinal.sqlite3"), create = FALSE)

ghap_cross <- src_sqlite(path = file.path(sqlDir, "ghap_cross_sectional.sqlite3"), create = FALSE)

#create search lists for each db
create_search_list=function(ghap_db){
  
  x <- data.frame(DOMAIN=head(dplyr::db_list_tables(ghap_db$con),-1),stringsAsFactors = FALSE)%>%
    plyr::ddply(.variables = c('DOMAIN'),.fun=function(df0){ 
      
    df <- tbl(ghap_db,df0$DOMAIN)%>%
      group_by(STUDYID)%>%
      summarise_all(funs(sum(is.na(.))<n()))%>%
      collect(n=Inf)
                                      
    df%>%reshape2::melt(.,id='STUDYID')%>%
      filter(value==1)%>%
      select(-value)%>%
      distinct()
    
      },.progress = 'text')


  #combine df to metadata (data.specs and study.info)
  x%>%
    mutate_if(is.factor,as.character)%>%
    left_join(plyr::ldply(data.specs[-c(1:2)],
                          .fun=function(df) df%>%
                            select(variable=NAME,LABEL)%>%
                            distinct,.id='DOMAIN')%>%
                            mutate_if(is.factor,as.character),
              by=c('DOMAIN','variable'))%>%
    left_join(data.specs$DATASETS%>%
                select(DOMAIN=MEMNAME,DOMAIN.LBL=LABEL,DOMAIN.STRUCTURE=Structur),
              by='DOMAIN')%>%
    left_join(study.info,by='STUDYID')
}


combined_list_long <- create_search_list(ghap_long)

combined_list_cross <- create_search_list(ghap_cross)

#combine data.frames
meta_ghap <- rbind(combined_list_long,combined_list_cross)

#rename for app
names(meta_ghap) <- c('DOMAIN','STUDY_ID','STUDY_VARIABLE','STUDY_VARIABLE_DESCRIPTION','DOMAIN_DESCRIPTION','DOMAIN_STRUCTURE','STUDY_ID_SHORT','STUDY_DESCRIPTION_SHORT','STUDY_DESCRIPTION','STUDY_ID_ALTERNATE','STUDY_SUBJECT_COUNT','STUDY_COUNTRY','REPOSITORY_DATA_STATUS','STUDY_TYPE','STUDY_INTERVENTION_TYPE','STUDY_AGE_LOWER_LIMIT','STUDY_AGE_UPPER_LIMIT','STUDY_START_YEAR','STUDY_STOP_YEAR','STUDY_POPULATION','STUDY_REPOSITORY_NAME','REPOSITORY_SUBFOLDER','STUDY_URL')

#save to data dir of shinyghap package
devtools::use_data(meta_ghap,pkg='H:/github/shinyghap')
