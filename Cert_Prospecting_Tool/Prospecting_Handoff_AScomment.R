## This model has two parts- the first is to use cosine similarity to compare prospect IPs to a fictional "ideal" IP
## The second part compares those prospects to similar certified senders to show what kind of IPR lift they could expect if they were to become certified.
## The similarity method used here is the "optimal profiling" method for lift prediction that defines buckets for each metric and creates profiles for each IP that reflect which combination of buckets yields the maximum IPR lift 
## (The cosine similarity method for lift prediction will be used for panel data and included at a later date)

# run once a week, and pull up to 60 days (data available could vary by IP). api will tell us the data start date in the report
# still playing with minimum days of data requirements to limit IPs that are run through the model

##load libraries required
library(plyr)
library(reshape2)

hive.connect <- function(){
  library('RODBC')
  hive <- odbcConnect("Hive")
  return(hive)
}
hive <- hive.connect()
library(RODBC) ## to connect to hive
hive <- odbcConnect("Hive")

library(ROracle)
drv <- dbDriver("Oracle")

sqlQuery(hive, "set hive.exec.reducers.max=50")
sqlQuery(hive, "set hive.auto.convert.join=true")


# AS Comment:  You should update this code to use Oracle wallet

prod1 <- dbConnect(drv, dbname = "PROD1", username = "db_reports", password = "kyvg9_RmDN")
# prod3 connection not working
#prod3 <- dbConnect(drv, dbname = "PROD3", username = "ss_reports", password = "niNF802_Mm")


referralquery <- paste("
                       SELECT /* adhoc smushovic */
                       TO_CHAR(bsp_compliance_hist.report_dt, 'YYYYMMDD') as report_dt
                       , ssc_owner.longtoip(bsp_ips_v.ip) AS ip_address
                       , group_summary_v.group_name
                       , user_ip_lists.category
                       , user_ip_lists.list_id 
                       , bsp_compliance_hist.jmpt_class
                       , bsp_compliance_hist.hm_class
                       , bsp_compliance_hist.hm_inbox_class
                       , bsp_compliance_hist.yahoo_class
                       , bsp_compliance_hist.yahoo_inbox_class
                       , bsp_compliance_hist.cc_class
                       , bsp_compliance_hist.sc_trap_sig_class
                       , bsp_compliance_hist.sc_trap_crit_class
                       , bsp_compliance_hist.cc_trap_class
                       , bsp_compliance_hist.cloudmark_rec_trap_class
                       , bsp_compliance_hist.unk_class
                       FROM
                       ssc_owner.bsp_ips_v
                       JOIN ssc_owner.group_summary_v ON (
                       group_summary_v.group_id = bsp_ips_v.group_id
                       AND group_summary_v.group_type_shortname = 'RFL'
                       AND group_summary_v.group_ssc_status IN ('APP','TST')
                       )
                       JOIN ss_owner.bsp_compliance_hist ON (
                       bsp_compliance_hist.ip_address = ss_owner.iptoraw(ssc_owner.longtoip(bsp_ips_v.ip))
                       AND bsp_compliance_hist.report_dt >  to_date(20160601, 'YYYYMMDD')
                       AND bsp_compliance_hist.report_dt <=  to_date(20160801, 'YYYYMMDD')
                       )
                       LEFT JOIN ssc_owner.user_ip_lists ON (
                       user_ip_lists.ip_address = bsp_ips_v.ip
                       )
                       ")



## Was pulling IPRs from oracle, but they didn't match hive 
## chose to use hive for prospect IPR because that's what the cert data will use in the lift prediction step


referrals <- dbGetQuery(prod1, referralquery, stringsAsFactors=FALSE)


length(unique(referrals$IP_ADDRESS))
length(unique(referrals$CATEGORY))
length(unique(referrals$LIST_ID))

## Create a column for the days that are good for all the compliance stds - labeled with "1"

compliant_fcn <- function(std1, std2, std3, std4, std5, std6, std7, std8) {
  ifelse((
    std1 %in% c("good","na")
    & std2 %in% c("good","na")
    & std3 %in% c("good","na")
    & std4 %in% c("good","na")
    & std5 %in% c("good","na")
    & std6 %in% c("good","na")
    & std7 %in% c("good","na")
    & std8 %in% c("good","na")
  ), 1, 0)
}

referrals$complt_num <- as.numeric(compliant_fcn(referrals$HM_CLASS
                                                 , referrals$JMPT_CLASS
                                                 , referrals$YAHOO_CLASS
                                                 , referrals$CC_CLASS
                                                 , referrals$SC_TRAP_SIG_CLASS
                                                 , referrals$SC_TRAP_CRIT_CLASS
                                                 , referrals$CC_TRAP_CLASS
                                                 , referrals$CLOUDMARK_REC_TRAP_CLASS))

save(referrals, file="Prospecting_Oracle.RData")


############################################################################################
## pull in the rest of the metrics the model will use

cert_noncert_metrics<- sqlQuery(hive, "select * from smushovic.cert_value_staging_2")
## Can't use total_volume from this table 

hive_cert_metrics<- subset(cert_noncert_metrics, cert_noncert == 1) #(for later) 

hive_noncert_metrics <- subset(cert_noncert_metrics, cert_noncert == 0)  
hive_noncert_metrics$traps <- as.numeric(hive_noncert_metrics$traps)


############################################################################################
## merge them 


## we only want days where there's data for both data sets (inner join). 
## do the day counts and percent_time_compliant after the join

### AS Comment -- I wouldn't hard code column numbers, rather reference their variable names.
######Which brings me to my next question, what is column 23 supposed to be?  It's 17 now.
######Also, missing the loading of plyr library for rename statement to work.  I recommend loading all libraries at the beginning.

oracle_metrics <- referrals[, c("REPORT_DT", "IP_ADDRESS", "GROUP_NAME", "CATEGORY", "LIST_ID","complt_num")] 
oracle_metrics<- unique(oracle_metrics)
oracle_metrics <- rename(oracle_metrics, c("REPORT_DT" = "day", "IP_ADDRESS" = "ip_address"))
prospects <- merge(oracle_metrics, hive_noncert_metrics, by = c("ip_address", "day"))

length(unique(hive_noncert_metrics$ip_address))
#This line didn't work for me as there is no data frame with this name # length(unique(referrals_sub$ip_address))
length(unique(prospects$ip_address))



#####################################################################
## Do some day counts to determine which ips have enough data to run through the model 


##AS Comment:  You can simplify the code below to use the which() function instead of a new line to subset
##Also, I'm a little confused here.. you want to use the data where complt_num >= 0 -- wouldn't that be everything?

#compliance_days <- subset(prospects, complt_num >= 0)
compliance_days <- ddply(prospects[which(prospects$complt_num >= 0),], c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"), summarize, 
                         total_oracle_days = length(unique(day)),
                         complt_days = sum(complt_num, na.rm=TRUE))

compliance_days$pct_days_complt<- compliance_days$complt_days / compliance_days$total_oracle_days

#yahoo_days<- subset(prospects, yahoo_volume >0)
yahoo_days <- ddply(prospects[which(prospects$yahoo_volume > 0),], c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"), summarize,
                    yahoo_days = length(unique(day)))

#hotmail_days<- subset(prospects, hotmail_volume >0)
hotmail_days <- ddply(prospects[which(prospects$hotmail_volume > 0),], c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"), summarize,
                      hotmail_days = length(unique(day)))                              

total_hive_days <- ddply(prospects, c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"), summarize,
                         total_hive_days = length(unique(day)))  

hive_daycounts<- merge(total_hive_days, hotmail_days, 
                       by = c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"),  all.x=TRUE)                              
hive_daycounts<- merge(hive_daycounts, yahoo_days, 
                       by = c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"),  all.x=TRUE)   



################################################################3
## merge daycounts back into prospects dataset and filter ips for data quality

prospects_data <- merge(prospects, compliance_days, by = c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"), all.x=TRUE)
prospects_data <- merge(prospects_data, hive_daycounts, by = c("ip_address", "GROUP_NAME", "CATEGORY", "LIST_ID"), all.x=TRUE)


## There aren't enough that have 30 days of yahoo volume to warrant extra logic (small subset of 636 can take hotmail AND yahoo) so use only hotmail volume

## Only ips who have 30 days of data in both sets
prospects_df <- subset(prospects_data, (total_oracle_days >=30 & total_hive_days >=30))
## Only ips who have 30 days of hotmail volume. 
prospects_df<- subset(prospects_df, (hotmail_days >= 30))    


length(unique(prospects_data$ip_address))
length(unique(hive_daycounts$ip_address))
length(unique(compliance_days$ip_address))
length(unique(yahoo_days$ip_address))
length(unique(hotmail_days$ip_address))
length(unique(prospects_df$ip_address))

###########################################################################3
## roll up ip-level for model 

##AS Comment:  Again, harcoding columns by column number can make you run into problems should
### some other data issue cause your columns to become out of order


prospects_ip <- prospects_df[ , -which(names(prospects_df) %in% c("GROUP_NAME","CATEGORY","LIST_ID"))]
#or you can use this subset(prospects_df, select=-c("GROUP_NAME","CATEGORY","LIST_ID"))
prospects_ip <- unique(prospects_ip)
prospects_ip <- ddply(prospects_ip, c("ip_address", "pct_days_complt"), summarize,
                      avg_score = mean(score, na.rm = TRUE),
                      avg_traps = mean(traps, na.rm = TRUE),
                      avg_bounce = mean(bounce_rate, na.rm = TRUE),
                      avg_comp = mean(complaint_rate, na.rm = TRUE),
                      avg_srd = mean(srd_rate, na.rm = TRUE),
                      avg_vol = mean(hotmail_volume, na.rm = TRUE),
                      avg_ipr = mean(hotmail_inbox_pct, na.rm = TRUE))

prospects_ip$avg_comp[is.na(prospects_ip$avg_comp)] <- 0 
prospects_ip$avg_srd[is.na(prospects_ip$avg_srd)] <- 0 
prospects_ip$avg_bounce[is.na(prospects_ip$avg_bounce)] <- 0 
prospects_ip$avg_traps[is.na(prospects_ip$avg_traps)] <- 0 


#########################################
## create the "ideal" ip and attach it to the ip prepped data

ideal_ip <- data.frame(
  ip_address = "ideal",
  pct_days_complt = 1,
  avg_score = 100,
  avg_traps = 0,
  avg_bounce = 0, 
  avg_comp = 0,
  avg_srd = 0,
  avg_vol = 500000,
  avg_ipr = .8
)

##AS Comment: minor naming issues here-- has ideal instead of ideal_ip
ipvsideal <- rbind(ideal_ip, prospects_ip)





##########################################3
## define function 


cos_sim <- function(df) {
  df$ip_address <- NULL
  df$GROUP_NAME <- NULL
  df$CATEGORY <- NULL
  df$LIST_ID <- NULL
  df$avg_vol <- NULL
  df$avg_flags <- NULL
  df <- scale(df)
  df <- as.matrix(df)
  x <- matrix(nrow =  dim(df)[1])
  
  for(j in 1:dim(df)[1]){
    A = df[1,]
    B = df[j,]
    x[j] = ( sum(A*B)/sqrt(sum(A^2)*sum(B^2)) )
  }
  return(x)
} 


## Run the model for ips

ipvsideal$prospecting_score_ip <- cos_sim(ipvsideal)

## Bring client info back in 

##AS Comment:  Again, don't do the hard coding of indices to reference columns.

client_info <- prospects_df[,c("ip_address","GROUP_NAME","CATEGORY","LIST_ID")]
client_info<- unique(client_info)
prospect_ranking <- merge(client_info, ipvsideal, by = c("ip_address"), all.x=TRUE)
prospect_ranking <- prospect_ranking[with(prospect_ranking, order(-prospecting_score_ip)), ]




##################### optional: flag the ips for use in group score ###########################
## Might just take this flagging out. it looks like avg_ip_score works just fine 

##AS Comment:  Why did you opt to do it this way?  I would have done an ifelse()

# primeips <- subset(prospect_ranking, prospecting_score_ip >.5)
# primeips <- subset(primeips, pct_days_complt >.8)
# primeips <- subset(primeips, avg_ipr > .5)
# primeips <- subset(primeips, avg_ipr < .95)
# primeips <- subset(primeips, avg_vol >10000)
# length(unique(primeips$ip_address))

prospect_ranking$prime_flag<- ifelse((prospect_ranking$prospecting_score > 0.5
                              & prospect_ranking$pct_days_complt > 0.8
                              & prospect_ranking$avg_ipr > 0.5 
                              & prospect_ranking$avg_ipr < 0.95
                              & prospect_ranking$avg_vol > 10000),1,0)

# primeips<- primeips[,c(1,14)]
# primeips <- unique(primeips)

# decentips <- subset(prospect_ranking, prospecting_score_ip >0)
# decentips <- subset(decentips, pct_days_complt >.5)
# decentips <- subset(decentips, avg_ipr > .5)
# decentips <- subset(decentips, avg_ipr < .95)
#decentips <- subset(decentips, avg_vol >10000)
# length(unique(decentips$ip_address))

prospect_ranking$decent_flag<- ifelse((prospect_ranking$prospecting_score > 0
                                      & prospect_ranking$pct_days_complt > 0.5
                                      & prospect_ranking$avg_ipr > 0.5 
                                      & prospect_ranking$avg_ipr < 0.95),1,0)
# decentips<- decentips[,c(1,14)]
# decentips <- unique(decentips)

# prospect_ranking<- merge(prospect_ranking, primeips, by = "ip_address", all.x=TRUE)
# prospect_ranking<- merge(prospect_ranking, decentips, by = "ip_address", all.x=TRUE)

# prospect_ranking$prime_flag[is.na(prospect_ranking$prime_flag)] <- 0 
# prospect_ranking$decent_flag[is.na(prospect_ranking$decent_flag)] <- 0 

prospect_ranking$flags<- prospect_ranking$prime_flag + prospect_ranking$decent_flag

##AS Comment: Same as before :-)
ip_flags<- prospect_ranking[,c("ip_address", "prospecting_score_ip", "flags")]
ip_flags<- unique(ip_flags)


prospects_group<- merge(prospects_df, ip_flags, by = "ip_address", all.x=TRUE)

#######################################  potentially remove above section and skip to next #########################################################



################################################
## roll up group-level for model (could choose to take out avg_flags)

#AS Comment:  Same thing :-)

ipscores<- prospect_ranking[,c("ip_address", "prospecting_score_ip")]
ipscores<- unique(ipscores)

prospects_group<- merge(prospects_df, ipscores, by = "ip_address", all.x=TRUE)

prospects_group <- ddply(prospects_group, c("GROUP_NAME", "CATEGORY", "LIST_ID"), summarize,
                         ip_count = length(unique(ip_address)),
                         pct_days_complt = mean(pct_days_complt, na.rm = TRUE),
                         avg_ip_score = mean(prospecting_score_ip, na.rm = TRUE),
                         #avg_flags = mean(flags, na.rm = TRUE),
                         avg_score = mean(score, na.rm = TRUE),
                         avg_traps = mean(traps, na.rm = TRUE),
                         avg_bounce = mean(bounce_rate, na.rm = TRUE),
                         avg_comp = mean(complaint_rate, na.rm = TRUE),
                         avg_srd = mean(srd_rate, na.rm = TRUE),
                         avg_vol = mean(hotmail_volume, na.rm = TRUE),
                         avg_ipr = mean(hotmail_inbox_pct, na.rm = TRUE))
                         
prospects_group$avg_comp[is.na(prospects_group$avg_comp)] <- 0 
prospects_group$avg_srd[is.na(prospects_group$avg_srd)] <- 0 
prospects_group$avg_bounce[is.na(prospects_group$avg_bounce)] <- 0 
prospects_group$avg_traps[is.na(prospects_group$avg_traps)] <- 0 

##Only run for actual groups with more than one ip

##Small typo, removed a space
prospects_group <- subset(prospects_group, ip_count > 1)

#########################################
## create the "ideal" group and attach it to the group prepped data


ideal_group <- data.frame(
  GROUP_NAME = "ideal",
  CATEGORY = "ideal",
  LIST_ID = 0,
  ip_count = 20,
  pct_days_complt = 1,
  avg_ip_score = 1,
  #avg_flags = 2,
  avg_score = 100,
  avg_traps = 0,
  avg_bounce = 0, 
  avg_comp = 0,
  avg_srd = 0,
  avg_vol = 500000,
  avg_ipr = .8
)

groupvsideal <- rbind(ideal_group, prospects_group)


#########################################
## run the model for groups 
groupvsideal$prospecting_score_group <- cos_sim(groupvsideal)
groupvsideal <- groupvsideal[with(groupvsideal, order(-prospecting_score_group)), ]



### Either stop here with two dataframes: groupvsideal and prospect_ranking, or combine into one dataframe:


resultsa<- prospect_ranking#[,c(-14, -15)]
resultsa$ip_count <- 1
resultsa$avg_ip_score<- NA
resultsa <- rename(resultsa, c("prospecting_score_ip" = "prospecting_score"))
#"flags" = "prime_indicator"))
resultsa <- resultsa[,c(1, 2, 3, 4, 14, 13, 15, 5, 6, 7, 8, 9, 10, 11, 12)]

resultsb<- groupvsideal
resultsb$ip_address<- "Group Rollup"
resultsb <- rename(resultsb, c("prospecting_score_group" = "prospecting_score"))
resultsb <- resultsb[,c(15, 1, 2, 3, 4, 14, 6, 5, 7, 8, 9, 10, 11, 12, 13)]

results<- rbind(resultsa, resultsb)
results <- results[with(results, order(LIST_ID, CATEGORY, ip_address, prospecting_score)), ]

results$prospecting_score_scaled<- ifelse(results$prospecting_score <0, 0, (round((results$prospecting_score*10), digits = 0)/2))







#####################################################################################################################################################
#########################################  PART 2: LIFT PREDICTION  #################################################################################
#####################################################################################################################################################



## remember from before, we have hive_cert_metrics and hive_noncert_metrics, which come from the cert_value_staging table, 
## and prospects_df, which is the daily data (hive metrics plus the pct_days_complt from oracle) 
## for the set of noncert ips that meet the day criteria (Only ips who have 30 days of hotmail volume)


#let's put the same 30 days of hotmail data requirement on hive_cert ips too

hotmail_days_cert<- subset(hive_cert_metrics, hotmail_volume >0)
hotmail_days_cert <- ddply(hotmail_days_cert, c("ip_address"), summarize,
                           hotmail_days = length(unique(day)))

hotmail_days_cert<- subset(hotmail_days_cert, hotmail_days > 30)
cert<- merge(hive_cert_metrics, hotmail_days_cert, by = "ip_address")


# then we'll take the columns we need from hive_cert_metrics and prospects_df and rbind them together 

cert<- cert[,c(1, 2, 3, 8, 11, 12, 15, 16, 17, 18)]
noncert_prospects<- prospects_df[,c(1, 7, 5, 12, 15, 16, 19, 20, 21, 22 )]

lift_df<- rbind(cert, noncert_prospects)


#############################
### The optimal profiling method for repnet data
#############################


## Just like in the cert value tool, we aggregate the data to make cuts based on an IP's 90 day average metrics
## This time we are only using hotmail for volume 

library(plyr)
avgdata <- ddply(lift_df, c("ip_address", "cert_noncert"), summarize,
                 avg_score = mean(score, na.rm = TRUE),
                 avg_traps = mean(traps, na.rm = TRUE),
                 avg_bounce = mean(bounce_rate, na.rm = TRUE),
                 avg_comp = mean(complaint_rate, na.rm = TRUE),
                 avg_vol = mean(hotmail_volume, na.rm = TRUE),
                 avg_srd = mean(srd_rate, na.rm = TRUE),
                 avg_inbox = mean(hotmail_inbox_pct, na.rm = TRUE))

## Set any NAs to zero

avgdata$avg_comp[is.na(avgdata$avg_comp)] <- 0 
avgdata$avg_srd[is.na(avgdata$avg_srd)] <- 0 

## Here we make the cuts

avgdata$volcuts <- cut(avgdata$avg_vol, c(-Inf, 10000, 50000, 150000, 500000, Inf), labels = c("Vol 0-10K", "Vol 10-50K","Vol 50-150K", "Vol 150K-500k", "Vol 500k+"),
                       include.lowest = FALSE, right = TRUE, dig.lab = 3,
                       ordered_result = FALSE)

avgdata$srdcuts <- cut(avgdata$avg_srd, c(-Inf, .001, .2, .4, Inf), labels = c("SRD 0%", "SRD 1-20%","SRD 20-40%", "SRD 40+%"),
                       include.lowest = FALSE, right = TRUE, dig.lab = 3,
                       ordered_result = FALSE)

avgdata$scorecuts <- cut(avgdata$avg_score, c(-Inf, 90, 95, 98, Inf), labels = c("Score < 90", "Score 90-95","Score 95-98", "Score 98-100"),
                         include.lowest = FALSE, right = TRUE, dig.lab = 3,
                         ordered_result = FALSE)

avgdata$compcuts <- cut(avgdata$avg_comp, c(-Inf, .0006, .002, .003, .007, Inf), labels = c("Comp 0-.06%", "Comp .06-.2%","Comp .2-.3%", "Comp .3-.7%", "Comp .7+%"),
                        include.lowest = FALSE, right = TRUE, dig.lab = 3,
                        ordered_result = FALSE)

avgdata$bouncecuts <- cut(avgdata$avg_bounce, c(-Inf, .001, .01, .1, Inf), labels = c("Bnce 0%", "Bnce 0.1-1%","Bnce 1-10%", "Bnce 10+%"),
                          include.lowest = FALSE, right = TRUE, dig.lab = 3,
                          ordered_result = FALSE)


avgdata$trapcuts <- cut(avgdata$avg_traps, c(-Inf, .001, 2, 5, Inf), labels = c("Trps 0", "Trps 1-2","Trps 3-5", "Trps 5+"),
                        include.lowest = FALSE, right = TRUE, dig.lab = 3,
                        ordered_result = FALSE)

avgdata$scorebucket <- cut(avgdata$avg_score, c(-Inf, 50, 70, 80, 85, 90, 93, 95, 96, 97, 98, 99, Inf), 
                           labels = c("0-50","50-70", "70-80", "80-85", "85-90", "90-93", "93-95","95-96","96-97","97-98","98-99", "99-100"),
                           include.lowest = FALSE, right = TRUE, dig.lab = 3,
                           ordered_result = FALSE)

## Here we create the profiles from combinations of cuts. "scorebucket" above will be its own profile

avgdata$profile <- paste(avgdata$scorecuts, avgdata$compcuts, avgdata$volcuts, sep='|')
avgdata$medprofile1 <- paste(avgdata$scorecuts, avgdata$compcuts, avgdata$srdcuts,  sep='|')
avgdata$medprofile2 <- paste(avgdata$scorecuts, avgdata$srdcuts, avgdata$volcuts, sep='|')
avgdata$medprofile3 <- paste(avgdata$scorecuts, avgdata$compcuts, avgdata$srdcuts, avgdata$volcuts, sep='|')
avgdata$deepprofile <- paste(avgdata$scorecuts, avgdata$compcuts, avgdata$volcuts, avgdata$srdcuts, avgdata$bouncecuts, avgdata$trapcuts, sep='|')

library(reshape2)
avgdata2<- melt(avgdata, id.vars=c("ip_address", "cert_noncert", "avg_inbox"), 
                measure.vars = c("scorebucket", "profile", "medprofile1", "medprofile2", "medprofile3", "deepprofile"))


### Now we'll pull some stats on the profiles

#first we find the average IPR for cert vs noncert in each profile, and use the difference to show an average inbox lift per profile

certprofs <- subset(avgdata2, cert_noncert == 1)
certprofs <- aggregate(avg_inbox~ value, certprofs, mean)

prospectprofs <- subset(avgdata2, cert_noncert == 0)
prospectprofs <- aggregate(avg_inbox~ value, prospectprofs, mean)

profdiffs <- merge(certprofs, prospectprofs, by = c("value"))
profdiffs <- rename(profdiffs,c("avg_inbox.x"= "cert_avg", "avg_inbox.y"= "prospect_avg"))
profdiffs$avg_inbox_lift <- profdiffs$cert_avg - profdiffs$prospect_avg

## count how many ips are in each profile for cert vs noncert, and merge that with the average lift per profile 

count <- function(x) { length(unique(na.omit(x))) } 
profilecounts <- aggregate(ip_address~ value + cert_noncert, avgdata2, FUN= "count")

profile_summary <- merge(profdiffs, profilecounts, by=c("value"))

### Now we join profile summaries to the ips and each of their profiles

profiles <- avgdata2[, c("ip_address", "avg_inbox", "value", "variable")]
profiles <- merge(profiles, profile_summary, by = c("value"))

## We have duplicate rows, so drop the cert_noncert variable and keep cert_count (Here the cert_noncert only referred to the profile anyway, not the individual ips)

profiles <- subset(profiles, cert_noncert == 1)
profiles <- rename(profiles, c("value"="profile", "ip_address.x" = "ip_address", "ip_address.y" = "cert_count", "variable"= "level", "avg_inbox" ="ip_avg_inbox"))

## Find the inbox lift for each ip. Then some ordering and sorting.

profiles$predicted_inbox_lift <- profiles$cert_avg - profiles$ip_avg_inbox
profiles<- profiles[, c("ip_address", "level", "profile", "predicted_inbox_lift", "cert_count", "avg_inbox_lift", "cert_avg", "prospect_avg")]
profiles <- profiles[with(profiles, order(ip_address, level)), ]

## To find the optimal profile level to use, first take out scorebucket and consider only the profiles with at least 15 noncert ips

optimize <- subset(profiles, cert_count >= 15)
optimize <- optimize[!(optimize$level == "scorebucket"),] 

## Then find the profile with the max inbox lift. Merge that max back into the rest of the data to get just the ips and their optimal profile

findmax <- aggregate(cbind(predicted_inbox_lift)~ ip_address, optimize, max)
maxbyip <- merge(findmax, profiles, by = c("ip_address", "predicted_inbox_lift"), all=FALSE)

## Combine with original similarity-scored prospect data ("results" from end of previous section) and present results 

predicted_lift <- merge(results, maxbyip, by = "ip_address", all.x=TRUE)

predicted_lift <- predicted_lift[with(predicted_lift, order(-LIST_ID, CATEGORY, ip_address, prospecting_score)), ]

## See exactly what form engineering wants the data set in and select variables of interest
#predicted_lift <- predicted_lift[,c(1, 2, 3, 4, 17, 13, 14, 15, 16)]

write.csv(prime_predictions, file = "Prospecting_Predictions_byGroup.csv")


