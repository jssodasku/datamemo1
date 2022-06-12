

### setting the work directory
setwd("/YOUR/WORK/DIRECTORY/HERE")
### loading libraries
library(dplyr)
library(ggplot2)
library(ggthemes)
library(arrow)
library(lubridate)
library(reshape2)
library(stargazer)
options(arrow.skip_nul = TRUE)


##########################################################################################################
########################################## Preparing the data ############################
##########################################################################################################

### loading the data
df_all <- arrow::read_feather("data/feather_latest_data/all_comments.feather")


### separating blocked pro-Kremlin channels from those that have not been blocked
df_all$type[df_all$blocked == 1] <-"Pro-Kremlin (blocked)" 
df_all$type[df_all$type == "pro_kremlin"] <-"Pro-Kremlin" 
df_all$type[df_all$type == "anti_kremlin"] <-"Regime-critical" 
df_all$type[df_all$type == "entertainment"] <-"Entertainment" 


### formatting publish_date as a date variable
df_all$publish_date_1 <- as.Date(df_all$publish_date) 

### counting number of days before and after YouTube's ban on RU state media 
start.date<- as.Date(c("2022-03-12")) # the day after YouTube announces that they are blocking other RU state-controlled media (and not just RT and Sputnik)
df_all$days<- as.numeric(difftime(df_all$publish_date_1, start.date, units = c("days"))) # the difference between ban date and the update date in number of days

### selecting only 40 days before and after
df_all <- df_all %>%
  filter(days >= -40, 
         days <= 40)


### creating a function that creates a data frame for each channel type where rows are days
activity_per_day_aggregate <- function(df, channel_type) {
  if (channel_type != "All") {
    ## aggregating activitiy for each day
    df_days <- df %>%
      filter(type == channel_type) %>%
      group_by(days) %>%
      mutate(sum_comments = n()) %>%
      distinct(days, .keep_all = T) %>%
      select(days, sum_comments) 
  }
  else {
    ## aggregating activitiy for each day
    df_days <- df %>%
      group_by(days) %>%
      mutate(sum_comments = n()) %>%
      distinct(days, .keep_all = T) %>%
      select(days, sum_comments) 
  }
  
  ### creating a dummy variable indicating whether comment activity is from before or after the ban
  df_days$after <- 0
  df_days$after[df_days$days >= 0] <- 1
  df_days$after <- as.factor(df_days$after)
  df_days$type <- channel_type
  df_days
}

### creating dataframe for each channel type where each row represents one day
df_days_pro <- activity_per_day_aggregate(df_all, "Pro-Kremlin")
df_days_pro_b <- activity_per_day_aggregate(df_all, "Pro-Kremlin (blocked)")
df_days_anti <- activity_per_day_aggregate(df_all, "Regime-critical")
df_days_enter <- activity_per_day_aggregate(df_all, "Entertainment")


### merging the three data sets above
df_days <- rbind(df_days_pro, df_days_pro_b, df_days_anti, df_days_enter)

##########################################################################################################
########################################## Analysing the data ############################
##########################################################################################################

### selecting only data for regime critical and pro-Kremlin (non-blocked) channel types
df_days_pro_anti <- df_days %>%
  filter(type != "Entertainment", 
         type != "Pro-Kremlin (blocked)")


####vizualising change over time
change_pro_anti <- ggplot(df_days_pro_anti, aes(x = days, y = sum_comments, color = after)) + 
  geom_point(alpha = 0.8) + 
  geom_vline(xintercept = 0, linetype = "dashed") + # 0 == 12th of March, the day after YouTube announces the ban
  geom_vline(xintercept = -8, linetype = "dashed", color = "red") + # -8 == 4th of March, the day Putin signs the anti-fake news law
  geom_vline(xintercept = -17, linetype = "dashed", color = "grey") + # -17 == 24th of February, the day of the invasion
  labs(x = "Days before and after the YouTube ban",
       y = "Number of comments", 
       color = "Time period") +
  stat_smooth(method = "lm", level = 0.95) +
  scale_colour_manual(values=c("light blue", "black"),
                      labels = c("Before the ban", "After the ban")) +
  theme(axis.text=element_text(size= 15),
        axis.title=element_text(size=15),
        strip.text = element_text(size = 14, face  = "bold")) +
  scale_x_continuous(limits = c(-40,40)) +
  scale_y_continuous(labels = scales::comma) +
  annotate("rect", xmin = 0, xmax = 40, ymin = 0 ,ymax = max(df_days_pro_anti$sum_comments)*1.1, alpha = 0.25, fill = "#8FBC8F") +
  facet_wrap(~type, nrow = 2) +
  theme_minimal() 

change_pro_anti

#exporting the figure
#ggsave("dataviz/pro_anti.pdf", width = 15, height = 12, units = "cm")


### doing the same but only for entertainment critical and pro-Kremlin (blocked) channels
df_days_enter_pro_b <- df_days %>%
  filter(type != "Regime-critical", 
         type != "Pro-Kremlin")


####vizualising change over time
change_enter_pro_b <- ggplot(df_days_enter_pro_b, aes(x = days, y = sum_comments, color = after)) + 
  geom_point(alpha = 0.8) + 
  #  scale_color_brewer(NULL, type = 'qual', palette = 6) + 
  geom_vline(xintercept = 0, linetype = "dashed") + # 0 == 12th of March, the day after YouTube announces the ban
  geom_vline(xintercept = -8, linetype = "dashed", color = "red") + # -8 == 4th of March, the day Putin signs the anti-fake news law
  geom_vline(xintercept = -17, linetype = "dashed", color = "grey") + # -17 == 24th of February, the day of the invasion
  labs(x = "Days before and after the YouTube ban",
       y = "Number of comments", 
       color = "Time period") +
  stat_smooth(method = "lm", level = 0.95) +
  scale_colour_manual(values=c("light blue", "black"),
                      labels = c("Before the ban", "After the ban")) +
  theme(axis.text=element_text(size= 10),
        axis.title=element_text(size=15),
        strip.text = element_text(size = 14, face  = "bold")) +
  scale_x_continuous(limits = c(-40,40)) +
  scale_y_continuous(labels = scales::comma) +
  #scale_y_continuous(limits = c(0,14)) +
  annotate("rect", xmin = 0, xmax = 40, ymin = 0 ,ymax = max(df_days_enter_pro_b$sum_comments)*1.1, alpha = 0.25, fill = "#8FBC8F") +
  facet_wrap(~type, nrow = 2) +
  theme_tufte() 

change_enter_pro_b

#exporting the figure
#ggsave("dataviz/change_enter_pro_b.pdf", width = 15, height = 12, units = "cm")


### ################## OLS regression models on aggregated data ###################
# simple lm for all 
lm_all_1 <- lm(sum_comments ~ days*after, df_days)
summary(lm_all_1)

# logged lm for all
lm_all_2 <- lm(log(sum_comments) ~ days*after, df_days)
summary(lm_all_2)

# simple lm for entertainment 
lm_enter_1 <- lm(sum_comments ~ days*after, df_days_enter)
summary(lm_enter_1)

# logged lm for entertainment 
lm_enter_2 <- lm(log(sum_comments) ~ days*after, df_days_enter)
summary(lm_enter_2)


# simple lm for regime critical
lm_anti_1 <- lm(sum_comments ~ days*after, df_days_anti)
summary(lm_anti_1)

# logged lm for regime critical
lm_anti_2 <- lm(log(sum_comments) ~ days*after, df_days_anti)
summary(lm_anti_2)

# simple lm for pro-Kremlin
lm_pro_1 <- lm(sum_comments ~ days*after, df_days_pro)
summary(lm_pro_1)


# logged lm for pro-Kremlin
lm_pro_2 <- lm(log(sum_comments) ~ days*after, df_days_pro)
summary(lm_pro_2)


# simple lm for pro-Kremlin blocked
lm_pro_b_b1 <- lm(sum_comments ~ days*after, df_days_pro_b)
summary(lm_pro_b_b1)


# logged lm for pro-Kremlin blocked
lm_pro_2_b <- lm(log(sum_comments) ~ days*after, df_days_pro_b)
summary(lm_pro_2_b)


### RD tables with four models models
stargazer(lm_pro_b_b1, lm_pro_1,
          title="Regression discontinuity in wall post activity",
          align = F,
          dep.var.labels=c( "Pro-Kremlin (banned)", "Pro-Kremlin (non-banned)"),
          covariate.labels=c("Days","Ban", "Days*ban"),
          omit.stat=c("LL","ser","f"),
          no.space = T)


