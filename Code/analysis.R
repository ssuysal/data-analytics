install.packages('lfe')
install.packages('huxtable')
install.packages('officer')
install.packages('flextable')
install.packages('openxlsx')
library('lfe')
library('huxtable')
library('officer')
library('flextable')
library('openxlsx')

# REPLACE_ME
setwd("/Users/serauysal/Desktop/data_analytics/Final")

data <- read.csv("data_1_with_topic_frequencies.csv", sep=';')
fe_1 <- felm(perc_votes ~ cluster_freq_0 + cluster_freq_2 + cluster_freq_3 + cluster_freq_4 + 
               cluster_freq_5 + cluster_freq_6 + cluster_freq_7 + cluster_freq_8 + cluster_freq_9 +
               Age_at_Election + duration_of_service + factor(Party) | MP_ID + year, data=data)
summary(fe_1)


data3 <- read.csv("data_3_with_topic_frequencies.csv", sep=';')
fe_3 <- felm(perc_votes ~ cluster_freq_0 + cluster_freq_2 + cluster_freq_3 + cluster_freq_4 + 
               cluster_freq_5 + cluster_freq_6 + cluster_freq_7 + cluster_freq_8 + cluster_freq_9 +
               Age_at_Election + duration_of_service + factor(Party) | MP_ID + year, data=data3)
summary(fe_3)

reg <- huxreg('FE (1-year interval)' = fe_1,
              'FE (4-years interval)' = fe_3,
       statistics = c('N' = 'nobs', R2 = "r.squared"))
reg
quick_xlsx(
  reg,
  file = "result.xlsx",
  borders = 0.4
)
