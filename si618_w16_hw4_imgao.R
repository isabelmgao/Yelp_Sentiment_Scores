library(ggplot2)
library(hexbin)
star_sentimentscore <- read.delim("~/Desktop/WINTER_2016/618/WEEK_4/HW/si618w16_hw4/star_sentimentscore_desired_output.txt", header=FALSE)
ggplot(star_sentimentscore, aes(x=V1, y=V2)) +
  geom_point(shape=1) +    # Use hollow circles
  geom_smooth(method=lm,   # Add linear regression line
              se=FALSE) 
qplot(V1, V2, data=star_sentimentscore, geom="hex")

library(Hmisc)
rcorr(as.matrix(star_sentimentscore), type="pearson")