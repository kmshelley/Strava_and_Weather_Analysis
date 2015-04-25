library(ggplot2)
testDF <- read.csv("joined.csv")
summary(testDF)

graph1 <- ggplot(testDF[!(testDF$athlete_gender==''),], aes(moving_time))
graph1 + geom_histogram(aes(fill=athlete_gender),position="dodge")

graph2 <- ggplot(testDF[!(testDF$athlete_gender==''),], aes(moving_time))
graph2 + geom_density(aes(fill=athlete_gender))

unique(testDF$WindDirMod)

testDF$WindDirMod <- as.numeric(as.character(testDF$WindDirection))

testDF$wind_diff <- ifelse(!is.na(testDF$WindDirMod),abs((testDF$Segment.Direction - testDF$WindDirMod)%%180 - 180),NA)

graph3 <- ggplot(testDF[!(testDF$athlete_gender==''),], aes(wind_diff,moving_time))
graph3 + geom_point(aes(color=athlete_gender)) + geom_smooth(method = "lm", aes(color=athlete_gender),alpha = 0.1) 