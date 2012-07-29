d<-read.table("/tmp/select.txt", sep="\t")
d$group<-1
d$date<-as.Date(V1)
e<-subset(d,date < as.Date("2100-01-01"))
p<-ggplot(e)+geom_line(aes(x=date,y=V2,colour=group))+
xlab("Year")+ylab("Cumulative citations")+
scale_x_date()+
theme_phenix()
print(p)