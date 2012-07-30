aaa<-read.table("./phenix.txt", sep="\t")
aaa$group<-factor(2)
aaa$date<-as.Date(aaa$V1)
aaa2<-subset(aaa,date < as.Date("2100-01-01"))

bbb<-read.table("./star.txt", sep="\t")
bbb$group<-factor(1)
bbb$date<-as.Date(bbb$V1)
bbb2<-subset(bbb,date < as.Date("2100-01-01"))

ccc<-read.table("./phobos.txt", sep="\t")
ccc$group<-factor(3)
ccc$date<-as.Date(ccc$V1)
ccc2<-subset(ccc,date < as.Date("2100-01-01"))

ddd<-read.table("./brahms.txt", sep="\t")
ddd$group<-factor(4)
ddd$date<-as.Date(ddd$V1)
ddd2<-subset(ddd,date < as.Date("2100-01-01"))

f<-rbind(aaa2,bbb2,ccc2,ddd2)

p<-ggplot(f)+geom_line(aes(x=date,y=V2,colour=group))+
xlab("")+ylab("cumulative citations")+
scale_colour_brewer(palette="Set1",name="",labels=c("STAR","PHENIX","PHOBOS","BRAHMS"),breaks=c(1,2,3,4))+
scale_x_date()+
theme_phenix()+
opts(legend.position=c(0.05,0.8))
print(p)