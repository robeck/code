/*
债券清洗：
1.必须是上海/深圳交易所交易债券 即sctcd=1/2
2.必须为公司债券，bondtype=02
3.发现的货币类型为RMB，currency=CNY
4.交易周期必须大于两年，term>=1
5.不可赎回，不可回售，crdeem crtsell=N
6.Ipaytypcd=2 周期付息

ps：关于付息方式，应该是周期付息类型，但由于后面会筛选还有AIT的bond，因此暂不考虑

*/
data c.bondinfo;
set c.bondinfo;
bondtype=input(bndtype,best12.);
terms=input(term,best12.);
sctcds=input(sctcd,best12.);
Ipaytypcds=input(Ipaytypcd,best12.);
run;

data c.bondinfo_china;
set c.bondinfo;
if sctcd=1 or sctcd=2;
if currency='CNY';
if crdeem='N';
if crtsell='N';
if bondtype=2;
if terms>=1;
if Ipaytypcds=2;
run;
