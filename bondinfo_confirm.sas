/*
债券清洗：
1.必须是上海/深圳交易所交易债券 即sctcd=1/2
2.必须为公司债券，bondtype=02
3.发现的货币类型为RMB，currency=CNY
4.交易周期必须大于两年，term>=2
5.不可赎回，不可回售，crdeem crtsell=N

ps：关于付息方式，应该是周期付息类型，但由于后面会筛选还有AIT的bond，因此暂不考虑


data c.bondinfo;
set c.bondinfo;
bondtype=input(bndtype,best12.);
terms=input(term,best12.);
run;
*/

proc sql;
create table c.bondinfo_china as
select *
from c.bondinfo
where Sctcd='1' or Sctcd='2' and Currency='CNY' and crdeem='N' and Crtsell='N';
quit;

data c.bondinfo_china;
set c.bondinfo_china;
if bondtype=2 then do;
	if terms>=2 then output;
end;
run;

data c.bondinfo_china;
set c.bondinfo_china;
coupon=intrrate*Parval*0.01;
if coupon='.' then delete;
run;
