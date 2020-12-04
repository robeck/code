
%macro bond_confirm(indata= ,outdata= )
/*ͳ��ծȯ�Ĺ۲�ֵtotal*/
data &indata;
set &indata;
n=1;
run;

proc sql noprint;
create table &indata as
select *,sum(n) as total
from &indata
group by liscd;
quit;
/************************/

/*Ŀǰ����������ò����ˣ�ʹ���˴�ƾ�������ffc5*/

proc sort data=&indata;by liscd Trddt;run;quit;
%mend;




%macro calculate();

/*ƥ��ɸѡ�õ�ծȯ�����������ݣ�����bond�ĳ�������*/

proc sql;
create table c.bond_match_china as
select a.*,b.coupon
from c.bond as a left join c.bondinfo_china as b on a.liscd=b.liscd;
quit;

data c.bond_match_china2;
set c.bond_match_china;
if coupon='.' then delete;
if Accurintrs='.' then delete;
if Accurintrs=0 then delete;
if total<100 then delete;
run;

data c.bond_match_china2;
set c.bond_match_china2;
date = input(put(Trddt,12.),yymmdd10.);
format date yymmdd10.;
run;

proc sort data=c.bond_match_china2;by liscd Trddt;run;quit;


data C.bond_match_china3(keep=liscd date clsprc Accurintrs coupon);
set C.bond_match_china2;
run;

data c.bond_match_china3(keep=liscd date clsprc Accurintrs coupon lambda lag_lambda ret);
set c.bond_match_china3;
lambda=clsprc+Accurintrs;
lag_lambda=lag(lambda);
ret=(lambda+coupon-lag_lambda)/lag_lambda;
by liscd;
if first.liscd then do;
ret='.';
end;
run;

proc sql noprint;
create table c.bond_match_china3 as
select a.*,b.mkt_rf,b.smb,b.hml
from c.bond_match_china3 as a left join c.ffc5 as b on a.date=b.trddy;
quit;

proc sort data=c.bond_match_china3;by liscd date;run;quit;

/******************************************************************/

/*�Լ���õ�ret��ȥ�޷������棬��Ϊ�������棺bond excess return*/

data c.bond_rf;
set c.bond_rf;
date=input(put(Clsdt,12.),yymmdd10.);
format date yymmdd10.;
run;

proc sql noprint;
create table c.bond_match_china3 as
select a.*,b.Nrrdaydt 
from c.bond_match_china3 as a left join c.bond_rf as b on a.date=b.date;
quit;

data c.bond_match_china3;
set c.bond_match_china3;
bond_excess_return=(ret-Nrrdaydt)*100;
run;

proc sort data=c.bond_match_china3; by Liscd date;run;

%mend;



/******************************************/
/*����term������Ҫ���ڣ����ڹ�ծ����
  def��������Ҫ���ڹ�ծ���ݺ�����/����ծȯ����Ϻ�����Ȩ����-���ڹ�ծ��������

/*���ڲ�������
data c.treasure;
set c.treasure;
date=input(put(Trddt,12.),yymmdd10.);
format date yymmdd10.;
run; 

*/
%macro term_def(indata,outdata);

/*term*/
proc sort data=&indata;by date Yeartomatu;run;

data work._ts_short work._ts_long;
set &indata;
by date;
if first.date then output work._ts_short;
if last.date then output work._ts_long;
run; 

proc sql noprint;
create table work._ts_1 as
select (a.Yield) as long_yield,a.date,(b.Yield) as short_yield
from work._ts_long as a left join work._ts_short as b on a.date=b.date;
quit;

data work.term;
set work._ts_1;
term=long_yield-short_yield;
run;
/**********************************************/
/*def*/

data work._ts_2;
set c.Bondinfo_china;
if terms>=10 then output;
if rating='NR' then delete;
	if Crdrate='AAA' then rating_n=0;
	if Crdrate='Aaa' then rating_n=0;
	if Crdrate='AA+' then rating_n=1;
	if Crdrate='Aa1' then rating_n=1;
	if Crdrate='AA' then rating_n=2;
	if Crdrate='Aa2' then rating_n=2;
	if Crdrate='AA-' then rating_n=3;
	if Crdrate='Aa3' then rating_n=3;
	if Crdrate='A+' then rating_n=4;
	if Crdrate='A1' then rating_n=4;
	if Crdrate='A' then rating_n=5;
	if Crdrate='A2' then rating_n=5;
	if Crdrate='A-' then rating_n=6;
	if Crdrate='A3' then rating_n=6;
	if Crdrate='BBB+' then rating_n=7;
	if Crdrate='Baa1' then rating_n=7;
	if Crdrate='BBB' then rating_n=8;
	if Crdrate='Baa2' then rating_n=8;
run;

data work._ts_2;
set work._ts_2;
if rating_n<=8 then output;
run;

proc sql noprint;
create table work._ts_3 as
select *
from c.Bond_match_china3 
where liscd in (select distinct liscd from work._ts_2);
quit;

/*�����ն����ݣ�ע��������ǿ����£���
 ������Ȼʹ�������ݣ������������ϵĺ���沢���ʺ�
 ����˵����������������ǲ�������ɡ�����ǰ�������
 ������ö���ʹ���¶�����*/
proc sort data=work._ts_3; by delta_day;run;
proc means data=work._ts_3;
	by delta_day;
	var bond_excess_return;
	output out=work._ts_4 mean=menas_return n=nobs;
quit;
run;



%mend;


%term_def(c.treasure,null);




