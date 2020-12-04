%macro term(indata,outdata);

/*term*/
/*��ծ�¶����ݹ��ƣ��ù�ծÿ�����һ�������������Ϊ��׼��*/
data work._term_1m work._term_30y;
set &indata;
if Yeartomatu='0.0800' then output work._term_1m;
if Yeartomatu='30.0000' then output work._term_30y;
run;

proc sql noprint;
create table work._term_1 as
select a.date,a.Yield as long_yield,b.Yield as short_yield
from work._term_30y as a left join work._term_1m as b on a.date=b.date;
quit;

data work._term;
set work._term_1;
term=long_yield-short_yield;
year_month=year(date)*100+month(date);
run;

proc sort data=work._term;by date year_month;run;

data c.term;
set work._term;
by year_month;
if last.year_month then output;
run;

/**********************************************/

%mend;


/*def*/
/*������������������
  ������û���ֳ����ݿ���ʹ��*/
%macro def(indata= ,cusip= ,date= ,closepr= );

/*ʮ������ծȯ����������BBB*/
data work._ts_2;
set c.Bondinfo_china;
if Crdrate='NR' then delete;
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
if terms>=10 then output;
run;

data work._ts_2;
set work._ts_2;
if rating_n<=8 then output;
run;
/************************/


/*�ع��µ�ծȯ���ϣ��������¶�value-weight������
  ���ȼ������ծȯ���¶ȳ������棬bond_excess_return
  ��μ�����ǰ��������ÿ���µ�value-weight-portiflio������
  �ô������ȥ�¶ȳ��ڹ�ծ���棬��Ϊterm*/


/************************************************************************/
/*1.����*/
/*ƥ�����Ҫ���ծȯ��ծȯ���׵��¶�����*/
proc sql;
create table work._ts_3 as
select a.*,b.*
from &indata as a left join work._ts_2 as b on a.liscd=b.liscd;
quit;

data work._ts_3;
set work._ts_3;
if coupon='.' then delete;
run;

proc sort data=work._ts_3;by liscd date;run;

/*�Կ�ȱ�²����������ݣ�����ǰһ�յĽ��׽������*/
/*�۸�Ϊ���̼�
 Ȩֵѡȡ�˷���������˶��ڵ�һծȯ�ڣ����н��׵�Ȩ��ͬ*/
data work._ts_4(rename=(_date=date) rename=(_price=Mclsprc) rename=(_Acisuquty=Acisuquty));
   set work._ts_3;
	by liscd;
   retain  _date _price _Acisuquty;
   format _date YYMMDD10.;
   if first.liscd then do;
          _date=date;
		  _price=Mclsprc;
		  _Acisuquty=Acisuquty;
          output;
    end;
     else do;
               _date=intnx('month',_date,1);
                  if _date<date then do until (_date=date);
				  _price=_price;
				  _Acisuquty=0;
               output;
              _date=intnx('month',_date,1);
                       end;
                 if _date=date then do;
								_price=&closepr;
								_Acisuquty=Acisuquty;
                                output;
                             end;
     end;
	 drop date;
	 drop Mclsprc;
	 drop Acisuquty;
run;


data work._ts_4;
set work._ts_4;
if coupon='.' then delete;
if Acisuquty=0 then delete;
weight=Acisuquty*Mclsprc;
run;

/*�޸�ծȯ�ڲ�����*/
data work._ts_5;
set work._ts_4;
/*
date = input(put(Trddt,12.),yymmdd10.);
format date yymmdd10.;
*/
frequency=input(Pintrt,12.);/*����Ƶ��*/
format frequency best12.;
first_interest_date=input(put(Intrdt,12.),yymmdd10.);/*�״μ�Ϣ��*/
format first_interest_date yymmdd10.;
maturity=input(put(Matdt,12.),yymmdd10.);/*������*/
format maturity yymmdd10.;
offering_date=input(put(Ipodt,12.),yymmdd10.);/*������*/
format offering_date yymmdd10.;
principal_amt=input(Pooprc,12.);/*���м�*/
format principal_amt best12.;
if IntBss = '30/360' then basis = 0;/*bisas*/
  else if IntBss = 'ACT/ACT' then basis = 1;
  else if IntBss = 'ACT/360' then basis = 2;
  else basis = 3;

if frequency=0 then delete;
run;

proc sort data=work._ts_5;by &cusip &date;run;quit;
/******************************************/
/*�¶�����������û��Ӧ����Ϣ�ģ���Ҫ�Լ���*/
/*coupon confirmed, ����bond return����������return��֮��Ҫʹ��month��week*/

data work._ts_6;
    set work._ts_5;
   	by liscd;
	* exclude obvious input errors;
	where date >= offering_date and date <= maturity;
    if first_interest_date < offering_date then 
    	first_interest_date = offering_date;
    * return formula: rt=((Pt+AIt+Ct)-(Pt-1+AIt-1))/(Pt-1+AIt-1);
    format last_date YYMMDD10.;
    if date <= first_interest_date then do;
      AI = finance('accrint',offering_date,first_interest_date,date,
         coupon,principal_amt,frequency,basis) /  principal_amt;
      C = 0;
    end;
    else do;         /* if date > first_interest_date */
      last_date = finance('couppcd',date,maturity,frequency,basis);
      if last_date <= first_interest_date then do;
        last_date = first_interest_date;
        I1 = (finance('accrint',offering_date,first_interest_date,last_date,
          coupon,principal_amt,frequency,basis) /  principal_amt);
      end;
      else do;    /* last_date > first_interest_date */
        I1 = finance('accrint',offering_date,first_interest_date,last_date,
          coupon,principal_amt,frequency,basis) / principal_amt;
      end;
      I2 = finance('accrint',offering_date,first_interest_date,date,
        coupon,principal_amt,frequency,basis) / principal_amt;
      AI = I2 - I1;
      C = 0;
      if date = last_date then do;     /* if date = last_date: I1 = I2, so AI = 0 by calculation */
        AI = 0;
        C = coupon / frequency;
      end;
    end;

    /*negative AI treatment*/
    if AI < 0 then do;
      if basis in (0,2) then AI = ((date - last_date) / 360) * coupon;
      else AI = ((date - last_date) / 365) * coupon;
    end;
	


    /* compute return;*/
    format lag_date YYMMDD10.;
	lambda=&closepr+AI;
    lag_lambda=lag(lambda);
    lag_date = lag(date);
	
    if first.&cusip then do;
      lag_lambda = .;  lag_date = .; ret = .;
    end;
    else do;
      if lag_date < last_date < date then C = (1 + int(yrdif(lag_date,date,IntBss) * frequency)) * (coupon / frequency);
	  /*C^=0, AI=0*/
	  if C^= 0 then AI = 0;

      ret = ((lambda) + C -(lag_lambda)) / (lag_lambda);
      
    end;
run;

data work._ts_6(keep=liscd date year_month ret weight lag_weight);
set work._ts_6;
year_month=year(date)*100+month(date);
lag_weight=lag(weight);
run;

proc sql noprint;
create table work._ts_6 as
select a.*,b.Nrrmtdt 
from work._ts_6 as a left join c.Rf_month as b on a.year_month=b.yearmonth;
quit;

proc sort data=work._ts_6;by liscd date;run;quit;

data work._ts_7;
set work._ts_6;
bond_excess_return=ret*100-Nrrmtdt;
run;
/*********************************************************************************************************/
/*2.����*/
proc sort data=work._ts_7;by liscd date;run;

data work._ts_7;
set work._ts_7;
by liscd;
retain mid;
	if first.liscd then do;
		mid=1;
		lag_weight='.';
	end;
	else do;
		mid=mid+1;
	end;
run;

proc sort data=work._ts_7;by mid;run;

data work._ts_7;
set work._ts_7;
if bond_excess_return='.' then delete;
run;

proc sql noprint;
create table work._ts_8 as
select *,sum(bond_excess_return*lag_weight) as total,sum(lag_weight) as total_weight
from work._ts_7
group by mid;
quit;

proc sort data=work._ts_8;by mid;run;

data work._ts_9(keep=mid vw_return);
set work._ts_8;
vw_return=total/total_weight;
by mid;
	if last.mid then output;
run;



%mend;

/*
%term(c.Treasure,null);
*/

%def(indata=c.Bond_month,cusip=liscd,date=date,closepr=Mclsprc);
