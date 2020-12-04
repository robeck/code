
%macro bond_confirm(indata= ,outdata= )
/*统计债券的观测值total*/
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

/*无风险收益的日期修正*/
data c.bond_rf;
set c.bond_rf;
date=input(put(Clsdt,12.),yymmdd10.);
format date yymmdd10.;
run;
/********************************/

/*目前这个三因子用不了了，使用人大财经的数据ffc5*/

proc sort data=&indata;by liscd Trddt;run;quit;
%mend;



/*indata=bond_month即债券的月度收益*/
%macro calculate(indata= ,cusip= ,date= ,closepr= );

/*匹配筛选好的债券和三因子数据，计算bond的超额收益*/

/*匹配符合要求的债券和债券交易的月度数据*/
proc sql;
create table c.bond_match_china as
select a.*,b.*
from &indata as a left join c.bondinfo_china as b on a.liscd=b.liscd;
quit;

proc sort data=c.bond_match_china;by &cusip &date;run;

/*对空缺月补上日期数据，并用前一日的交易进行替代*/
/*价格为收盘价
 权值选取了发行量，因此对于单一债券内，所有交易的权相同*/
data work._t_1(rename=(_date=date) rename=(_price=&closepr) rename=(_Acisuquty=Acisuquty));
   set c.bond_match_china;
	by liscd;
   retain  _date _price _Acisuquty;
   format _date YYMMDD10.;
   if first.liscd then do;
          _date=date;
		  _price=&closepr;
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
	 drop &closepr;
	 drop Acisuquty;
run;

/*剔除coupon不符合标准，同时记录权值*/
data work._t_1;
set work._t_1;
if coupon='.' then delete;
if Acisuquty=0 then delete;
weight=Acisuquty*Mclsprc;
run;

data work._t_2;
set work._t_1;
/*
date = input(put(Trddt,12.),yymmdd10.);
format date yymmdd10.;
*/
frequency=input(Pintrt,12.);/*交割频率*/
format frequency best12.;
first_interest_date=input(put(Intrdt,12.),yymmdd10.);/*首次计息日*/
format first_interest_date yymmdd10.;
maturity=input(put(Matdt,12.),yymmdd10.);/*到期日*/
format maturity yymmdd10.;
offering_date=input(put(Ipodt,12.),yymmdd10.);/*发行日*/
format offering_date yymmdd10.;
principal_amt=input(Pooprc,12.);/*发行价*/
format principal_amt best12.;
if IntBss = '30/360' then basis = 0;/*bisas*/
  else if IntBss = 'ACT/ACT' then basis = 1;
  else if IntBss = 'ACT/360' then basis = 2;
  else basis = 3;

if frequency=0 then delete;
run;

proc sort data=work._t_2;by &cusip &date;run;quit;
/******************************************/
/*月度数据这里是没有应计利息的，需要自己算*/
/*coupon confirmed, 计算bond return，这里是日return，之后要使用month，week*/

data work._t_3;
    set work._t_2;
   	by &cusip;
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

/******************************************/


data work._t_4(keep=liscd date year_month ret weight);
set work._t_3;
year_month=year(date)*100+month(date);
run;




proc sql noprint;
create table work._t_5 as
select a.*,b.mkt_rf,b.smb,b.hml
from work._t_4 as a left join c.Ffc5_month as b on a.year_month=b.trdmn;
quit;

/*对计算好的ret减去无风险收益，作为超额收益：bond excess return*/

proc sql noprint;
create table work._t_6 as
select a.*,b.Nrrmtdt 
from work._t_5 as a left join c.Rf_month as b on a.year_month=b.yearmonth;
quit;

proc sort data=work._t_6;by liscd date;run;quit;

data c.bond_china_match_2;
set work._t_6;
bond_excess_return=ret*100-Nrrmtdt;
run;

proc sort data=c.bond_china_match_2;by &cusip &date;run;
/******************************************************************/




%mend;



/******************************************/
/*计算term因子需要长期，短期国债数据
  def因子则需要长期国债数据和优秀/长期债券的组合横截面加权收益-长期国债收益数据

/*日期参数修正
data c.treasure;
set c.treasure;
date=input(put(Trddt,12.),yymmdd10.);
format date yymmdd10.;
run; 

**国债月度数据估计（用国债每月最后一天的利率数据做为标准）

*/


%macro match_def_term();

/*match term*/
proc sql noprint;
create table c.bond_match_china3 as
select a.*,b.term
from c.bond_match_china3 as a left join c.term as b on a.yearmonth=b.yearmonth;
quit;

/*match def*/
proc sort data=c.bond_match_china3;by liscd;run;

data c.bond_match_china4(rename=(_date=date));
   set c.bond_match_china3;
	by liscd;
   retain  _date;
   format _date YYMMDD10.;
   if first.liscd then do;
          _date=date;
          output;
    end;
     else do;
               _date=intnx('month',_date,1);
                  if _date<date then do until (_date=date);
				  bond_excess_return='.';
               output;
              _date=intnx('month',_date,1);
                       end;
                 if _date=date then do;
								
                                output;
                             end;
     end;
	 drop date;
run;

data c.bond_match_china4;
set c.bond_match_china4;
by liscd;
retain t_period;
if first.liscd then t_period=1;
else do;
	t_period=t_period+1;
end;
run;

proc sql noprint;
create table c.bond_match_china4 as
select a.*,b.def
from c.bond_match_china4 as a left join c.def as b on a.t_period=b.T_period;
quit;



%mend;



%calculate(indata=c.Bond_month,cusip=liscd,date=date,closepr=Mclsprc);

/*
%term_def(c.treasure,null);

%match_def_term();
*/



proc sort data=work._t_4;by date;run;
