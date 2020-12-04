




%macro FM_Rolling(indata= ,outdata= ,depvar= ,indvars= , stdm= , eddm= ,cusip= ,bydate= ,reqnum=20,reqdate=60);

	proc sort data=&indata;by &cusip date;run;

	data work._tm;
	set &indata;
	if bond_excess_return='.' then delete;
	if smb='.' then delete;
	run;
    /*首先对输入的数据做处理*/
	data work._tm(keep=&cusip &indvars &depvar &bydate date);
	set work._tm;
	by &cusip;
	format strd yymmdd10.;
	retain strd;
		if first.&cusip then do;
			strd=date;
		end;
		else do;
			strd=strd;
		end;
	delta_day=intck('day',strd,date);
	run;

	proc printto log=_null_; run;	
	
	proc datasets lib=work nolist; delete &outdata; quit;
	%do dm = &stdm %to &eddm %by 30;
		data work._tm_1; set work._tm (keep=&cusip &bydate &depvar &indvars); 
			if &bydate <= &dm and &bydate > &dm-&reqdate;
			dm=&dm; 
		run;
		proc sort data=work._tm_1; by &cusip dm; run;
		data work._tm_1;set work._tm_1;minus=dm-delta_day;run;
		proc sql;
			create table work._tm_2 as
				select *
					from work._tm_1 group by &cusip having n(&depvar) ge &reqnum and min(minus)>0;
		quit;

		proc append base=&outdata data=work._tm_2 force; run;

	%end;
	
	/*proc datasets lib=work nolist; delete _tm:; quit;*/
	proc printto log=log; run;
	proc sort data=&outdata; by dm &cusip; run;
	data &outdata(drop=minus);set &outdata;run;

%mend FM_Rolling;

%macro	Rsquared_test;

/*
1.为了研究异质波动率，我们希望上述五因子模型能够尽可能解释个股收益率在时序上的波动，
即时序回归的 R2 不能太小，否则研究的就不是异质波动率而是波动率了。下图显示了 n 取过去 1 至 6 个月内的交易日时，
五因子模型对个股回归的 R2 在截面上的均值随时间的变化。

我们需要对截面的债券数据做回归检测R_squared
*/

 


%mend;

%macro IVOL_estimate(indata= ,outdata= ,depvar= ,indvars= ,cusip= ,bytm= ,);

	proc sort data=&indata;by &cusip &bytm;run;

	proc reg data=&indata noprint;
		by &cusip &bytm ;
		model &depvar = &indvars/adjrsq;
		output out=work._fm_1 r=resdiual;
	run; quit;


	proc means data=work._fm_1 noprint;
		by &cusip &bytm;
		var resdiual;
		output out=&outdata std=IVOL;/*n输出了t周有多少个债券 stdvars输出了在t周全部债券，每一个vars估计值beta的平均值*/
	run;



%mend;


%FM_Rolling(indata=c.Bond_match_china3,outdata=Bond_match_china4,depvar=bond_excess_return,indvars=mkt_rf smb hml, stdm=61, eddm=5823,cusip=Liscd,bydate=delta_day,reqnum=20,reqdate=60);


%IVOL_estimate(indata=Bond_match_china4,outdata=c.IVOL,depvar=bond_excess_return,indvars=mkt_rf smb hml,cusip=Liscd,bytm=dm);
