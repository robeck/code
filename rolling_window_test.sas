
%macro rolling_window(indata= ,outdata= ,indvars= ,depvar= ,bydate=dm,reqnum=12,reqdate=20);

proc sort data=&indata; by cusip_id date;run;
proc datasets lib=work nolist; delete &outdata; quit;


data rolling_window1(keep=cusip_id &indvars &depvar dm date);
set &indata;
by cusip_id;
format strd yymmdd10.;
retain strd;
	if first.cusip_id then do;
		strd=date;
	end;
	else do;
		strd=strd;
	end;
dm=intck('week',strd,date);
run;


%do dw =20 %to 783 %by 1;
	data work._ts1;set rolling_window1;
		if &bydate<=&dw and &bydate>=&dw-&reqdate then do;
		dw=&dw;
		output;
		end;
	run;
	
	proc sort data=work._ts1;by cusip_id dw;run;
	data work._ts1;set work._ts1;minus=dw-dm;run;


	proc sql noprint;
	create table work._ts2 as
	select *
	from work._ts1
	group by cusip_id,dw having n(&depvar)>= &reqnum and min(minus)=0;
	quit;

	
	proc append base=&outdata data=work._ts2 force; run;
	

%end;

proc sort data=&outdata;by cusip_id dw dm;run;



%mend;



%macro split_datasets(indata= ,outdata= ,name= ,);

/*选取最大值*/
data _null_;
set &name nobs=nobs;
call symput('last',nobs);
run;

%do i=1 %to 1;

/*
proc sql noprint;
create table work._ts&i as
select a.*
from rolling1 as a
where cusip_id in (select cusip_id from &name where id=&i);
quit;
*/
proc sql noprint;
create table work.ts1 as
select * from rolling1
where cusip_id='00079FHN5';
quit;

data work.ts1;
set work.ts1;
by cusip_id;
if first.cusip_id then do;
	call symput('min',dw);
end;
if last.cusip_id then do;
	call symput("max",dw);
end;
run;

proc sort data=work.ts1;by cusip_id dw dm;run;


%end;




%mend;


%split_datasets(indata=test,outdata=test,name=A.name);


%rolling_window(indata=b.Trace_enhanced_10505_total,outdata=work.rolling1,indvars=rsj rsk rovl,depvar=ret);



