
/******************************************************
indata=输入数据集
outdata=输出数据集
depvar=依赖变量
indvars=回归的独立变量，例如smb hml等
stdm=*理解为月度差，即和起始日期相比的月度差值*
eddm=*理解为最大的月度差值，这里代表我们最多的周数*
bydate=债券中和起始日期相比的差值
reqnum=要求的观测
reqdate=要求的日期差

最终输出的数据集中 DM代表了第几周的回归数据集合


存在问题：rolling循环的到enddm仍然未停止
***************************************************/


/****************************************************
未完成调试的问题：
1.关于空return的处理，保留或者删除
2.return是去除无风险的收益还是保留无风险收益


********************************************************/
%macro FM_tm(indata=,outdata=,depvar=,indvars=, stdm=, eddm=,cusip=liscd,bydate=delta_month,reqnum=12,reqdate=24);

	proc sort data=&indata;by &cusip date;run;

	data work.data;
	set &indata;
	if bond_excess_return='.' then delete;
	run;
    /*首先对输入的数据做处理*/
	data work.data(keep=&cusip &indvars &depvar &bydate date);
	set work.data;
	by liscd;
	format strd yymmdd10.;
	retain strd;
		if first.liscd then do;
			strd=date;
		end;
		else do;
			strd=strd;
		end;
	delta_month=intck('month',strd,date);
	run;

	proc printto log=_null_; run;	
	proc datasets lib=work nolist; delete &outdata; quit;
	%do dm = &stdm %to &eddm %by 1;
		data work._tm_1; set work.data (keep=&cusip &bydate &depvar &indvars); 
			if &bydate <= &dm and &bydate ge &dm-&reqdate;
			dm=&dm; 
		run;
		proc sort data=work._tm_1; by &cusip dm; run;
		data work._tm_1;set work._tm_1;minus=dm-delta_month;run;
		proc sql;
			create table work._tm_2 as
				select *
					from work._tm_1 group by &cusip having n(&depvar) ge &reqnum and min(minus)=0;
		quit;

		proc append base=&outdata data=work._tm_2 force; run;

	%end;
	proc datasets lib=work nolist; delete _tm:; quit;
	proc printto log=log; run;
	proc sort data=&outdata; by dm &cusip; run;
	data &outdata(drop=minus);set &outdata;run;

%mend FM_tm;

%macro FM_cross(rawdata=,tmdata=,fmbeta=,depvar=,indvars=,kvar=none,lag=3,cusip=liscd,bytm=dm,bydate=delta_month,bc='yes',bc2='no');
	
	
	
	%let xn=1; %let stdvars=; %let bvars=;
	%do %until (%scan(&indvars,&xn)= );
		%let token = %scan(&indvars,&xn);
		%let stdvars= &stdvars std&token;
		%let bvars= &bvars b&token;
		%let xn=%eval(&xn+1);
	%end;

	%let xn=%eval(&xn-1);

	
	proc sort data=&tmdata; by &bytm &cusip; run;

	proc reg data=&tmdata outest=work._fm_1 noprint;
		by &bytm &cusip;
		model &depvar = &indvars/adjrsq;
	run; quit;

	data work._rawbeta (keep=&cusip &bytm &indvars); 
		set work._fm_1; if _EDF_=0 or _ADJRSQ_ = 1 or _RSQ_=1 then delete; 
	run;

	data &rawdata;set &rawdata;delta_month=delta_month-1;run;
	
	%if &bc2='yes' %then %do;
		proc sql;	
			create table work._fm_2 as
				select A.*, B.&depvar, B.rating, B.maturity_n, B.rsj, B.rsk, B.rkt, B.rovl
					from work._rawbeta as A
						left join
						&rawdata as B
						on A.&cusip=B.&cusip and A.&bytm=B.&bydate;
		quit;

		%end;
	%else %do;
		proc sql;	
			create table work._fm_2 as
				select A.*, B.&depvar
					from work._rawbeta as A
						left join
						&rawdata as B
						on A.&cusip=B.&cusip and A.&bytm=B.&bydate;
		quit;
		%end;
	
	data work._fm_2; set work._fm_2; if missing(&depvar) then delete; run;

	proc sort data=work._fm_2; by &bytm &cusip; run;
	
	/**
	proc standard data=work._fm_2 mean=0 std=1 out=work._fm_3;
	by &bytm;
	var rating maturity_n rsj rsk rkt rovl;
	run;
	*/
	%winsor(dsetin=work._fm_2, dsetout=work._fm_beta, byvar=&bytm, vars=&indvars, type=winsor, pctl=1 99);

	proc means data=work._fm_beta noprint;
		by &bytm;
		var &indvars;
		output out=work._fm_4 std=&stdvars n=bondnum;/*n输出了t周有多少个债券 stdvars输出了在t周全部债券，每一个vars估计值beta的平均值*/
	run;
	
	data work._fm_5; 
		merge work._fm_beta work._fm_4;
		by &bytm;
		if bondnum < 36 then delete;
	run;
	data &fmbeta(drop=&stdvars xi);
		set work._fm_5;
		array stdv{&xn} &stdvars;
		array bv{&xn} &bvars;
		array indv{&xn} &indvars;
		do xi=1 to &xn by 1;
			bv{xi}=indv{xi}/stdv{xi};
		end;
	run;

	%put &xn; %put &bvars;

%mend FM_cross;

%macro winsor(dsetin=, dsetout=, byvar=, vars=, type=winsor, pctl=1 99);        
                                                                                    
%if &dsetout = %then %let dsetout = &dsetin;  
     
%let varL=;
%let varH=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
    %let varL = &varL &token.L;   
    %let varH = &varH &token.H;
    %let xn=%EVAL(&xn + 1);               
%end;                                                                  
  
%let xn=%eval(&xn-1);       
  
data work.xtemp;               
    set &dsetin;
    run;
  
%if &byvar = none %then %do;       
  
    data work.xtemp;
        set work.xtemp;
        xbyvar = 1;                
        run;
  
    %let byvar = xbyvar;
  
%end;
  
proc sort data = work.xtemp;      
    by &byvar;                   
    run;
  
proc univariate data = work.xtemp noprint;
    by &byvar;
    var &vars;                   
    output out = work.xtemp_pctl PCTLPTS = &pctl PCTLPRE = &vars PCTLNAME = L H;  
    run;
  
data &dsetout;
    merge work.xtemp work.xtemp_pctl;  
    by &byvar;                          
    array trimvars{&xn} &vars;  
    array trimvarl{&xn} &varL;   
    array trimvarh{&xn} &varH;  
  
    do xi = 1 to dim(trimvars);  
  
        %if &type = winsor %then %do;  
            if not missing(trimvars{xi}) then do;                                                                        
              if (trimvars{xi} < trimvarl{xi}) then trimvars{xi} = trimvarl{xi};  
              if (trimvars{xi} > trimvarh{xi}) then trimvars{xi} = trimvarh{xi};   
            end;                                                                  
        %end;
  
        %else %do;                  
            if not missing(trimvars{xi}) then do;
              if (trimvars{xi} < trimvarl{xi}) then delete;
              if (trimvars{xi} > trimvarh{xi}) then delete;
            end;
        %end;
  
    end;
    drop &varL &varH xbyvar xi;
    run;
  
%mend winsor;


%MACRO NWORDS (INVAR);
	%local N W;
	%let N = 0;
	%let W = 1;
	%do %while (%nrquote(%scan(&invar,&W,%str( ))) ^= %str());
	  %let N = %eval(&N+1);
	  %let W = %eval(&W+1);
	%end; 
	&N
%MEND NWORDS;

%MACRO FM_piece(INSET=,OUTSET=,DATEVAR=,DEPVAR=, INDVARS=,LAG=,ws=1 99,prec=7.2);
	/*save existing options*/
 	%local oldoptions errors;
 	%let oldoptions=%sysfunc(getoption(mprint)) %sysfunc(getoption(notes))
                 	%sysfunc(getoption(source));
	%let errors=%sysfunc(getoption(errors));
	options nonotes nomprint nosource errors=0;
 
    %put ### START;
    %put ### SORTING...PREPARE DATA FOR RUNNING FM REGRESSIONS;
	proc sql; drop table &outset; quit;
    proc sort data=&inset out=_temp;
       by &datevar;
    run;
    %put ### RUNNING CROSS-SECTIONAL FM REGRESSIONS;
    proc printto log=junk;run;
    proc reg data=_temp outest=_results edf adjrsq noprint;
		by &datevar;
        model &depvar=&indvars;
    run;
	%winsor(dsetin=_results, dsetout=_results, byvar=none, vars=&indvars, type=winsor, pctl=&ws);
    proc printto;run;
    /*create a dummy dataset for appending the results of FM regressions*/
    data &outset; set _null_;
    	format parameter $32. estimate best8. stderr d8. tvalue 7.2 probt pvalue6.4
        df best12. stderr_uncorr best12. tvalue_uncorr 7.2  probt_uncorr pvalue6.4 PARAM $10. T $8.;
        label stderr='Corrected standard error of FM coefficient';
        label tvalue='Corrected t-stat of FM coefficient';
        label probt='Corrected p-value of FM coefficient';
        label stderr_uncorr='Uncorrected standard error of FM coefficient';
        label tvalue_uncorr='Uncorrected t-stat of FM coefficient';
        label probt_uncorr='Uncorrected p-value of FM coefficient';
        label df='Degrees of Freedom';
    run;
    %put ### COMPUTING FAMA-MACBETH COEFFICIENTS...;
	%let indvar2=%str(Intercept &indvars _ADJRSQ_);
	ods select none;
    %do k=1 %to %nwords(&indvar2);
    	%let var=%scan(&indvar2,&k,%str(' '));
        /*1. Compute Fama-MacBeth coefficients as time-series means*/
/*        ods listing close;*/
        proc means data=_results n std t probt;
        	var &var;
          	ods output summary=_uncorr;
        run;
        /*2. Perform Newey-West adjustment using Bart kernel in PROC MODEL*/
        proc model data=_results plots=none;
        	instruments const;
        	&var=const;
        	fit &var/gmm kernel=(bart,%eval(&lag+1),0);
        	ods output parameterestimates=_param;
        quit;

/*        ods listing;*/
		
      	/*3. put the results together*/
        data _params (drop=&var._n);
        	merge _param
                  _uncorr (rename=(&var._stddev=stderr_uncorr
                                   &var._t=tvalue_uncorr
                                   &var._probt=probt_uncorr));
            stderr_uncorr=stderr_uncorr/&var._n**0.5;
          	parameter="&var";
            drop esttype;
        run;
        proc printto log=junk;run;
        proc append base=&outset data=_params force; run;
        proc printto;run;
	%end;
	ods select default;

	data _outset (keep=parameter PARAM T); set &outset;
		 tvalue2=put(tvalue,&prec); if probt<0.1 then p='*  ';
		 if probt<0.05 then p='** '; if probt<0.01 then p='***';
		PARAM=compress(put(estimate,&prec)||p);
		T=compress('('||tvalue2||')');
		if parameter='_ADJRSQ_' then do;
			PARAM=put(estimate,7.3); call missing(T); end;
		drop tvalue2 p;
	run;
	proc transpose data=_outset out=&outset;
		id parameter;
		var PARAM T;
	run;
    /*house cleaning */
/*    proc sql; drop table _temp, _params, _param, _uncorr;quit;*/
 	options &oldoptions errors=&errors;
    %put ### DONE ;
    %put ### OUTPUT IN THE DATASET &outset;

	dm 'odsresults; clear';
%MEND;

%macro appned_all(num= ,name= );

proc datasets lib=b nolist;delete fm_results_&name;quit;

%do x=&num %to 1 %by -1;
proc append base=b.fm_results_&name data=b.fm_results&x force;run;

proc datasets lib=b nolist;delete fm_results&x / memtype=data;quit;

%end;
%mend;


%macro FM_regression(indvar= ,name= );
/*这里需要调整的是输入数据，同时需要对参数进行修正*/
%FM_tm(indata=c.Bond_china_match_2,outdata=out,depvar=bond_excess_return,indvars=&indvar,stdm=25,eddm=220,cusip=liscd,bydate=delta_month,reqnum=12,reqdate=24);

%FM_cross(rawdata=work.data,tmdata=out,fmbeta=b.fambeth,depvar=bond_excess_return,indvars=&indvar,kvar=none,lag=3,cusip=liscd,bytm=dm,bydate=delta_month,bc='yes',bc2='no');

/*11 all regressiong*/
%FM_piece(INSET=b.fambeth,OUTSET=b.fm_results11,DATEVAR=dm,DEPVAR=bond_excess_return,INDVARS=&indvar,LAG=,ws=1 99,prec=7.2);
/*rsj rsk rkt rovl rating maturity*/



%mend;

%FM_regression(indvar=mkt_rf smb hml,name=ffc3);





