/*
ծȯ��ϴ��
1.�������Ϻ�/���ڽ���������ծȯ ��sctcd=1/2
2.����Ϊ��˾ծȯ��bondtype=02
3.���ֵĻ�������ΪRMB��currency=CNY
4.�������ڱ���������꣬term>=2
5.������أ����ɻ��ۣ�crdeem crtsell=N

ps�����ڸ�Ϣ��ʽ��Ӧ�������ڸ�Ϣ���ͣ������ں����ɸѡ����AIT��bond������ݲ�����


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
