/*
ծȯ��ϴ��
1.�������Ϻ�/���ڽ���������ծȯ ��sctcd=1/2
2.����Ϊ��˾ծȯ��bondtype=02
3.���ֵĻ�������ΪRMB��currency=CNY
4.�������ڱ���������꣬term>=1
5.������أ����ɻ��ۣ�crdeem crtsell=N
6.Ipaytypcd=2 ���ڸ�Ϣ

ps�����ڸ�Ϣ��ʽ��Ӧ�������ڸ�Ϣ���ͣ������ں����ɸѡ����AIT��bond������ݲ�����

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
