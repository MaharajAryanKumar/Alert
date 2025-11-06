* PROGRAM   : CB6_Alerts_Cards.sas                                *
* PROJECT NAME : CB6 Alerts Cards Monitoring                          *
* CREATED BY   : Gene Wang                                        *
* PURPOSE      : Build Cards Alert Monitoring Report for Accuracy, timeliness and *
* completeness                                     *
*****************************************************************
* *
* CHANGE LOG                          *
* *
* Date         BY          DESCRIPTIONS                           *
* --------------------------------------------------------------- *
* 2022-07-12 Gene Wang     Created                                *
* 2022-07-22 Gene Wang     Enhanced base population script for preference change *
* 2022-09-21 Gene Wang     Updated Connection string to Hive      *
* 2023-02-13 Gene Wang     Updated Hive connection                *
* 2025-03-04 Jenny Wu      Optimized efficiency of Hive data extraction *
* *
;

%global env regpath logpath outpath sasfile logfile lstfile;
%let env = PROD; /*Change to PROD when running production manually*/
%let env = %sysfunc(ifc(%length(&env)="", %sysfunc(ifc(%upcase(%substr(%sysget(USER),1,1)) = U and &SYSHOSTNAME ^= wasastst1, PROD, DEV)), &env));
%let Regpath = %sysfunc(ifc(&sysparm ^= "", &sysparm, %sysfunc(ifc(&env=PROD, /sas/RSD/REG, /sas/RSD/REG_DEV))));
%include "&Regpath/script/common/security/pswd.sas";
%include "&Regpath/sas/pswd.sas";/*Remove this for production*/
options sasautos = (sasautos, "&Regpath/script/common/macros/volatile", "&Regpath/script/common/macros/general");
options pagesize=max noquotelenmax;
%let syscc = 0; /*System macro variable*/
%let logpath = &Regpath/CB6/log/alert/cards;
%let outpath = &Regpath/CB6/output/alert/cards;
%let ymd = %sysfunc(today(), yymmddn8.);
%let logfile = &logpath/CB600J_Alerts_Cards_&ymd..log;
%let lstfile = &logpath/CB600J_Alerts_Cards_&ymd..lst;

%put &=regpath;
%put &=logfile;
%put &=lstfile;

%CreateDirectory(&logpath);
proc printto log="&logfile" print="&lstfile" new;
run;

%GLOBAL _SASPROGRAMFILE;
%let sasfile = %sysfunc(ifc("&_SASPROGRAMFILE"="", %scan(&SYSPROCESSNAME, -1, %str( )), %scan(&_SASPROGRAMFILE, -1, %str(/%))));
%put >>>>>>>>>> Name of This Program - &sasfile <<<<<<<<<<;
%put >>>>>>>>>> Starting Running Time - %left(%sysfunc(datetime(), datetime20.)) <<<<<<<<<<;
%put >>>>>>>>>> User ID - %sysget(USER) <<<<<<<<<<;
%put >>>>>>>>>> Platform - &SYSHOSTNAME <<<<<<<<<<;
%put >>>>>>>>>> Prod/Dev - &env <<<<<<<<<<;

%let report_dt = %sysfunc(today());
%let start_dt_ini = '30JUN2022'd;
%let week_end_dt_ini = '25JUL2022'd;
%let end_dt = %eval(%sysfunc(intnx(week.4, &report_dt, 0))-2);
%let start_dt = %sysfunc(ifc(&end_dt<&week_end_dt_ini, &start_dt_ini, %eval(&end_dt-6)));
%let week_start_dt = %str(%'')%sysfunc(sum(&start_dt,0), yymmdd10.)%str(%');
%let week_end_dt = %str(%'')%sysfunc(sum(&end_dt,0), yymmdd10.)%str(%');
%let pardt = %sysfunc(ifc(%sysparm ^= "", &sysparm, %sysfunc(sum(&start_dt,-7), yymmdd10.)%str(')));
%let week_end_dt_p1 = %str(%'')%sysfunc(sum(&end_dt,1), yymmdd10.)%str(%');
%let label = %sysfunc(sum(&report_dt,0), yymmddn8.);
%put &=week_start_dt;
%put &=week_end_dt;
%put &=week_end_dt_p1;
%put &=pardt;
%put report_dt=%sysfunc(abs(&report_dt), yymmdd10.);

%CreateDirectory(&outpath/&label.);
libname dataout "&outpath/&label.";
libname ac "&outpath/&label.";
/*libname user "&outpath/&label.";*/

X 'cd; kinit -f PRYUBSRVWIN@MAPLE.FG.RBC.COM -t PRYUBSRVWIN_Prod.kt';

%macro connect_to_hive;
connect to hadoop as hive (
URI="jdbc:hive2://strlplpaed12007.fg.rbc.com:2181,strlplpaed12008.fg.rbc.com:2181,strlplpaed12009.fg.rbc.com:2181,strlplpaed12010.fg.rbc.com:2181, strlplpaed12013.fg.rbc.com:2181/;serviceDiscoveryMode=zooKeeper;ssl=true;zooKeeperNamespace=hiveserver2"
schema=prod_brt0_ess
DBMAX_TEXT=256
LOGIN_TIMEOUT=600);
execute(set tez.queue.name=PRYUB) by hive;
execute(set hive.execution.engine=tez) by hive;
execute(set hive.compute.query.using.stats=true) by hive;
%mend connect_to_hive;

proc sql;
%connect_to_hive;
create table xb80_cards as
select * from connection to hive
(
  select
    event_activity_type,
    source_event_id,
    partition_date,
    cast(regexp_replace(eventAttributes['ess_process_timestamp'],"T|Z"," ") as timestamp) as ess_process_timestamp,
    cast(regexp_replace(eventAttributes['ess_src_event_timestamp'],"T|Z"," ") as timestamp) as ess_src_event_timestamp,
    get_json_object(eventAttributes['SourceEventHeader'],'$.eventId') as eventId,
    cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'],'$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp,
    get_json_object(eventAttributes['eventPayload'],'$.accountId') as accountId,
    get_json_object(eventAttributes['eventPayload'],'$.alertType') as alertType,
    cast(get_json_object(eventAttributes['eventPayload'],'$.thresholdAmount') as decimal(10,2)) as thresholdAmount,
    get_json_object(eventAttributes['eventPayload'],'$.customerID') as customerId,
    get_json_object(eventAttributes['eventPayload'],'$.accountCurrency') as accountCurrency,
    cast(get_json_object(eventAttributes['eventPayload'],'$.creditLimit') as decimal(10,2)) as creditLimit,
    get_json_object(eventAttributes['eventPayload'],'$.maskedAccount') as maskedAccount,
    get_json_object(eventAttributes['eventPayload'],'$.decisionId') as decisionId,
    cast(get_json_object(eventAttributes['eventPayload'],'$.alertAmount') as decimal(10,2)) as alertAmount
    from prod_brt0_ess.xb80_credit_card_system_interface
    where
      partition_date > &pardt.
      and event_activity_type = 'Alert Decision Cards'
      and get_json_object(eventAttributes['eventPayload'],'$.alertType') = 'AVAIL_CREDIT_REMAINING'
      and event_timestamp between &week_start_dt and &week_end_dt_p1
);
disconnect from hive;
quit;

proc sql;
%connect_to_hive;
create table cards_dec_pref as
select * from connection to hive
(
  (select event_timestamp as event_timestamp_p,
    event_channel_type as event_channel_type_p,
    event_activity_type as event_activity_type_p,
    partition_date as partition_date_p,
    cast(regexp_replace(eventAttributes['ess_process_timestamp'],"T|Z"," ") as timestamp) as ess_process_timestamp_p,
    cast(regexp_replace(eventAttributes['ess_src_event_timestamp'],"T|Z"," ") as timestamp) as ess_src_event_timestamp_p,
    cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'],'$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp_p,
    get_json_object(eventAttributes['SourceEventHeader'],'$.eventActivityName') as eventActivityName_p,
    get_json_object(eventAttributes['eventPayload'],'$.preferenceType') as preferenceType_p,
    get_json_object(eventAttributes['eventPayload'],'$.clientID') as clientID_p,
    get_json_object(eventAttributes['eventPayload'],'$.isBusiness') as isBusiness_p,
    get_json_object(eventAttributes['eventPayload'],'$.sendAlertEligible') as sendAlertEligible_p,
    get_json_object(eventAttributes['eventPayload'],'$.active') as active_p,
    get_json_object(eventAttributes['eventPayload'],'$.threshold') as threshold_p,
    get_json_object(eventAttributes['eventPayload'],'$.custID') as custID_p,
    get_json_object(eventAttributes['eventPayload'],'$.account') as account_p,
    get_json_object(eventAttributes['eventPayload'],'$.maskAccountNo') as maskedAccountNo_p,
    get_json_object(eventAttributes['eventPayload'],'$.externalAccount') as externalAccount_p,
    get_json_object(eventAttributes['eventPayload'],'$.productType') as productType_p
    from prod_brt0_ess.ffs0_client_alert_preferences_dep_initial_load
    where event_activity_type in ('Create Account Preference', 'Update Account Preference')
    and get_json_object(eventAttributes['eventPayload'],'$.preferenceType') = 'AVAIL_CREDIT_REMAINING'
    and get_json_object(eventAttributes['eventPayload'],'$.productType') = 'CREDIT_CARD'
    and partition_date = '20220324'
  ) as a

  union

  select b.* from
  (select event_timestamp as event_timestamp_p,
    event_channel_type as event_channel_type_p,
    event_activity_type as event_activity_type_p,
    partition_date as partition_date_p,
    cast(regexp_replace(eventAttributes['ess_process_timestamp'],"T|Z"," ") as timestamp) as ess_process_timestamp_p,
    cast(regexp_replace(eventAttributes['ess_src_event_timestamp'],"T|Z"," ") as timestamp) as ess_src_event_timestamp_p,
    cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'],'$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp_p,
    get_json_object(eventAttributes['SourceEventHeader'],'$.eventActivityName') as eventActivityName_p,
    get_json_object(eventAttributes['eventPayload'],'$.preferenceType') as preferenceType_p,
    get_json_object(eventAttributes['eventPayload'],'$.clientID') as clientID_p,
    get_json_object(eventAttributes['eventPayload'],'$.isBusiness') as isBusiness_p,
    get_json_object(eventAttributes['eventPayload'],'$.sendAlertEligible') as sendAlertEligible_p,
    get_json_object(eventAttributes['eventPayload'],'$.active') as active_p,
    get_json_object(eventAttributes['eventPayload'],'$.threshold') as threshold_p,
    get_json_object(eventAttributes['eventPayload'],'$.custID') as custID_p,
    get_json_object(eventAttributes['eventPayload'],'$.account') as account_p,
    get_json_object(eventAttributes['eventPayload'],'$.maskAccountNo') as maskedAccountNo_p,
    get_json_object(eventAttributes['eventPayload'],'$.externalAccount') as externalAccount_p,
    get_json_object(eventAttributes['eventPayload'],'$.productType') as productType_p
    from prod_brt0_ess.ffs0_client_alert_preferences_dep
    where event_timestamp > &week_end_dt_p1
    and event_activity_type in ('Create Account Preference', 'Update Account Preference')
    and get_json_object(eventAttributes['eventPayload'],'$.preferenceType') = 'AVAIL_CREDIT_REMAINING'
    and get_json_object(eventAttributes['eventPayload'],'$.productType') = 'CREDIT_CARD'
  ) as b
) as p
;
disconnect from hive;
quit;

proc sql;
create table dataout.xb80_cards_dec_pref as
select * from
xb80_cards d
left join
cards_dec_pref p
on d.accountID = p.externalAccount_p
and d.customerID = p.custID_p
;
run;

proc sql;
%connect_to_hive;
create table dataout.fft0_inbox as
select * from connection to hive
(
  select
    cast(regexp_replace(eventAttributes['ess_process_timestamp'],"T|Z"," ") as timestamp) as ess_process_timestamp_a,
    event_activity_type as event_activity_type_a,
    source_event_id as source_event_id_a,
    partition_date as partition_date_a,
    event_timestamp as event_timestamp_a,
    get_json_object(eventAttributes['SourceEventHeader'],'$.eventId') as eventId_a,
    cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'],'$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp_a,
    get_json_object(eventAttributes['SourceEventHeader'],'$.eventActivityName') as eventActivityName_a,
    get_json_object(eventAttributes['eventPayload'],'$.alertSent') as alertSent_a,
    get_json_object(eventAttributes['eventPayload'],'$.sendInbox') as sendInbox_a,
    get_json_object(eventAttributes['eventPayload'],'$.alertType') as alertType_a,
    cast(get_json_object(eventAttributes['eventPayload'],'$.thresholdAmount') as decimal(12,2)) as thresholdAmount_a,
    get_json_object(eventAttributes['eventPayload'],'$.sendSMS') as sendSMS_a,
    get_json_object(eventAttributes['eventPayload'],'$.sendPush') as sendPush_a,
    get_json_object(eventAttributes['eventPayload'],'$.maskedAccount') as maskedAccount_a,
    get_json_object(eventAttributes['eventPayload'],'$.reasonCode') as reasonCode_a,
    get_json_object(eventAttributes['eventPayload'],'$.decisionId') as decisionId_a,
    cast(get_json_object(eventAttributes['eventPayload'],'$.alertAmount') as decimal(12,2)) as alertAmount_a,
    get_json_object(eventAttributes['eventPayload'],'$.accountID') as accountId_a,
    get_json_object(eventAttributes['eventPayload'],'$.account') as account_a,
    get_json_object(eventAttributes['eventPayload'],'$.accountProduct') as accountProduct_a,
    get_json_object(eventAttributes['eventPayload'],'$.sendEmail') as sendEmail_a
    from prod_brt0_ess.fft0_alert_inbox_dep
    where event_activity_type = 'Alert Delivery Audit'
    and get_json_object(eventAttributes['eventPayload'],'$.alertType') = 'AVAIL_CREDIT_REMAINING'
    and event_timestamp > &week_start_dt
);
disconnect from hive;
quit;

data xb80_cards_dec_pref;
set dataout.xb80_cards_dec_pref;
length dec_tm_ge_pref_tm $1;
dec_tm_ge_pref_tm = ifc(eventTimestamp > eventTimestamp_p, 'Y', 'N');
run;

proc sort data = xb880_cards_dec_pref;
by decisionId eventTimestamp accountID customerID descending dec_tm_ge_pref_tm descending eventTimestamp_p descending externalAccount_p;
run;

proc sort data = xb80_cards_dec_pref out=xb80_cards_dec_pref2 dupout=xb80_cards_dec_pref_dup nodupkey;
by decisionId;
run;

proc sort data = dataout.fft0_inbox;
by decisionId_a;
run;

proc sort data = dataout.fft0_inbox out=fft0_inbox2 dupout=fft0_inbox_dup nodupkey;
by decisionId_a;
run;

proc sql;
create table cards_dec_pref_inbox as
select a.*,
       b.*
from xb80_cards_dec_pref2 as a
left join fft0_inbox2 as b
on a.decisionId=b.decisionId_a
;
quit;

proc sort data = cards_dec_pref_inbox nodupkey;
by decisionId;
run;

proc freq data=cards_dec_pref_inbox;
tables dec_tm_ge_pref_tm isbusiness_p dec_tm_ge_pref_tm*isbusiness_p/list missing;
run;

data dataout.cards_alert_final;
set cards_dec_pref_inbox;
if isbusiness_p='true' and dec_tm_ge_pref_tm = 'Y' then delete;
format src_event_date process_date event_date decision_date date9. Alert_Time $100. threshold_limit_check $1.;
src_event_date = datepart(ess_src_event_timestamp);
process_date = datepart(ess_process_timestamp);
event_date = datepart(input(compress(eventTimestamp,'a'), VMDTTM26.));
decision_date = datepart(eventTimestamp);
threshold_limit_check = ifc(alertAmount<thresholdAmount, 'Y', 'N');
if eventTimestamp_a = . or eventTimestamp = . then Found_Missing = 'Y';
Time_Diff = eventTimestamp_a - eventTimestamp;
if Time_Diff <= 1800 and Time_Diff ^= . then SLA_Ind = 'Y';
else SLA_Ind = 'N';
if Time_Diff = .        then Alert_Time = '99 - Timestamp is missing';
else if Time_Diff < 0      then Alert_Time = '00 - Less than 0 seconds';
else if Time_Diff <= 1800  then Alert_Time = '01 - Less than or equal to 30 minutes';
else if Time_Diff <= 3600  then Alert_Time = '02 - Greater than 30 mins and less than or equal to 60 mins';
else if Time_Diff <= 3600*2 then Alert_Time = '03 - Greater than 1 hour and less than or equal to 2 hours';
else if Time_Diff <= 3600*3 then Alert_Time = '04 - Greater than 2 hours and less than or equal to 3 hours';
else if Time_Diff <= 3600*4 then Alert_Time = '05 - Greater than 3 hours and less than or equal to 4 hours';
else if Time_Diff <= 3600*5 then Alert_Time = '06 - Greater than 4 hours and less than or equal to 5 hours';
else if Time_Diff <= 3600*6 then Alert_Time = '07 - Greater than 5 hours and less than or equal to 6 hours';
else if Time_Diff <= 3600*7 then Alert_Time = '08 - Greater than 6 hours and less than or equal to 7 hours';
else if Time_Diff <= 3600*8 then Alert_Time = '09 - Greater than 7 hours and less than or equal to 8 hours';
else if Time_Diff <= 3600*9 then Alert_Time = '10 - Greater than 8 hours and less than or equal to 9 hours';
else if Time_Diff <= 3600*10 then Alert_Time = '11 - Greater than 9 hours and less than or equal to 10 hours';
else if Time_Diff <= 3600*24 then Alert_Time = '12 - Greater than 10 hours and less than or equal to 24 hours';
else if Time_Diff <= 3600*48 then Alert_Time = '13 - Greater than 1 day and less than or equal to 2 days';
else if Time_Diff <= 3600*72 then Alert_Time = '14 - Greater than 2 days and less than or equal to 3 days';
else Alert_Time = '15 - Greater than 3 days';
run;

proc sql;
create table ac.Cards_Alert_Time_Count as
select Alert_Time,
       count(*) as Decision_Count
from dataout.cards_alert_final
group by 1
order by 1;
quit;

proc format;
value $isblank
' ' = 'Blank'
other = 'Not Blank';
run;

proc freq data=dataout.cards_alert_final;
format decisionId_a $isblank.;
tables partition_date src_event_date process_date event_date decision_date threshold_limit_check alertType
       partition_date*src_event_date*process_date*event_date*decision_date*SLA_Ind*Alert_Time/ list missing;
tables decisionId_a Found_Missing decision_date alertType event_activity_type isbusiness_p threshold_limit_check Alert_Time/ list missing;
run;

proc sql;
select count(*) as Total_Records,
       count(distinct decisionId) as Total_Distinct_DecisionId
from dataout.cards_alert_final;
quit;

proc sort data = dataout.cards_alert_final;
by decision_date decisionId;
run;

proc surveysselect data = dataout.cards_alert_final
    out = dataout.alert_card_base_samples(drop=SelectionProb SamplingWeight)
    method=srs
    sampsize=10;
    strata decision_date;
run;

proc sql;
create table Accuracy as
select 'Accuracy'                as ControlRisk format=$50.,
       'Sample'                  as TestType format=$50.,
       'Alert010_Accuracy_Available_Credit' as RDE format=$50.,
       case when threshold_limit_check = 'Y' then 'COM16'
            else 'COM19' end     as CommentCode,
       put(decision_date, yymmddn.) as Segment10,
       &report_dt                as DateCompleted format=yymmdd10.,
       intnx('week.3',decision_date,0,'e') as SnapDate format=yymmdd10.,
       count(*)                  as Volume,
       sum(alertAmount)          as bal,
       sum(thresholdAmount)      as Amount
from dataout.alert_card_base_samples
group by 1,2,3,4,5,6,7
order by 5,6,7,3,4;
quit;

proc sql;
create table Timeliness as
select 'Timeliness'              as ControlRisk format=$50.,
       'Anomaly'                 as TestType format=$50.,
       'Alert011_Timeliness_SLA' as RDE format=$50.,
       case when SLA_Ind = 'Y' then 'COM16'
            else 'COM19' end     as CommentCode,
       put(decision_date, yymmddn.) as Segment10,
       &report_dt                as DateCompleted format=yymmdd10.,
       intnx('week.3',decision_date,0,'e') as SnapDate format=yymmdd10.,
       count(*)                  as Volume,
       sum(alertAmount)          as bal,
       sum(thresholdAmount)      as Amount
from dataout.cards_alert_final
group by 1,2,3,4,5,6,7
order by 5,6,7,3,4;
quit;

proc sql;
create table Completeness_Recon as
select 'Completeness'            as ControlRisk format=$50.,
       'Reconciliation'          as TestType format=$50.,
       'Alert012_Completeness_All_Clients' as RDE format=$50.,
       case when decisionId_a ^= '' then 'COM16'
            else 'COM19' end     as CommentCode,
       put(decision_date, yymm6.)  as Segment10,
       &report_dt                as DateCompleted format=yymmdd10.,
       intnx('week.3',decision_date,0,'e') as SnapDate format=yymmdd10.,
       count(*)                  as Volume,
       sum(alertAmount)          as bal,
       sum(thresholdAmount)      as Amount
from dataout.cards_alert_final
group by 1,2,3,4,5,6,7
order by 5,6,7,3,4;
quit;

%comment_format;

/*Autocomplete Table*/
proc sql;
create table dataout.Alert_Cards_AC_week as
select 'CB6'                   as RegulatoryName,
       'Credit Cards'            as LOB,
       'CB6 Alerts'              as ReportName,
                                 ControlRisk,
       TestType,
       'Portfolio'               as TestPeriod,
       'Credit Cards'            as ProductType format=$30.,
       RDE,
       '.'                       as SubDE,
       '.'                       as Segment,
       '.'                       as Segment2,
       '.'                       as Segment3,
       '.'                       as Segment4,
       '.'                       as Segment5,
       '.'                       as Segment6,
       '.'                       as Segment7,
       '.'                       as Segment8,
       '.'                       as Segment9,
       Segment10,
       'N'                       as HoldoutFlag,
       CommentCode,
       put(CommentCode,$cmt.)    as Comments,
       Volume,
       Bal,
       Amount,
       DateCompleted,
       SnapDate
from (select * from Accuracy
      union
      select * from Timeliness
      union
      select * from Completeness_Recon
     ) as a;
quit;

/*Potential Fail Detail*/
proc sql;
create table dataout.Completeness_Fail as
select put(decision_date, yymm6.) as event_month,
       &report_dt                as reporting_date format=yymmdd10.,
       intnx('week.3',decision_date,0,'e') as event_week_ending format=yymmdd10.,
       'Credit Cards'            as LOB,
       'Credit Cards'            as Product,
       accountid                 as account_number,
       thresholdamount,
       alertAmount               as available_credit,
       decisionid,
/* 'data not available from source' as account_status,*/
       decision_date             as event_date,
       '******'||substr(customerid,7,3) as custid_mask
from dataout.cards_alert_final
where decisionId_a = '';
quit;

proc sql;
create table dataout.Timeliness_Fail as
select put(decision_date, yymm6.) as event_month,
       &report_dt                as reporting_date format=yymmdd10.,
       intnx('week.3',decision_date,0,'e') as event_week_ending format=yymmdd10.,
       'Credit Cards'            as LOB,
       'Credit Cards'            as Product,
       accountid                 as account_number,
       thresholdamount,
       alertAmount               as available_credit,
       decisionid,
/* 'data not available from source' as account_status,*/
       decision_date             as event_date,
       eventTimestamp,
       eventTimestamp_a          as sent_timestamp,
       ceil(Time_Diff/60)        as total_minutes format=12.0,
       '******'||substr(customerid,7,3) as custid_mask
from dataout.cards_alert_final
where SLA_Ind ^= 'Y';
quit;

/*None, so temporary to make a fake one*/
proc sql;
create table dataout.Accuracy_Fail as
select put(decision_date, yymm6.) as event_month,
       &report_dt                as reporting_date format=yymmdd10.,
       intnx('week.3',decision_date,0,'e') as event_week_ending format=yymmdd10.,
       'Credit Cards'            as LOB,
       'Credit Cards'            as Product,
       accountid                 as account_number,
       'AlertDecision'           as decision,
       thresholdamount,
       alertAmount               as available_credit,
       decisionid,
/* 'data not available from source' as account_status,*/
       decision_date             as event_date,
       '******'||substr(customerid,7,3) as custid_mask
from dataout.alert_card_base_samples
where threshold_limit_check ^= 'Y';
quit;

proc export data=dataout.Completeness_Fail
    outfile="&outpath/&label./Alert_Cards_Completeness_Detail.xlsx"
    dbms=xlsx replace;
    sheet="Alert_Cards_Completeness_Detail";
run;

proc export data=dataout.Timeliness_Fail
    outfile="&outpath/&label./Alert_Cards_Timeliness_Detail.xlsx"
    dbms=xlsx replace;
    sheet="Alert_Cards_Timeliness_Detail";
run;

proc export data=dataout.Accuracy_Fail
    outfile="&outpath/&label./Alert_Cards_Accuracy_Detail.xlsx"
    dbms=xlsx replace;
    sheet="Alert_Cards_Accuracy_Detail";
run;

proc append base=ac.alert_cards_ac data=dataout.Alert_Cards_AC_week(obs=0) force nowarn; /*In case base file doesn't exist*/
run;

proc sql;
create table alert_cards_ac as
select * from dataout.Alert_Cards_AC_week
union
(select * from ac.alert_cards_ac where SnapDate not in (select distinct SnapDate from dataout.Alert_Cards_AC_week));
quit;

proc sort data = alert_cards_ac out = ac.alert_cards_ac;
by SnapDate;
run;

x "find &Regpath. -user $USER -mmin +720 -exec chmod 777 {} \;";

proc printto; run;
%ScanLog(&logfile);
