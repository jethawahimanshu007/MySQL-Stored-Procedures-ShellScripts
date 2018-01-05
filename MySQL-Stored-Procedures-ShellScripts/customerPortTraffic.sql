

DROP PROCEDURE IF EXISTS custPorts;
DELIMITER |
CREATE PROCEDURE custPorts(startTime timestamp,endTime timestamp)
BEGIN
DROP TABLE IF EXISTS TMP_ROUTERTRAFFIC_VLANPRT_TBL;
DROP TABLE IF EXISTS TMP_INTRAFFICPEAK_TBL;
DROP TABLE IF EXISTS TMP_OUTTRAFFICPEAK_TBL;
DROP TABLE IF EXISTS TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL;
DROP TABLE IF EXISTS PORT_IN_TBL;
DROP TABLE IF EXISTS PORT_OUT_TBL;
DROP TABLE IF EXISTS TMP_ROUTERTRAFFIC_VLANPRT_TBL;
DROP TABLE IF EXISTS TEMP_THRES_CALC;
DROP TABLE IF EXISTS TEMP_THRES_COUNT;
DROP TABLE IF EXISTS TEMP_CIRCUIT_ID;
DROP TABLE IF EXISTS TEMP_ALL_VALUES;
DROP TABLE IF EXISTS TEMP_PARAMS;
DROP TABLE IF EXISTS FinalOutput;
CREATE TABLE TMP_ROUTERTRAFFIC_VLANPRT_TBL (PortID int,InTraffic float, OutTraffic float);


insert into TMP_ROUTERTRAFFIC_VLANPRT_TBL select b.PortID, RcvOctets/1000,TxOctets/1000 from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a,TEMP_AIRTEL_CSV b where a.PortID=b.PortID and  Time_1>startTime and Time_1<endTime;

CREATE TABLE TMP_INTRAFFICPEAK_TBL(PortID int,InTrafficPeak float);
CREATE TABLE TMP_OUTTRAFFICPEAK_TBL(PortID int,OutTrafficPeak float);
SELECT COUNT(*) into @countPeak   FROM (SELECT distinct PortID from TMP_ROUTERTRAFFIC_VLANPRT_TBL)a ;

CREATE TABLE TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL LIKE TMP_ROUTERTRAFFIC_VLANPRT_TBL;
INSERT INTO TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL
SELECT * from TMP_ROUTERTRAFFIC_VLANPRT_TBL;
CREATE TABLE PORT_IN_TBL(iIndex int primary key auto_increment,PortID int,InTraffic float);
CREATE INDEX PortIdIndex1 ON TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL(PortId);
CREATE TABLE PORT_OUT_TBL(iIndex int primary key auto_increment,PortID int,OutTraffic float);
/*
WHILE(@countPeak>0)
DO

SELECT a.PortID into @PortID from (select distinct PortID from TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL order by PortId asc limit 1)a;

INSERT INTO PORT_IN_TBL(PortID,InTraffic)
SELECT PortID, Intraffic from TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL where PortID=@PortID order by InTraffic desc;
INSERT INTO PORT_OUT_TBL(PortID,OutTraffic)
SELECT PortID, OutTraffic from TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL where PortID=@PortID order by OutTraffic desc;
INSERT INTO TMP_INTRAFFICPEAK_TBL
SELECT PortId, ROUND(InTraffic,2) from PORT_IN_TBL  where iIndex=3;
INSERT INTO TMP_OUTTRAFFICPEAK_TBL
SELECT PortId, ROUND(OutTraffic,2) from PORT_OUT_TBL  where iIndex=3;

DELETE from TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL where PortID=@PortID; 
TRUNCATE PORT_IN_TBL;
TRUNCATE PORT_OUT_TBL;
SET @countPeak=@countPeak-1;
END WHILE;
*/
INSERT INTO TMP_INTRAFFICPEAK_TBL SELECT PortID, ROUND(max(InTraffic),2) from TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL group by PortID;
INSERT INTO TMP_OUTTRAFFICPEAK_TBL SELECT PortID, ROUND(max(OutTraffic),2) from TMP_ROUTERTRAFFIC_IN_VLANPRT_TBL group by PortID;

CREATE TABLE IF NOT EXISTS TEMP_THRES_CALC(PortID int, utilIn float, utilOut float);
CREATE TABLE IF NOT EXISTS TEMP_THRES_COUNT(PortId int, ExceedCount int);
INSERT INTO TEMP_THRES_CALC(PortID , utilIn , utilOut ) SELECT a.PortID, a.InTraffic/(b.IfSpeed)*100, a.OutTraffic/(b.IfSpeed)*100 from TMP_ROUTERTRAFFIC_VLANPRT_TBL a,TEMP_AIRTEL_CSV b where a.PortID=b.PortId and  (a.InTraffic/(b.IfSpeed)*100>70 or a.OutTraffic/(b.IfSpeed)*100>70);

insert into TEMP_THRES_COUNT(PortID, ExceedCount)  select a.PortID,  count(*) from TEMP_THRES_CALC a group by a.PortID;
insert into TEMP_THRES_COUNT (SELECT PrtId,0 FROM  VLANPRT_TBL t1 WHERE NOT EXISTS (SELECT 1 FROM TEMP_THRES_COUNT t2 WHERE t1.PrtID = t2.PortID));
CREATE TABLE TEMP_CIRCUIT_ID(PortID int,RouterName varchar(128),IfDescr varchar(128),CircuitID INTEGER DEFAULT NULL,CustomerName varchar(128),Location VARCHAR(128),IfSpeed bigint(20));

INSERT IGNORE INTO TEMP_CIRCUIT_ID  select PortID,RouterName,IfDescr,SUBSTRING_INDEX(REPLACE(CircuitID,':','-'),'-',-1),CustomerName,Location,IfSpeed from TEMP_AIRTEL_CSV;
CREATE TABLE TEMP_ALL_VALUES(PortID bigint(20),ExceedCount int,NodeName varchar(256),IfDescr varchar(256),circuitID varchar(256),custName varchar(256),Location varchar(256),IfSpeed bigint(20));
CREATE INDEX PortIDIndex1 on TEMP_CIRCUIT_ID(PortID);
CREATE INDEX PortIDIndex2 on TEMP_THRES_COUNT(PortID);
INSERT INTO TEMP_ALL_VALUES 
SELECT a.PortID,b.ExceedCount,RouterName,IfDescr,CircuitID,CustomerName,Location,IfSpeed from TEMP_CIRCUIT_ID a, TEMP_THRES_COUNT b where a.PortID=b.PortID;


CREATE INDEX PORTIDIndexInTraf ON TMP_INTRAFFICPEAK_TBL(PortId);
CREATE INDEX PORTIDIndexOutTraf ON TMP_OUTTRAFFICPEAK_TBL(PortId);
CREATE TABLE TEMP_PARAMS(PortID bigint(10),maxTrafficValueOut float,maxTrafficValueIn float, avgTrafficValueIn float,avgTrafficValueOut float);

INSERT INTO TEMP_PARAMS
SELECT a.PortID, c.OutTrafficPeak, b.InTrafficPeak, ROUND(avg(a.InTraffic),2),ROUND(avg(OutTraffic),2) from TMP_ROUTERTRAFFIC_VLANPRT_TBL a,TMP_INTRAFFICPEAK_TBL b, TMP_OUTTRAFFICPEAK_TBL c where a.PortID=b.PortID  and b.PortID=c.PortID group by a.PortID;
CREATE INDEX PORTIDCir on TEMP_CIRCUIT_ID(PortID);
CREATE INDEX PORTIDParams on TEMP_PARAMS(PortID);

CREATE TABLE FinalOutput(circuitID varchar(256),NodeName varchar(256),IfDescr varchar(256),Location varchar(256),maxInUtil varchar(256),maxOutUtil varchar(256),avgInUtil varchar(256),avgOutUtil varchar(256),bw varchar(256),custName varchar(256),Threshold varchar(256),NumberOfTimes varchar(256));

INSERT INTO FinalOutput values( 'LSI (circuit ID)', 'Device Name','Interface Description','Loc', 'Max IN Util(KBps)', 'Max Out Util(KBps)', 'Avg IN Util(KBps)', 'Avg Out Util(KBps)','BW (KBps)', 'Cust Name', 'Threshold value set By Customer','Number Of times threshold crossed');
INSERT INTO FinalOutput
SELECT a.circuitID,NodeName,IfDescr,Location,ROUND(maxTrafficValueIn,2),ROUND(maxTrafficValueOut,2),ROUND(avgTrafficValueIn,2),ROUND(avgTrafficValueOut,2),IfSpeed, custName,'70.00%' as Threshold,ExceedCount from TEMP_ALL_VALUES a,TEMP_PARAMS b where a.PortID=b.PortID;
SELECT * from FinalOutput into outfile '/tmp/customer.csv' fields terminated by ',';

END |









