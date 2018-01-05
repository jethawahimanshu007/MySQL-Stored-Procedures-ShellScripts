DELIMITER |
DROP PROCEDURE IF EXISTS updateCsv |
CREATE PROCEDURE updateCsv(startTime timestamp, endTime timestamp)
BEGIN

DROP TABLE IF EXISTS TEMP_MAX_UTIL;
DROP TABLE IF EXISTS TEMP_THRES_COUNT;
DROP TABLE IF EXISTS TEMP_CIRCUIT_ID;
DROP TABLE IF EXISTS TEMP_CSV;
DROP TABLE IF EXISTS TEMP_THRES_CALC;
DROP TABLE IF EXISTS TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TABLE IF EXISTS TEMP_VLANPRT_TBL;
DROP TABLE IF EXISTS TEMP_VLANPRT_WITH_NAMES;
DROP TABLE IF EXISTS TEMP_USAGE;
DROP TABLE IF EXISTS TEMP_NODEIF_TBL;

CREATE TABLE IF NOT EXISTS TEMP_THRES_COUNT(PortId int, ExceedCount int);
CREATE TABLE IF NOT EXISTS TEMP_THRES_CALC(PortID int, utilIn float, utilOut float);

CREATE TABLE TEMP_NODEIF_TBL(PortID int,ifIndex int, NodeNumber int,ifSpeed int);
INSERT INTO TEMP_NODEIF_TBL 
SELECT PrtID,b.ifIndex,NodeNumber,ifSpeed from NODEIF_TBL b,VLANPRT_TBL c where b.IfIndex=c.IfIndex and b.NodeNumber = c.NodeID;

INSERT INTO TEMP_THRES_CALC(PortID , utilIn , utilOut ) SELECT a.PortID, a.RcvOctets/(10*b.IfSpeed), a.TxOctets/(10*b.IfSpeed) from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a,TEMP_NODEIF_TBL b where a.PortID=b.PortId and  (a.RcvOctets/(10*b.IfSpeed)>70 or a.TxOctets/(10*b.IfSpeed)>70) and startTime<a.Time_1 and endTime>a.Time_1;


/*
INSERT INTO TEMP_THRES_CALC(PortID , utilIn , utilOut ) SELECT a.PortID, a.RcvOctets/(10*c.IfSpeed), a.TxOctets/(10*c.IfSpeed) from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a,VLANPRT_TBL b, NODEIF_TBL c where a.PortID=b.PrtId and b.IfIndex=c.IfIndex and b.NodeID = c.NodeNumber and (a.RcvOctets/(10*c.IfSpeed)>70 or a.TxOctets/(10*c.IfSpeed)>70) and startTime<a.Time_1 and endTime>a.Time_1;
*/
/*
INSERT INTO TEMP_THRES_CALC(PortID , utilIn , utilOut ) SELECT a.PortID, a.RcvOctets/(10*c.IfSpeed), a.TxOctets/(10*c.IfSpeed) from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a,VLANPRT_TBL b, NODEIF_TBL c where a.PortID=b.PrtId and b.IfIndex=c.IfIndex and b.NodeID = c.NodeNumber and (a.RcvOctets/(10*c.IfSpeed)>70 or a.TxOctets/(10*c.IfSpeed)>70) and startTime<a.Time_1 and endTime>a.Time_1;
*/
insert into TEMP_THRES_COUNT(PortID, ExceedCount)  select a.PortID,  count(*) from TEMP_THRES_CALC a group by a.PortID;
insert into TEMP_THRES_COUNT (SELECT PrtId,0 FROM  VLANPRT_TBL t1 WHERE NOT EXISTS (SELECT 1 FROM TEMP_THRES_COUNT t2 WHERE t1.PrtID = t2.PortID));


CREATE TABLE TEMP_CIRCUIT_ID(RouterName varchar(128),IfDescr varchar(128),CircuitID INTEGER DEFAULT NULL,CustomerName varchar(128),Location VARCHAR(128));
CREATE TABLE IF NOT EXISTS TEMP_CSV like TEMP_AIRTEL_CSV;
INSERT INTO TEMP_CSV SELECT * FROM TEMP_AIRTEL_CSV;
INSERT IGNORE INTO TEMP_CIRCUIT_ID  select RouterName,IfDescr,SUBSTRING_INDEX(REPLACE(CircuitID,':','-'),'-',-1),CustomerName,Location from TEMP_AIRTEL_CSV;

CREATE TABLE TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL(PortId INTEGER,InTrafficPeak FLOAT,InTrafficAvg FLOAT,OutTrafficPeak FLOAT, OutTrafficAvg FLOAT,Time_1 timestamp);

INSERT INTO TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL select PortId,Max(RcvOctets)/(1000) ,Avg(RcvOctets)/(1000),Max(TxOctets)/(1000), Avg(TxOctets)/(1000),Time_1 from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL T1 WHERE startTime<T1.Time_1 and endTime>T1.Time_1 GROUP BY T1.PortID;







CREATE TABLE TEMP_VLANPRT_TBL (
  PrtID bigint(20),
  NodeID smallint(6),
  IfIndex int(11),
  exceedCount int
);

CREATE INDEX PortId on TEMP_VLANPRT_TBL(PrtID);
CREATE INDEX PortId on TEMP_THRES_COUNT(PortID);

INSERT INTO TEMP_VLANPRT_TBL 
SELECT a.PrtID,a.NodeID,a.IfIndex,exceedCount FROM VLANPRT_TBL a,TEMP_THRES_COUNT b where a.PrtID=b.PortID;
DROP INDEX PortId on TEMP_VLANPRT_TBL;
DROP INDEX PortId on TEMP_THRES_COUNT;

CREATE TABLE TEMP_VLANPRT_WITH_NAMES (PrtID int,NodeName varchar(256),ifDescr varchar(256),ifSpeed bigint(20) unsigned,exceedCount int);
CREATE INDEX PortID on TEMP_VLANPRT_WITH_NAMES(PrtID);


SELECT date(startTime) into @daily;
INSERT INTO TEMP_VLANPRT_WITH_NAMES
SELECT PrtID, NodeName, ifDescr,ifSpeed,exceedCount from NODE_TBL a, NODEIF_TBL b, TEMP_VLANPRT_TBL c where a.NodeNumber=b.NodeNumber and c.ifIndex=b.ifIndex and c.NodeID=b.NodeNumber;


CREATE TABLE TEMP_USAGE(NodeName varchar(256),InTrafficPeak float,OutTrafficPeak float, InTrafficAvg float, OutTrafficAvg float,IfDescr varchar(256), IfSpeed bigint(10),ExceedCount int);
CREATE INDEX pid on TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL(PortId);

INSERT INTO TEMP_USAGE
SELECT T2.NodeName,
        T1.IntrafficPeak,T1.OutTrafficPeak,T1.InTrafficAvg,T1.OutTrafficAvg,T2.ifDescr,T2.IfSpeed,T2.ExceedCount
        FROM TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL T1, TEMP_VLANPRT_WITH_NAMES T2
        where T1.PortID=T2.PrtID ;

DROP INDEX pid on TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;

CREATE INDEX NodeName ON TEMP_USAGE(NodeName);
CREATE INDEX NodeName ON TEMP_CIRCUIT_ID(RouterName);

SELECT @daily as Daily,f.CircuitID,T1.NodeName,f.IfDescr,f.Location,
        T1.IntrafficPeak,T1.OutTrafficPeak,T1.InTrafficAvg,T1.OutTrafficAvg,T1.IfSpeed,f.CustomerName,'70.00 %' as Threshold,T1.ExceedCount
        FROM TEMP_USAGE  T1,TEMP_CIRCUIT_ID f 
        where f.RouterName=T1.NodeName and f.ifDescr=T1.ifDescr;
/*        into OUTFILE '/tmp/data/customer_links_data.csv' fields terminated by ','*/


DROP INDEX NodeName ON TEMP_USAGE;       


/*
SELECT @daily  as Daily,f.CircuitID,NodeName,f.IfDescr,f.Location,
	InTrafficPeak,OutTrafficPeak,InTrafficAvg,OutTrafficAvg,T3.IfSpeed,f.CustomerName,'70.00 %' as Threshold,e.ExceedCount  
	FROM TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL T1, VLANPRT_TBL T2, NODEIF_TBL T3, NODE_TBL T4,TEMP_CIRCUIT_ID f ,TEMP_THRES_COUNT e
	WHERE T1.PortID = T2.PrtID 
	AND T2.NodeID = T3.NodeNumber  
	AND T2.IfIndex = T3.IfIndex 
	AND T4.NodeNumber = T2.NodeID 
	AND e.PortID=T2.PrtID 
	AND f.RouterName=T4.NodeName 
	and f.IfDescr = T3.IfDescr  
	GROUP BY T1.PortID ORDER BY T1.PortID 
	into OUTFILE '/tmp/data/customer_links_data.csv' fields terminated by ',';


*/

/*
SELECT 'Daily', 'Circuit ID', 'Node Name', 'Interface Description', 'Location', 'Max In Traffic', 'Max Out Traffic', 'Average In Traffic', 'Average Out Traffic', 'Bandwidth', 'Customer Name', 'Threshold', 'Number of times exceeeding threshold' into OUTFILE '/tmp/head/customer_links_head.csv' fields terminated by ',';
*/
END |
DELIMITER ;



