DROP PROCEDURE IF EXISTS 6_4_1n2_PortUtil; 
 DELIMITER | 
 CREATE PROCEDURE 6_4_1n2_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
BEGIN
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL_1;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEName_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEANDIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODEName_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100),PortID BIGINT(20) DEFAULT 0);
CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL LIKE ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
CREATE TEMPORARY TABLE TRAFFIC_TBL(
	PortID                  BIGINT(20),
	NodeName		VARCHAR(100),
	IfDescr			VARCHAR(100),
	IfSpeed			VARCHAR(100),
	InErrPkts		BIGINT(20),
	RcvOctets		BIGINT(20),
	TxOctets		BIGINT(20),
	Time_1			TIMESTAMP);

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(
	PortID 			BIGINT(20), 
	maxTrafficValueIn 	BIGINT(20) DEFAULT 0,
	maxOutTrafficValueOut 	BIGINT(20) DEFAULT 0,
	avgTrafficValueIn 	BIGINT(20) DEFAULT 0,
	avgTrafficValueOut 	BIGINT(20) DEFAULT 0,
	CRCError		BIGINT(20) DEFAULT 0,
	UpTime			INTEGER DEFAULT 0,
	Reliablity		INTEGER DEFAULT 0,
	AvgUtilIn 		float DEFAULT 0,
	AvgUtilOut 		float DEFAULT 0,
	PeakUtilIn 		float DEFAULT 0, 
	PeakUtilOut 		float DEFAULT 0,
	ThresholdExceed		INTEGER DEFAULT 0,
	inPeakTime 		timestamp DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		timestamp DEFAULT '0000-00-00 00:00:00');


IF service='ALL'
THEN
	INSERT INTO TEMP_SERVICE_TBL
	SELECT distinct NodeName, NodeNumber from NODE_TBL;
ELSE
set @a=1;
set @like="";
select REPLACE(SUBSTRING(SUBSTRING_INDEX(service, ',', @a),LENGTH(SUBSTRING_INDEX(service, ',', @a -1)) + 1),',','') into @service;
while(@service != "")
DO
	IF(@like = "")
	THEN
		SET @like = CONCAT("NodeName like '%",SUBSTRING(@service,2,3),"%'");
	ELSE
		SET @like = CONCAT(@like," or NodeName like '%",SUBSTRING(@service,2,3),"%'");
	END IF;
	set @a=@a+1;
	select REPLACE(SUBSTRING(SUBSTRING_INDEX(service, ',', @a),LENGTH(SUBSTRING_INDEX(service, ',', @a -1)) + 1),',','') into @service;
END WHILE;
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from NODE_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1= CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where substring(NodeName,1,3) in (',inputCity,')');

IF inputCity='ALL'
THEN
        INSERT INTO TEMP_CITY_TBL
        SELECT distinct NodeName, NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1= CONCAT('INSERT INTO TEMP_NODEName_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where NodeName in (',inputNodeName,')');

IF inputNodeName='ALL'
THEN
        INSERT INTO TEMP_NODEName_TBL
        SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
ELSE
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

CREATE INDEX i1 ON TEMP_NODEIF_TBL(NodeNumber,IfIndex);

IF (utilType='ALL')
THEN
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber;
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where IfAlias like '%AESI-IN%';
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where IfAlias NOT like '%AESI-IN%' and (IfAlias like '%SWH%' or IfAlias like '%AES%' or IfAlias like '%RTR%');
	END IF;
END IF;

CREATE INDEX portIDIndex1 on TEMP_NODEANDIF_TBL(PortID);
CREATE INDEX i2 ON TEMP_NODEIF_TBL_1(NodeNumber,IfIndex);
CREATE INDEX i3 ON TEMP_NODEANDIF_TBL(NodeNumber,IfIndex);

IF(p_networkType != 'ALL')
THEN
set @r1= SUBSTRING(p_networkType,2,3);
set @r2= SUBSTRING(p_networkType,8,3);
set @r3= SUBSTRING(p_networkType,14,3);
	SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r1,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;

	if(@r2 != "")
	THEN
	SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r2,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;
		IF(@r3 != "")
		THEN
		SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r3,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;
		END IF;
	END IF;

	INSERT INTO TEMP_NODEANDIF_TBL(NodeName,NodeNumber,IfIndex,IfSpeed,IfDescr,IfAlias)  SELECT a.NodeName,a.NodeNumber,c.IfIndex,c.IfSpeed,c.IfDescr,c.IfAlias  from TEMP_NODEName_TBL a, TEMP_NODEIF_TBL_1 c where a.NodeNumber=c.nodenumber;

ELSE
	INSERT INTO TEMP_NODEANDIF_TBL(NodeName,NodeNumber,IfIndex,IfSpeed,IfDescr,IfAlias)  SELECT a.NodeName,a.NodeNumber,c.IfIndex,c.IfSpeed,c.IfDescr,c.IfAlias  from TEMP_NODEName_TBL a, TEMP_NODEIF_TBL c where a.NodeNumber=c.nodenumber;
END IF;

update TEMP_NODEANDIF_TBL a,VLANPRT_TBL b  set a.PortID = PrtID  where a.NodeNumber = b.NodeID and a.IfIndex = b.IfIndex;

delete from TEMP_NODEANDIF_TBL where PortID = 0;



INSERT INTO TRAFFIC_TBL
        SELECT a.PortID,NodeName,IfDescr,IfSpeed,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID  where Time_1>startTime and Time_1<endTime;

DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
CREATE TEMPORARY TABLE EXCEED_COUNT (PortID BIGINT(20), Exceed INTEGER);

INSERT INTO EXCEED_COUNT
        select PortID,count(RcvOctets) from TRAFFIC_TBL where (RcvOctets/(IfSpeed*10)>70 or TxOctets/(IfSpeed*10)>70) group by PortID;

INSERT INTO TEMP_TRAFFIC_TBL (PortID, maxTrafficValueIn,maxOutTrafficValueOut,avgTrafficValueIn,avgTrafficValueOut,CRCError,AvgUtilIn,AvgUtilOut,PeakUtilIn,PeakUtilOut)
	SELECT  PortID,max(RcvOctets),max(TxOctets),avg(RcvOctets),avg(TxOctets), sum(InErrPkts),
	IF((IfSpeed=0),"0",(avg(RcvOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(avg(TxOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(max(RcvOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(max(TxOctets)/(IfSpeed*10)))
		from TRAFFIC_TBL group by PortID;

CREATE INDEX portIDIndex2 on TEMP_TRAFFIC_TBL(PortID);
CREATE INDEX portIDIndex3 on EXCEED_COUNT(PortID);

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=B.Time_1 where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=B.Time_1 where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
CASE
WHEN IfAlias like '%SWH%' THEN "Trunk"
WHEN IfAlias like '%AES%' and IfAlias not like '%Peering%' THEN "Backbone"
WHEN IfAlias like '%RTR%' and IfAlias not like '%AES%' THEN "Back-to-Back"
ELSE '-'
END ,
NodeName,IfDescr,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(CRCError/1000,2),0,0,ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID ;





END | 
 DELIMITER ;
 
 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_1_PortUtil; 
 DELIMITER | 
 CREATE PROCEDURE mpls_reporting_6_4_1_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL_1;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEName_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEANDIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODEName_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100),PortID BIGINT(20) DEFAULT 0);
CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL LIKE ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
CREATE TEMPORARY TABLE TRAFFIC_TBL(
	PortID                  BIGINT(20),
	NodeName		VARCHAR(100),
	IfDescr			VARCHAR(100),
	IfSpeed			VARCHAR(100),
	InErrPkts		BIGINT(20),
	RcvOctets		BIGINT(20),
	TxOctets		BIGINT(20),
	Time_1			TIMESTAMP);

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(
	PortID 			BIGINT(20), 
	maxTrafficValueIn 	BIGINT(20) DEFAULT 0,
	maxOutTrafficValueOut 	BIGINT(20) DEFAULT 0,
	avgTrafficValueIn 	BIGINT(20) DEFAULT 0,
	avgTrafficValueOut 	BIGINT(20) DEFAULT 0,
	CRCError		BIGINT(20) DEFAULT 0,
	UpTime			INTEGER DEFAULT 0,
	Reliablity		INTEGER DEFAULT 0,
	AvgUtilIn 		float DEFAULT 0,
	AvgUtilOut 		float DEFAULT 0,
	PeakUtilIn 		float DEFAULT 0, 
	PeakUtilOut 		float DEFAULT 0,
	ThresholdExceed		INTEGER DEFAULT 0,
	inPeakTime 		timestamp DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		timestamp DEFAULT '0000-00-00 00:00:00');


IF service='ALL'
THEN
	INSERT INTO TEMP_SERVICE_TBL
	SELECT distinct NodeName, NodeNumber from NODE_TBL;
ELSE
set @a=1;
set @like="";
select REPLACE(SUBSTRING(SUBSTRING_INDEX(service, ',', @a),LENGTH(SUBSTRING_INDEX(service, ',', @a -1)) + 1),',','') into @service;
while(@service != "")
DO
	IF(@like = "")
	THEN
		SET @like = CONCAT("NodeName like '%",SUBSTRING(@service,2,3),"%'");
	ELSE
		SET @like = CONCAT(@like," or NodeName like '%",SUBSTRING(@service,2,3),"%'");
	END IF;
	set @a=@a+1;
	select REPLACE(SUBSTRING(SUBSTRING_INDEX(service, ',', @a),LENGTH(SUBSTRING_INDEX(service, ',', @a -1)) + 1),',','') into @service;
END WHILE;
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from NODE_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1= CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where substring(NodeName,1,3) in (',inputCity,')');

IF inputCity='ALL'
THEN
        INSERT INTO TEMP_CITY_TBL
        SELECT distinct NodeName, NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1= CONCAT('INSERT INTO TEMP_NODEName_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where NodeName in (',inputNodeName,')');

IF inputNodeName='ALL'
THEN
        INSERT INTO TEMP_NODEName_TBL
        SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
ELSE
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

CREATE INDEX i1 ON TEMP_NODEIF_TBL(NodeNumber,IfIndex);

IF (utilType='ALL')
THEN
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber;
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where IfAlias like '%AESI-IN%';
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where IfAlias NOT like '%AESI-IN%' and (IfAlias like '%SWH%' or IfAlias like '%AES%' or IfAlias like '%RTR%');
	END IF;
END IF;

CREATE INDEX portIDIndex1 on TEMP_NODEANDIF_TBL(PortID);
CREATE INDEX i2 ON TEMP_NODEIF_TBL_1(NodeNumber,IfIndex);
CREATE INDEX i3 ON TEMP_NODEANDIF_TBL(NodeNumber,IfIndex);

IF(p_networkType != 'ALL')
THEN
set @r1= SUBSTRING(p_networkType,2,3);
set @r2= SUBSTRING(p_networkType,8,3);
set @r3= SUBSTRING(p_networkType,14,3);
	SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r1,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;

	if(@r2 != "")
	THEN
	SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r2,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;
		IF(@r3 != "")
		THEN
		SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r3,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;
		END IF;
	END IF;

	INSERT INTO TEMP_NODEANDIF_TBL(NodeName,NodeNumber,IfIndex,IfSpeed,IfDescr,IfAlias)  SELECT a.NodeName,a.NodeNumber,c.IfIndex,c.IfSpeed,c.IfDescr,c.IfAlias  from TEMP_NODEName_TBL a, TEMP_NODEIF_TBL_1 c where a.NodeNumber=c.nodenumber;

ELSE
	INSERT INTO TEMP_NODEANDIF_TBL(NodeName,NodeNumber,IfIndex,IfSpeed,IfDescr,IfAlias)  SELECT a.NodeName,a.NodeNumber,c.IfIndex,c.IfSpeed,c.IfDescr,c.IfAlias  from TEMP_NODEName_TBL a, TEMP_NODEIF_TBL c where a.NodeNumber=c.nodenumber;
END IF;

update TEMP_NODEANDIF_TBL a,VLANPRT_TBL b  set a.PortID = PrtID  where a.NodeNumber = b.NodeID and a.IfIndex = b.IfIndex;

delete from TEMP_NODEANDIF_TBL where PortID = 0;



INSERT INTO TRAFFIC_TBL
        SELECT a.PortID,NodeName,IfDescr,IfSpeed,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID  where Time_1>startTime and Time_1<endTime;

DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
CREATE TEMPORARY TABLE EXCEED_COUNT (PortID BIGINT(20), Exceed INTEGER);

INSERT INTO EXCEED_COUNT
        select PortID,count(RcvOctets) from TRAFFIC_TBL where (RcvOctets/(IfSpeed*10)>70 or TxOctets/(IfSpeed*10)>70) group by PortID;

INSERT INTO TEMP_TRAFFIC_TBL (PortID, maxTrafficValueIn,maxOutTrafficValueOut,avgTrafficValueIn,avgTrafficValueOut,CRCError,AvgUtilIn,AvgUtilOut,PeakUtilIn,PeakUtilOut)
	SELECT  PortID,max(RcvOctets),max(TxOctets),avg(RcvOctets),avg(TxOctets), sum(InErrPkts),
	IF((IfSpeed=0),"0",(avg(RcvOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(avg(TxOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(max(RcvOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(max(TxOctets)/(IfSpeed*10)))
		from TRAFFIC_TBL group by PortID;

CREATE INDEX portIDIndex2 on TEMP_TRAFFIC_TBL(PortID);
CREATE INDEX portIDIndex3 on EXCEED_COUNT(PortID);

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=B.Time_1 where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=B.Time_1 where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;
CREATE TEMPORARY TABLE FINAL_FETCH(serviceType varchar(256),NWType varchar(256), Router varchar(256),Interface varchar(256),IfAlias varchar(256), InUtilPeak varchar(256), OutUtilPeak varchar(256),InUtilAvg varchar(256),OutUtilAvg varchar(256), CRCInError varchar(256),UpTime varchar(256),Reliability varchar(256),AvgUtilPercIn varchar(256),AvgUtilPercOut varchar(256),PeakUtilPercIn varchar(256),PeakUtilPercOut varchar(256),Threshold varchar(256),inPeakTime varchar(256),outPeakTime varchar(256));
INSERT INTO FINAL_FETCH values('Service','NW Type', 'Router', 'Interface', 'Interface Alias','In Traffic Peak (Kbps)', 'Out Traffic Peak(Kbps)', 'In Traffic Avg(Kbps)', 'Out Traffic Avg(Kbps)', 'CRC (In Error)','Up Time', 'Reliabilty', 'Avg Util(%) IN','Avg Util(%) OUT','Peak Util(%) IN','Peak Util(%) OUT','Number of times peak threshold crossed during the Report Duration' ,'In Peak Util Time','Out Peak Util Time');
INSERT INTO FINAL_FETCH
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
CASE
WHEN IfAlias like '%SWH%' THEN "Trunk"
WHEN IfAlias like '%AES%' and IfAlias not like '%Peering%' THEN "Backbone"
WHEN IfAlias like '%RTR%' and IfAlias not like '%AES%' THEN "Back-to-Back"
ELSE '-'
END ,
NodeName,IfDescr,IfAlias,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(CRCError/1000,2),0,0,ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID ;


SET @fileName=CONCAT(@dir_name,'/NWP_Domestic_Backbone_Service_wise_Link_Utilization_',DATE(startTime),'.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \',\'');

PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;




END | 
 DELIMITER ; 
 
  DROP PROCEDURE IF EXISTS mpls_web_6_4_1_PortUtil; 
 DELIMITER | 
 CREATE PROCEDURE mpls_web_6_4_1_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL_1;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEName_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEANDIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODEName_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(100),PortID BIGINT(20) DEFAULT 0);
CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL LIKE ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
CREATE TEMPORARY TABLE TRAFFIC_TBL(
	PortID                  BIGINT(20),
	NodeName		VARCHAR(100),
	IfDescr			VARCHAR(100),
	IfSpeed			VARCHAR(100),
	InErrPkts		BIGINT(20),
	RcvOctets		BIGINT(20),
	TxOctets		BIGINT(20),
	Time_1			TIMESTAMP);

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(
	PortID 			BIGINT(20), 
	maxTrafficValueIn 	BIGINT(20) DEFAULT 0,
	maxOutTrafficValueOut 	BIGINT(20) DEFAULT 0,
	avgTrafficValueIn 	BIGINT(20) DEFAULT 0,
	avgTrafficValueOut 	BIGINT(20) DEFAULT 0,
	CRCError		BIGINT(20) DEFAULT 0,
	UpTime			INTEGER DEFAULT 0,
	Reliablity		INTEGER DEFAULT 0,
	AvgUtilIn 		float DEFAULT 0,
	AvgUtilOut 		float DEFAULT 0,
	PeakUtilIn 		float DEFAULT 0, 
	PeakUtilOut 		float DEFAULT 0,
	ThresholdExceed		INTEGER DEFAULT 0,
	inPeakTime 		timestamp DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		timestamp DEFAULT '0000-00-00 00:00:00');


IF service='ALL'
THEN
	INSERT INTO TEMP_SERVICE_TBL
	SELECT distinct NodeName, NodeNumber from NODE_TBL;
ELSE
set @a=1;
set @like="";
select REPLACE(SUBSTRING(SUBSTRING_INDEX(service, ',', @a),LENGTH(SUBSTRING_INDEX(service, ',', @a -1)) + 1),',','') into @service;
while(@service != "")
DO
	IF(@like = "")
	THEN
		SET @like = CONCAT("NodeName like '%",SUBSTRING(@service,2,3),"%'");
	ELSE
		SET @like = CONCAT(@like," or NodeName like '%",SUBSTRING(@service,2,3),"%'");
	END IF;
	set @a=@a+1;
	select REPLACE(SUBSTRING(SUBSTRING_INDEX(service, ',', @a),LENGTH(SUBSTRING_INDEX(service, ',', @a -1)) + 1),',','') into @service;
END WHILE;
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from NODE_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1= CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where substring(NodeName,1,3) in (',inputCity,')');

IF inputCity='ALL'
THEN
        INSERT INTO TEMP_CITY_TBL
        SELECT distinct NodeName, NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1= CONCAT('INSERT INTO TEMP_NODEName_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where NodeName in (',inputNodeName,')');

IF inputNodeName='ALL'
THEN
        INSERT INTO TEMP_NODEName_TBL
        SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
ELSE
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

CREATE INDEX i1 ON TEMP_NODEIF_TBL(NodeNumber,IfIndex);

IF (utilType='ALL')
THEN
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber;
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where IfAlias like '%AESI-IN%';
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where IfAlias NOT like '%AESI-IN%';
	END IF;
END IF;

CREATE INDEX portIDIndex1 on TEMP_NODEANDIF_TBL(PortID);
CREATE INDEX i2 ON TEMP_NODEIF_TBL_1(NodeNumber,IfIndex);
CREATE INDEX i3 ON TEMP_NODEANDIF_TBL(NodeNumber,IfIndex);

IF(p_networkType != 'ALL')
THEN
set @r1= SUBSTRING(p_networkType,2,3);
set @r2= SUBSTRING(p_networkType,8,3);
set @r3= SUBSTRING(p_networkType,14,3);
	SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r1,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;

	if(@r2 != "")
	THEN
	SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r2,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;
		IF(@r3 != "")
		THEN
		SET @query1= CONCAT("INSERT INTO TEMP_NODEIF_TBL_1 SELECT  NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from TEMP_NODEIF_TBL where IfAlias like '%",@r3,"%'");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
	DEALLOCATE Prepare statement1;
		END IF;
	END IF;

	INSERT INTO TEMP_NODEANDIF_TBL(NodeName,NodeNumber,IfIndex,IfSpeed,IfDescr,IfAlias)  SELECT a.NodeName,a.NodeNumber,c.IfIndex,c.IfSpeed,c.IfDescr,c.IfAlias  from TEMP_NODEName_TBL a, TEMP_NODEIF_TBL_1 c where a.NodeNumber=c.nodenumber;

ELSE
	INSERT INTO TEMP_NODEANDIF_TBL(NodeName,NodeNumber,IfIndex,IfSpeed,IfDescr,IfAlias)  SELECT a.NodeName,a.NodeNumber,c.IfIndex,c.IfSpeed,c.IfDescr,c.IfAlias  from TEMP_NODEName_TBL a, TEMP_NODEIF_TBL c where a.NodeNumber=c.nodenumber;
END IF;

update TEMP_NODEANDIF_TBL a,VLANPRT_TBL b  set a.PortID = PrtID  where a.NodeNumber = b.NodeID and a.IfIndex = b.IfIndex;

delete from TEMP_NODEANDIF_TBL where PortID = 0;



INSERT INTO TRAFFIC_TBL
        SELECT a.PortID,NodeName,IfDescr,IfSpeed,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID  where Time_1>startTime and Time_1<endTime;

DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
CREATE TEMPORARY TABLE EXCEED_COUNT (PortID BIGINT(20), Exceed INTEGER);

INSERT INTO EXCEED_COUNT
        select PortID,count(RcvOctets) from TRAFFIC_TBL where (RcvOctets/(IfSpeed*10)>70 or TxOctets/(IfSpeed*10)>70) group by PortID;

INSERT INTO TEMP_TRAFFIC_TBL (PortID, maxTrafficValueIn,maxOutTrafficValueOut,avgTrafficValueIn,avgTrafficValueOut,CRCError,AvgUtilIn,AvgUtilOut,PeakUtilIn,PeakUtilOut)
	SELECT  PortID,max(RcvOctets),max(TxOctets),avg(RcvOctets),avg(TxOctets), sum(InErrPkts),
	IF((IfSpeed=0),"0",(avg(RcvOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(avg(TxOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(max(RcvOctets)/(IfSpeed*10))),
	IF((IfSpeed=0),"0",(max(TxOctets)/(IfSpeed*10)))
		from TRAFFIC_TBL group by PortID;

CREATE INDEX portIDIndex2 on TEMP_TRAFFIC_TBL(PortID);
CREATE INDEX portIDIndex3 on EXCEED_COUNT(PortID);

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=B.Time_1 where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=B.Time_1 where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
CASE
WHEN IfAlias like '%SWH%' THEN "Trunk"
WHEN IfAlias like '%AES%' and IfAlias not like '%Peering%' THEN "Backbone"
WHEN IfAlias like '%RTR%' and IfAlias not like '%AES%' THEN "Back-to-Back"
ELSE '-'
END ,
NodeName,IfDescr,IfAlias,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(CRCError/1000,2),0,0,ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID ;





END | 
 DELIMITER ;
