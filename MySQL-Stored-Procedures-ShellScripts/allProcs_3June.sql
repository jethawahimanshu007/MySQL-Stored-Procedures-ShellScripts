\d |
 DROP PROCEDURE IF EXISTS 6_4_3_Pearing_Link_Util; 
 CREATE PROCEDURE 6_4_3_Pearing_Link_Util(p_peeringScope varchar(5000), p_Region varchar(5000), p_peeringType varchar(1000),p_RouterName VARCHAR(5000),p_PeeringPartner VARCHAR(5000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TMP_PEER_SCOPE ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_REGION ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_TYPE ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_PARTNER ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_NODE ;
DROP TEMPORARY TABLE IF EXISTS TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS PEER_LINK_UTIL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
CREATE TEMPORARY TABLE TMP_PEER_SCOPE LIKE PEERING_VPN;
CREATE TEMPORARY TABLE TMP_PEER_REGION LIKE PEERING_VPN;
CREATE TEMPORARY TABLE TMP_PEER_TYPE LIKE PEERING_VPN;
CREATE TEMPORARY TABLE TMP_PEER_PARTNER LIKE PEERING_VPN;
CREATE TEMPORARY TABLE TMP_PEER_NODE LIKE PEERING_VPN;
CREATE TEMPORARY TABLE TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL LIKE ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
CREATE TEMPORARY TABLE TMP_TRAFFIC_TBL (
	PortID BIGINT(20),
	RcvOctets BIGINT(20),
	TxOctets BIGINT(20),
	Time_1 TIMESTAMP);

CREATE TEMPORARY TABLE PEER_LINK_UTIL(
	PortID		BIGINT(20),
        peeringScope    varchar(100),
        Region          varchar(100),
        peeringType     varchar(100),
        RouterName      varchar(128),
        IfDescr         varchar(128),
        PeeringPartner  varchar(100),
        95PerIn         FLOAT DEFAULT 0,
        95PerOut        FLOAT DEFAULT 0,
        PeakIn          FLOAT DEFAULT 0,
        PeakOut         FLOAT DEFAULT 0,
        AvgIn           FLOAT DEFAULT 0,
        AvgOut          FLOAT DEFAULT 0,
        MinIn           FLOAT DEFAULT 0,
        MinOut          FLOAT DEFAULT 0,
        VolumeIn        FLOAT DEFAULT 0,
        VolumeOut       FLOAT DEFAULT 0
);
set @ST = startTime;
SET @ET = endTime;
SET @KBPS=1000;
IF(p_RouterName = 'ALL')
THEN
	INSERT INTO TMP_PEER_NODE select * from PEERING_VPN;
ELSE
	set @query = CONCAT("INSERT INTO TMP_PEER_NODE select * from PEERING_VPN where NodeName IN (",p_RouterName,")");
	PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;

IF(p_peeringScope = 'ALL')
THEN
	INSERT INTO TMP_PEER_SCOPE select * from TMP_PEER_NODE;
ELSE

	set @query = CONCAT("INSERT INTO TMP_PEER_SCOPE select * from TMP_PEER_NODE where NEFunction IN (",p_peeringScope,")");
	PREPARE stmt1 from @query;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
END IF;
IF(p_Region = 'ALL')
THEN
	INSERT INTO TMP_PEER_REGION select * from TMP_PEER_SCOPE;
ELSE 
        set @query = CONCAT("INSERT INTO TMP_PEER_REGION select * from TMP_PEER_SCOPE where peerVPN IN (",p_Region,")");
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;
IF(p_peeringType = 'ALL')
THEN
	INSERT INTO  TMP_PEER_TYPE select * from TMP_PEER_REGION;
ELSE
	set @query = CONCAT("INSERT INTO TMP_PEER_TYPE select * from TMP_PEER_REGION where serviceType IN (",p_peeringType,")");
	PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;     

IF(p_PeeringPartner = 'ALL')
THEN 
	INSERT INTO TMP_PEER_PARTNER select * from TMP_PEER_TYPE;
ELSE
	set @query = CONCAT("INSERT INTO TMP_PEER_PARTNER select * from TMP_PEER_TYPE where interfaceDest IN (",p_PeeringPartner,")");
	PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;


CREATE INDEX iLinkIndex ON TMP_PEER_PARTNER(LinkID);
INSERT INTO TMP_TRAFFIC_TBL 
	SELECT a.PortID,a.RcvOctets,a.TxOctets,a.Time_1 from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN TMP_PEER_PARTNER b on a.PortID = b.LinkID where  Time_1 > startTime and Time_1 <= endTime ;

CREATE INDEX iPortIndex ON TMP_TRAFFIC_TBL(PortID);
CREATE INDEX iTimeIndex ON TMP_TRAFFIC_TBL(Time_1);

INSERT INTO PEER_LINK_UTIL(PortID,peeringScope,Region,peeringType,RouterName,IfDescr,PeeringPartner,PeakIn,PeakOut,AvgIn,AvgOut,MinIn,MinOut,VolumeIn,VolumeOut)
	select b.PortID,NEFunction,peerVPN,serviceType,NodeName,IfDescr,interfaceDest,Max(RcvOctets),Max(TxOctets),Avg(RcvOctets),Avg(TxOctets),Min(RcvOctets),Min(TxOctets),SUM(RcvOctets),SUM(TxOctets) from TMP_PEER_PARTNER a JOIN TMP_TRAFFIC_TBL b ON a.LinkID = b.PortID group by b.PortID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","RcvOctets");


Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b 
	set 95PerIn = Traffic 
	where a.PortID = b.EntityID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","TxOctets");

Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerOut = Traffic
        where a.PortID = b.EntityID;

select peeringScope,Region,peeringType,RouterName,IfDescr,PeeringPartner,ROUND((95PerIn/@KBPS),2),ROUND(95PerOut/@KBPS,2),ROUND(PeakIn/@KBPS,2),ROUND(PeakOut/@KBPS,2),ROUND(AvgIn/@KBPS,2),ROUND(AvgOut/@KBPS,2),ROUND(MinIn/@KBPS,2),ROUND(MinOut/@KBPS,2),ROUND(VolumeIn/@KBPS,2),ROUND(VolumeOut/@KBPS,2) from PEER_LINK_UTIL;


DROP TEMPORARY TABLE IF EXISTS TMP_PEER_SCOPE ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_REGION ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_TYPE ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_PARTNER ;
DROP TEMPORARY TABLE IF EXISTS TMP_PEER_NODE ;
DROP TEMPORARY TABLE IF EXISTS TMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS PEER_LINK_UTIL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

END | 
 

 DROP PROCEDURE IF EXISTS 6_4_6_Latency; 
 CREATE PROCEDURE 6_4_6_Latency(sourceCity varchar(1000),destCity varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS SHADOW_SOURCE;
DROP TEMPORARY TABLE IF EXISTS SHADOW_DEST;
DROP TEMPORARY TABLE IF EXISTS DELAY_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
CREATE TEMPORARY TABLE SHADOW_SOURCE LIKE NODE_TBL;
CREATE TEMPORARY TABLE SHADOW_DEST  LIKE NODE_TBL;

CREATE TEMPORARY TABLE DELAY_TBL (
        Source  VARCHAR(100),
        Destination VARCHAR(100),
        SourceIP        VARCHAR(100),
        DestinationIP VARCHAR(100),
        Latency FLOAT(12,2),
        Jitter  INTEGER(10),
        PacketLoss INTEGER(10),
        PeakLatency     FLOAT(12,2),
        PeakLatencyTime VARCHAR(100));
IF(sourceCity = 'ALL')
THEN
        INSERT INTO SHADOW_SOURCE select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_SOURCE select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',sourceCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;
IF(destCity = 'ALL')
THEN
        INSERT INTO SHADOW_DEST select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_DEST select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',destCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;


INSERT INTO DELAY_TBL
select Ori.NodeName,Ter.NodeName,SourceIP,DestinationIP,AvgDelay,AvgJitter,PacketLoss,Max(AvgDelay),"" from VPN_DELAY_TABLE a JOIN SHADOW_SOURCE Ori ON a.SourceIP = Ori.NodeID JOIN
SHADOW_DEST Ter ON a.DestinationIP = Ter.NodeID where a.Time_1>startTime and a.Time_1<endTime group by SourceIP,DestinationIP;

CREATE INDEX i1 ON DELAY_TBL(SourceIP,DestinationIP);
Update DELAY_TBL a JOIN VPN_DELAY_TABLE b ON  b.SourceIP = a.SourceIP and b.DestinationIP = a.DestinationIP set PeakLatencyTime = if(ROUND(Latency,2)=0,'NA',b.Time_1) where AvgDelay = Latency and Time_1 > startTime and Time_1 < endTime ;



select b.city,c.city,SourceIP,DestinationIP,ROUND(Latency,2),ROUND(Jitter,2),ROUND(PacketLoss,2),ROUND(PeakLatency,2),PeakLatencyTime from DELAY_TBL a , nodeToCity b, nodeToCity c where a.SourceIp=b.NodeIp and a.DestinationIp=c.NodeIp;


END | 
 

 DROP PROCEDURE IF EXISTS 6_4_6_OnlyJitter; 
 CREATE PROCEDURE 6_4_6_OnlyJitter(sourceCity varchar(1000),destCity varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS SHADOW_SOURCE;
DROP TEMPORARY TABLE IF EXISTS SHADOW_DEST;
DROP TEMPORARY TABLE IF EXISTS DELAY_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
CREATE TEMPORARY TABLE SHADOW_SOURCE LIKE NODE_TBL;
CREATE TEMPORARY TABLE SHADOW_DEST  LIKE NODE_TBL;

CREATE TEMPORARY TABLE DELAY_TBL (
        Source  VARCHAR(100),
        Destination VARCHAR(100),
        SourceIP        VARCHAR(100),
        DestinationIP VARCHAR(100),
        Jitter  INTEGER(10)
        );
IF(sourceCity = 'ALL')
THEN
        INSERT INTO SHADOW_SOURCE select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_SOURCE select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',sourceCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;
IF(destCity = 'ALL')
THEN
        INSERT INTO SHADOW_DEST select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_DEST select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',destCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;


INSERT INTO DELAY_TBL
select Ori.NodeName,Ter.NodeName,SourceIP,DestinationIP,AVG(AvgJitter) from VPN_DELAY_TABLE a JOIN SHADOW_SOURCE Ori ON a.SourceIP = Ori.NodeID JOIN
SHADOW_DEST Ter ON a.DestinationIP = Ter.NodeID where a.Time_1>startTime and a.Time_1<endTime group by SourceIP,DestinationIP;

select b.city,c.city,ROUND(Jitter,2) from DELAY_TBL a , nodeToCity b, nodeToCity c where a.SourceIp=b.NodeIp and a.DestinationIp=c.NodeIp;
END | 
 

 DROP PROCEDURE IF EXISTS 6_4_6_OnlyLatency; 
 CREATE PROCEDURE 6_4_6_OnlyLatency(sourceCity varchar(1000),destCity varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS SHADOW_SOURCE;
DROP TEMPORARY TABLE IF EXISTS SHADOW_DEST;
DROP TEMPORARY TABLE IF EXISTS DELAY_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
CREATE TEMPORARY TABLE SHADOW_SOURCE LIKE NODE_TBL;
CREATE TEMPORARY TABLE SHADOW_DEST  LIKE NODE_TBL;

CREATE TEMPORARY TABLE DELAY_TBL (
        Source  VARCHAR(100),
        Destination VARCHAR(100),
        SourceIP        VARCHAR(100),
        DestinationIP VARCHAR(100),
        Latency FLOAT(12,2)
        );
IF(sourceCity = 'ALL')
THEN
        INSERT INTO SHADOW_SOURCE select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_SOURCE select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',sourceCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;
IF(destCity = 'ALL')
THEN
        INSERT INTO SHADOW_DEST select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_DEST select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',destCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;


INSERT INTO DELAY_TBL
select Ori.NodeName,Ter.NodeName,SourceIP,DestinationIP,AVG(AvgDelay) from VPN_DELAY_TABLE a JOIN SHADOW_SOURCE Ori ON a.SourceIP = Ori.NodeID JOIN
SHADOW_DEST Ter ON a.DestinationIP = Ter.NodeID where a.Time_1>startTime and a.Time_1<endTime group by SourceIP,DestinationIP;

select b.city,c.city,ROUND(Latency,2) from DELAY_TBL a , nodeToCity b, nodeToCity c where a.SourceIp=b.NodeIp and a.DestinationIp=c.NodeIp;
END | 
 

 DROP PROCEDURE IF EXISTS 6_4_6_OnlyPacketLoss; 
 CREATE PROCEDURE 6_4_6_OnlyPacketLoss(sourceCity varchar(1000),destCity varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS SHADOW_SOURCE;
DROP TEMPORARY TABLE IF EXISTS SHADOW_DEST;
DROP TEMPORARY TABLE IF EXISTS DELAY_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
CREATE TEMPORARY TABLE SHADOW_SOURCE LIKE NODE_TBL;
CREATE TEMPORARY TABLE SHADOW_DEST  LIKE NODE_TBL;

CREATE TEMPORARY TABLE DELAY_TBL (
        Source  VARCHAR(100),
        Destination VARCHAR(100),
        SourceIP        VARCHAR(100),
        DestinationIP VARCHAR(100),
        PacketLoss INTEGER(10)
        );
IF(sourceCity = 'ALL')
THEN
        INSERT INTO SHADOW_SOURCE select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_SOURCE select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',sourceCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;
IF(destCity = 'ALL')
THEN
        INSERT INTO SHADOW_DEST select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_DEST select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',destCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;


INSERT INTO DELAY_TBL
select Ori.NodeName,Ter.NodeName,SourceIP,DestinationIP,AVG(PacketLoss) from VPN_DELAY_TABLE a JOIN SHADOW_SOURCE Ori ON a.SourceIP = Ori.NodeID JOIN
SHADOW_DEST Ter ON a.DestinationIP = Ter.NodeID where a.Time_1>startTime and a.Time_1<endTime group by SourceIP,DestinationIP;

select b.city,c.city,ROUND(PacketLoss,2) from DELAY_TBL a , nodeToCity b, nodeToCity c where a.SourceIp=b.NodeIp and a.DestinationIp=c.NodeIp;
END | 
 

 DROP PROCEDURE IF EXISTS 6_6_10_CustomerUtil; 
 CREATE PROCEDURE 6_6_10_CustomerUtil(city varchar(5000), service varchar(5000), inputNodeName varchar(8000),customerName varchar(17000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODENAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_TBL;

CREATE TEMPORARY TABLE TEMP_CITY_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_NODENAME_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_CUSTOMER_TBL like CUSTOMER_VRF_TBL;


SET @query1 := CONCAT(' INSERT INTO TEMP_CUSTOMER_TBL SELECT * from CUSTOMER_VRF_TBL where VpnName in(',customerName);
SET @query2 := CONCAT(@query1,')');
IF customerName='ALL'
THEN
INSERT INTO TEMP_CUSTOMER_TBL
SELECT * from CUSTOMER_VRF_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT * from TEMP_CUSTOMER_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT * from TEMP_CUSTOMER_TBL;
ELSE

PREPARE statement1 from @query2;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT * from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT * from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODENAME_TBL SELECT * from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODENAME_TBL
SELECT * from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;


CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL(
	PortID BIGINT(20),
	RcvOctets BIGINT(20),
	TxOctets BIGINT(20),
	Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(PortID int, maxTrafficValueIn bigint(20),maxOutTrafficValueOut bigint(20),avgTrafficValueIn bigint(20),avgTrafficValueOut bigint(20),inPeakTime varchar(20) DEFAULT '0000-00-00 00:00:00',outPeakTime varchar(20) DEFAULT '0000-00-00 00:00:00');

CREATE INDEX portIdIndex1 on TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL(PortId);
CREATE INDEX portIDIndex2 on TEMP_TRAFFIC_TBL(PortID);
CREATE INDEX portIDIndex3 on TEMP_NODENAME_TBL(PortID);

INSERT INTO TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL 
	SELECT a.PortId,a.rcvOctets,a.txoctets,a.Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN TEMP_NODENAME_TBL b ON a.PortID = b.PortID where Time_1>startTime and Time_1<endTime;

INSERT INTO TEMP_TRAFFIC_TBL (PortID, maxTrafficValueIn,maxOutTrafficValueOut,avgTrafficValueIn,avgTrafficValueOut)
	SELECT  a.PortID,max(a.RcvOctets),max(a.TxOctets),avg(a.RcvOctets),avg(a.RcvOctets) from TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL a group by a.PortID;

UPDATE TEMP_TRAFFIC_TBL A JOIN TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL B ON A.PortId=B.PortId set inPeakTime= IF(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL B ON A.PortId=B.PortId set OutPeakTime= IF(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;



SELECT (substring(NodeName,1,3)) as City,(NodeName),(a.IfDescr),(a.VpnName),
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
a.IfSpeed,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),inPeakTime,outPeakTime  from CUSTOMER_VRF_TBL a, TEMP_TRAFFIC_TBL b where a.PortID=b.PortID and avgTrafficValueOut >0 and avgTrafficValueIn > 0;




END | 
 

 DROP PROCEDURE IF EXISTS 6_6_1_VRFUtil; 
 CREATE PROCEDURE 6_6_1_VRFUtil(city varchar(64), service varchar(64),inputNodeName varchar(256),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_TBL;
DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_CITY_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_NODE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE EXCEED_COUNT(QSTID BIGINT(20),exceed INTEGER);
CREATE TEMPORARY TABLE TRAFFIC_TBL(QSTID integer,IfSpeed integer,TxOctets bigint(20),Time_1 timeStamp DEFAULT '0000-00-00 00:00:00');

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT * from CUSTOMER_VRF_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT * from CUSTOMER_VRF_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT * from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT * from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT * from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT * from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

CREATE INDEX NodeNumberIndexNODEIF on TEMP_NODE_TBL(NodeNumber,IfIndex);

DROP TEMPORARY TABLE IF EXISTS TEMP_COSFCNAME_TBL;
CREATE TEMPORARY TABLE TEMP_COSFCNAME_TBL(QSTID int,NodeName varchar(256),IfDescr varchar(256),VRFName varchar(128), CosFCName varchar(64),IfSpeed INTEGER);
CREATE INDEX QSTIDIndex on TEMP_COSFCNAME_TBL(QSTID);

INSERT INTO TEMP_COSFCNAME_TBL
	SELECT QSTID, NodeName, IfDescr,vrfName, CosFCNAME,IfSpeed from TEMP_NODE_TBL a, COSFC_TBL b, COSQSTAT_TBL c  where c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber=c.QNumber;



INSERT INTO TRAFFIC_TBL
	SELECT QSTId,IfSpeed,a.TxOctets,Time_1 from ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL a JOIN TEMP_COSFCNAME_TBL b ON DiffId = QSTID where  Time_1>startTime and Time_1<endTime;

INSERT INTO EXCEED_COUNT
        select QSTId,count(TxOctets) from TRAFFIC_TBL where (TxOctets/(IfSpeed*10)>70 or TxOctets/(IfSpeed*10)>70) group by QSTId;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(DiffId int, maxTrafficValue bigint(20), avgTrafficValue bigint(20),peakTime varchar(20) DEFAULT '0000-00-00 00:00:00',exceedCount INTEGER DEFAULT 0);
CREATE INDEX DiffIDIndex2 on TEMP_TRAFFIC_TBL(DiffID);
CREATE INDEX QSTIdIn on TRAFFIC_TBL(QSTId);

INSERT INTO TEMP_TRAFFIC_TBL(DiffId, maxTrafficValue, avgTrafficValue)
SELECT QSTId,max(TxOctets),avg(TxOctets) FROM TRAFFIC_TBL group by QSTId;

UPDATE TEMP_TRAFFIC_TBL a set peakTime=(SELECT if(ROUND(a.maxTrafficValue/1000,2)=0,'NA',Time_1) from TRAFFIC_TBL b where a.DiffId=QSTId and a.maxTrafficValue=b.TxOctets order by Time_1 desc limit 1);

UPDATE TEMP_TRAFFIC_TBL a JOIN EXCEED_COUNT b ON a.DiffId = b.QSTId set a.exceedCount = b.exceed;


select substring(NodeName,1,3),
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
NodeName,VRFName,IfDescr,CosFCNAME,ROUND(maxTrafficValue/1000,2),ROUND(avgTrafficValue/1000,2),peakTime,exceedCount from TEMP_TRAFFIC_TBL JOIN TEMP_COSFCNAME_TBL ON DiffId = QSTID and avgTrafficValue >0 group by QSTID;



DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
END | 
 

 DROP PROCEDURE IF EXISTS 6_6_2_LspUtil; 
 CREATE PROCEDURE 6_6_2_LspUtil(p_NodeName varchar(50000),p_city varchar(50000),p_LspName varchar(50000),startTime timestamp, endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS LSP_MAX_TRAFFIC;
DROP TEMPORARY TABLE IF EXISTS LSP_PEAK_TIME;
DROP TEMPORARY TABLE IF EXISTS finalFetching;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE1_TBL;

CREATE TEMPORARY TABLE TEMP_NODE_TBL(NodeName varchar(256),NodeNumber int);


CREATE TEMPORARY TABLE LSP_MAX_TRAFFIC(LspID smallint(5) unsigned, maxTraffic bigint(20) unsigned,avgTraffic bigint(20) unsigned,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
SET @where=' where 1';
IF(p_NodeName!="ALL")
THEN
SET @where=CONCAT(@where," and NodeName in (",p_NodeName,")");
END IF;
IF(p_city!="ALL")
THEN
SET @where=CONCAT(@where," and substring(NodeName,1,3) in (",p_city,")");
END IF;

SET @query=CONCAT("INSERT INTO TEMP_NODE_TBL  SELECT NodeName,NodeNumber from NODE_TBL",@where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


IF(p_LspName = 'ALL')
THEN
	INSERT INTO LSP_MAX_TRAFFIC(LspID , maxTraffic, avgTraffic) SELECT a.LspID,MAX(LspOctets),AVG(LspOctets) FROM ROUTERTRAFFIC_LSP_SCALE1_TBL a where startTime<a.Time_1 AND endTime>a.Time_1 GROUP BY LspID;
ELSE
	SET @query1=CONCAT("INSERT INTO LSP_MAX_TRAFFIC(LspID , maxTraffic, avgTraffic) SELECT a.LspID,MAX(LspOctets),AVG(LspOctets) FROM ROUTERTRAFFIC_LSP_SCALE1_TBL a JOIN LSP_TBL b On a.lspId = b.lspID where b.LspName IN (",p_LspName,") AND '",startTime,"'<a.Time_1 AND '",endTime,"'>a.Time_1 GROUP BY LspID ");
	PREPARE stmt1 from @query1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
END IF;
UPDATE LSP_MAX_TRAFFIC a JOIN ROUTERTRAFFIC_LSP_SCALE1_TBL b ON a.LspID=b.LspID and a.maxTraffic=b.LspOctets set a.peakTime=if(ROUND(a.maxTraffic/1000,2)=0,'NA',Time_1) where startTime<b.Time_1 AND endTime>b.Time_1;



CREATE TEMPORARY TABLE finalFetching (LspID smallint(6), LspName varchar(128),PathName VARCHAR(500),OriNodeName varchar(128), TerNodeName varchar(128),PathType VARCHAR(30), status int,maxTraffic bigint(20) unsigned, avgTraffic bigint(20) unsigned,peakTime varchar(20));

INSERT INTO finalFetching( LspID,LspName,PathName, OriNodeName, TerNodeName,PathType,Status,maxTraffic, avgTraffic,peakTime) SELECT b.LspID,b.LspName,b.PathName,c.NodeName,d.NodeName,b.PathType,b.Status,maxTraffic,avgTraffic,peakTime  FROM LSP_MAX_TRAFFIC a, LSP_TBL b ,TEMP_NODE_TBL c, NODE_TBL d  WHERE a.LspID=b.LspID and b.OriNodeNumber=c.NodeNumber AND b.TerNodeNumber=d.NodeNumber;



SELECT LspName,PathName,OriNodeName, TerNodeName,PathType,IF((status=1),'UP','DOWN'),ROUND( maxTraffic/1000,2), ROUND(avgTraffic/1000,2),peakTime FROM finalFetching ORDER BY LspID ;




END | 
 

 DROP PROCEDURE IF EXISTS 6_6_3_TempUtil; 
 CREATE PROCEDURE 6_6_3_TempUtil(city varchar(5000), service varchar(5000), inputNodeName varchar(8000),startTime timestamp,endTime timestamp)
BEGIN
DECLARE countNo int;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_TEMPERATURE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TEMPERATURE_STATS_TBL;

CREATE TEMPORARY TABLE TEMP_MAX_AVG_TEMPERATURE(NodeNumber int, NodeName varchar(128), TempName varchar(50), maxTempValue int(10) unsigned, avgTempValue int(10) unsigned,PeakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

 CREATE TEMPORARY TABLE `TEMP_TEMPERATURE_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `TempName` varchar(50) DEFAULT NULL,
  `TempValue` int(10) unsigned DEFAULT NULL,
  `TimeStamp` timestamp
  );

INSERT INTO TEMP_TEMPERATURE_STATS_TBL
SELECT b.* from TEMP_STATS_TBL b JOIN TEMP_NODE_TBL a ON a.NodeNumber=b.NodeNo where TimeStamp>startTime AND TimeStamp<endTime;

INSERT INTO TEMP_MAX_AVG_TEMPERATURE (NodeNumber, NodeName, TempName, maxTempValue, avgTempValue)
SELECT a.NodeNumber, a.NodeName,TempName, MAX(b.TempValue), AVG(b.TempValue) FROM TEMP_NODE_TBL a,TEMP_TEMPERATURE_STATS_TBL b WHERE a.NodeNumber=b.NodeNo GROUP BY b.NodeNo,b.TempName;

CREATE INDEX Index1 ON TEMP_MAX_AVG_TEMPERATURE(NodeNumber,TempName);
CREATE INDEX Index2 ON TEMP_TEMPERATURE_STATS_TBL(NodeNo,TempName);

update TEMP_MAX_AVG_TEMPERATURE A JOIN TEMP_TEMPERATURE_STATS_TBL B  ON B.NodeNo=A.NodeNumber and A.TempName=B.TempName  Set PeakTime=if(ROUND(A.maxTempValue,2)=0,'NA',B.TimeStamp) where  A.maxTempValue=B.TempValue ;

DROP  INDEX Index1 ON TEMP_MAX_AVG_TEMPERATURE;
DROP INDEX Index2 ON TEMP_TEMPERATURE_STATS_TBL;


SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(TempName,',',':'), ROUND(maxTempValue,2), ROUND(avgTempValue,2),PeakTime from TEMP_MAX_AVG_TEMPERATURE;




DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_TEMPERATURE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TEMPERATURE_STATS_TBL;
END | 
 

 DROP PROCEDURE IF EXISTS 6_6_4_BufferUtil; 
 CREATE PROCEDURE 6_6_4_BufferUtil(city varchar(5000), service varchar(5000), inputNodeName varchar(8000),startTime timestamp,endTime timestamp)
BEGIN

DECLARE countNo int;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_BUFFER;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_BUFFER_STATS_TBL;

CREATE TEMPORARY TABLE TEMP_MAX_AVG_BUFFER(NodeNumber int, NodeName varchar(128), BufferName varchar(50), maxBufferValue int(10) unsigned, avgBufferValue int(10) unsigned,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;


CREATE TEMPORARY TABLE `TEMP_BUFFER_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `BufferName` varchar(50) DEFAULT NULL,
  `BufferValue` int(10) unsigned DEFAULT NULL,
  `TimeStamp` timestamp
);

INSERT INTO TEMP_BUFFER_STATS_TBL
SELECT a.* from BUFFER_STATS_TBL a JOIN TEMP_NODE_TBL b ON b.NodeNumber=a.NodeNo where TimeStamp>startTime AND TimeStamp<endTime;
SELECT COUNT(*) INTO countNo FROM TEMP_NODE_TBL;

INSERT INTO TEMP_MAX_AVG_BUFFER (NodeNumber, NodeName, BufferName, maxBufferValue, avgBufferValue)
SELECT a.NodeNumber, a.NodeName,BufferName, MAX(b.BufferValue), AVG(b.BufferValue) FROM TEMP_NODE_TBL a,TEMP_BUFFER_STATS_TBL b  where a.NodeNumber=b.NodeNo GROUP BY b.NodeNo,b.BufferName;

CREATE INDEX Index1 ON TEMP_MAX_AVG_BUFFER(NodeNumber,BufferName);
CREATE INDEX Index2 ON TEMP_BUFFER_STATS_TBL(NodeNo,BufferName);


update TEMP_MAX_AVG_BUFFER A JOIN TEMP_BUFFER_STATS_TBL B ON B.NodeNo=A.NodeNumber and A.BufferName=B.BufferName set  PeakTime=if(ROUND(A.maxBufferValue,2)=0,'NA',B.TimeStamp) where A.maxBufferValue=B.BufferValue ;

DROP INDEX Index1 ON TEMP_MAX_AVG_BUFFER;
DROP INDEX Index2 ON TEMP_BUFFER_STATS_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;

SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(BufferName,',',':'), ROUND(maxBufferValue,2), ROUND(avgBufferValue,2), peakTime  FROM TEMP_MAX_AVG_BUFFER order by NodeName;


DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_BUFFER;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_BUFFER_STATS_TBL;
END | 
 

 DROP PROCEDURE IF EXISTS 6_6_5_CpuUtil; 
 CREATE PROCEDURE 6_6_5_CpuUtil(city varchar(20000), service varchar(20000), inputNodeName varchar(20000),startTime timestamp, endTime timestamp)
BEGIN

DECLARE countNo int;
DECLARE i int;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_UTIL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_TIME;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_CPU;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_STATS_TBL;


CREATE TEMPORARY TABLE TEMP_MAX_AVG_CPU(NodeNumber int, NodeName varchar(128), cpuName varchar(50), maxCpuUtil int(10) unsigned, avgCpuUtil int(10) unsigned,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int,VendorName varchar(20));
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int,VendorName varchar(20));
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int,VendorName varchar(20));

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber,VendorName from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber,VendorName from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber,VendorName from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

 CREATE TEMPORARY TABLE `TEMP_CPU_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `CpuName` varchar(50) DEFAULT NULL,
  `CpuUtil` int(10) unsigned DEFAULT NULL,
  `CompState` int(10) unsigned DEFAULT NULL,
  `TimeStamp` timestamp 
 );

INSERT INTO TEMP_CPU_STATS_TBL
SELECT a.* from CPU_STATS_TBL a JOIN TEMP_NODE_TBL b ON b.NodeNumber=a.NodeNo where TimeStamp>startTime AND TimeStamp<endTime;
SELECT COUNT(*) INTO countNo FROM TEMP_NODE_TBL;
SET i=0;


INSERT INTO TEMP_MAX_AVG_CPU (NodeNumber, NodeName, cpuName, maxCpuUtil, avgCpuUtil)
SELECT a.NodeNumber, a.NodeName, cpuName, MAX(b.cpuUtil), AVG(b.cpuUtil) FROM TEMP_NODE_TBL a JOIN TEMP_CPU_STATS_TBL b   ON a.NodeNumber=b.NodeNo where (a.VendorName like '%cisco%') or  (a.VendorName like '%juniper%' and cpuName like '%Routing Engine%') GROUP BY b.NodeNo,b.cpuName;



CREATE INDEX index1 ON TEMP_MAX_AVG_CPU(NodeNumber,cpuName);
CREATE INDEX index2 ON TEMP_CPU_STATS_TBL(NodeNo,cpuName);
update TEMP_MAX_AVG_CPU A JOIN TEMP_CPU_STATS_TBL B ON B.NodeNo=A.NodeNumber and A.cpuName=B.cpuName set PeakTime= if(ROUND(A.maxCpuUtil,2)=0,'NA',B.TimeStamp) where A.maxCpuUtil=B.cpuUtil ;
DROP INDEX index1 ON TEMP_MAX_AVG_CPU;
DROP INDEX index2 ON TEMP_CPU_STATS_TBL;

SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(cpuName,',',':'),ROUND(maxCpuUtil,2), ROUND(avgCpuUtil,2), peakTime  FROM TEMP_MAX_AVG_CPU order by NodeName;

DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_CPU;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;


END | 
 

 DROP PROCEDURE IF EXISTS 6_6_6_StorageUtil; 
 CREATE PROCEDURE 6_6_6_StorageUtil(city varchar(20000), service varchar(20000), inputNodeName varchar(20000),startTime timestamp, endTime timestamp)
BEGIN

DECLARE countNo int;
DECLARE i int;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_UTIL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_TIME;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_CPU;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_STORAGE_STATS_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_STORAGE;

CREATE TEMPORARY TABLE TEMP_MAX_AVG_STORAGE(NodeNumber int, NodeName varchar(128), storageName varchar(50), maxStorageUtil float, avgStorageUtil float,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

 CREATE TEMPORARY TABLE `TEMP_STORAGE_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `StorageName` varchar(50) DEFAULT NULL,
   StorageUtil float,
   TimeStamp timestamp
  );

INSERT INTO TEMP_STORAGE_STATS_TBL
SELECT a.NodeNo,a.StorageName,(a.StorageUsed/StorageSize)*100,a.TimeStamp from STORAGE_STATS_TBL a JOIN TEMP_NODE_TBL b ON b.NodeNumber=a.NodeNo where StorageSize != 0 and TimeStamp>startTime AND TimeStamp<endTime;

INSERT INTO TEMP_MAX_AVG_STORAGE (NodeNumber, NodeName, storageName, maxStorageUtil, avgStorageUtil)
SELECT a.NodeNumber, a.NodeName,storageName, MAX(b.StorageUtil), AVG(b.StorageUtil) FROM TEMP_NODE_TBL a JOIN TEMP_STORAGE_STATS_TBL b   ON a.NodeNumber=b.NodeNo  GROUP BY b.NodeNo,b.StorageName;

CREATE INDEX index1 ON TEMP_MAX_AVG_STORAGE(NodeNumber,storageName);
CREATE INDEX index2 ON TEMP_STORAGE_STATS_TBL(NodeNo,storageName);
update TEMP_MAX_AVG_STORAGE A JOIN TEMP_STORAGE_STATS_TBL B ON B.NodeNo=A.NodeNumber and A.storageName=B.storageName set PeakTime= if(ROUND(A.maxStorageUtil,2)=0,'NA',B.TimeStamp) where A.maxStorageUtil=B.storageUtil;
DROP INDEX index1 ON TEMP_MAX_AVG_STORAGE;
DROP INDEX index2 ON TEMP_STORAGE_STATS_TBL;


SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(storageName,',',':'),ROUND(maxStorageUtil,2), ROUND(avgStorageUtil,2), peakTime  FROM TEMP_MAX_AVG_STORAGE order by NodeName ;




DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_STORAGE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;


END | 
 

 DROP PROCEDURE IF EXISTS 6_6_7_unusedServicePolicy; 
 CREATE PROCEDURE 6_6_7_unusedServicePolicy(city varchar(5000), service varchar(5000), inputNodeName varchar(8000))
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODENAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_POLICIES;

CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),nodeNumber integer,VendorName VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),nodeNumber integer,VendorName VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),nodeNumber integer,VendorName VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_POLICIES(QNumber integer,CosFCName varchar(256));

SET SESSION group_concat_max_len = 1000000;

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber,VendorName from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber,VendorName from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber,VendorName from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL1;
CREATE TEMPORARY TABLE TEMP_NODE_TBL1 as select * from TEMP_NODE_TBL;

DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_POLICY_TBL;
CREATE TEMPORARY TABLE FINAL_UNUSED_POLICY_TBL(
	ServiceFinal varchar(256),
	CityFinal varchar(256),
	RouterName varchar(256), 
	ConfigPolicyCount varchar(100), 
	UsedPolicyCount varchar(100), 
	UnusedPolicyCount varchar(100), 
	UnusedPolicyPer varchar(100), 
	UnusedList varchar(50000));


drop temporary table if exists totalPolicyCount;
create temporary table totalPolicyCount as 
	SELECT a.NodeNumber,COUNT(*) as policyCount FROM TEMP_NODE_TBL a JOIN COSFC_TBL b where a.VendorName like 'Juniper' and a.NodeNumber = b.NodeNumber and CosFCName not like '%COPP%' group by a.NodeNumber;

drop temporary table if exists cosfc;
create temporary table cosfc(index (NodeNumber,CosQNumber)) as select NodeNumber,CosQNumber,CosFCName from COSFC_TBL where NodeNumber IN (select NodeNumber from TEMP_NODE_TBL where VendorName like 'Juniper');

drop temporary table if exists cosqstat;
create temporary table cosqstat(index (NodeNumber,QNumber)) as select NodeNumber,QNumber from COSQSTAT_TBL where NodeNumber IN (select NodeNumber from TEMP_NODE_TBL where VendorName like 'Juniper');

drop temporary table if exists unUsedPolicyCount;
create temporary table unUsedPolicyCount as
	SELECT A.NodeNumber,count(*) as unUsedCount,GROUP_CONCAT(CosFCName) as unusedlist FROM cosfc A where CONCAT(A.NodeNumber,'-',A.CosQNumber) NOT IN
                (SELECT CONCAT(B.NodeNumber,'-',B.QNumber) FROM cosqstat B) 
		and CosFCName not like '%COPP%' group by A.NodeNumber; 
drop temporary table if exists cosfc;
drop temporary table if exists cosqstat;

insert into totalPolicyCount
	SELECT a.NodeNumber,COUNT(*) as policyCount FROM TEMP_NODE_TBL a JOIN QOS_NAME_TBL b where a.VendorName like 'Cisco' and a.NodeNumber = b.NodeNumber group by a.NodeNumber;
insert into unUsedPolicyCount 
	SELECT a.NodeNumber,count(*) as unUsedCount ,GROUP_CONCAT(Name) as unusedlist FROM TEMP_NODE_TBL a JOIN QOS_NAME_TBL b ON a.NodeNumber = b.NodeNumber
LEFT JOIN QOS_TBL c ON b.NodeNumber = c.NodeNumber and b.ConfigIndex = c.ConfigIndex where c.NodeNumber IS NULL and c.configIndex IS NULL group by a.NodeNumber;


	INSERT INTO FINAL_UNUSED_POLICY_TBL 
	select 
	CASE
	WHEN c.Nodename like '%MPL%' THEN "MPL"
	WHEN c.Nodename like '%ISP%' THEN "ISP"
	WHEN c.Nodename like '%CNV%' THEN "CNV"
	ELSE "-"
	END ,
	substring(c.Nodename,1,3),(c.Nodename),policyCount,(policyCount-unUsedCount),unUsedCount,ROUND((unUsedCount*100/policyCount),2),(unusedlist) from totalPolicyCount a,unUsedPolicyCount b,TEMP_NODE_TBL c where a.NodeNumber = b.NodeNumber and a.NodeNumber = c.NodeNumber;

SELECT * FROM FINAL_UNUSED_POLICY_TBL; 


END | 
 

 DROP PROCEDURE IF EXISTS 6_6_8_unusedVrf; 
 CREATE PROCEDURE 6_6_8_unusedVrf(city varchar(5000), service varchar(5000), inputNodeName varchar(8000))
BEGIN
DECLARE vrfCount INTEGER;
DECLARE usedVrfCount INTEGER;
DECLARE unUsedVrfCount INTEGER;
DECLARE countNo INTEGER;
DECLARE unusedPer FLOAT;
SET @unusedlist="";

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODENAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_VRF;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_VRF_TBL;

CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_VRF(QNumber integer,VpnName VARCHAR(256));

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;



SELECT COUNT(*) INTO countNo FROM TEMP_NODE_TBL;

DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_VRF_TBL;
CREATE TEMPORARY TABLE FINAL_UNUSED_VRF_TBL(
	ServiceFinal varchar(100),
	CityFinal varchar(100),
	RouterName varchar(256), 
	ConfigVrfCount varchar(100), 
	UsedVrfCount varchar(100), 
	UnusedVrfCount varchar(100), 
	UnusedVrfPerc varchar(100),
	UnusedVRFList VARCHAR(50000));


WHILE(countNo>0)
DO

SELECT nodeNumber,nodeName INTO @node, @routername FROM TEMP_NODE_TBL LIMIT 1;
SELECT COUNT(*) INTO vrfCount FROM (SELECT DISTINCT VpnID FROM MAP_VPN_NODE_TBL WHERE VpnNode= @node)c;
SELECT COUNT(*) INTO usedVrfCount FROM (SELECT DISTINCT VpnID FROM VPN_IFINDEX_TBL WHERE VpnNode=@node) c;
SET unUsedVrfCount = vrfCount - usedVrfCount;



INSERT INTO TEMP_VRF(QNumber)
SELECT distinct VpnId FROM  MAP_VPN_NODE_TBL where VpnNode=@node and VpnId NOT IN
(SELECT  VpnId FROM VPN_IFINDEX_TBL WHERE VpnNode=@node) group by VpnId;

UPDATE TEMP_VRF a, VPN_TBL b set a.VpnName = b.VpnName where a.QNumber = b.VpnID;

SET @unusedlist="";
SELECT count(*) INTO @listcount FROM TEMP_VRF;
        WHILE(@listcount > 0)
        DO
                SELECT VpnName INTO @qno FROM TEMP_VRF LIMIT 1;
                SET @unusedlist=CONCAT(@qno,",",@unusedlist);
                DELETE FROM TEMP_VRF LIMIT 1;
                SET @listcount = @listcount - 1;
        END WHILE;

SELECT substring(@unusedlist,1,length(@unusedlist)-1) INTO @unusedlist;
SET unusedPer=unUsedVrfCount*100/vrfCount;
IF unUsedVrfCount=0
THEN
        SET @unusedlist="";
        SET unusedPer=0.0;
END IF;



INSERT INTO FINAL_UNUSED_VRF_TBL VALUES(
CASE
WHEN @routername like '%MPL%' THEN "MPL"
WHEN @routername like '%ISP%' THEN "ISP"
WHEN @routername like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(@routername,1,3),(@routername),vrfCount,usedVrfCount,unUsedVrfCount,ROUND(unusedPer,2),(@unusedlist));

DELETE FROM TEMP_NODE_TBL LIMIT 1;
SET countNo=countNo-1;
END WHILE;



SELECT * FROM FINAL_UNUSED_VRF_TBL; 


DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_VRF_TBL;
END | 
 

 DROP PROCEDURE IF EXISTS 6_6_9_PortUtil; 
 CREATE PROCEDURE 6_6_9_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
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
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500),PortID BIGINT(20) DEFAULT 0);
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
	inPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00');


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
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias like '%AESI-IN%' OR UPPER(IFAlias) like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%';
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias NOT like '%AESI-IN%'  AND UPPER(IFAlias) not like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%';
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

set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_NODEANDIF_TBL;


set @query = CONCAT("INSERT INTO TRAFFIC_TBL(PortID,InErrPkts,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;

update TRAFFIC_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID set a.IfDescr = b.IfDescr , a.NodeName = b.NodeName , a.IfSpeed = b.IfSpeed,a.Time_1 = a.Time_1;

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

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

SELECT (substring(NodeName,1,3)) as City,
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
(NodeName),(IfDescr),ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID;








END | 
 

 DROP PROCEDURE IF EXISTS 95percentile; 
 CREATE PROCEDURE 95percentile(targetTbl VARCHAR(500), joinTbl VARCHAR(500), priKey VARCHAR(50), targetCol VARCHAR(1000))
BEGIN

DROP TEMPORARY TABLE  IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS minRowForPort;
DROP TEMPORARY TABLE IF EXISTS entityIDRowNumber;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL (indexColumn INTEGER primary key auto_increment not null,entityID INTEGER, traffic BIGINT);
CREATE TEMPORARY TABLE minRowForPort(entityID INTEGER, minRow INTEGER);
SET @INSERTQuery=CONCAT("INSERT INTO TEMP_TRAFFIC_TBL(entityID, traffic) SELECT ",priKey,',',targetCol, ' FROM ', targetTbl ,' order by ',priKey,',',targetCol,' ASC');
PREPARE stmt3 FROM @INSERTQuery;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;
INSERT INTO minRowForPort SELECT entityID,min(indexColumn) FROM TEMP_TRAFFIC_TBL GROUP BY entityID;

CREATE TEMPORARY TABLE entityIDRowNumber (entityID INTEGER, rowNumber INTEGER);
INSERT INTO entityIDRowNumber(entityID,rowNumber) SELECT entityID, ROUND(95*count(*)/100) FROM TEMP_TRAFFIC_TBL GROUP BY entityID;

CREATE INDEX entityIndex1 ON TEMP_TRAFFIC_TBL(entityID);
CREATE INDEX entityIndex2 ON minRowForPort(entityID);
CREATE INDEX entityIndex3 ON entityIDRowNumber(entityID);

DROP TEMPORARY TABLE IF EXISTS FINAL_TRAF95_TBL;
CREATE TEMPORARY TABLE FINAL_TRAF95_TBL(EntityID INTEGER, Traffic BIGINT,rowNumber int);

INSERT INTO FINAL_TRAF95_TBL SELECT a.entityID, a.traffic, indexColumn FROM TEMP_TRAFFIC_TBL a, minRowForPort b , entityIDRowNumber c WHERE a.entityID=b.entityID AND b.entityID=c.entityID AND indexColumn=(minRow+rowNumber-1) ;

DROP INDEX entityIndex1 ON TEMP_TRAFFIC_TBL;
DROP INDEX entityIndex2 ON minRowForPort;
DROP INDEX entityIndex3 ON entityIDRowNumber;

END | 
 

 DROP PROCEDURE IF EXISTS addColumnInCOSQSTAT; 
 CREATE PROCEDURE addColumnInCOSQSTAT()
BEGIN
ALTER TABLE COSQSTAT_TBL ADD COLUMN PollingEnabled INTEGER;
UPDATE COSQSTAT_TBL A JOIN VLANPRT_TBL B SET PollingEnabled=1 where A.NodeNumber=B.NodeID and A.IfIndex=B.IfIndex and Class IN ('A','B','C');
UPDATE COSQSTAT_TBL A JOIN VLANPRT_TBL B SET PollingEnabled=0 where A.NodeNumber=B.NodeID and A.IfIndex=B.IfIndex and Class='D';
END | 
 

 DROP PROCEDURE IF EXISTS anec_customerUsage; 
 CREATE PROCEDURE anec_customerUsage(startTime timestamp, endTime timestamp)
BEGIN

SET @startTime=startTime;
SET @endTime=endTime;

DROP TEMPORARY TABLE IF EXISTS TEMP_TOTAL_USAGE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTUSAGE;
CREATE TEMPORARY TABLE TEMP_TOTAL_USAGE (PortID integer, TotalUsage float);
CREATE TEMPORARY TABLE TEMP_CUSTUSAGE(startTime varchar(256), endTime varchar(256), CustomerName varchar(256),TotalUsage varchar(256));

INSERT INTO TEMP_TOTAL_USAGE
SELECT PortID,SUM((TxOctets+RcvOctets)/(1000*1000))  FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL WHERE Time_1>startTime and Time_1<endTime GROUP BY PortID ;

SET @startTimeTemp=DATE_FORMAT(@startTime,'%m/%d/%Y %H:%i:00:000000');
SET @endTimeTemp=DATE_FORMAT(@endTime,'%m/%d/%Y %H:%i:00:000000');
SET @startTimeFinal= SUBSTRING(@startTimeTemp,1,CHAR_LENGTH(@startTimeTemp)-4);
SET @endTimeFinal= SUBSTRING(@endTimeTemp,1,CHAR_LENGTH(@endTimeTemp)-4);

SET @fileNameTime=DATE_FORMAT(@endTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @fileName='/export/home/anec/Files_for_ANEC/IPPMS_Internet_Data_Usage_';
SET @finalFileName=CONCAT(CONCAT(@fileName,@fileNameTimeFinal),'.csv');

INSERT INTO TEMP_CUSTUSAGE
SELECT 'Start Time','End Time','Customer Name','Total Usage';
SET @query1=CONCAT('INSERT INTO TEMP_CUSTUSAGE SELECT  @startTimeFinal, @endTimeFinal, CONCAT(\"\'\",a.VpnName,\"\'\"), ROUND(SUM(TotalUsage),2) FROM CUSTOMER_VRF_TBL a, TEMP_TOTAL_USAGE b WHERE a.PortId=b.PortId  GROUP BY a.VpnName');
PREPARE stmt1 from @query1;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

set @query=CONCAT(CONCAT('select * from TEMP_CUSTUSAGE into outfile \'',@finalFileName),'\' fields terminated by \',\' ');
PREPARE stmt2 from @query;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

END | 
 

 DROP PROCEDURE IF EXISTS anec_customerUsage_test; 
 CREATE PROCEDURE anec_customerUsage_test(startTime timestamp, endTime timestamp)
BEGIN

SET @startTime=startTime;
SET @endTime=endTime;

DROP TEMPORARY TABLE IF EXISTS TEMP_TOTAL_USAGE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTUSAGE;
CREATE TEMPORARY TABLE TEMP_TOTAL_USAGE (PortID integer, TotalUsage float);
CREATE TEMPORARY TABLE TEMP_CUSTUSAGE(startTime varchar(256), endTime varchar(256), CustomerName varchar(256),TotalUsage varchar(256));

INSERT INTO TEMP_TOTAL_USAGE
SELECT PortID,SUM((TxOctets+RcvOctets)/(1000*1000))  FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL WHERE Time_1>startTime and Time_1<endTime GROUP BY PortID ;

SET @startTimeTemp=DATE_FORMAT(@startTime,'%m/%d/%Y %H:%i:%s:%f');
SET @endTimeTemp=DATE_FORMAT(@endTime,'%m/%d/%Y %H:%i:%s:%f');
SET @startTimeFinal= SUBSTRING(@startTimeTemp,1,CHAR_LENGTH(@startTimeTemp)-4);
SET @endTimeFinal= SUBSTRING(@endTimeTemp,1,CHAR_LENGTH(@endTimeTemp)-4);

SET @fileNameTime=DATE_FORMAT(@endTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @fileName='/export/home/anec/Files_for_ANEC_testing/IPPMS_Internet_Data_Usage_';
SET @finalFileName=CONCAT(CONCAT(@fileName,@fileNameTimeFinal),'.csv');

INSERT INTO TEMP_CUSTUSAGE
SELECT 'Start Time','End Time','Customer Name','Total Usage';
SET @query1=CONCAT('INSERT INTO TEMP_CUSTUSAGE SELECT  @startTimeFinal, @endTimeFinal, CONCAT(\"\'\",a.VpnName,\"\'\"), ROUND(SUM(TotalUsage),2) FROM CUSTOMER_VRF_TBL a, TEMP_TOTAL_USAGE b WHERE a.PortId=b.PortId  GROUP BY a.VpnName');
PREPARE stmt1 from @query1;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

set @query=CONCAT(CONCAT('select * from TEMP_CUSTUSAGE into outfile \'',@finalFileName),'\' fields terminated by \',\' ');
PREPARE stmt2 from @query;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

END | 
 

 DROP PROCEDURE IF EXISTS anec_delayJitter; 
 CREATE PROCEDURE anec_delayJitter(startTime timestamp, endTime timestamp)
BEGIN


DECLARE fileNameF varchar(256);
SET @startTime=startTime;
DROP TEMPORARY TABLE IF EXISTS TEMP_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ANECDELAY_TBL;
CREATE TEMPORARY TABLE TEMP_TBL(LinkName varchar(256),avgDelay float,packetLoss int,avgJitter int);
CREATE TEMPORARY TABLE TEMP_ANECDELAY_TBL(startTime varchar(256),endTime varchar(256),LinkName varchar(256),avgDelay varchar(256),packetLoss varchar(256),avgJitter varchar(256));

INSERT INTO TEMP_TBL
SELECT DISTINCT CONCAT(CONCAT('\'',CONCAT(CONCAT(d.Location,'::'),e.Location)),'\''),AvgDelay,PacketLoss,AvgJitter FROM VPN_DELAY_TABLE a, NODE_TBL b, NODE_TBL c ,shadowRouter d, shadowRouter e  where   a.SourceIp=b.NodeId and a.DestinationIp=c.NodeId and b.NodeName=d.NodeName and c.NodeName=e.NodeName  and Time_1>@startTime and Time_1<endTime group by CONCAT(CONCAT(d.Location,'::'),e.Location);



SET @endTime=endTime;
SET @startTimeTemp=DATE_FORMAT(@startTime,'%m/%d/%Y %k:%i:00:000000');
SET @endTimeTemp=DATE_FORMAT(@endTime,'%m/%d/%Y %k:%i:00:000000');
SET @startTimeFinal= SUBSTRING(@startTimeTemp,1,CHAR_LENGTH(@startTimeTemp)-4);
SET @endTimeFinal= SUBSTRING(@endTimeTemp,1,CHAR_LENGTH(@endTimeTemp)-4);

SET @fileNameTime=DATE_FORMAT(@endTime,'%m_%d_%Y-%k_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @fileName='/export/home/anec/Files_for_ANEC/IPPMS_MPLS_Servicel_KPIs_';
SET @finalFileName=CONCAT(CONCAT(@fileName,@fileNameTimeFinal),'.csv');
set fileNameF=@finalFileName;
INSERT INTO TEMP_ANECDELAY_TBL 
SELECT 'Start Time','End Time','Link Name', 'Latency', 'Packet Loss', 'Jitter';


INSERT INTO TEMP_ANECDELAY_TBL
SELECT @startTimeFinal,@endTimeFinal,LinkName,avgDelay,packetLoss, avgJitter from TEMP_TBL;


SET @query=CONCAT(CONCAT('select * from TEMP_ANECDELAY_TBL into outfile \'',fileNameF),'\' fields terminated by \',\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS anec_delayJitter_test; 
 CREATE PROCEDURE anec_delayJitter_test(startTime timestamp, endTime timestamp)
BEGIN


DECLARE fileNameF varchar(256);
SET @startTime=startTime;
DROP TEMPORARY TABLE IF EXISTS TEMP_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ANECDELAY_TBL;
CREATE TEMPORARY TABLE TEMP_TBL(LinkName varchar(256),avgDelay float,packetLoss int,avgJitter int);
CREATE TEMPORARY TABLE TEMP_ANECDELAY_TBL(startTime varchar(256),endTime varchar(256),LinkName varchar(256),avgDelay varchar(256),packetLoss varchar(256),avgJitter varchar(256));

INSERT INTO TEMP_TBL
SELECT DISTINCT CONCAT(CONCAT('\'',CONCAT(CONCAT(d.Location,'::'),e.Location)),'\''),AvgDelay,PacketLoss,AvgJitter FROM VPN_DELAY_TABLE a, NODE_TBL b, NODE_TBL c ,shadowRouter d, shadowRouter e  where   a.SourceIp=b.NodeId and a.DestinationIp=c.NodeId and b.NodeName=d.NodeName and c.NodeName=e.NodeName  and Time_1>@startTime and Time_1<endTime group by CONCAT(CONCAT(d.Location,'::'),e.Location);



SET @endTime=endTime;
SET @startTimeTemp=DATE_FORMAT(@startTime,'%m/%d/%Y %k:%i:%s:%f');
SET @endTimeTemp=DATE_FORMAT(@endTime,'%m/%d/%Y %k:%i:%s:%f');
SET @startTimeFinal= SUBSTRING(@startTimeTemp,1,CHAR_LENGTH(@startTimeTemp)-4);
SET @endTimeFinal= SUBSTRING(@endTimeTemp,1,CHAR_LENGTH(@endTimeTemp)-4);

SET @fileNameTime=DATE_FORMAT(@endTime,'%m_%d_%Y-%k_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @fileName='/export/home/anec/Files_for_ANEC_testing/IPPMS_MPLS_Servicel_KPIs_';
SET @finalFileName=CONCAT(CONCAT(@fileName,@fileNameTimeFinal),'.csv');
set fileNameF=@finalFileName;
INSERT INTO TEMP_ANECDELAY_TBL 
SELECT 'Start Time','End Time','Link Name', 'Latency', 'Packet Loss', 'Jitter';


INSERT INTO TEMP_ANECDELAY_TBL
SELECT @startTimeFinal,@endTimeFinal,LinkName,ROUND(avgDelay,2),ROUND(packetLoss,2),ROUND( avgJitter,2) from TEMP_TBL;

SELECT "Delay Jitter Repoft file : ", fileNameF;

SET @query=CONCAT(CONCAT('select * from TEMP_ANECDELAY_TBL into outfile \'',fileNameF),'\' fields terminated by \',\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS anec_DSLUsage; 
 CREATE PROCEDURE anec_DSLUsage(StartTime TimeStamp, EndTime TimeStamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_VLAN_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_TBL;
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_VLAN_TBL(PortID BIGINT(20));
CREATE TEMPORARY TABLE IF NOT EXISTS FINAL_TBL(StartTime VARCHAR(100), EndTime VARCHAR(100), DSL_Usage VARCHAR(50),EnterpriseUsage VARCHAR(50));

INSERT INTO TEMP_VLAN_TBL
SELECT PrtID FROM DSL_TBL a,VLANPRT_TBL b 
WHERE a.NodeNumber=b.NodeID AND a.IfIndex=b.IfIndex;

SET @fileNameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%k_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @finalFileName=CONCAT(CONCAT('/export/home/anec/Files_for_ANEC/IPPMS_Bandwidth_Usage_',@fileNameTimeFinal),'.csv');




INSERT INTO FINAL_TBL VALUES('StartTime','EndTime','DSL Usage','Enterprise Usage');

INSERT INTO FINAL_TBL
SELECT DATE_FORMAT(StartTime,"%m/%d/%Y %k:%i:00:00"),DATE_FORMAT(EndTime,"%m/%d/%Y %k:%i:00:00"),SUM(RVlan.TxOctets+RVlan.RcvOctets)/(1000*1000*1000), 0 AS EnterpriseUsage 
FROM TEMP_VLAN_TBL T, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL RVlan 
WHERE T.PortID=RVlan.PortID AND RVlan.Time_1>StartTime AND RVlan.Time_1<EndTime;

set @final = CONCAT("select * from FINAL_TBL into outfile \'",@finalFileName,"\' FIELDS TERMINATED BY ',' ") ;


PREPARE stmt1 FROM @final;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS anec_DSLUsage_test; 
 CREATE PROCEDURE anec_DSLUsage_test(StartTime TimeStamp, EndTime TimeStamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_VLAN_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_TBL;
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_VLAN_TBL(PortID INTEGER);
CREATE TEMPORARY TABLE IF NOT EXISTS FINAL_TBL(StartTime VARCHAR(100), EndTime VARCHAR(100), DSL_Usage VARCHAR(50),EnterpriseUsage VARCHAR(50));

INSERT INTO TEMP_VLAN_TBL
SELECT PrtID FROM DSL_TBL a,VLANPRT_TBL b 
WHERE a.NodeNumber=b.NodeID AND a.IfIndex=b.IfIndex;

SET @fileNameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%k_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @finalFileName=CONCAT(CONCAT('/export/home/anec/Files_for_ANEC_testing/IPPMS_Bandwidth_Usage_',@fileNameTimeFinal),'.csv');




INSERT INTO FINAL_TBL VALUES('StartTime','EndTime','DSL Usage','Enterprise Usage');

INSERT INTO FINAL_TBL
SELECT DATE_FORMAT(StartTime,"%m/%d/%Y %k:%i:%s:%f"),DATE_FORMAT(EndTime,"%m/%d/%Y %k:%i:%s:%f"),ROUND((SUM(RVlan.TxOctets+RVlan.RcvOctets)/(1000*1000*1000)),2), 0 AS EnterpriseUsage 
FROM TEMP_VLAN_TBL T, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL RVlan 
WHERE T.PortID=RVlan.PortID AND RVlan.Time_1>StartTime AND RVlan.Time_1<EndTime;

set @final = CONCAT("select * from FINAL_TBL into outfile \'",@finalFileName,"\' FIELDS TERMINATED BY ',' Lines terminated by '
'") ;


PREPARE stmt1 FROM @final;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS anec_int_dom_peer; 
 CREATE PROCEDURE anec_int_dom_peer(StartTime TIMESTAMP,EndTime TIMESTAMP)
BEGIN

SET @StartTime='';
SET @StartTimeFinal='';
SET @EndTime=''; 
SET @EndTimeFinal='';
SET @filenameTime=''; 
SET @filenameTimeFinal='';

SET @StartTime=DATE_FORMAT(StartTime,'%m/%d/%Y %H:%i:00:000000');
SET @StartTimeFinal=SUBSTRING(@StartTime,1,CHAR_LENGTH(@StartTime)-4);

SET @EndTime=DATE_FORMAT(EndTime,'%m/%d/%Y %H:%i:00:000000');
SET @EndTimeFinal=SUBSTRING(@EndTime,1,CHAR_LENGTH(@EndTime)-4);

SET @filenameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @filenameTimeFinal=SUBSTRING(@filenameTime,1,CHAR_LENGTH(@filenameTime)-4);

SET @FileName=CONCAT(CONCAT('IPPMS_Int_Dom_Peering_Link_Util_',@filenameTimeFinal),'.csv');


DROP TEMPORARY TABLE IF EXISTS INT_DOM_PEER;
CREATE TEMPORARY TABLE IF NOT EXISTS INT_DOM_PEER(
STARTTIME VARCHAR(50),
ENDTIME VARCHAR(50),
PEERING_VPN VARCHAR(100),
INGRESS_UTILIZATION VARCHAR(50),
EGRESS_UTLIZATION VARCHAR(50));


INSERT INTO INT_DOM_PEER values ('STARTTIME','ENDTIME','PEERING_VPN','INGRESS_UTILIZATION','EGRESS_UTLIZATION');
INSERT INTO INT_DOM_PEER(select @StartTimeFinal,@EndTimeFinal,QUOTE(peerVPN),ROUND(SUM(RcvOctets)/(1000*1000*1000),2),ROUND(SUM(TxOctets)/(1000*1000*1000),2) 
from PEERING_VPN p, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL r 
where p.LinkID = r.PortID 
and r.Time_1 > startTime 
and r.Time_1 <= endTime 
group by peerVPN );

set @Query = CONCAT("select * from INT_DOM_PEER into outfile '/export/home/anec/Files_for_ANEC/",@FileName,"' FIELDS TERMINATED BY ',' ") ;
SELECT @Query;
PREPARE stmt1 from @Query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


END | 
 

 DROP PROCEDURE IF EXISTS anec_int_dom_peer_test; 
 CREATE PROCEDURE anec_int_dom_peer_test(StartTime TIMESTAMP,EndTime TIMESTAMP)
BEGIN

SET @StartTime='';
SET @StartTimeFinal='';
SET @EndTime=''; 
SET @EndTimeFinal='';
SET @filenameTime=''; 
SET @filenameTimeFinal='';

SET @StartTime=DATE_FORMAT(StartTime,'%m/%d/%Y %H:%i:%s:%f');
SET @StartTimeFinal=SUBSTRING(@StartTime,1,CHAR_LENGTH(@StartTime)-4);

SET @EndTime=DATE_FORMAT(EndTime,'%m/%d/%Y %H:%i:%s:%f');
SET @EndTimeFinal=SUBSTRING(@EndTime,1,CHAR_LENGTH(@EndTime)-4);

SET @filenameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @filenameTimeFinal=SUBSTRING(@filenameTime,1,CHAR_LENGTH(@filenameTime)-4);

SET @FileName=CONCAT(CONCAT('IPPMS_Int_Dom_Peering_Link_Util_',@filenameTimeFinal),'.csv');


DROP TEMPORARY TABLE IF EXISTS INT_DOM_PEER;
CREATE TEMPORARY TABLE IF NOT EXISTS INT_DOM_PEER(
STARTTIME VARCHAR(50),
ENDTIME VARCHAR(50),
PEERING_VPN VARCHAR(100),
INGRESS_UTILIZATION VARCHAR(50),
EGRESS_UTLIZATION VARCHAR(50));


INSERT INTO INT_DOM_PEER values ('STARTTIME','ENDTIME','PEERING_VPN','INGRESS_UTILIZATION','EGRESS_UTLIZATION');
INSERT INTO INT_DOM_PEER(select @StartTimeFinal,@EndTimeFinal,QUOTE(peerVPN),ROUND(SUM(RcvOctets)/(1000*1000*1000),2),ROUND(SUM(TxOctets)/(1000*1000*1000),2) 
from PEERING_VPN p, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL r 
where p.LinkID = r.PortID 
and r.Time_1 > startTime 
and r.Time_1 <= endTime 
group by peerVPN );

set @Query = CONCAT("select * from INT_DOM_PEER into outfile '/export/home/anec/Files_for_ANEC_testing/",@FileName,"' FIELDS TERMINATED BY ',' ") ;
PREPARE stmt1 from @Query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


END | 
 

 DROP PROCEDURE IF EXISTS anec_priv_peer; 
 CREATE PROCEDURE anec_priv_peer(startTime TIMESTAMP,endTime TIMESTAMP)
BEGIN

SET @StartTime=''; 
SET @StartTimeFinal='';
SET @EndTime=''; 
SET @EndTimeFinal='';
SET @filenameTime=''; 
SET @filenameTimeFinal='';

SET @StartTime=DATE_FORMAT(StartTime,'%m/%d/%Y %H:%i:00:000000');
SET @StartTimeFinal=SUBSTRING(@StartTime,1,CHAR_LENGTH(@StartTime)-4);

SET @EndTime=DATE_FORMAT(EndTime,'%m/%d/%Y %H:%i:00:000000');
SET @EndTimeFinal=SUBSTRING(@EndTime,1,CHAR_LENGTH(@EndTime)-4);

SET @filenameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @filenameTimeFinal=SUBSTRING(@filenameTime,1,CHAR_LENGTH(@filenameTime)-4);

SET @FileName=CONCAT(CONCAT('IPPMS_Private_Peering_Link_Util_',@filenameTimeFinal),'.csv');


DROP TEMPORARY TABLE IF EXISTS PRIVATE_PEER;
CREATE TEMPORARY TABLE IF NOT EXISTS PRIVATE_PEER(
        STARTTIME       VARCHAR(50),
        ENDTIME         VARCHAR(50),
        PEERING_NE_FUNCTION     VARCHAR(100),
        PEERING_DESTINATION     VARCHAR(100),
        INGRESS_UTILIZATION     VARCHAR(50),
        EGRESS_UTLIZATION       VARCHAR(50));

INSERT INTO PRIVATE_PEER values ('STARTTIME','ENDTIME','PEERING_NE_FUNCTION','PEERING_DESTINATION','INGRESS_UTILIZATION','EGRESS_UTLIZATION');
INSERT INTO PRIVATE_PEER
select @StartTimeFinal,@EndTimeFinal,
        QUOTE(NEFunction),QUOTE(interfaceDest),ROUND(SUM(RcvOctets)/(1000*1000*1000),2),ROUND(SUM(TxOctets)/(1000*1000*1000),2)
        from PEERING_VPN p, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL r
        where p.LinkID = r.PortID
        and r.Time_1 > startTime
        and r.Time_1 <= endTime
        group by interfaceDest;


set @Query = CONCAT("select * from PRIVATE_PEER into outfile \'/export/home/anec/Files_for_ANEC/",@FileName,"\' FIELDS TERMINATED BY ',' Lines terminated by '
'") ;

PREPARE stmt1 from @Query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS anec_priv_peer_test; 
 CREATE PROCEDURE anec_priv_peer_test(startTime TIMESTAMP,endTime TIMESTAMP)
BEGIN

SET @StartTime=''; 
SET @StartTimeFinal='';
SET @EndTime=''; 
SET @EndTimeFinal='';
SET @filenameTime=''; 
SET @filenameTimeFinal='';

SET @StartTime=DATE_FORMAT(StartTime,'%m/%d/%Y %H:%i:%s:%f');
SET @StartTimeFinal=SUBSTRING(@StartTime,1,CHAR_LENGTH(@StartTime)-4);

SET @EndTime=DATE_FORMAT(EndTime,'%m/%d/%Y %H:%i:%s:%f');
SET @EndTimeFinal=SUBSTRING(@EndTime,1,CHAR_LENGTH(@EndTime)-4);

SET @filenameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @filenameTimeFinal=SUBSTRING(@filenameTime,1,CHAR_LENGTH(@filenameTime)-4);

SET @FileName=CONCAT(CONCAT('IPPMS_Private_Peering_Link_Util_',@filenameTimeFinal),'.csv');


DROP TEMPORARY TABLE IF EXISTS PRIVATE_PEER;
CREATE TEMPORARY TABLE IF NOT EXISTS PRIVATE_PEER(
        STARTTIME       VARCHAR(50),
        ENDTIME         VARCHAR(50),
        PEERING_NE_FUNCTION     VARCHAR(100),
        PEERING_DESTINATION     VARCHAR(100),
        INGRESS_UTILIZATION     VARCHAR(50),
        EGRESS_UTLIZATION       VARCHAR(50));

INSERT INTO PRIVATE_PEER values ('STARTTIME','ENDTIME','PEERING_NE_FUNCTION','PEERING_DESTINATION','INGRESS_UTILIZATION','EGRESS_UTLIZATION');
INSERT INTO PRIVATE_PEER
select @StartTimeFinal,@EndTimeFinal,
        QUOTE(NEFunction),QUOTE(interfaceDest),ROUND(SUM(RcvOctets)/(1000*1000*1000),2),ROUND(SUM(TxOctets)/(1000*1000*1000),2)
        from PEERING_VPN p, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL r
        where p.LinkID = r.PortID
        and r.Time_1 > startTime
        and r.Time_1 <= endTime
        group by interfaceDest;


set @Query = CONCAT("select * from PRIVATE_PEER into outfile \'/export/home/anec/Files_for_ANEC_testing/",@FileName,"\' FIELDS TERMINATED BY ',' Lines terminated by '
'") ;

PREPARE stmt1 from @Query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS CustomerTrafFilter; 
 CREATE PROCEDURE CustomerTrafFilter(startTime timestamp,endTime timestamp,inputCustomerName varchar(13000),inputVrfName varchar(256),inputLocation varchar(256))
BEGIN

IF(inputCustomerName != '' AND inputVrfName != '' AND inputLocation != '')
THEN
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMERTRAF;
DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
DROP TEMPORARY TABLE IF EXISTS BW_REACHED_COUNT;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_VRF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP1_CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE EXCEED_COUNT(PortID BIGINT(20),exceed INTEGER);
CREATE TEMPORARY TABLE BW_REACHED_COUNT(PortID BIGINT(20),bwCount INTEGER);
CREATE TEMPORARY TABLE TEMP_CUSTOMERTRAF
(
	CircuitID       INTEGER DEFAULT 0,
	NodeName	VARCHAR(100),
	IfDescr		VARCHAR(200),
	PortID		BIGINT(20),
	Location	VARCHAR(100),
	InUtil		FLOAT(20,2),
	OutUtil		FLOAT(20,2),
	Bandwidth	FLOAT(10,2),
	CustomerName	VARCHAR(100),
	VrfName 	VARCHAR(100),
	Threshold	VARCHAR(10),
	ExceedCount	INTEGER DEFAULT 0,
	BWReachedCount INTEGER DEFAULT 0
);

CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL LIKE ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
CREATE TEMPORARY TABLE TEMP_CUSTOMER_VRF_TBL LIKE CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP1_CUSTOMER_VRF_TBL LIKE CUSTOMER_VRF_TBL;

CREATE INDEX CustomerTraf10 ON TEMP_CUSTOMER_VRF_TBL(PortID);


IF(inputVrfName = "'ALL'")
THEN
	INSERT INTO TEMP1_CUSTOMER_VRF_TBL select * from CUSTOMER_VRF_TBL;
ELSE
	set @query=CONCAT('INSERT INTO TEMP1_CUSTOMER_VRF_TBL select * from CUSTOMER_VRF_TBL where VrfName in (',inputVrfName,')');
	PREPARE stmt1 FROM @query;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
END IF;

if(inputLocation = "'ALL'")
THEN
	SET @query=CONCAT('INSERT INTO TEMP_CUSTOMER_VRF_TBL 
        select * from TEMP1_CUSTOMER_VRF_TBL where VpnName in (',inputCustomerName,')');
ELSE

SET @query=CONCAT('INSERT INTO TEMP_CUSTOMER_VRF_TBL 
	select * from TEMP1_CUSTOMER_VRF_TBL where VpnName in (',inputCustomerName,') and Location in (',inputLocation,')');

END IF;
PREPARE stmt1 FROM @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

UPDATE TEMP_CUSTOMER_VRF_TBL a JOIN NODEIF_TBL b SET a.IfSpeed=b.IfSpeed where a.NodeNumber=b.NodeNumber and a.IfIndex=b.IfIndex;

INSERT INTO TEMP_CUSTOMERTRAF(CircuitID,NodeName,IfDescr,PortID,Location,InUtil,OutUtil,Bandwidth,CustomerName,VrfName,Threshold)
	select  a.CircuitID,a.NodeName,a.IfDescr,a.PortID,a.Location,b.RcvOctets,b.TxOctets,a.IfSpeed,a.VpnName,VrfName,'70%'
		from TEMP_CUSTOMER_VRF_TBL a JOIN ROUTERTRAFFIC_VLANPRT_SCALE1_TBL b
		ON a.PortID = b.PortID where Time_1>startTime and Time_1<endTime; 

INSERT INTO EXCEED_COUNT
        select PortID,count(InUtil) from TEMP_CUSTOMERTRAF where (InUtil/(Bandwidth*10)>70 or OutUtil/(Bandwidth*10)>70) group by PortID;
INSERT INTO BW_REACHED_COUNT
	select PortID, count(InUtil) from TEMP_CUSTOMERTRAF where (InUtil/1000>=Bandwidth or OutUtil/1000>=Bandwidth) group by PortID;
update TEMP_CUSTOMERTRAF a,EXCEED_COUNT b set ExceedCount = exceed where a.PortID = b.PortID;

update TEMP_CUSTOMERTRAF a,BW_REACHED_COUNT b set BWReachedCount=bwCount where a.PortID=b.PortID;



select CircuitID,NodeName,IfDescr,Location,ROUND(MAX(InUtil)/1000,2),ROUND(MAX(OutUtil)/1000,2),ROUND(AVG(InUtil)/1000,2),ROUND(AVG(OutUtil)/1000,2),Bandwidth,CustomerName,Threshold,ExceedCount,BWReachedCount FROM TEMP_CUSTOMERTRAF group by PortID ;



DROP INDEX CustomerTraf10 ON TEMP_CUSTOMER_VRF_TBL;

END IF;
END | 
 

 DROP PROCEDURE IF EXISTS CustomerTrafFilter_reporting; 
 CREATE PROCEDURE CustomerTrafFilter_reporting(startTime timestamp,endTime timestamp,inputCustomerName varchar(13000),inputVrfName varchar(256),inputLocation varchar(256))
BEGIN

IF(inputCustomerName != '' AND inputVrfName != '' AND inputLocation != '')
THEN
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMERTRAF;
DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
DROP TEMPORARY TABLE IF EXISTS BW_REACHED_COUNT;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_VRF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP1_CUSTOMER_VRF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_FINAL_OP;
CREATE TEMPORARY TABLE EXCEED_COUNT(PortID BIGINT(20),exceed INTEGER);
CREATE TEMPORARY TABLE BW_REACHED_COUNT(PortID BIGINT(20),bwCount INTEGER);
CREATE TEMPORARY TABLE TEMP_CUSTOMERTRAF
(
	CircuitID       INTEGER DEFAULT 0,
	NodeName	VARCHAR(100),
	IfDescr		VARCHAR(200),
	PortID		BIGINT(20),
	Location	VARCHAR(100),
	InUtil		FLOAT(20,2),
	OutUtil		FLOAT(20,2),
	Bandwidth	FLOAT(10,2),
	CustomerName	VARCHAR(100),
	VrfName 	VARCHAR(100),
	Threshold	VARCHAR(10),
	ExceedCount	INTEGER DEFAULT 0,
	BWReachedCount INTEGER DEFAULT 0
);
CREATE TEMPORARY TABLE TEMP_FINAL_OP
(
	CircuitID varchar(500),
	NodeName  varchar(500),
	IfDescr varchar(500),
	Location varchar(500),
	MaxInUtil varchar(100),
	MaxOutUtil varchar(100),
	AvgInUtil varchar(100),
	AvgOutUtil varchar(100),
	Bandwidth varchar(100),
	CustomerName varchar(100),
	Threshold varchar(100),
	ExceedCount varchar(100),
	BWReachedCount varchar(100)
);

CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE1_TBL LIKE ROUTERTRAFFIC_VLANPRT_SCALE1_TBL;
CREATE TEMPORARY TABLE TEMP_CUSTOMER_VRF_TBL LIKE CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP1_CUSTOMER_VRF_TBL LIKE CUSTOMER_VRF_TBL;

CREATE INDEX CustomerTraf10 ON TEMP_CUSTOMER_VRF_TBL(PortID);


IF(inputVrfName = "'ALL'")
THEN
	INSERT INTO TEMP1_CUSTOMER_VRF_TBL select * from CUSTOMER_VRF_TBL;
ELSE
	set @query=CONCAT('INSERT INTO TEMP1_CUSTOMER_VRF_TBL select * from CUSTOMER_VRF_TBL where VrfName in (',inputVrfName,')');
	PREPARE stmt1 FROM @query;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
END IF;

if(inputLocation = "'ALL'")
THEN
	SET @query=CONCAT('INSERT INTO TEMP_CUSTOMER_VRF_TBL 
        select * from TEMP1_CUSTOMER_VRF_TBL where VpnName in (',inputCustomerName,')');
ELSE

SET @query=CONCAT('INSERT INTO TEMP_CUSTOMER_VRF_TBL 
	select * from TEMP1_CUSTOMER_VRF_TBL where VpnName in (',inputCustomerName,') and Location in (',inputLocation,')');

END IF;
PREPARE stmt1 FROM @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

UPDATE TEMP_CUSTOMER_VRF_TBL a JOIN NODEIF_TBL b SET a.IfSpeed=b.IfSpeed where a.NodeNumber=b.NodeNumber and a.IfIndex=b.IfIndex;

INSERT INTO TEMP_CUSTOMERTRAF(CircuitID,NodeName,IfDescr,PortID,Location,InUtil,OutUtil,Bandwidth,CustomerName,VrfName,Threshold)
	select  a.CircuitID,a.NodeName,a.IfDescr,a.PortID,a.Location,b.RcvOctets,b.TxOctets,a.IfSpeed,a.VpnName,VrfName,'70%'
		from TEMP_CUSTOMER_VRF_TBL a JOIN ROUTERTRAFFIC_VLANPRT_SCALE1_TBL b
		ON a.PortID = b.PortID where Time_1>startTime and Time_1<endTime; 

INSERT INTO EXCEED_COUNT
        select PortID,count(InUtil) from TEMP_CUSTOMERTRAF where (InUtil/(Bandwidth*10)>70 or OutUtil/(Bandwidth*10)>70) group by PortID;
INSERT INTO BW_REACHED_COUNT
	select PortID, count(InUtil) from TEMP_CUSTOMERTRAF where (InUtil/1000>=Bandwidth or OutUtil/1000>=Bandwidth) group by PortID;
update TEMP_CUSTOMERTRAF a,EXCEED_COUNT b set ExceedCount = exceed where a.PortID = b.PortID;

update TEMP_CUSTOMERTRAF a,BW_REACHED_COUNT b set BWReachedCount=bwCount where a.PortID=b.PortID;

INSERT INTO TEMP_FINAL_OP values("Circuit ID","Node Name","Interface","Location","Max In Utilization(%)","Max In Utilization(%)","Average In Utilization(%)","Average Out Utilization(%)","Bandwidth","Customer Name","Threshold","Exceed Count","Bandwidth Reached Count");

INSERT INTO TEMP_FINAL_OP
select CircuitID,NodeName,IfDescr,Location,ROUND(MAX(InUtil)/1000,2),ROUND(MAX(OutUtil)/1000,2),ROUND(AVG(InUtil)/1000,2),ROUND(AVG(OutUtil)/1000,2),Bandwidth,CustomerName,Threshold,ExceedCount,BWReachedCount FROM TEMP_CUSTOMERTRAF group by PortID;

SET @fileName=CONCAT(@dir_name,'/CustomerTrafficReport.csv');
SET @query=CONCAT('SELECT * from TEMP_FINAL_OP INTO OUTFILE \'',@fileName,'\' fields terminated by \',\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;




DROP INDEX CustomerTraf10 ON TEMP_CUSTOMER_VRF_TBL;

END IF;
END | 
 

 DROP PROCEDURE IF EXISTS customerUsage; 
 CREATE PROCEDURE customerUsage(startTime timestamp, endTime timestamp)
BEGIN

SET @startTime=startTime;
SET @endTime=endTime;
select @startTime;
DROP TEMPORARY TABLE IF EXISTS TEMP_TOTAL_USAGE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTUSAGE;
CREATE TEMPORARY TABLE TEMP_TOTAL_USAGE (PortID integer, TotalUsage float);
CREATE TEMPORARY TABLE TEMP_CUSTUSAGE(startTime varchar(256), endTime varchar(256), CustomerName varchar(256),TotalUsage varchar(256));

INSERT INTO TEMP_TOTAL_USAGE
SELECT PortID,SUM((TxOctets+RcvOctets)/(1000*1000))  FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL WHERE Time_1>startTime and Time_1<endTime GROUP BY PortID ;

SET @startTimeTemp=DATE_FORMAT(@startTime,'%m/%d/%Y %H:%i:%s:%f');
SET @endTimeTemp=DATE_FORMAT(@endTime,'%m/%d/%Y %H:%i:%s:%f');
SET @startTimeFinal= SUBSTRING(@startTimeTemp,1,CHAR_LENGTH(@startTimeTemp)-4);
SET @endTimeFinal= SUBSTRING(@endTimeTemp,1,CHAR_LENGTH(@endTimeTemp)-4);

SELECT @startTimeFinal;
SELECT @endTimeFinal;
SET @fileNameTime=DATE_FORMAT(@endTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @fileName='/export/home/anec/Files_for_ANEC/IPPMS_Internet_Data_Usage_';
SET @finalFileName=CONCAT(CONCAT(@fileName,@fileNameTimeFinal),'.csv');

INSERT INTO TEMP_CUSTUSAGE
SELECT 'Start Time','End Time','Customer Name','Total Usage';
SET @query1=CONCAT('INSERT INTO TEMP_CUSTUSAGE SELECT  @startTimeFinal, @endTimeFinal, CONCAT(\"\'\",a.CustomerName,\"\'\"), ROUND(SUM(TotalUsage),2) FROM TEMP_AIRTEL_CSV a, TEMP_TOTAL_USAGE b WHERE a.PortId=b.PortId  GROUP BY a.CustomerName');
PREPARE stmt1 from @query1;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

set @query=CONCAT(CONCAT('select * from TEMP_CUSTUSAGE into outfile \'',@finalFileName),'\' fields terminated by \',\' ');
PREPARE stmt2 from @query;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;
DROP TEMPORARY TABLE IF EXISTS TEMP_TOTAL_USAGE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTUSAGE;

END | 
 

 DROP PROCEDURE IF EXISTS CustomerVrf; 
 CREATE PROCEDURE CustomerVrf()
BEGIN

DECLARE iIndex INTEGER DEFAULT 1;
DECLARE iCount INTEGER DEFAULT 0;
DROP TEMPORARY TABLE IF EXISTS CUSTOMER_VRF;

CREATE TEMPORARY TABLE CUSTOMER_VRF(
		CircuitID VARCHAR(100),
		VpnName VARCHAR(100),
		VrfName VARCHAR(100),
		Location VARCHAR(100),
		NodeName VARCHAR(100),
		IfDescr VARCHAR(100),
		NodeNumber INTEGER,
		IfIndex INTEGER,
		PortID BIGINT(20),
		IfSpeed BIGINT(20));

CREATE INDEX i1 ON CUSTOMER_VRF(NodeName);
CREATE INDEX i2 ON CUSTOMER_VRF(PortID);
CREATE INDEX i3 ON CUSTOMER_VRF(NodeNumber);
CREATE INDEX i4 ON CUSTOMER_VRF(IfIndex);
CREATE INDEX i5 ON CUSTOMER_VRF(VrfName);
CREATE INDEX i6 ON CUSTOMER_VRF(IfDescr);


INSERT INTO CUSTOMER_VRF(CircuitID,VpnName,VrfName,Location,NodeName,IfDescr) 
	select IF((SUBSTRING_INDEX(REPLACE(CircuitID,':','-'),'-',-1) REGEXP '^-?[0-9]+$'),SUBSTRING_INDEX(REPLACE(CircuitID,':','-'),'-',-1),0),
	VpnName,VrfName,Location,NodeName,IfDescr from TEMP_CUSTOMER_VRF ;


DROP VIEW IF EXISTS TEMP_VPNIF_FULL;

CREATE VIEW TEMP_VPNIF_FULL AS
	SELECT VpnName,VpnNode,NodeName,VpnIfIndex,IfDescr from VPN_TBL a ,VPN_IFINDEX_TBL b,NODE_TBL c,NODEIF_TBL d where a.VpnID = b.VpnID and
	b.VpnNode = c.NodeNumber and b.VpnNode = d.NodeNumber and b.VpnIfIndex = d.IfIndex;


update CUSTOMER_VRF a,NODE_TBL b
	set a.NodeNumber = b.NodeNumber
	where a.NodeName = b.NodeName ;
drop temporary table if exists nodeif;
create temporary table nodeif (index (NodeNumber ,IfDescr)) as select NodeNumber,IfIndex,IfDescr,IfSpeed from NODEIF_TBL;
update CUSTOMER_VRF a,nodeif b
        set a.IfIndex = b.IfIndex,
	a.IfSpeed = b.IfSpeed
        where a.NodeNumber = b.NodeNumber 
	and a.IfDescr = b.IfDescr;

drop temporary table if exists nodeif;
update CUSTOMER_VRF a,VLANPRT_TBL b
        set a.PortID = b.PrtID
        where a.NodeNumber = b.NodeID
        and a.IfIndex = b.IfIndex;

DROP TABLE IF EXISTS CUSTOMER_VRF_TBL;
create table CUSTOMER_VRF_TBL 
	select b.* from CUSTOMER_VRF b,TEMP_VPNIF_FULL a 
	where a.VpnNode = b.NodeNumber and a.VpnIfIndex = b.IfIndex and VrfName != "";

select count(*) into iCount from CUSTOMER_VRF_TBL where Location = "";

while(iIndex <= iCount)
DO
	update CUSTOMER_VRF_TBL set Location = CONCAT('City_',iIndex) where Location = "" limit 1;
	set iIndex = iIndex +1;

END WHILE;



DROP TEMPORARY TABLE IF EXISTS CUSTOMER_VRF;

DROP VIEW IF EXISTS TEMP_VPNIF_FULL;
DROP TABLE IF EXISTS TEMP_CUSTOMER_VRF;

DROP VIEW IF EXISTS CUSTOMER_VRF_VIEW;

CREATE VIEW CUSTOMER_VRF_VIEW AS
       SELECT CircuitID,VpnName,VrfName,Location,NodeName,a.IfDescr,a.NodeNumber,a.IfIndex,PortID,a.IfSpeed,b.IfAlias from CUSTOMER_VRF_TBL a,NODEIF_TBL b where a.NodeNumber = b.NodeNumber and a.IfIndex = b.IfIndex;



END | 
 

 DROP PROCEDURE IF EXISTS delayJitter; 
 CREATE PROCEDURE delayJitter(startTime timestamp, endTime timestamp)
BEGIN


DECLARE fileNameF varchar(256);
SET @startTime=startTime;
DROP TEMPORARY TABLE IF EXISTS TEMP_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ANECDELAY_TBL;
CREATE TEMPORARY TABLE TEMP_TBL(LinkName varchar(256),avgDelay float,packetLoss int,avgJitter int);
CREATE TEMPORARY TABLE TEMP_ANECDELAY_TBL(startTime varchar(256),endTime varchar(256),LinkName varchar(256),avgDelay varchar(256),packetLoss varchar(256),avgJitter varchar(256));

select @startTime;
select endTime;
INSERT INTO TEMP_TBL
select distinct CONCAT(CONCAT('\'',CONCAT(CONCAT(d.Location,'::'),e.Location)),'\''),AvgDelay,PacketLoss,AvgJitter FROM VPN_DELAY_TABLE a, NODE_TBL b, NODE_TBL c ,shadowRouter d, shadowRouter e  where   a.SourceIp=b.NodeId and a.DestinationIp=c.NodeId and b.NodeName=d.NodeName and c.NodeName=e.NodeName  and Time_1>@startTime and Time_1<endTime group by CONCAT(CONCAT(d.Location,'::'),e.Location);



SET @endTime=endTime;
SET @startTimeTemp=DATE_FORMAT(@startTime,'%m/%d/%Y %k:%i:%s:%f');
SET @endTimeTemp=DATE_FORMAT(@endTime,'%m/%d/%Y %k:%i:%s:%f');
SET @startTimeFinal= SUBSTRING(@startTimeTemp,1,CHAR_LENGTH(@startTimeTemp)-4);
SET @endTimeFinal= SUBSTRING(@endTimeTemp,1,CHAR_LENGTH(@endTimeTemp)-4);

SET @fileNameTime=DATE_FORMAT(@endTime,'%m_%d_%Y-%k_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @fileName='/export/home/anec/Files_for_ANEC/IPPMS_MPLS_Servicel_KPIs_';
SET @finalFileName=CONCAT(CONCAT(@fileName,@fileNameTimeFinal),'.csv');
set fileNameF=@finalFileName;
INSERT INTO TEMP_ANECDELAY_TBL 
SELECT 'Start Time','End Time','Link Name', 'Latency', 'Packet Loss', 'Jitter';


INSERT INTO TEMP_ANECDELAY_TBL
SELECT @startTimeFinal,@endTimeFinal,LinkName,avgDelay,packetLoss, avgJitter from TEMP_TBL;


set @query=CONCAT(CONCAT('select * from TEMP_ANECDELAY_TBL into outfile \'',fileNameF),'\' fields terminated by \',\'');
prepare stmt1 from @query;
EXECUTE stmt1;

DEALLOCATE PREPARE stmt1;


END | 
 

 DROP PROCEDURE IF EXISTS deleteBeforeData; 
 CREATE PROCEDURE deleteBeforeData()
BEGIN 

SELECT NOW() into @time1;
SELECT TIMESTAMPADD(day, -2, @time1) into @time1Before;

delete from ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL where Time_1<@time1Before;
delete from ROUTERTRAFFIC_LINK_SCALE1_TBL where Time_1<@time1Before;
delete from ROUTERTRAFFIC_LINK_SCALE2_TBL where Time_2<@time1Before;
delete from ROUTERTRAFFIC_LINK_SCALE3_TBL where Time_3<@time1Before;
delete from ROUTERTRAFFIC_LINK_SCALE4_TBL where Time_4<@time1Before;
delete from ROUTERTRAFFIC_LSP_SCALE1_TBL where Time_1<@time1Before; 
delete from ROUTERTRAFFIC_LSP_SCALE2_TBL where Time_2<@time1Before;
delete from ROUTERTRAFFIC_LSP_SCALE3_TBL where Time_3<@time1Before;
delete from ROUTERTRAFFIC_LSP_SCALE4_TBL where Time_4<@time1Before;

delete from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where Time_1<@time1Before and PortID NOT IN (92593,107052,103988,103982,88133,108100,96769,120279,120945,121219,85988,103750,103632,87844,99521,88580,85870,103984,90765,121214,103918);

delete from ROUTERTRAFFIC_VPN_SCALE1_TBL where Time_1<@time1Before;
delete from CPU_STATS_TBL where TimeStamp<@time1Before;
delete from BUFFER_STATS_TBL  where TimeStamp<@time1Before;
delete from TEMP_STATS_TBL  where TimeStamp<@time1Before;
delete from STORAGE_STATS_TBL  where TimeStamp<@time1Before;
delete from VPN_DELAY_TABLE where Time_1<@time1Before;

END | 
 

 DROP PROCEDURE IF EXISTS diffServeLink; 
 CREATE PROCEDURE diffServeLink()
BEGIN
        DECLARE iCount INTEGER DEFAULT 0;
        DECLARE iLinkID INTEGER DEFAULT 0;
        DECLARE iLinks INTEGER DEFAULT 0;

        DROP TEMPORARY TABLE IF EXISTS LINKDIFF_TBL;
        CREATE TEMPORARY TABLE LINKDIFF_TBL(LinkId INTEGER UNSIGNED,
				   LinkName VARCHAR(100),
				   CosFCName varchar(255),
				   DiffID INTEGER);

Insert into LINKDIFF_TBL
select f.LinkId, f.LinkName,a.CosFCName,b.QSTID as DiffID
from COSFC_TBL a, COSQSTAT_TBL b,TEMP_LINK_NAME f, LINK_TBL g
where  a.NodeNumber = b.NodeNumber and a.CosQNumber =b.QNumber and
f.OriNodeNumber = b.NodeNumber and f.OriIfIndex = b.IfIndex and g.LinkId = f.LinkId;

select * from LINKDIFF_TBL;

END | 
 

 DROP PROCEDURE IF EXISTS DSLUsage; 
 CREATE PROCEDURE DSLUsage(StartTime TimeStamp, EndTime TimeStamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_VLAN_TBL;
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_VLAN_TBL(PortID INTEGER);

INSERT INTO TEMP_VLAN_TBL
SELECT PrtID FROM DSL_TBL a,VLANPRT_TBL b 
WHERE a.NodeNumber=b.NodeID AND a.IfIndex=b.IfIndex;

SET @fileNameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%k_%i_%s_%f');
SET @fileNameTimeFinal=SUBSTRING(@fileNameTime,1,CHAR_LENGTH(@fileNameTime)-4);
SET @finalFileName=CONCAT(CONCAT('/export/home/anec/Files_for_ANEC/IPPMS_Bandwidth_Usage_',@fileNameTimeFinal),'.csv');



DROP TEMPORARY TABLE IF EXISTS FINAL_TBL;
CREATE TEMPORARY TABLE IF NOT EXISTS FINAL_TBL(StartTime VARCHAR(100), EndTime VARCHAR(100), DSL_Usage VARCHAR(50),EnterpriseUsage VARCHAR(50));

INSERT INTO FINAL_TBL VALUES('StartTime','EndTime','DSL Usage','Enterprise Usage');

INSERT INTO FINAL_TBL
SELECT DATE_FORMAT(StartTime,"%m/%d/%Y %k:%i:%s:%f"),DATE_FORMAT(EndTime,"%m/%d/%Y %k:%i:%s:%f"),SUM(RVlan.TxOctets+RVlan.RcvOctets)/(1000*1000*1000), 0 AS EnterpriseUsage 
FROM TEMP_VLAN_TBL T, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL RVlan 
WHERE T.PortID=RVlan.PortID AND RVlan.Time_1>StartTime AND RVlan.Time_1<EndTime;

set @final = CONCAT("select * from FINAL_TBL into outfile \'",@finalFileName,"\' FIELDS TERMINATED BY ',' Lines terminated by '\n'") ;



PREPARE stmt1 FROM @final;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 


 DROP PROCEDURE IF EXISTS getHistoricThreshold; 
 CREATE PROCEDURE getHistoricThreshold(p_NodeName VARCHAR(100),p_IfDescr VARCHAR(100),StartTime TIMESTAMP,EndTime TIMESTAMP)
BEGIN

if (p_NodeName = 'ALL' and p_IfDescr = 'ALL')
then
	set @query=CONCAT("select NodeName,IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,Time_1,class from THRESHOLD_TBL where Time_1 >= \'",StartTime,"\' and Time_1 <= \'",EndTime,"\'");
else if(p_NodeName = 'ALL' and p_IfDescr != 'ALL')
then
	set @query=CONCAT("select NodeName,IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,Time_1,class from THRESHOLD_TBL where IfDescr IN (",p_IfDescr,") and Time_1 >= \'",StartTime,"\' and Time_1 <=\'", EndTime,"\'");
else if(p_NodeName != 'ALL' and p_IfDescr = 'ALL')
then
	set @query=CONCAT("select NodeName,IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,Time_1,class from THRESHOLD_TBL where NodeName IN (",p_NodeName,") and Time_1 >= \'",StartTime,"\' and Time_1 <=\'", EndTime,"\'");
else if(p_NodeName != 'ALL' and p_IfDescr != 'ALL')
then
        set @query=CONCAT("select NodeName,IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,Time_1,class from THRESHOLD_TBL where NodeName IN (",p_NodeName,") and IfDescr IN (",p_IfDescr,") and Time_1 >= \'",StartTime,"\' and Time_1 <=\'", EndTime,"\'");
end if;	
end if;	
end if;	
end if;	
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS getLatestCustomerThreshold; 
 CREATE PROCEDURE getLatestCustomerThreshold(p_CustomerName VARCHAR(1000))
BEGIN

DECLARE tTime TIMESTAMP DEFAULT NULL;

drop temporary table if exists customerPorts;
if(p_CustomerName = "ALL")
then 
	SET @query1 = CONCAT("create temporary table customerPorts(index (PortID)) as select PortID from CUSTOMER_VRF_TBL");
else
	SET @query1 = CONCAT("create temporary table customerPorts(index (PortID)) as select PortID from CUSTOMER_VRF_TBL where VrfName IN (",p_CustomerName,");");
end if;
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;


select now() into @tempTime;
select max(Time_1)  into @tempTime from THRESHOLD_TBL;
set @tempTime = TIMESTAMPADD(MINUTE,-30,@tempTime);
drop temporary table if exists tmp_threshold;
create temporary table tmp_threshold(index (NodeName,IfDescr,Time_1)) as select NodeName,IfDescr,max(Time_1) as Time_1 from THRESHOLD_TBL a JOIN customerPorts b on a.PortID = b.PortID group by NodeName,IfDescr ;

select A.NodeName,A.IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,A.Time_1,class from THRESHOLD_TBL A JOIN tmp_threshold B ON A.NodeName = B.NodeName and A.IfDescr = B.IfDescr and A.Time_1 = B.Time_1 where A.Time_1 > @tempTime;


END | 
 

 DROP PROCEDURE IF EXISTS getLatestThreshold; 
 CREATE PROCEDURE getLatestThreshold()
BEGIN

DECLARE tTime TIMESTAMP DEFAULT NULL;

select now() into @tempTime;
set @tempTime = TIMESTAMPADD(MINUTE,-30,@tempTime);
drop temporary table if exists tmp_threshold;
create temporary table tmp_threshold(index (NodeName,IfDescr,Time_1)) as select NodeName,IfDescr,max(Time_1) as Time_1 from THRESHOLD_TBL group by NodeName,IfDescr ;

select A.NodeName,A.IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,A.Time_1,class from THRESHOLD_TBL A JOIN tmp_threshold B ON A.NodeName = B.NodeName and A.IfDescr = B.IfDescr and A.Time_1 = B.Time_1 where A.Time_1 > @tempTime;

END | 
 

 DROP PROCEDURE IF EXISTS getLatestWebThreshold; 
 CREATE PROCEDURE getLatestWebThreshold()
BEGIN

DECLARE tTime TIMESTAMP DEFAULT NULL;

select max(Time_1) into @tempTime from WEB_THRESHOLD_TBL;
set @tempTime = TIMESTAMPADD(MINUTE,-10,@tempTime);

drop temporary table if exists tmp_threshold;
create temporary table tmp_threshold(index (NodeName,URL,Time_1)) as select NodeName,URL,max(Time_1) as Time_1 from WEB_THRESHOLD_TBL group by URL,NodeName ;

select a.URL,a.NodeName,HostedLocation,AcceptableTh,Latency,LatencyFlag,TCPGetTime,TCPGetTimeFlag,HTTPGetTime,DNSLookUpTime,PageLoadTime,a.Time_1 from WEB_THRESHOLD_TBL a JOIN tmp_threshold b ON a.NodeName = b.NodeName and a.URL = b.URL and a.Time_1 = b.Time_1 where a.Time_1 > @tempTime;

END | 
 

 DROP PROCEDURE IF EXISTS getPortThresholdValue; 
 CREATE PROCEDURE getPortThresholdValue( p_NodeName VARCHAR(1000))
BEGIN

select NodeNumber into @node from NODE_TBL where NodeName = p_NodeName;

drop temporary table if exists nodeif;
create temporary table nodeif(index(NodeNumber,IfIndex)) as select NodeNumber,IfIndex,IfDescr from NODEIF_TBL where NodeNumber = @node;

drop temporary table if exists vlanprt;
create temporary table vlanprt(index(PrtID),index(NodeID,IfIndex)) as select PrtID,NodeID,IfIndex from VLANPRT_TBL where NodeID = @node;

drop temporary table if exists vlanprt_tbl;
create temporary table vlanprt_tbl (index(PrtID)) as select PrtID,NodeName,IfDescr from vlanprt a JOIN NODE_TBL b on a.NodeId = b.NodeNumber JOIN nodeif c on a.NodeID = c.NodeNumber and a.IfIndex = c.IfIndex;

select PortID,NodeName,IfDescr,AbsoluteTh from vlanprt_tbl a JOIN PORT_THRESHOLD_TBL b on a.PrtID = b.PortID ;


END | 
 

 DROP PROCEDURE IF EXISTS int_dom_peer; 
 CREATE PROCEDURE int_dom_peer(StartTime TIMESTAMP,EndTime TIMESTAMP)
BEGIN

SET @StartTime=''; SET @StartTimeFinal='';
SET @EndTime=''; SET @EndTimeFinal='';
SET @filenameTime=''; SET @filenameTimeFinal='';

SET @StartTime=DATE_FORMAT(StartTime,'%m/%d/%Y %H:%i:%s:%f');
SET @StartTimeFinal=SUBSTRING(@StartTime,1,CHAR_LENGTH(@StartTime)-4);

SET @EndTime=DATE_FORMAT(EndTime,'%m/%d/%Y %H:%i:%s:%f');
SET @EndTimeFinal=SUBSTRING(@EndTime,1,CHAR_LENGTH(@EndTime)-4);

SET @filenameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @filenameTimeFinal=SUBSTRING(@filenameTime,1,CHAR_LENGTH(@filenameTime)-4);

SET @FileName=CONCAT(CONCAT('IPPMS_Int_Dom_Peering_Link_Util_',@filenameTimeFinal),'.csv');


DROP TEMPORARY TABLE IF EXISTS INT_DOM_PEER;
CREATE TEMPORARY TABLE IF NOT EXISTS INT_DOM_PEER(
	STARTTIME	VARCHAR(50),
	ENDTIME		VARCHAR(50),
	PEERING_VPN	VARCHAR(100),
	INGRESS_UTILIZATION	VARCHAR(50),
	EGRESS_UTLIZATION	VARCHAR(50));


INSERT INTO INT_DOM_PEER values ('STARTTIME','ENDTIME','PEERING_VPN','INGRESS_UTILIZATION','EGRESS_UTLIZATION');
INSERT INTO INT_DOM_PEER(select @StartTimeFinal,@EndTimeFinal,
	QUOTE(peerVPN),ROUND(SUM(RcvOctets)/(1000*1000*1000),2),ROUND(SUM(TxOctets)/(1000*1000*1000),2) 
	from PEERING_VPN p, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL r 
	where p.LinkID = r.PortID 
	and r.Time_1 > startTime 
	and r.Time_1 <= endTime 
	group by peerVPN );

set @Query = CONCAT("select * from INT_DOM_PEER into outfile \'/tmp/",@FileName,"\' FIELDS TERMINATED BY ',' Lines terminated by '\n'") ;
PREPARE stmt1 from @Query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


END | 
 

 DROP PROCEDURE IF EXISTS maxIn24; 
 CREATE PROCEDURE maxIn24(inPortId int)
BEGIN

SELECT b.IfSpeed INTO @ifSpeed from VLANPRT_TBL a, NODEIF_TBL b where a.PrtId=inPortId and a.NodeId=b.NodeNumber and a.IfIndex=b.IfIndex;
SELECT MAX(Time_1) INTO @time1 from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where PortId=inPortId;
SELECT RcvOctets/(1000*1000),TxOctets/(1000*1000),IF(@ifSpeed=0,0,RcvOctets/(10*@ifSpeed)),IF((@ifSpeed=0),0,TxOctets/(10*@ifSpeed)) INTO @currentIn,@currentOut,@currentInUtil, @currentOutUtil FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a  where  a.PortId=inPortId and Time_1=@time1;


SET @time1Before=TIMESTAMPADD(day, -1, @time1);

SELECT MAX(RcvOctets)/(1000*1000),MAX(TxOctets)/(1000*1000),IF(@ifSpeed=0,0,MAX(RcvOctets)/(10*@ifSpeed)),IF(@ifSpeed=0,0,MAX(TxOctets)/(10*@ifSpeed)) into @maxIn24,@maxOut24,@maxIn24Util,@maxOut24Util FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where PortId=inPortId and Time_1>=@time1Before and Time_1<=@time1;

SELECT ROUND(@currentIn,2),ROUND(@currentInUtil,2),ROUND(@currentOut,2),ROUND(@currentOutUtil,2),ROUND(@maxIn24,2),ROUND(@maxIn24Util,2),ROUND(@maxOut24,2),ROUND(@maxOut24Util,2);
END | 
 

 DROP PROCEDURE IF EXISTS maxIn24WithPortId; 
 CREATE PROCEDURE maxIn24WithPortId(inPortId int)
BEGIN

SELECT a.IfSpeed,NodeNumber,a.IfDescr into @ifSpeed,@nodeNumber,@ifDescr from NODEIF_TBL a, VLANPRT_TBL b where PrtId=inPortId and a.NodeNumber=b.NodeID and a.IfIndex=b.IfIndex;

SELECT MAX(Time_1) INTO @time1 from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where PortId=inPortId;

SELECT RcvOctets/(1000*1000),TxOctets/(1000*1000),IF(@ifSpeed=0,0,RcvOctets/(10*@ifSpeed)),IF((@ifSpeed=0),0,TxOctets/(10*@ifSpeed)) INTO @currentIn,@currentOut,@currentInUtil, @currentOutUtil FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a  where  a.PortId=inPortId and Time_1=@time1;

SELECT NodeName into @nodeName from NODE_TBL where NodeNumber=@nodeNumber;

SET @time1Before=TIMESTAMPADD(day, -1, @time1);

SELECT MAX(RcvOctets),MAX(TxOctets),IF(@ifSpeed=0,0,MAX(RcvOctets)/(10*@ifSpeed)),IF(@ifSpeed=0,0,MAX(TxOctets)/(10*@ifSpeed)) into @maxIn24,@maxOut24,@maxIn24Util,@maxOut24Util FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where PortId=inPortId and Time_1>=@time1Before and Time_1<=@time1 ;


SELECT if(ROUND(@maxOut24/(1000*1000),2)=0,'NA',Time_1) INTO @outPeakTime from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where PortId=inPortId and Time_1>=@time1Before and Time_1<=@time1 and TxOctets=@maxOut24 GROUP By inPortId;

SELECT if(ROUND(@maxIn24/(1000*1000),2)=0,'NA',Time_1) INTO @inPeakTime from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL where PortId=inPortId and Time_1>=@time1Before and Time_1<=@time1 and RcvOctets=@maxIn24 GROUP BY inPortId;



SELECT @nodeName,@IfDescr,ROUND(@currentIn,2),ROUND(@currentInUtil,2),ROUND(@currentOut,2),ROUND(@currentOutUtil,2),ROUND(@maxIn24/(1000*1000),2),ROUND(@maxIn24Util,2),ROUND(@maxOut24/(1000*1000),2),ROUND(@maxOut24Util,2),@inPeakTime, @outPeakTime;
END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_1_PortUtil; 
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
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500),PortID BIGINT(20) DEFAULT 0);
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
	inPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00');


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
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias like '%AESI-IN%' OR UPPER(IFAlias) like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%'  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias NOT like '%AESI-IN%'  AND UPPER(IFAlias) not like '%INT-BACK-TO-BACK%') and (IfAlias like '%SWH%' or IfAlias like '%AES%' or IfAlias like '%RTR%') and IfAlias not like '%Peering%'  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
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

set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_NODEANDIF_TBL;


set @query = CONCAT("INSERT INTO TRAFFIC_TBL(PortID,InErrPkts,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;

update TRAFFIC_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID set a.IfDescr = b.IfDescr , a.NodeName = b.NodeName , a.IfSpeed = b.IfSpeed,a.Time_1 = a.Time_1;

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

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;
CREATE TEMPORARY TABLE FINAL_FETCH(serviceType varchar(256),NWType varchar(256), Router varchar(256),Interface varchar(256),IfAlias VARCHAR(500), InUtilPeak varchar(256), OutUtilPeak varchar(256),InUtilAvg varchar(256),OutUtilAvg varchar(256), CRCInError varchar(256),UpTime varchar(256),Reliability varchar(256),AvgUtilPercIn varchar(256),AvgUtilPercOut varchar(256),PeakUtilPercIn varchar(256),PeakUtilPercOut varchar(256),Threshold varchar(256),inPeakTime varchar(256),outPeakTime varchar(256));
INSERT INTO FINAL_FETCH values('Service','NW Type', 'Router', 'Interface', 'Interface Alias','In Traffic Peak (Kbps)', 'Out Traffic Peak(Kbps)', 'In Traffic Avg(Kbps)', 'Out Traffic Avg(Kbps)', 'CRC (In Error)','Up Time', 'Reliabilty', 'Avg Util(%) IN','Avg Util(%) OUT','Peak Util(%) IN','Peak Util(%) OUT','Number of times peak threshold crossed during the Report Duration' ,'In Peak Util Time','Out Peak Util Time');
INSERT INTO FINAL_FETCH
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
IF(IFAlias like '%AESI-IN%',
CASE
WHEN IFAlias like '%RTR%' THEN "Back-to-Back"
WHEN IFAlias like '%SWH%' THEN "Trunk"
WHEN IfAlias not like '%RTR%' and IfAlias not like '%SWH%' THEN "Backbone"
ELSE '-'
END,
CASE
WHEN IfAlias like '%AES%' THEN "Backbone"
WHEN IfAlias like '%RTR%' and IfAlias not like '%AES%' THEN "Back-to-Back"
WHEN IfAlias like '%SWH%' THEN "Trunk"
ELSE '-'
END ),
NodeName,REPLACE(IfDescr,'^',' '),REPLACE(IfAlias,'^',' '),ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(CRCError,2),0,0,ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID group by a.PortID;


SET @fileName=CONCAT(@dir_name,'/NWP_Domestic_Backbone_Service_wise_Link_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');

PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;




END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_2_PortUtil; 
 CREATE PROCEDURE mpls_reporting_6_4_2_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
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
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500),PortID BIGINT(20) DEFAULT 0);
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
	inPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00');


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
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias like '%AESI-IN%' OR UPPER(IFAlias) like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%'  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias NOT like '%AESI-IN%'  AND UPPER(IFAlias) not like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%'  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
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


set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_NODEANDIF_TBL;


set @query = CONCAT("INSERT INTO TRAFFIC_TBL(PortID,InErrPkts,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;

update TRAFFIC_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID set a.IfDescr = b.IfDescr , a.NodeName = b.NodeName , a.IfSpeed = b.IfSpeed,a.Time_1 = a.Time_1;

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

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

CREATE TEMPORARY TABLE FINAL_FETCH(serviceType varchar(256),NWType varchar(256),Router varchar(256),Interface varchar(256), IfAlias VARCHAR(500), InUtilPeak varchar(256), OutUtilPeak varchar(256),InUtilAvg varchar(256),OutUtilAvg varchar(256), CRCInError varchar(256),UpTime varchar(256),Reliability varchar(256),AvgUtilPercIn varchar(256),AvgUtilPercOut varchar(256),PeakUtilPercIn varchar(256),PeakUtilPercOut varchar(256),Threshold varchar(256),inPeakTime varchar(256),outPeakTime varchar(256));
INSERT INTO FINAL_FETCH values('Service','NW Type', 'Router', 'Interface', 'Interface Alias', 'In Traffic Peak (Kbps)', 'Out Traffic Peak(Kbps)', 'In Traffic Avg(Kbps)', 'Out Traffic Avg(Kbps)', 'CRC (In Error)','Up Time', 'Reliabilty', 'Avg Util(%) IN','Avg Util(%) OUT','Peak Util(%) IN','Peak Util(%) OUT','Number of times peak threshold crossed during the Report Duration' ,'In Peak Util Time','Out Peak Util Time');
INSERT INTO FINAL_FETCH
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
IF(IFAlias like '%AESI-IN%',
CASE
WHEN IFAlias like '%RTR%' THEN "Back-to-Back"
WHEN IFAlias like '%SWH%' THEN "Trunk"
WHEN IfAlias not like '%RTR%' and IfAlias not like '%SWH%' THEN "Backbone"
ELSE '-'
END,
CASE
WHEN IfAlias like '%AES%' THEN "Backbone"
WHEN IfAlias like '%RTR%' and IfAlias not like '%AES%' THEN "Back-to-Back"
WHEN IfAlias like '%SWH%' THEN "Trunk"
ELSE '-'
END ),
NodeName,REPLACE(IfDescr,'^',' '), REPLACE(IfAlias,'^',' '), ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(CRCError,2),0,0,ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID group by a.PortID;


SET @fileName=CONCAT(@dir_name,'/NWP_International_Backbone_Service_wise_Link_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');

PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;




END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_3_Pearing_Link_Util; 
 CREATE PROCEDURE mpls_reporting_6_4_3_Pearing_Link_Util(p_peeringScope varchar(5000), p_Region varchar(5000), p_peeringType varchar(1000),p_RouterName VARCHAR(5000),p_PeeringPartner VARCHAR(5000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_AllFields;
DROP TEMPORARY TABLE IF EXISTS TEMP_InternationalAllFields;
DROP TEMPORARY TABLE IF EXISTS TEMP_FILTER_AllFields;
DROP TEMPORARY TABLE IF EXISTS PEER_LINK_UTIL;
DROP TEMPORARY TABLE IF EXISTS TMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_AllFields(peeringScope varchar(128),peeringRegion varchar(128), peeringType varchar(128),peeringPartner varchar(128),NodeNumber int,IfIndex int,NodeName varchar(128),PortId bigint(20),IfDescr varchar(256),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_FILTER_AllFields like TEMP_AllFields;

CREATE TEMPORARY TABLE TMP_TRAFFIC_TBL (
        PortID BIGINT(20),
        RcvOctets BIGINT(20),
        TxOctets BIGINT(20),
        Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE PEER_LINK_UTIL(
        PortID          BIGINT(20),
        peeringScope    varchar(100),
        Region          varchar(100),
        peeringType     varchar(100),
        RouterName      varchar(128),
        IfDescr         varchar(128),
	IfAlias	varchar(500),
        PeeringPartner  varchar(100),
        95PerIn         FLOAT DEFAULT 0,
        95PerOut        FLOAT DEFAULT 0,
        PeakIn          FLOAT DEFAULT 0,
        PeakOut         FLOAT DEFAULT 0,
        AvgIn           FLOAT DEFAULT 0,
        AvgOut          FLOAT DEFAULT 0,
        MinIn           FLOAT DEFAULT 0,
        MinOut          FLOAT DEFAULT 0,
        VolumeIn        FLOAT DEFAULT 0,
        VolumeOut       FLOAT DEFAULT 0
);
SET @KBPS=1000;
INSERT INTO TEMP_AllFields(peeringScope,peeringRegion,peeringType,peeringPartner,NodeNumber,IfIndex,IfDescr,IfAlias)
SELECT 
'Domestic',
TRIM(SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,LOCATE(' ',SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,CHAR_LENGTH(IfAlias) )))),
TRIM(SUBSTRING(IfAlias,LOCATE('Domestic',IfAlias)+8,LOCATE('Peering',IfAlias)-(LOCATE('Domestic',IfAlias)+8))), 
TRIM(SUBSTRING(IfAlias,LOCATE('Peering:',IfAlias)+8,LOCATE('Region:',IfAlias)-(LOCATE('Peering',IfAlias)+8))),
NodeNumber, IfIndex, IfDescr, IfAlias
FROM (select IfAlias,NodeNumber,IfIndex,IfDescr from NODEIF_TBL where IfAlias like '%Domestic%' and IfAlias like '%Peering%')a;

INSERT INTO TEMP_AllFields(peeringScope,peeringRegion,peeringType,peeringPartner,NodeNumber,IfIndex,IfDescr,IfAlias)
SELECT
'International',
TRIM(SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,LOCATE(' ',SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,CHAR_LENGTH(IfAlias) )))),
TRIM(SUBSTRING(IfAlias,LOCATE('International',IfAlias)+13,LOCATE('Peering',IfAlias)-(LOCATE('International',IfAlias)+13))),
TRIM(SUBSTRING(IfAlias,LOCATE('Peering:',IfAlias)+8,LOCATE('Region:',IfAlias)-(LOCATE('Peering',IfAlias)+8))),
NodeNumber, IfIndex, IfDescr, IfAlias
FROM (select IfAlias,NodeNumber,IfIndex,IfDescr from NODEIF_TBL where IfAlias like '%International%' and IfAlias like '%Peering%')a;

UPDATE TEMP_AllFields a JOIN VLANPRT_TBL b ON a.NodeNumber=b.NodeID and a.IfIndex=b.IfIndex set a.PortId=b.PrtId;
UPDATE TEMP_AllFields a JOIN NODEIF_TBL b ON a.NodeNumber=b.NodeNumber and a.IfIndex=b.IfIndex set a.IfDescr=b.IfDescr;
UPDATE TEMP_AllFields a JOIN NODE_TBL b ON a.NodeNumber=b.NodeNumber set a.NodeName=b.NodeName;

set @where=' 1';
IF(p_peeringScope!='ALL')
THEN
set @where=CONCAT(@where,' and peeringScope in (',p_peeringScope,')');
END IF;

IF(p_Region!='ALL')
THEN
set @where=CONCAT(@where,' and peeringRegion in (',p_Region,')');
END IF;

IF(p_peeringType!='ALL')
THEN
set @where=CONCAT(@where,' and peeringType in (',p_peeringType,')');
END IF;

IF(p_peeringPartner!='ALL')
THEN
set @where=CONCAT(@where,' and peeringPartner in (',p_peeringPartner,')');
END IF;
SET @query=CONCAT("INSERT INTO TEMP_FILTER_AllFields SELECT * FROM TEMP_AllFields where ", @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

CREATE INDEX iPortIndex ON TEMP_FILTER_AllFields(PortID);

set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_FILTER_AllFields;

set @query = CONCAT("INSERT INTO TMP_TRAFFIC_TBL(PortID,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;



CREATE INDEX iPortIndex ON TMP_TRAFFIC_TBL(PortID);
CREATE INDEX iTimeIndex ON TMP_TRAFFIC_TBL(Time_1);

INSERT INTO PEER_LINK_UTIL(PortID,peeringScope,Region,peeringType,RouterName,IfDescr,IfAlias,PeeringPartner,PeakIn,PeakOut,AvgIn,AvgOut,MinIn,MinOut,VolumeIn,VolumeOut)
        select b.PortID,peeringScope,peeringRegion,peeringType,NodeName,IfDescr,IfAlias,peeringPartner,Max(RcvOctets),Max(TxOctets),Avg(RcvOctets),Avg(TxOctets),Min(RcvOctets),Min(TxOctets),SUM(RcvOctets),SUM(TxOctets) from TEMP_FILTER_AllFields a JOIN TMP_TRAFFIC_TBL b ON a.PortID = b.PortID group by b.PortID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","RcvOctets");
Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerIn = Traffic
        where a.PortID = b.EntityID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","TxOctets");

Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerOut = Traffic
        where a.PortID = b.EntityID;

CREATE TEMPORARY TABLE FINAL_FETCH(peeringScope varchar(256),Region varchar(256),PeeringType varchar(256),NodeName varchar(256),IfDescr varchar(256), IfAlias VARCHAR(500), PeeringPartner varchar(256),95PercIn varchar(256),95PercOut varchar(256),PeakIn varchar(256),PeakOut varchar(256),AvgIn varchar(256),AvgOut varchar(256),MinIn varchar(256),MinOut varchar(256),VolumeIn varchar(256),VolumeOut varchar(256));

INSERT INTO FINAL_FETCH values('Peering Scope','Region', 'Peering Type','Router','Interface', 'Interface Alias', 'Peering Partner', '95 Percentile IN(Kbps)','95 Percentile OUT(Kbps)','Peak Traffic IN(Kbps)','Peak Traffic OUT(Kbps)', 'Average Traffic IN(Kbps)','Average Traffic OUT(Kbps)', 'Min Traffic IN(Kbps)', 'Min Traffic OUT(Kbps)','Volume IN(Kbps)','Volume OUT(Kbps)');
INSERT INTO FINAL_FETCH
select peeringScope,Region,peeringType,RouterName,REPLACE(IfDescr,'^',' '),REPLACE(IfAlias,'^',' '),PeeringPartner,ROUND((95PerIn/@KBPS),2),ROUND(95PerOut/@KBPS,2),ROUND(PeakIn/@KBPS,2),ROUND(PeakOut/@KBPS,2),ROUND(AvgIn/@KBPS,2),ROUND(AvgOut/@KBPS,2),ROUND(MinIn/@KBPS,2),ROUND(MinOut/@KBPS,2),ROUND(VolumeIn/@KBPS,2),ROUND(VolumeOut/@KBPS,2) from PEER_LINK_UTIL;

SET @fileName=CONCAT(@dir_name,'/NWP_Peering_Link_Utilization_Report.csv');

SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_5_classOfService; 
 CREATE PROCEDURE mpls_reporting_6_4_5_classOfService(city varchar(64), service varchar(64),inputNodeName varchar(256),p_className varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_COSFCNAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NAME_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),nodeNumber integer);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;


CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int, NodeName varchar(256),IfIndex int,IfDescr varchar(256));
INSERT INTO TEMP_NODEIF_TBL
select a.NodeNumber,a.NodeName,b.IfIndex,b.IfDescr from NODEIF_TBL b, TEMP_NODE_TBL a where a.NodeNumber=b.NodeNumber; 

CREATE INDEX NodeNumberIndexNODEIF on TEMP_NODEIF_TBL(NodeNumber,IfIndex);

CREATE TEMPORARY TABLE TEMP_COSFCNAME_TBL(QSTID int,NodeName varchar(256),IfDescr varchar(256),NodENumber INTEGER,IfIndex INTEGER,configIndex INTEGER, PolicyIndex INTEGER ,ObjectIndex INTEGER,CosFCName varchar(64));

if(p_className = 'ALL')
THEN
	INSERT INTO TEMP_COSFCNAME_TBL SELECT QSTID, NodeName, IfDescr,a.NodeNumber,a.IfIndex,c.configIndex,c.PolicyIndex,c.ObjectIndex,CosFCNAME from TEMP_NODEIF_TBL a, COSFC_TBL b, COSQSTAT_TBL c where c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber = c.QNumber;
ELSE
	SET @query1= CONCAT("INSERT INTO TEMP_COSFCNAME_TBL SELECT QSTID, NodeName, IfDescr,a.NodeNumber,a.IfIndex,c.configIndex,c.PolicyIndex,c.ObjectIndex, CosFCNAME from TEMP_NODEIF_TBL a, COSFC_TBL b, COSQSTAT_TBL c where b.CosFCNAME IN (",p_className,") AND c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber = c.QNumber ");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
END IF;

CREATE index i1 ON TEMP_COSFCNAME_TBL(QSTID);
create index i2 on TEMP_COSFCNAME_TBL(NodeNumber,ConfigIndex);
create index i3 on TEMP_COSFCNAME_TBL(NodeNumber,ConfigIndex,PolicyIndex,ObjectIndex);

drop temporary table if exists childQOS;
create temporary table childQOS like QOS_TBL;
insert into childQOS select A.* from QOS_TBL A JOIN TEMP_COSFCNAME_TBL B ON A.Nodenumber = B.NodeNumber and A.ConfigIndex = B.ConfigIndex and A.PolicyIndex = B.PolicyIndex and A.ObjectIndex = B.ObjectIndex and A.IfIndex = B.IfIndex;

drop temporary table if exists parentQOS;
create temporary table parentQOS (NodENumber INTEGER,ConfigIndex INTEGER,ObjectIndex INTEGER,PolicyIndex INTEGER,Name VARCHAR(100),Direction VARCHAR(10),Type VARCHAR(10));
insert into parentQOS select B.NodeNumber,B.ConfigIndex,B.ObjectIndex , B.PolicyIndex ,Name,A.Direction,A.Type from QOS_TBL A JOIN childQOS B on A.NodENumber = B.NodeNumber and A.PolicyIndex = B.PolicyIndex and A.ObjectIndex = B.ParentIndex JOIN QOS_NAME_TBL C ON A.NodeNumber = C.NodENumber and A.ConfigIndex =C.ConfigIndex;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(DiffId int,dropRate bigint(20),minTrafficValue bigint(20), maxTrafficValue bigint(20), avgTrafficValue bigint(20),peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE INDEX DiffIDIndex2 on TEMP_TRAFFIC_TBL(DiffID);


INSERT INTO TEMP_TRAFFIC_TBL(DiffId,dropRate,minTrafficValue, maxTrafficValue, avgTrafficValue)
SELECT DiffId,sum(DropOctets),min(TxOctets), max(TxOctets),avg(TxOctets) FROM ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL a JOIN TEMP_COSFCNAME_TBL b ON a.DiffID=b.QSTID where Time_1>startTime and Time_1<endTime group by DiffID;



UPDATE TEMP_TRAFFIC_TBL a JOIN ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL b ON a.DiffId=b.DiffId set peakTime=if(ROUND(a.maxTrafficValue/1000,2)=0,'NA',b.Time_1) where a.maxTrafficValue=b.TxOctets and Time_1>startTime and Time_1<endTime;





select 'N/A' as ServiceType,
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName,IfDescr,CosFCNAME,if(Name IS NOT NULL,Name,'N/A') as PolicyName,dropRate,ROUND(minTrafficValue/1000,2),ROUND(maxTrafficValue/1000,2),ROUND(avgTrafficValue/1000,2),peakTime from TEMP_TRAFFIC_TBL JOIN TEMP_COSFCNAME_TBL A ON DiffId = QSTID LEFT JOIN parentQOS B ON A.NodeNumber=B.NodeNumber and A.ConfigIndex = B.ConfigIndex and A.PolicyIndex = B.PolicyIndex and A.ObjectIndex = B.ObjectIndex ;



END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_5_classOfService_24; 
 CREATE PROCEDURE mpls_reporting_6_4_5_classOfService_24(city varchar(64), service varchar(64),inputNodeName varchar(256),p_className varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_COSFCNAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NAME_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),nodeNumber integer);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;


CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int, NodeName varchar(256),IfIndex int,IfDescr varchar(256));
INSERT INTO TEMP_NODEIF_TBL
select a.NodeNumber,a.NodeName,b.IfIndex,b.IfDescr from NODEIF_TBL b, TEMP_NODE_TBL a where a.NodeNumber=b.NodeNumber; 

CREATE INDEX NodeNumberIndexNODEIF on TEMP_NODEIF_TBL(NodeNumber,IfIndex);

CREATE TEMPORARY TABLE TEMP_COSFCNAME_TBL(QSTID int,NodeName varchar(256),IfDescr varchar(256),NodENumber INTEGER,IfIndex INTEGER,configIndex INTEGER, PolicyIndex INTEGER ,ObjectIndex INTEGER,CosFCName varchar(64));

if(p_className = 'ALL')
THEN
	INSERT INTO TEMP_COSFCNAME_TBL SELECT QSTID, NodeName, IfDescr,a.NodeNumber,a.IfIndex,c.configIndex,c.PolicyIndex,c.ObjectIndex,CosFCNAME from TEMP_NODEIF_TBL a, COSFC_TBL b, COSQSTAT_TBL c where c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber = c.QNumber;
ELSE
	SET @query1= CONCAT("INSERT INTO TEMP_COSFCNAME_TBL SELECT QSTID, NodeName, IfDescr,a.NodeNumber,a.IfIndex,c.configIndex,c.PolicyIndex,c.ObjectIndex, CosFCNAME from TEMP_NODEIF_TBL a, COSFC_TBL b, COSQSTAT_TBL c where b.CosFCNAME IN (",p_className,") AND c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber = c.QNumber ");
	PREPARE statement1 from @query1;
	EXECUTE statement1;
END IF;

CREATE index i1 ON TEMP_COSFCNAME_TBL(QSTID);
create index i2 on TEMP_COSFCNAME_TBL(NodeNumber,ConfigIndex);
create index i3 on TEMP_COSFCNAME_TBL(NodeNumber,ConfigIndex,PolicyIndex,ObjectIndex);

drop temporary table if exists childQOS;
create temporary table childQOS like QOS_TBL;
insert into childQOS select A.* from QOS_TBL A JOIN TEMP_COSFCNAME_TBL B ON A.Nodenumber = B.NodeNumber and A.ConfigIndex = B.ConfigIndex and A.PolicyIndex = B.PolicyIndex and A.ObjectIndex = B.ObjectIndex and A.IfIndex = B.IfIndex;

drop temporary table if exists parentQOS;
create temporary table parentQOS (NodENumber INTEGER,ConfigIndex INTEGER,ObjectIndex INTEGER,PolicyIndex INTEGER,Name VARCHAR(100),Direction VARCHAR(10),Type VARCHAR(10));
insert into parentQOS select B.NodeNumber,B.ConfigIndex,B.ObjectIndex , B.PolicyIndex ,Name,A.Direction,A.Type from QOS_TBL A JOIN childQOS B on A.NodENumber = B.NodeNumber and A.PolicyIndex = B.PolicyIndex and A.ObjectIndex = B.ParentIndex JOIN QOS_NAME_TBL C ON A.NodeNumber = C.NodENumber and A.ConfigIndex =C.ConfigIndex;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(DiffId int,dropRate bigint(20),minTrafficValue bigint(20), maxTrafficValue bigint(20), avgTrafficValue bigint(20),peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE INDEX DiffIDIndex2 on TEMP_TRAFFIC_TBL(DiffID);


INSERT INTO TEMP_TRAFFIC_TBL(DiffId,dropRate,minTrafficValue, maxTrafficValue, avgTrafficValue)
SELECT DiffId,sum(DropOctets),min(TxOctets), max(TxOctets),avg(TxOctets) FROM ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL a JOIN TEMP_COSFCNAME_TBL b ON a.DiffID=b.QSTID where Time_1>startTime and Time_1<endTime group by DiffID;



UPDATE TEMP_TRAFFIC_TBL a JOIN ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL b ON a.DiffId=b.DiffId set peakTime=if(ROUND(a.maxTrafficValue/1000,2)=0,'NA',b.Time_1) where a.maxTrafficValue=b.TxOctets and Time_1>startTime and Time_1<endTime;




CREATE TEMPORARY TABLE FINAL_FETCH(serviceType varchar(256),Service varchar(256),city varchar(256),Router varchar(256),Interface varchar(256),CosFCName varchar(256),PolicyName VARCHAR(256),dropRate varchar(256),minTraffValue varchar(256),maxTrafficValue varchar(256),avgTraffic varchar(256),peakTime varchar(256) DEFAULT '0000-00-00 00:00:00');

INSERT INTO FINAL_FETCH values('Service Type','Service','City','Router','Interface','Class of Service','Policy Name','Drop Rate','Min Traffic(Kbps)','Max Traffic(Kbps)','Average Traffic(Kbps)','Peak Traffic Time');
INSERT INTO FINAL_FETCH

select 'N/A' as ServiceType,
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName,REPLACE(IfDescr,'^',' '),CosFCNAME,if(Name IS NOT NULL,Name,'N/A') as PolicyName,dropRate,minTrafficValue/1000,maxTrafficValue/1000,avgTrafficValue/1000,peakTime from TEMP_TRAFFIC_TBL JOIN TEMP_COSFCNAME_TBL A ON DiffId = QSTID LEFT JOIN parentQOS B ON A.NodeNumber=B.NodeNumber and A.ConfigIndex = B.ConfigIndex and A.PolicyIndex = B.PolicyIndex and A.ObjectIndex = B.ObjectIndex ;



SET @fileName=CONCAT(@dir_name,'/NWP_IPMPLS_CORE_NETWORK_CLASS_OF_SERVICE_TRAFFIC.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 

END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_4_6_Latency; 
 CREATE PROCEDURE mpls_reporting_6_4_6_Latency(sourceCity varchar(1000),destCity varchar(1000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS SHADOW_SOURCE;
DROP TEMPORARY TABLE IF EXISTS SHADOW_DEST;
DROP TEMPORARY TABLE IF EXISTS DELAY_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
CREATE TEMPORARY TABLE SHADOW_SOURCE LIKE NODE_TBL;
CREATE TEMPORARY TABLE SHADOW_DEST  LIKE NODE_TBL;

CREATE TEMPORARY TABLE DELAY_TBL (
        Source  VARCHAR(100),
        Destination VARCHAR(100),
        SourceIP        VARCHAR(100),
        DestinationIP VARCHAR(100),
        Latency FLOAT(12,2),
        Jitter  INTEGER(10),
        PacketLoss INTEGER(10),
        PeakLatency     FLOAT(12,2),
        PeakLatencyTime VARCHAR(100));
IF(sourceCity = 'ALL')
THEN
        INSERT INTO SHADOW_SOURCE select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_SOURCE select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',sourceCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;
IF(destCity = 'ALL')
THEN
        INSERT INTO SHADOW_DEST select * from NODE_TBL;
ELSE
        set @query=CONCAT('INSERT INTO SHADOW_DEST select a.* from NODE_TBL a, nodeToCity b where a.NodeID=b.NodeIp and City IN (',destCity,')');
        PREPARE stmt1 from @query;
        EXECUTE stmt1;
        DEALLOCATE PREPARE stmt1;
END IF;


INSERT INTO DELAY_TBL
select Ori.NodeName,Ter.NodeName,SourceIP,DestinationIP,AvgDelay,AvgJitter,PacketLoss,Max(AvgDelay),"" from VPN_DELAY_TABLE a JOIN SHADOW_SOURCE Ori ON a.SourceIP = Ori.NodeID JOIN
SHADOW_DEST Ter ON a.DestinationIP = Ter.NodeID where a.Time_1>startTime and a.Time_1<endTime group by SourceIP,DestinationIP;

CREATE INDEX i1 ON DELAY_TBL(SourceIP,DestinationIP);
Update DELAY_TBL a JOIN VPN_DELAY_TABLE b ON  b.SourceIP = a.SourceIP and b.DestinationIP = a.DestinationIP set PeakLatencyTime = if(ROUND(Latency/2)=0,'NA',b.Time_1) where AvgDelay = Latency and Time_1 > startTime and Time_1 < endTime ;

CREATE TEMPORARY TABLE FINAL_FETCH( Source  VARCHAR(100),
        Destination VARCHAR(100),
        SourceIP        VARCHAR(100),
        DestinationIP VARCHAR(100),
        Latency VARCHAR(256),
        Jitter  VARCHAR(256),
        PacketLoss VARCHAR(256),
        PeakLatency     VARCHAR(256),
        PeakLatencyTime VARCHAR(100));
INSERT INTO FINAL_FETCH values('Source Router', 'Destination Router', 'Source IP','Destination IP', 'Latency','Jitter','Packet-Drops','Peak Latency Value','Peak Latency Time');
INSERT INTO FINAL_FETCH
select b.city,c.city,SourceIP,DestinationIP,ROUND(Latency,2),ROUND(Jitter,2),ROUND(PacketLoss,2),ROUND(PeakLatency,2),PeakLatencyTime from DELAY_TBL a , nodeToCity b, nodeToCity c where a.SourceIp=b.NodeIp and a.DestinationIp=c.NodeIp;

SET @fileName=CONCAT(@dir_name,'/NWP_Latency_Jitter_PacketDrops.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_10_CustomerUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_10_CustomerUtil(city varchar(5000), service varchar(5000), inputNodeName varchar(8000),customerName varchar(17000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODENAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_TBL;

CREATE TEMPORARY TABLE TEMP_CITY_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_NODENAME_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_CUSTOMER_TBL like CUSTOMER_VRF_TBL;


SET @query1 := CONCAT(' INSERT INTO TEMP_CUSTOMER_TBL SELECT * from CUSTOMER_VRF_TBL where VpnName in(',customerName);
SET @query2 := CONCAT(@query1,')');
IF customerName='ALL'
THEN
INSERT INTO TEMP_CUSTOMER_TBL
SELECT * from CUSTOMER_VRF_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT * from TEMP_CUSTOMER_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT * from TEMP_CUSTOMER_TBL;
ELSE

PREPARE statement1 from @query2;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT * from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT * from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODENAME_TBL SELECT * from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODENAME_TBL
SELECT * from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;


CREATE TEMPORARY TABLE TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL(
	PortID BIGINT(20),
	RcvOctets BIGINT(20),
	TxOctets BIGINT(20),
	Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(PortID int, maxTrafficValueIn bigint(20),maxOutTrafficValueOut bigint(20),avgTrafficValueIn bigint(20),avgTrafficValueOut bigint(20),inPeakTime varchar(20) DEFAULT '0000-00-00 00:00:00',outPeakTime varchar(20) DEFAULT '0000-00-00 00:00:00');

CREATE INDEX portIdIndex1 on TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL(PortId);
CREATE INDEX portIDIndex2 on TEMP_TRAFFIC_TBL(PortID);
CREATE INDEX portIDIndex3 on TEMP_NODENAME_TBL(PortID);

INSERT INTO TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL 
	SELECT a.PortId,a.rcvOctets,a.txoctets,a.Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN TEMP_NODENAME_TBL b ON a.PortID = b.PortID where Time_1>startTime and Time_1<endTime;

INSERT INTO TEMP_TRAFFIC_TBL (PortID, maxTrafficValueIn,maxOutTrafficValueOut,avgTrafficValueIn,avgTrafficValueOut)
	SELECT  a.PortID,max(a.RcvOctets),max(a.TxOctets),avg(a.RcvOctets),avg(a.RcvOctets) from TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL a group by a.PortID;

UPDATE TEMP_TRAFFIC_TBL A JOIN TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL B ON A.PortId=B.PortId set inPeakTime= if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TEMP_ROUTERTRAFFIC_VLANPRT_SCALE_TBL B ON A.PortId=B.PortId set OutPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA', B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;


DROP TEMPORARY TABLE IF EXISTS FINAL_TBL;
CREATE TEMPORARY TABLE FINAL_TBL(
	City		VARCHAR(100),
	RouterName	VARCHAR(100),
	IfDescr		VARCHAR(100),
	CustomerName	VARCHAR(100),
	ServiceType	VARCHAR(100),
	BW		VARCHAR(100),
	MaxInUtil	VARCHAR(100),
	MaxOutUtil	VARCHAR(100),
	AvgInUtil	VARCHAR(100),
	AvgOutUtil	VARCHAR(100),
	PeakInTime	VARCHAR(100),
	PeakOutTime	VARCHAR(100));

INSERT INTO FINAL_TBL VALUES( "City","RouterName","IfDescr","CustomerName","ServiceType","BW(Kbps)","Max In Traffic(Kbps)","Max Out Traffic(Kbps)","Avg In Traffic(Kbps)","Avg Out Traffic(Kbps)","PeakInTime","PeakOutTime");

INSERT INTO FINAL_TBL

SELECT (substring(NodeName,1,3)) as City,(NodeName),REPLACE(a.IfDescr,'^',' '),(a.VpnName),
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
a.IfSpeed,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),inPeakTime,outPeakTime  from CUSTOMER_VRF_TBL a, TEMP_TRAFFIC_TBL b where a.PortID=b.PortID and avgTrafficValueOut >0 and avgTrafficValueIn > 0;


SET @fileName=CONCAT(@dir_name,'/MPLS_CityWise_Customer_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 



END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_1_VRFUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_1_VRFUtil(city varchar(64), service varchar(64),inputNodeName varchar(256),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_TBL;
DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_CITY_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_NODE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE EXCEED_COUNT(QSTID BIGINT(20),exceed INTEGER);
CREATE TEMPORARY TABLE TRAFFIC_TBL(QSTID integer,IfSpeed integer,TxOctets bigint(20),Time_1 timeStamp DEFAULT '0000-00-00 00:00:00');

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT * from CUSTOMER_VRF_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT * from CUSTOMER_VRF_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT * from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT * from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT * from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT * from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

CREATE INDEX NodeNumberIndexNODEIF on TEMP_NODE_TBL(NodeNumber,IfIndex);

DROP TEMPORARY TABLE IF EXISTS TEMP_COSFCNAME_TBL;
CREATE TEMPORARY TABLE TEMP_COSFCNAME_TBL(QSTID int,NodeName varchar(256),IfDescr varchar(256),CircuitID varchar(128), CosFCName varchar(64),IfSpeed BIGINT(20),VrfName varchar(128),NodeNumber int, IfIndex int);
CREATE INDEX QSTIDIndex on TEMP_COSFCNAME_TBL(QSTID);

drop temporary table if exists cosqstat;
create temporary table cosqstat (index(NodeNumber,IfIndex),index(NodeNumber,QNumber)) as select * from COSQSTAT_TBL ;
INSERT INTO TEMP_COSFCNAME_TBL(QSTID,NodeName,IfDescr,CircuitID, CosFCName,IfSpeed,NodeNumber,IfIndex)
	SELECT QSTID, NodeName, IfDescr, CircuitID , CosFCNAME,IfSpeed,a.NodeNumber,c.IfIndex from TEMP_NODE_TBL a, COSFC_TBL b, cosqstat c  where c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber=c.QNumber;

drop temporary table if exists vpnif;
create temporary table vpnif (index(VpnNode,VpnIFIndex))as select VpnId,VpnNode,VpnIfIndex from VPN_IFINDEX_TBL;

UPDATE TEMP_COSFCNAME_TBL a, VPN_TBL b, vpnif c  set a.VrfName=b.VpnName where a.NodeNumber=c.VpnNode and a.IfIndex=c.VpnIfIndex and c.VpnId=b.VpnId;

INSERT INTO TRAFFIC_TBL
	SELECT QSTId,IfSpeed,a.TxOctets,Time_1 from ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL a JOIN TEMP_COSFCNAME_TBL b ON DiffId = QSTID where  Time_1>startTime and Time_1<endTime;

INSERT INTO EXCEED_COUNT
        select QSTId,count(TxOctets) from TRAFFIC_TBL where (TxOctets/(IfSpeed*10)>70 or TxOctets/(IfSpeed*10)>70) group by QSTId;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(DiffId int, maxTrafficValue bigint(20), avgTrafficValue bigint(20),peakTime varchar(20) DEFAULT '0000-00-00 00:00:00',exceedCount INTEGER DEFAULT 0);
CREATE INDEX DiffIDIndex2 on TEMP_TRAFFIC_TBL(DiffID);
CREATE INDEX QSTIdIn on TRAFFIC_TBL(QSTId);

INSERT INTO TEMP_TRAFFIC_TBL(DiffId, maxTrafficValue, avgTrafficValue)
SELECT QSTId,max(TxOctets),avg(TxOctets) FROM TRAFFIC_TBL group by QSTId;

UPDATE TEMP_TRAFFIC_TBL a set peakTime=(SELECT if(ROUND(a.maxTrafficValue/1000,2)=0,'NA',Time_1) from TRAFFIC_TBL b where a.DiffId=QSTId and a.maxTrafficValue=b.TxOctets order by Time_1 desc limit 1);

UPDATE TEMP_TRAFFIC_TBL a JOIN EXCEED_COUNT b ON a.DiffId = b.QSTId set a.exceedCount = b.exceed;

CREATE TEMPORARY TABLE FINAL_FETCH(City varchar(100),service varchar(100), NodeName varchar(100),CircuitID varchar(100),IfDescr varchar(256),CosFCNAME varchar(100),maxTrafficValue varchar(100),avgTrafficValue varchar(100),peakTime varchar(100),exceedCount varchar(100),VrfName varchar(100)); 
INSERT INTO FINAL_FETCH values('City','Service','Location (PE Router Name)','CircuitID','Interface Name','Class Name','Max Traffic(Kbps)','Average Traffic(Kbps)','Peak Time', 'Number of times threshold crossed','Vrf Name');

INSERT INTO FINAL_FETCH
select substring(NodeName,1,3),
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
NodeName,CircuitID,REPLACE(IfDescr,'^',' '),CosFCNAME,ROUND(maxTrafficValue/1000,2),ROUND(avgTrafficValue/1000,2),peakTime,exceedCount,VrfName from TEMP_TRAFFIC_TBL JOIN TEMP_COSFCNAME_TBL ON DiffId = QSTID and avgTrafficValue >0 group by QSTID;

SET @fileName=CONCAT(@dir_name,'/MPLS_VRF_UTILIZATION.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 





END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_2_LspUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_2_LspUtil(p_LspName varchar(1000),startTime timestamp, endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS LSP_MAX_TRAFFIC;
DROP TEMPORARY TABLE IF EXISTS LSP_PEAK_TIME;
DROP TEMPORARY TABLE IF EXISTS finalFetching;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE LSP_MAX_TRAFFIC(LspID smallint(5) unsigned, maxTraffic bigint(20) unsigned,avgTraffic bigint(20) unsigned,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
IF(p_LspName = 'ALL')
THEN
	INSERT INTO LSP_MAX_TRAFFIC(LspID , maxTraffic, avgTraffic) SELECT a.LspID,MAX(LspOctets),AVG(LspOctets) FROM ROUTERTRAFFIC_LSP_SCALE1_TBL a where startTime<a.Time_1 AND endTime>a.Time_1 GROUP BY LspID;
ELSE
	SET @query1=CONCAT("INSERT INTO LSP_MAX_TRAFFIC(LspID , maxTraffic, avgTraffic) SELECT a.LspID,MAX(LspOctets),AVG(LspOctets) FROM ROUTERTRAFFIC_LSP_SCALE1_TBL a JOIN LSP_TBL b On a.lspId = b.lspID where b.LspName IN (",p_LspName,") AND '",startTime,"'<a.Time_1 AND '",endTime,"'>a.Time_1 GROUP BY LspID ");
	PREPARE stmt1 from @query1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
END IF;
UPDATE LSP_MAX_TRAFFIC a set a.peakTime=(select if(ROUND(a.maxTraffic/1000,2)=0,'NA',Time_1) from ROUTERTRAFFIC_LSP_SCALE1_TBL b where a.LspID=b.LspID and a.maxTraffic=b.LspOctets AND startTime<b.Time_1 AND endTime>b.Time_1 order by Time_1 desc limit 1);



CREATE TEMPORARY TABLE finalFetching (LspID smallint(6), LspName varchar(128),PathName VARCHAR(500),OriNodeName varchar(128), TerNodeName varchar(128),PathType VARCHAR(30), status int,maxTraffic bigint(20) unsigned, avgTraffic bigint(20) unsigned,peakTime varchar(20));

INSERT INTO finalFetching( LspID,LspName,PathName, OriNodeName, TerNodeName,PathType,Status,maxTraffic, avgTraffic,peakTime) SELECT b.LspID,b.LspName,b.PathName,c.NodeName,d.NodeName,b.PathType,b.Status,maxTraffic,avgTraffic,peakTime  FROM LSP_MAX_TRAFFIC a, LSP_TBL b ,NODE_TBL c, NODE_TBL d  WHERE a.LspID=b.LspID and b.OriNodeNumber=c.NodeNumber AND b.TerNodeNumber=d.NodeNumber;


CREATE TEMPORARY TABLE FINAL_FETCH(LspName varchar(100),PathName varchar(100),OriNodeName varchar(256), TerNodeName varchar(256),PathType varchar(256),Status varchar(256), maxTraffic varchar(256), avgTraffic varchar(256),peakTime varchar(256));
INSERT INTO FINAL_FETCH values('Lsp Name','Path Name','Source Router','Destination Router','Path Type','Status','Peak Traffic(Kbps)','Avg Traffic(Kbps)','Peak Time');
INSERT INTO FINAL_FETCH
SELECT LspName,PathName,OriNodeName, TerNodeName,PathType,IF((status=1),'UP','DOWN'),ROUND( maxTraffic/1000,2), ROUND(avgTraffic/1000,2),peakTime FROM finalFetching ORDER BY LspID ;

SET @fileName=CONCAT(@dir_name,'/MPLS_LSP_Path_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCH INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 





END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_3_TempUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_3_TempUtil(city varchar(5000), service varchar(5000), inputNodeName varchar(8000),startTime timestamp,endTime timestamp)
BEGIN
DECLARE countNo int;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_TEMPERATURE;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TEMPERATURE_STATS_TBL;

CREATE TEMPORARY TABLE TEMP_MAX_AVG_TEMPERATURE(NodeNumber int, NodeName varchar(128), TempName varchar(50), maxTempValue int(10) unsigned, avgTempValue int(10) unsigned,PeakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

 CREATE TEMPORARY TABLE `TEMP_TEMPERATURE_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `TempName` varchar(50) DEFAULT NULL,
  `TempValue` int(10) unsigned DEFAULT NULL,
  `TimeStamp` timestamp
  );

INSERT INTO TEMP_TEMPERATURE_STATS_TBL
SELECT b.* from TEMP_STATS_TBL b JOIN TEMP_NODE_TBL a ON a.NodeNumber=b.NodeNo where TimeStamp>startTime AND TimeStamp<endTime;

INSERT INTO TEMP_MAX_AVG_TEMPERATURE (NodeNumber, NodeName, TempName, maxTempValue, avgTempValue)
SELECT a.NodeNumber, a.NodeName,TempName, MAX(b.TempValue), AVG(b.TempValue) FROM TEMP_NODE_TBL a,TEMP_TEMPERATURE_STATS_TBL b WHERE a.NodeNumber=b.NodeNo GROUP BY b.NodeNo,b.TempName;

CREATE INDEX Index1 ON TEMP_MAX_AVG_TEMPERATURE(NodeNumber,TempName);
CREATE INDEX Index2 ON TEMP_TEMPERATURE_STATS_TBL(NodeNo,TempName);

update TEMP_MAX_AVG_TEMPERATURE A JOIN TEMP_TEMPERATURE_STATS_TBL B  ON B.NodeNo=A.NodeNumber and A.TempName=B.TempName  Set PeakTime=if(ROUND(A.maxTempValue,2)=0,'NA',B.TimeStamp) where  A.maxTempValue=B.TempValue  ;

DROP  INDEX Index1 ON TEMP_MAX_AVG_TEMPERATURE;
DROP INDEX Index2 ON TEMP_TEMPERATURE_STATS_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;

CREATE TEMPORARY TABLE FINAL_FETCHING(
	ServiceFinal VARCHAR(100),
	CityFinal VARCHAR(100),
	RouterName VARCHAR(100),
	TempName VARCHAR(100),
	AverageTemp VARCHAR(100),
	MaxTemp VARCHAR(100),
	PeakTime VARCHAR(100));

INSERT INTO FINAL_FETCHING VALUES("Service","City","RouterName","TempName","Max Temperature (Deg .C)","Average Temperature (Deg. C)","Max Temperature Time");

INSERT INTO FINAL_FETCHING 
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(TempName,',',':'), ROUND(maxTempValue,2), ROUND(avgTempValue,2),PeakTime from TEMP_MAX_AVG_TEMPERATURE;


SET @fileName=CONCAT(@dir_name,'/MPLS_Router_Temperature.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCHING INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 



END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_4_BufferUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_4_BufferUtil(city varchar(5000), service varchar(5000), inputNodeName varchar(8000),startTime timestamp,endTime timestamp)
BEGIN

DECLARE countNo int;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_BUFFER;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_BUFFER_STATS_TBL;

CREATE TEMPORARY TABLE TEMP_MAX_AVG_BUFFER(NodeNumber int, NodeName varchar(128), BufferName varchar(50), maxBufferValue int(10) unsigned, avgBufferValue int(10) unsigned,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;


CREATE TEMPORARY TABLE `TEMP_BUFFER_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `BufferName` varchar(50) DEFAULT NULL,
  `BufferValue` int(10) unsigned DEFAULT NULL,
  `TimeStamp` timestamp
);

INSERT INTO TEMP_BUFFER_STATS_TBL
SELECT a.* from BUFFER_STATS_TBL a JOIN TEMP_NODE_TBL b ON b.NodeNumber=a.NodeNo where TimeStamp>startTime AND TimeStamp<endTime;
SELECT COUNT(*) INTO countNo FROM TEMP_NODE_TBL;

INSERT INTO TEMP_MAX_AVG_BUFFER (NodeNumber, NodeName, BufferName, maxBufferValue, avgBufferValue)
SELECT a.NodeNumber, a.NodeName,BufferName, MAX(b.BufferValue), AVG(b.BufferValue) FROM TEMP_NODE_TBL a,TEMP_BUFFER_STATS_TBL b  where a.NodeNumber=b.NodeNo GROUP BY b.NodeNo,b.BufferName;

CREATE INDEX Index1 ON TEMP_MAX_AVG_BUFFER(NodeNumber,BufferName);
CREATE INDEX Index2 ON TEMP_BUFFER_STATS_TBL(NodeNo,BufferName);


update TEMP_MAX_AVG_BUFFER A JOIN TEMP_BUFFER_STATS_TBL B ON B.NodeNo=A.NodeNumber and A.BufferName=B.BufferName set  PeakTime=if(ROUND(A.maxBufferValue,2)=0,'NA',B.TimeStamp) where A.maxBufferValue=B.BufferValue ;

DROP INDEX Index1 ON TEMP_MAX_AVG_BUFFER;
DROP INDEX Index2 ON TEMP_BUFFER_STATS_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;

CREATE TEMPORARY TABLE FINAL_FETCHING(
	ServiceFinal VARCHAR(100),
	CityFinal VARCHAR(100),
	RouterName VARCHAR(100),
	BufferName VARCHAR(100),
	AverageBuffer VARCHAR(100),
	MaxBuffer VARCHAR(100),
	PeakTime VARCHAR(100));

INSERT INTO FINAL_FETCHING VALUES("Service","City","RouterName","BufferName","Maximum Buffer Size","Average Buffer Size","PeakTime");

INSERT INTO FINAL_FETCHING 
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(BufferName,',',':'), ROUND(maxBufferValue,2), ROUND(avgBufferValue,2), peakTime  FROM TEMP_MAX_AVG_BUFFER order by NodeName;

SET @fileName=CONCAT(@dir_name,'/MPLS_Router_Buffer_Report.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCHING INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 


END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_5_CpuUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_5_CpuUtil(city varchar(20000), service varchar(20000), inputNodeName varchar(20000),startTime timestamp, endTime timestamp)
BEGIN

DECLARE countNo int;
DECLARE i int;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_UTIL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_TIME;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_CPU;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_STATS_TBL;


CREATE TEMPORARY TABLE TEMP_MAX_AVG_CPU(NodeNumber int, NodeName varchar(128), cpuName varchar(50), maxCpuUtil int(10) unsigned, avgCpuUtil int(10) unsigned,peakTime timestamp DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int,VendorName varchar(20));
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int,VendorName varchar(20));
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int,VendorName varchar(20));

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber,VendorName from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber,VendorName from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber,VendorName from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

 CREATE TEMPORARY TABLE `TEMP_CPU_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `CpuName` varchar(50) DEFAULT NULL,
  `CpuUtil` int(10) unsigned DEFAULT NULL,
  `CompState` int(10) unsigned DEFAULT NULL,
  `TimeStamp` timestamp 
 );

INSERT INTO TEMP_CPU_STATS_TBL
SELECT a.* from CPU_STATS_TBL a JOIN TEMP_NODE_TBL b ON b.NodeNumber=a.NodeNo where TimeStamp>startTime AND TimeStamp<endTime;
SELECT COUNT(*) INTO countNo FROM TEMP_NODE_TBL;
SET i=0;


INSERT INTO TEMP_MAX_AVG_CPU (NodeNumber, NodeName, cpuName, maxCpuUtil, avgCpuUtil)
SELECT a.NodeNumber, a.NodeName, cpuName, MAX(b.cpuUtil), AVG(b.cpuUtil) FROM TEMP_NODE_TBL a JOIN TEMP_CPU_STATS_TBL b   ON a.NodeNumber=b.NodeNo where (a.VendorName like '%cisco%') or  (a.VendorName like '%juniper%' and cpuName like '%Routing Engine%') GROUP BY b.NodeNo,b.cpuName;



CREATE INDEX index1 ON TEMP_MAX_AVG_CPU(NodeNumber,cpuName);
CREATE INDEX index2 ON TEMP_CPU_STATS_TBL(NodeNo,cpuName);
update TEMP_MAX_AVG_CPU A JOIN TEMP_CPU_STATS_TBL B ON B.NodeNo=A.NodeNumber and A.cpuName=B.cpuName set PeakTime= if(ROUND(A.maxCpuUtil,2)=0,'NA',B.TimeStamp) where A.maxCpuUtil=B.cpuUtil;
DROP INDEX index1 ON TEMP_MAX_AVG_CPU;
DROP INDEX index2 ON TEMP_CPU_STATS_TBL;

DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
CREATE TEMPORARY TABLE FINAL_FETCHING(
	ServiceFinal VARCHAR(100),
	CityFinal VARCHAR(100),
	RouterName VARCHAR(100),
	CpuName VARCHAR(100),
	AverageCPU VARCHAR(100),
	MaxCPU VARCHAR(100),
	PeakTime VARCHAR(100));

INSERT INTO FINAL_FETCHING VALUES("Service","City","RouterName","CpuName","Maximum CPU Util (%)","Average CPU Util (%)","PeakTime");
INSERT INTO FINAL_FETCHING 
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(cpuName,',',':'),ROUND(maxCpuUtil,2), ROUND(avgCpuUtil,2), peakTime  FROM TEMP_MAX_AVG_CPU order by NodeName;

SET @fileName=CONCAT(@dir_name,'/MPLS_Router_CPU_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCHING INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 




END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_6_StorageUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_6_StorageUtil(city varchar(20000), service varchar(20000), inputNodeName varchar(20000),startTime timestamp, endTime timestamp)
BEGIN

DECLARE countNo int;
DECLARE i int;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_UTIL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CPU_TIME;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_CPU;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_STORAGE_STATS_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_MAX_AVG_STORAGE;

CREATE TEMPORARY TABLE TEMP_MAX_AVG_STORAGE(NodeNumber int, NodeName varchar(128), storageName varchar(50), maxStorageUtil float, avgStorageUtil float,peakTime varchar(20) DEFAULT '0000-00-00 00:00:00');
CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),NodeNumber int);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),NodeNumber int);

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

 CREATE TEMPORARY TABLE `TEMP_STORAGE_STATS_TBL` (
  `NodeNo` smallint(5) unsigned DEFAULT NULL,
  `StorageName` varchar(50) DEFAULT NULL,
   StorageUtil float,
   TimeStamp timestamp
  );

INSERT INTO TEMP_STORAGE_STATS_TBL
SELECT a.NodeNo,a.StorageName,(a.StorageUsed/StorageSize)*100,a.TimeStamp from STORAGE_STATS_TBL a JOIN TEMP_NODE_TBL b ON b.NodeNumber=a.NodeNo where StorageSize != 0 and TimeStamp>startTime AND TimeStamp<endTime;

INSERT INTO TEMP_MAX_AVG_STORAGE (NodeNumber, NodeName, storageName, maxStorageUtil, avgStorageUtil)
SELECT a.NodeNumber, a.NodeName,storageName, MAX(b.StorageUtil), AVG(b.StorageUtil) FROM TEMP_NODE_TBL a JOIN TEMP_STORAGE_STATS_TBL b   ON a.NodeNumber=b.NodeNo  GROUP BY b.NodeNo,b.StorageName;

CREATE INDEX index1 ON TEMP_MAX_AVG_STORAGE(NodeNumber,storageName);
CREATE INDEX index2 ON TEMP_STORAGE_STATS_TBL(NodeNo,storageName);
update TEMP_MAX_AVG_STORAGE A JOIN TEMP_STORAGE_STATS_TBL B ON B.NodeNo=A.NodeNumber and A.storageName=B.storageName set PeakTime= if(ROUND(A.maxStorageUtil,2)=0,'NA',B.TimeStamp) where A.maxStorageUtil=B.storageUtil;
DROP INDEX index1 ON TEMP_MAX_AVG_STORAGE;
DROP INDEX index2 ON TEMP_STORAGE_STATS_TBL;

DROP TEMPORARY TABLE IF EXISTS FINAL_FETCHING;
CREATE TEMPORARY TABLE FINAL_FETCHING(
	ServiceFinal VARCHAR(100),
	CityFinal VARCHAR(100),
	RouterName VARCHAR(100),
	CpuName VARCHAR(100),
	AverageCPU VARCHAR(100),
	MaxCPU VARCHAR(100),
	PeakTime VARCHAR(100));

INSERT INTO FINAL_FETCHING VALUES("Service","City","RouterName","Storage Name","Maximum Storage Util (%)","Average Storage Util (%)","PeakTime");
INSERT INTO FINAL_FETCHING 
SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(NodeName,1,3),NodeName, REPLACE(storageName,',',':'),ROUND(maxStorageUtil,2), ROUND(avgStorageUtil,2), peakTime  FROM TEMP_MAX_AVG_STORAGE order by NodeName ;

SET @fileName=CONCAT(@dir_name,'/MPLS_Router_Storage_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_FETCHING INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 



END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_7_unusedServicePolicy; 
 CREATE PROCEDURE mpls_reporting_6_6_7_unusedServicePolicy(city varchar(5000), service varchar(5000), inputNodeName varchar(8000))
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODENAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_POLICIES;

CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),nodeNumber integer,VendorName VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),nodeNumber integer,VendorName VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),nodeNumber integer,VendorName VARCHAR(100));
CREATE TEMPORARY TABLE TEMP_POLICIES(QNumber integer,CosFCName varchar(256));

SET SESSION group_concat_max_len = 1000000;

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber,VendorName from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber,VendorName from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber,VendorName from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber,VendorName from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL1;
CREATE TEMPORARY TABLE TEMP_NODE_TBL1 as select * from TEMP_NODE_TBL;

DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_POLICY_TBL;
CREATE TEMPORARY TABLE FINAL_UNUSED_POLICY_TBL(
	ServiceFinal varchar(256),
	CityFinal varchar(256),
	RouterName varchar(256), 
	ConfigPolicyCount varchar(100), 
	UsedPolicyCount varchar(100), 
	UnusedPolicyCount varchar(100), 
	UnusedPolicyPer varchar(100), 
	UnusedList varchar(50000));

INSERT INTO FINAL_UNUSED_POLICY_TBL VALUES(
"Service", "City", "Router", "Config Policy count", "Used Policy count", "Unused Policy count", "Unused Policy(%)", "Unused policy list"
);

drop temporary table if exists totalPolicyCount;
create temporary table totalPolicyCount as 
	SELECT a.NodeNumber,COUNT(*) as policyCount FROM TEMP_NODE_TBL a JOIN COSFC_TBL b where a.VendorName like 'Juniper' and a.NodeNumber = b.NodeNumber and CosFCName not like '%COPP%' group by a.NodeNumber;

drop temporary table if exists cosfc;
create temporary table cosfc(index (NodeNumber,CosQNumber)) as select NodeNumber,CosQNumber,CosFCName from COSFC_TBL where NodeNumber IN (select NodeNumber from TEMP_NODE_TBL where VendorName like 'Juniper');

drop temporary table if exists cosqstat;
create temporary table cosqstat(index (NodeNumber,QNumber)) as select NodeNumber,QNumber from COSQSTAT_TBL where NodeNumber IN (select NodeNumber from TEMP_NODE_TBL where VendorName like 'Juniper');

drop temporary table if exists unUsedPolicyCount;
create temporary table unUsedPolicyCount as
	SELECT A.NodeNumber,count(*) as unUsedCount,GROUP_CONCAT(CosFCName) as unusedlist FROM cosfc A where CONCAT(A.NodeNumber,'-',A.CosQNumber) NOT IN
                (SELECT CONCAT(B.NodeNumber,'-',B.QNumber) FROM cosqstat B) 
		and CosFCName not like '%COPP%' group by A.NodeNumber; 
drop temporary table if exists cosfc;
drop temporary table if exists cosqstat;

insert into totalPolicyCount
	SELECT a.NodeNumber,COUNT(*) as policyCount FROM TEMP_NODE_TBL a JOIN QOS_NAME_TBL b where a.VendorName like 'Cisco' and a.NodeNumber = b.NodeNumber group by a.NodeNumber;
insert into unUsedPolicyCount 
	SELECT a.NodeNumber,count(*) as unUsedCount ,GROUP_CONCAT(Name) as unusedlist FROM TEMP_NODE_TBL a JOIN QOS_NAME_TBL b ON a.NodeNumber = b.NodeNumber
LEFT JOIN QOS_TBL c ON b.NodeNumber = c.NodeNumber and b.ConfigIndex = c.ConfigIndex where c.NodeNumber IS NULL and c.configIndex IS NULL group by a.NodeNumber;


	INSERT INTO FINAL_UNUSED_POLICY_TBL 
	select 
	CASE
	WHEN c.Nodename like '%MPL%' THEN "MPL"
	WHEN c.Nodename like '%ISP%' THEN "ISP"
	WHEN c.Nodename like '%CNV%' THEN "CNV"
	ELSE "-"
	END ,
	substring(c.Nodename,1,3),(c.Nodename),policyCount,(policyCount-unUsedCount),unUsedCount,ROUND((unUsedCount*100/policyCount),2),(unusedlist) from totalPolicyCount a,unUsedPolicyCount b,TEMP_NODE_TBL c where a.NodeNumber = b.NodeNumber and a.NodeNumber = c.NodeNumber;



SET @fileName=CONCAT(@dir_name,'/MPLS_Unused_Service_Policy.csv');
SET @query=CONCAT('SELECT * FROM FINAL_UNUSED_POLICY_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\' optionally enclosed by \'\"\' escaped by \'\"\'  ');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 



END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_8_unusedVrf; 
 CREATE PROCEDURE mpls_reporting_6_6_8_unusedVrf(city varchar(5000), service varchar(5000), inputNodeName varchar(8000))
BEGIN
DECLARE vrfCount INTEGER;
DECLARE usedVrfCount INTEGER;
DECLARE unUsedVrfCount INTEGER;
DECLARE countNo INTEGER;
DECLARE unusedPer FLOAT;
SET @unusedlist="";

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODENAME_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_VRF;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;
DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_VRF_TBL;

CREATE TEMPORARY TABLE TEMP_CITY_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_NODE_TBL(nodeName varchar(256),nodeNumber integer);
CREATE TEMPORARY TABLE TEMP_VRF(QNumber integer,VpnName VARCHAR(256));

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT distinct NodeName, NodeNumber from NODE_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT NodeName, NodeNumber from NODE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT distinct NodeName, NodeNumber from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT distinct NodeName,NodeNumber from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT distinct NodeName,NodeNumber from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;



SELECT COUNT(*) INTO countNo FROM TEMP_NODE_TBL;

DROP TEMPORARY TABLE IF EXISTS FINAL_UNUSED_VRF_TBL;
CREATE TEMPORARY TABLE FINAL_UNUSED_VRF_TBL(
	ServiceFinal varchar(100),
	CityFinal varchar(100),
	RouterName varchar(256), 
	ConfigVrfCount varchar(100), 
	UsedVrfCount varchar(100), 
	UnusedVrfCount varchar(100), 
	UnusedVrfPerc varchar(100),
	UnusedVRFList VARCHAR(50000));


INSERT INTO FINAL_UNUSED_VRF_TBL values("Service", "City", "Router", "Config Vrf Count", "Used Vrf Count", "Unused Vrf Count", "Unused Vrf(%)","Unused VRF list");

WHILE(countNo>0)
DO

SELECT nodeNumber,nodeName INTO @node, @routername FROM TEMP_NODE_TBL LIMIT 1;
SELECT COUNT(*) INTO vrfCount FROM (SELECT DISTINCT VpnID FROM MAP_NODE_VPN_TBL WHERE VpnNode= @node)c;
SELECT COUNT(*) INTO usedVrfCount FROM (SELECT DISTINCT VpnID FROM MAP_VPN_NODE_TBL WHERE VpnNode=@node) c;
SET unUsedVrfCount = vrfCount - usedVrfCount;



INSERT INTO TEMP_VRF(QNumber)
SELECT distinct VpnId FROM  MAP_NODE_VPN_TBL where VpnNode=@node and VpnId NOT IN
(SELECT  VpnId FROM MAP_VPN_NODE_TBL WHERE VpnNode=@node) group by VpnId;

UPDATE TEMP_VRF a, VPN_TBL b set a.VpnName = b.VpnName where a.QNumber = b.VpnID;

SET @unusedlist="";
SELECT count(*) INTO @listcount FROM TEMP_VRF;
        WHILE(@listcount > 0)
        DO
                SELECT VpnName INTO @qno FROM TEMP_VRF LIMIT 1;
                SET @unusedlist=CONCAT(@qno,",",@unusedlist);
                DELETE FROM TEMP_VRF LIMIT 1;
                SET @listcount = @listcount - 1;
        END WHILE;

SELECT substring(@unusedlist,1,length(@unusedlist)-1) INTO @unusedlist;
SET unusedPer=unUsedVrfCount*100/vrfCount;
IF unUsedVrfCount=0
THEN
        SET @unusedlist="";
        SET unusedPer=0.0;
END IF;



INSERT INTO FINAL_UNUSED_VRF_TBL VALUES(
CASE
WHEN @routername like '%MPL%' THEN "MPL"
WHEN @routername like '%ISP%' THEN "ISP"
WHEN @routername like '%CNV%' THEN "CNV"
ELSE "-"
END ,
substring(@routername,1,3),(@routername),vrfCount,usedVrfCount,unUsedVrfCount,ROUND(unusedPer,2),(@unusedlist));

DELETE FROM TEMP_NODE_TBL LIMIT 1;
SET countNo=countNo-1;
END WHILE;


SET @fileName=CONCAT(@dir_name,'/MPLS_Unused_VRF.csv');
SET @query=CONCAT('SELECT * FROM FINAL_UNUSED_VRF_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\' optionally enclosed by \'\"\' escaped by \'\"\'   ');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 




END | 
 

 DROP PROCEDURE IF EXISTS mpls_reporting_6_6_9_PortUtil; 
 CREATE PROCEDURE mpls_reporting_6_6_9_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
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
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500),PortID BIGINT(20) DEFAULT 0);
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
	inPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00');


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
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias like '%AESI-IN%' OR UPPER(IFAlias) like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%' and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias NOT like '%AESI-IN%'  AND UPPER(IFAlias) not like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%' and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%'); 
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


set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_NODEANDIF_TBL;


set @query = CONCAT("INSERT INTO TRAFFIC_TBL(PortID,InErrPkts,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;

update TRAFFIC_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID set a.IfDescr = b.IfDescr , a.NodeName = b.NodeName , a.IfSpeed = b.IfSpeed,a.Time_1 = a.Time_1;

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

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

DROP TEMPORARY TABLE IF EXISTS FINAL_TBL;


CREATE TEMPORARY TABLE FINAL_TBL(
	City	VARCHAR(100)	,
	Service	VARCHAR(100),
	RouterName	VARCHAR(100),
	Interface	VARCHAR(100),
	IfAlias 	VARCHAR(100),
	InUtilPeak	VARCHAR(100),
	OutUtilPeak	VARCHAR(100),
	InUtilAvg	VARCHAR(100),
	OutUtilAvg	VARCHAR(100),
	AvgUtilInPer	VARCHAR(100),
	AvgUtilOutPer	VARCHAR(100),
	PeakUtilInPer	VARCHAR(100),
	PeakUtilOutPer	VARCHAR(100),
	ThreshCrossed	VARCHAR(100),
	PeakInTime	VARCHAR(100),
	PeakOutTime	VARCHAR(100));


INSERT INTO FINAL_TBL VALUES("City","Service","RouterName","Interface","Interface Alias","In Traffic Peak(Kbps)","Out Traffic Peak(Kbps)","In Traffic Avg(Kbps)","Out Traffic Avg(Kbps)",
	"Avg Util In(%)","Avg Util Out(%)","Peak Util In(%)","Peak Util Out(%)","ThreshCrossed","PeakInTime","PeakOutTime");
INSERT INTO FINAL_TBL

SELECT (substring(NodeName,1,3)) as City,
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
(NodeName),REPLACE(IfDescr,'^',' '),REPLACE(IfAlias,'^',' '),ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID;



SET @fileName=CONCAT(@dir_name,'/MPLS_RouterWise_Interface_Utilization.csv');
SET @query=CONCAT('SELECT * FROM FINAL_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1; 






END | 
 

 DROP PROCEDURE IF EXISTS mpls_web_6_4_1_PortUtil; 
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
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500),PortID BIGINT(20) DEFAULT 0);
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
	inPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00');


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
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias like '%AESI-IN%' OR UPPER(IFAlias) like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%'  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias NOT like '%AESI-IN%'  AND UPPER(IFAlias) not like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%'  and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
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

set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_NODEANDIF_TBL;


set @query = CONCAT("INSERT INTO TRAFFIC_TBL(PortID,InErrPkts,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;

update TRAFFIC_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID set a.IfDescr = b.IfDescr , a.NodeName = b.NodeName , a.IfSpeed = b.IfSpeed,a.Time_1 = a.Time_1;

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

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

SELECT 
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
IF(IFAlias like '%AESI-IN%',
CASE
WHEN IFAlias like '%RTR%' THEN "Back-to-Back"
WHEN IFAlias like '%SWH%' THEN "Trunk"
WHEN IfAlias not like '%RTR%' and IfAlias not like '%SWH%' THEN "Backbone"
ELSE '-'
END,
CASE
WHEN IfAlias like '%AES%' THEN "Backbone"
WHEN IfAlias like '%RTR%' and IfAlias not like '%AES%' THEN "Back-to-Back"
WHEN IfAlias like '%SWH%' THEN "Trunk"
ELSE '-'
END ),
NodeName,IfDescr,IfAlias,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(CRCError,2),0,0,ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID group by a.PortID;

END | 
 

 DROP PROCEDURE IF EXISTS mpls_web_6_4_3_Pearing_Link_Util; 
 CREATE PROCEDURE mpls_web_6_4_3_Pearing_Link_Util(p_peeringScope varchar(5000), p_Region varchar(5000), p_peeringType varchar(1000),p_RouterName VARCHAR(5000),p_PeeringPartner VARCHAR(5000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_AllFields;
DROP TEMPORARY TABLE IF EXISTS TEMP_InternationalAllFields;
DROP TEMPORARY TABLE IF EXISTS TEMP_FILTER_AllFields;
DROP TEMPORARY TABLE IF EXISTS PEER_LINK_UTIL;
DROP TEMPORARY TABLE IF EXISTS TMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_AllFields(peeringScope varchar(128),peeringRegion varchar(128), peeringType varchar(128),peeringPartner varchar(128),NodeNumber int,IfIndex int,NodeName varchar(128),PortId bigint(20),IfDescr varchar(256),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_FILTER_AllFields like TEMP_AllFields;

CREATE TEMPORARY TABLE TMP_TRAFFIC_TBL (
        PortID BIGINT(20),
        RcvOctets BIGINT(20),
        TxOctets BIGINT(20),
        Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE PEER_LINK_UTIL(
        PortID          BIGINT(20),
        peeringScope    varchar(100),
        Region          varchar(100),
        peeringType     varchar(100),
        RouterName      varchar(128),
        IfDescr         varchar(128),
	IfAlias		varchar(500),
        PeeringPartner  varchar(100),
        95PerIn         FLOAT DEFAULT 0,
        95PerOut        FLOAT DEFAULT 0,
        PeakIn          FLOAT DEFAULT 0,
        PeakOut         FLOAT DEFAULT 0,
        AvgIn           FLOAT DEFAULT 0,
        AvgOut          FLOAT DEFAULT 0,
        MinIn           FLOAT DEFAULT 0,
        MinOut          FLOAT DEFAULT 0,
        VolumeIn        FLOAT DEFAULT 0,
        VolumeOut       FLOAT DEFAULT 0
);
SET @KBPS=1000;
INSERT INTO TEMP_AllFields(peeringScope,peeringRegion,peeringType,peeringPartner,NodeNumber,IfIndex,IfDescr,IfAlias)
SELECT 
'Domestic',
TRIM(SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,LOCATE(' ',SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,CHAR_LENGTH(IfAlias) )))),
TRIM(SUBSTRING(IfAlias,LOCATE('Domestic',IfAlias)+8,LOCATE('Peering',IfAlias)-(LOCATE('Domestic',IfAlias)+8))), 
TRIM(SUBSTRING(IfAlias,LOCATE('Peering:',IfAlias)+8,LOCATE('Region:',IfAlias)-(LOCATE('Peering',IfAlias)+8))),
NodeNumber, IfIndex, IfDescr, IfAlias
FROM (select IfAlias,NodeNumber,IfIndex,IfDescr from NODEIF_TBL where IfAlias like '%Domestic%' and IfAlias like '%Peering%')a;

INSERT INTO TEMP_AllFields(peeringScope,peeringRegion,peeringType,peeringPartner,NodeNumber,IfIndex,IfDescr,IfAlias)
SELECT
'International',
TRIM(SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,LOCATE(' ',SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,CHAR_LENGTH(IfAlias) )))),
TRIM(SUBSTRING(IfAlias,LOCATE('International',IfAlias)+13,LOCATE('Peering',IfAlias)-(LOCATE('International',IfAlias)+13))),
TRIM(SUBSTRING(IfAlias,LOCATE('Peering:',IfAlias)+8,LOCATE('Region:',IfAlias)-(LOCATE('Peering',IfAlias)+8))),
NodeNumber, IfIndex, IfDescr, IfAlias
FROM (select IfAlias,NodeNumber,IfIndex,IfDescr from NODEIF_TBL where IfAlias like '%International%' and IfAlias like '%Peering%')a;

UPDATE TEMP_AllFields a JOIN VLANPRT_TBL b ON a.NodeNumber=b.NodeID and a.IfIndex=b.IfIndex set a.PortId=b.PrtId;
UPDATE TEMP_AllFields a JOIN NODEIF_TBL b ON a.NodeNumber=b.NodeNumber and a.IfIndex=b.IfIndex set a.IfDescr=b.IfDescr;
UPDATE TEMP_AllFields a JOIN NODE_TBL b ON a.NodeNumber=b.NodeNumber set a.NodeName=b.NodeName;

set @where=' 1';
IF(p_peeringScope!='ALL')
THEN
set @where=CONCAT(@where,' and peeringScope in (',p_peeringScope,')');
END IF;

IF(p_Region!='ALL')
THEN
set @where=CONCAT(@where,' and peeringRegion in (',p_Region,')');
END IF;

IF(p_peeringType!='ALL')
THEN
set @where=CONCAT(@where,' and peeringType in (',p_peeringType,')');
END IF;

IF(p_peeringPartner!='ALL')
THEN
set @where=CONCAT(@where,' and peeringPartner in (',p_peeringPartner,')');
END IF;
SET @query=CONCAT("INSERT INTO TEMP_FILTER_AllFields SELECT * FROM TEMP_AllFields where ", @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

CREATE INDEX iPortIndex ON TEMP_FILTER_AllFields(PortID);

set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_FILTER_AllFields;

set @query = CONCAT("INSERT INTO TMP_TRAFFIC_TBL(PortID,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;



CREATE INDEX iPortIndex ON TMP_TRAFFIC_TBL(PortID);
CREATE INDEX iTimeIndex ON TMP_TRAFFIC_TBL(Time_1);

INSERT INTO PEER_LINK_UTIL(PortID,peeringScope,Region,peeringType,RouterName,IfDescr,IfAlias,PeeringPartner,PeakIn,PeakOut,AvgIn,AvgOut,MinIn,MinOut,VolumeIn,VolumeOut)
        select b.PortID,peeringScope,peeringRegion,peeringType,NodeName,IfDescr,IfAlias,peeringPartner,Max(RcvOctets),Max(TxOctets),Avg(RcvOctets),Avg(TxOctets),Min(RcvOctets),Min(TxOctets),SUM(RcvOctets),SUM(TxOctets) from TEMP_FILTER_AllFields a JOIN TMP_TRAFFIC_TBL b ON a.PortID = b.PortID group by b.PortID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","RcvOctets");
Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerIn = Traffic
        where a.PortID = b.EntityID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","TxOctets");

Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerOut = Traffic
        where a.PortID = b.EntityID;


select peeringScope,Region,peeringType,RouterName,IfDescr,IfAlias,PeeringPartner,ROUND((95PerIn/@KBPS),2),ROUND(95PerOut/@KBPS,2),ROUND(PeakIn/@KBPS,2),ROUND(PeakOut/@KBPS,2),ROUND(AvgIn/@KBPS,2),ROUND(AvgOut/@KBPS,2),ROUND(MinIn/@KBPS,2),ROUND(MinOut/@KBPS,2),ROUND(VolumeIn/@KBPS,2),ROUND(VolumeOut/@KBPS,2) from PEER_LINK_UTIL;



END | 
 

 DROP PROCEDURE IF EXISTS mpls_web_6_6_1_VRFUtil; 
 CREATE PROCEDURE mpls_web_6_6_1_VRFUtil(city varchar(64), service varchar(64),inputNodeName varchar(256),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_CITY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_SERVICE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_NODEIF_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL;
DROP TEMPORARY TABLE IF EXISTS TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_CUSTOMER_TBL;
DROP TEMPORARY TABLE IF EXISTS EXCEED_COUNT;
DROP TEMPORARY TABLE IF EXISTS FINAL_FETCH;

CREATE TEMPORARY TABLE TEMP_CITY_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_SERVICE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE TEMP_NODE_TBL like CUSTOMER_VRF_TBL;
CREATE TEMPORARY TABLE EXCEED_COUNT(QSTID BIGINT(20),exceed INTEGER);
CREATE TEMPORARY TABLE TRAFFIC_TBL(QSTID integer,IfSpeed integer,TxOctets bigint(20),Time_1 timeStamp DEFAULT '0000-00-00 00:00:00');

SET @query1 := CONCAT('INSERT INTO TEMP_CITY_TBL SELECT * from CUSTOMER_VRF_TBL where substring(NodeName,1,3) in (',city);
SET @query2 := CONCAT(@query1,')');
IF city='ALL'
THEN
INSERT INTO TEMP_CITY_TBL
SELECT * from CUSTOMER_VRF_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;

END IF;

IF service='ALL'
THEN
INSERT INTO TEMP_SERVICE_TBL
SELECT * from TEMP_CITY_TBL;
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
	
SET @query1 := CONCAT("INSERT INTO TEMP_SERVICE_TBL SELECT * from TEMP_CITY_TBL where  ",@like);
PREPARE statement1 from @query1;
EXECUTE statement1;
DEALLOCATE Prepare statement1;
END IF;

SET @query1 := CONCAT('INSERT INTO TEMP_NODE_TBL SELECT * from TEMP_SERVICE_TBL where nodeName in (',inputNodeName);
SET @query2 := CONCAT(@query1,')');
IF inputNodeName='ALL'
THEN
INSERT INTO TEMP_NODE_TBL
SELECT * from TEMP_SERVICE_TBL;
ELSE
PREPARE statement1 from @query2;
EXECUTE statement1;
END IF;

CREATE INDEX NodeNumberIndexNODEIF on TEMP_NODE_TBL(NodeNumber,IfIndex);

DROP TEMPORARY TABLE IF EXISTS TEMP_COSFCNAME_TBL;
CREATE TEMPORARY TABLE TEMP_COSFCNAME_TBL(QSTID int,NodeName varchar(256),IfDescr varchar(256),CircuitID varchar(128), CosFCName varchar(64),IfSpeed BIGINT(20),VrfName varchar(128),NodeNumber int, IfIndex int);
CREATE INDEX QSTIDIndex on TEMP_COSFCNAME_TBL(QSTID);

drop temporary table if exists cosqstat;
create temporary table cosqstat (index(NodeNumber,IfIndex),index(NodeNumber,QNumber)) as select * from COSQSTAT_TBL ;
INSERT INTO TEMP_COSFCNAME_TBL(QSTID,NodeName,IfDescr,CircuitID, CosFCName,IfSpeed,NodeNumber,IfIndex)
	SELECT QSTID, NodeName, IfDescr, CircuitID , CosFCNAME,IfSpeed,a.NodeNumber,c.IfIndex from TEMP_NODE_TBL a, COSFC_TBL b, cosqstat c  where c.NodeNumber=a.NodeNumber and c.IfIndex=a.IfIndex and b.NodeNumber=c.NodeNumber and b.CosQNumber=c.QNumber;

drop temporary table if exists vpnif;
create temporary table vpnif (index(VpnNode,VpnIFIndex))as select VpnId,VpnNode,VpnIfIndex from VPN_IFINDEX_TBL;

UPDATE TEMP_COSFCNAME_TBL a, VPN_TBL b, vpnif c  set a.VrfName=b.VpnName where a.NodeNumber=c.VpnNode and a.IfIndex=c.VpnIfIndex and c.VpnId=b.VpnId;

INSERT INTO TRAFFIC_TBL
	SELECT QSTId,IfSpeed,a.TxOctets,Time_1 from ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL a JOIN TEMP_COSFCNAME_TBL b ON DiffId = QSTID where  Time_1>startTime and Time_1<endTime;

INSERT INTO EXCEED_COUNT
        select QSTId,count(TxOctets) from TRAFFIC_TBL where (TxOctets/(IfSpeed*10)>70 or TxOctets/(IfSpeed*10)>70) group by QSTId;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL(DiffId int, maxTrafficValue bigint(20), avgTrafficValue bigint(20),peakTime varchar(20) DEFAULT '0000-00-00 00:00:00',exceedCount INTEGER DEFAULT 0);
CREATE INDEX DiffIDIndex2 on TEMP_TRAFFIC_TBL(DiffID);
CREATE INDEX QSTIdIn on TRAFFIC_TBL(QSTId);

INSERT INTO TEMP_TRAFFIC_TBL(DiffId, maxTrafficValue, avgTrafficValue)
SELECT QSTId,max(TxOctets),avg(TxOctets) FROM TRAFFIC_TBL group by QSTId;

UPDATE TEMP_TRAFFIC_TBL a set peakTime=(SELECT if(ROUND(a.maxTrafficValue/1000,2)=0,'NA',Time_1) from TRAFFIC_TBL b where a.DiffId=QSTId and a.maxTrafficValue=b.TxOctets order by Time_1 desc limit 1);

UPDATE TEMP_TRAFFIC_TBL a JOIN EXCEED_COUNT b ON a.DiffId = b.QSTId set a.exceedCount = b.exceed;


select substring(NodeName,1,3),
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
NodeName,CircuitID,IfDescr,CosFCNAME,ROUND(maxTrafficValue/1000,2),ROUND(avgTrafficValue/1000,2),peakTime,exceedCount,VrfName from TEMP_TRAFFIC_TBL JOIN TEMP_COSFCNAME_TBL ON DiffId = QSTID and avgTrafficValue >0 group by QSTID;






END | 
 

 DROP PROCEDURE IF EXISTS mpls_web_6_6_9_PortUtil; 
 CREATE PROCEDURE mpls_web_6_6_9_PortUtil(utilType VARCHAR(100),service varchar(5000),p_networkType VARCHAR(20),inputCity varchar(5000),inputNodeName varchar(5000), startTime timestamp,endTime timestamp)
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
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEIF_TBL_1(NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500));
CREATE TEMPORARY TABLE TEMP_NODEANDIF_TBL(NodeName Varchar(256),NodeNumber int,IfIndex INTEGER,IfSpeed FLOAT,IfDescr VARCHAR(100),IfAlias VARCHAR(500),PortID BIGINT(20) DEFAULT 0);
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
	inPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00',
	outPeakTime 		varchar(20) DEFAULT '0000-00-00 00:00:00');


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
	INSERT INTO TEMP_NODEIF_TBL SELECT distinct a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
ELSE 
	If (utilType = "'AESI-IN'")
	THEN
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias like '%AESI-IN%' OR UPPER(IFAlias) like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%' and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
	ELSE 
		INSERT INTO TEMP_NODEIF_TBL SELECT a.NodeNumber, IfIndex,IfSpeed,IfDescr,IfAlias from NODEIF_TBL  a JOIN TEMP_NODEName_TBL b ON a.NodeNumber = b.NodeNumber where (IfAlias NOT like '%AESI-IN%'  AND UPPER(IFAlias) not like '%INT-BACK-TO-BACK%') and IfAlias not like '%Peering%' and (IfAlias not like '%ML3-%' and IfAlias not like '%ML2-%' and IfAlias not like '%-ILP-%');
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


set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_NODEANDIF_TBL;


set @query = CONCAT("INSERT INTO TRAFFIC_TBL(PortID,InErrPkts,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,InErrPkts,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;

update TRAFFIC_TBL a JOIN TEMP_NODEANDIF_TBL b ON a.PortID = b.PortID set a.IfDescr = b.IfDescr , a.NodeName = b.NodeName , a.IfSpeed = b.IfSpeed,a.Time_1 = a.Time_1;

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

UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET inPeakTime=if(ROUND(A.maxTrafficValueIn/1000,2)=0,'NA',B.Time_1) where B.RcvOctets=A.maxTrafficValueIn;
UPDATE TEMP_TRAFFIC_TBL A JOIN TRAFFIC_TBL B ON A.PortId=B.PortId SET outPeakTime=if(ROUND(A.maxOutTrafficValueOut/1000,2)=0,'NA',B.Time_1) where B.TxOctets=A.maxOutTrafficValueOut;

UPDATE TEMP_TRAFFIC_TBL A JOIN EXCEED_COUNT B ON (A.PortID = B.PortID)
	set ThresholdExceed = Exceed;

DROP TEMPORARY TABLE IF EXISTS FINAL_TBL;


SELECT (substring(NodeName,1,3)) as City,
CASE
WHEN NodeName like '%MPL%' THEN "MPL"
WHEN NodeName like '%ISP%' THEN "ISP"
WHEN NodeName like '%CNV%' THEN "CNV"
ELSE "-"
END ,
(NodeName),(IfDescr),IfAlias,ROUND(maxTrafficValueIn/1000,2),ROUND(maxOutTrafficValueOut/1000,2),ROUND(avgTrafficValueIn/1000,2),ROUND(avgTrafficValueOut/1000,2),ROUND(AvgUtilIn,2),ROUND(AvgUtilOut,2),ROUND(PeakUtilIn,2),ROUND(PeakUtilOut,2),ThresholdExceed,inPeakTime,outPeakTime  from  TEMP_TRAFFIC_TBL a ,TEMP_NODEANDIF_TBL b where a.PortID = b.PortID;








END | 
 

 DROP PROCEDURE IF EXISTS mpls_web_reporting_6_4_3_Pearing_Link_Util; 
 CREATE PROCEDURE mpls_web_reporting_6_4_3_Pearing_Link_Util(p_peeringScope varchar(5000), p_Region varchar(5000), p_peeringType varchar(1000),p_RouterName VARCHAR(5000),p_PeeringPartner VARCHAR(5000),startTime timestamp,endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_AllFields;
DROP TEMPORARY TABLE IF EXISTS TEMP_InternationalAllFields;
DROP TEMPORARY TABLE IF EXISTS TEMP_FILTER_AllFields;
DROP TEMPORARY TABLE IF EXISTS PEER_LINK_UTIL;
DROP TEMPORARY TABLE IF EXISTS TMP_TRAFFIC_TBL;

CREATE TEMPORARY TABLE TEMP_AllFields(peeringScope varchar(128),peeringRegion varchar(128), peeringType varchar(128),peeringPartner varchar(128),NodeNumber int,IfIndex int,NodeName varchar(128),PortId bigint(20),IfDescr varchar(256));
CREATE TEMPORARY TABLE TEMP_FILTER_AllFields like TEMP_AllFields;

CREATE TEMPORARY TABLE TMP_TRAFFIC_TBL (
        PortID BIGINT(20),
        RcvOctets BIGINT(20),
        TxOctets BIGINT(20),
        Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE PEER_LINK_UTIL(
        PortID          BIGINT(20),
        peeringScope    varchar(100),
        Region          varchar(100),
        peeringType     varchar(100),
        RouterName      varchar(128),
        IfDescr         varchar(128),
        PeeringPartner  varchar(100),
        95PerIn         FLOAT DEFAULT 0,
        95PerOut        FLOAT DEFAULT 0,
        PeakIn          FLOAT DEFAULT 0,
        PeakOut         FLOAT DEFAULT 0,
        AvgIn           FLOAT DEFAULT 0,
        AvgOut          FLOAT DEFAULT 0,
        MinIn           FLOAT DEFAULT 0,
        MinOut          FLOAT DEFAULT 0,
        VolumeIn        FLOAT DEFAULT 0,
        VolumeOut       FLOAT DEFAULT 0
);
SET @KBPS=1000;
INSERT INTO TEMP_AllFields(peeringScope,peeringRegion,peeringType,peeringPartner,NodeNumber,IfIndex,IfDescr)
SELECT 
'Domestic',
TRIM(SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,LOCATE(' ',SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,CHAR_LENGTH(IfAlias) )))),
TRIM(SUBSTRING(IfAlias,LOCATE('Domestic',IfAlias)+8,LOCATE('Peering',IfAlias)-(LOCATE('Domestic',IfAlias)+8))), 
TRIM(SUBSTRING(IfAlias,LOCATE('Peering:',IfAlias)+8,LOCATE('Region:',IfAlias)-(LOCATE('Peering',IfAlias)+8))),
NodeNumber, IfIndex, IfDescr
FROM (select IfAlias,NodeNumber,IfIndex,IfDescr from NODEIF_TBL where IfAlias like '%Domestic%' and IfAlias like '%Peering%')a;

INSERT INTO TEMP_AllFields(peeringScope,peeringRegion,peeringType,peeringPartner,NodeNumber,IfIndex,IfDescr)
SELECT
'International',
TRIM(SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,LOCATE(' ',SUBSTRING(IfAlias,LOCATE('Region:',IfAlias)+7,CHAR_LENGTH(IfAlias) )))),
TRIM(SUBSTRING(IfAlias,LOCATE('International',IfAlias)+13,LOCATE('Peering',IfAlias)-(LOCATE('International',IfAlias)+13))),
TRIM(SUBSTRING(IfAlias,LOCATE('Peering:',IfAlias)+8,LOCATE('Region:',IfAlias)-(LOCATE('Peering',IfAlias)+8))),
NodeNumber, IfIndex, IfDescr
FROM (select IfAlias,NodeNumber,IfIndex,IfDescr from NODEIF_TBL where IfAlias like '%International%' and IfAlias like '%Peering%')a;

UPDATE TEMP_AllFields a JOIN VLANPRT_TBL b ON a.NodeNumber=b.NodeID and a.IfIndex=b.IfIndex set a.PortId=b.PrtId;
UPDATE TEMP_AllFields a JOIN NODEIF_TBL b ON a.NodeNumber=b.NodeNumber and a.IfIndex=b.IfIndex set a.IfDescr=b.IfDescr;
UPDATE TEMP_AllFields a JOIN NODE_TBL b ON a.NodeNumber=b.NodeNumber set a.NodeName=b.NodeName;

set @where=' 1';
IF(p_peeringScope!='ALL')
THEN
set @where=CONCAT(@where,' and peeringScope in (',p_peeringScope,')');
END IF;

IF(p_Region!='ALL')
THEN
set @where=CONCAT(@where,' and peeringRegion in (',p_Region,')');
END IF;

IF(p_peeringType!='ALL')
THEN
set @where=CONCAT(@where,' and peeringType in (',p_peeringType,')');
END IF;

IF(p_peeringPartner!='ALL')
THEN
set @where=CONCAT(@where,' and peeringPartner in (',p_peeringPartner,')');
END IF;
SET @query=CONCAT("INSERT INTO TEMP_FILTER_AllFields SELECT * FROM TEMP_AllFields where ", @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

CREATE INDEX iPortIndex ON TEMP_FILTER_AllFields(PortID);

set session group_concat_max_len=1000000;
select GROUP_CONCAT(PortID) into @portID from TEMP_FILTER_AllFields;

set @query = CONCAT("INSERT INTO TMP_TRAFFIC_TBL(PortID,RcvOctets,TxOctets,Time_1)
        SELECT a.PortID,RcvOctets,TxOctets,Time_1 FROM ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a where Time_1>'",startTime,"' and Time_1<'",endTime,"' and PortID in (",@portID,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE Prepare statement1;



CREATE INDEX iPortIndex ON TMP_TRAFFIC_TBL(PortID);
CREATE INDEX iTimeIndex ON TMP_TRAFFIC_TBL(Time_1);

INSERT INTO PEER_LINK_UTIL(PortID,peeringScope,Region,peeringType,RouterName,IfDescr,PeeringPartner,PeakIn,PeakOut,AvgIn,AvgOut,MinIn,MinOut,VolumeIn,VolumeOut)
        select b.PortID,peeringScope,peeringRegion,peeringType,NodeName,IfDescr,peeringPartner,Max(RcvOctets),Max(TxOctets),Avg(RcvOctets),Avg(TxOctets),Min(RcvOctets),Min(TxOctets),SUM(RcvOctets),SUM(TxOctets) from TEMP_FILTER_AllFields a JOIN TMP_TRAFFIC_TBL b ON a.PortID = b.PortID group by b.PortID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","RcvOctets");
Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerIn = Traffic
        where a.PortID = b.EntityID;

call 95percentile("TMP_TRAFFIC_TBL","","PortID","TxOctets");

Update PEER_LINK_UTIL a,FINAL_TRAF95_TBL b
        set 95PerOut = Traffic
        where a.PortID = b.EntityID;



select peeringScope,Region,peeringType,RouterName,IfDescr,PeeringPartner,ROUND((95PerIn/@KBPS),2),ROUND(95PerOut/@KBPS,2),ROUND(PeakIn/@KBPS,2),ROUND(PeakOut/@KBPS,2),ROUND(AvgIn/@KBPS,2),ROUND(AvgOut/@KBPS,2),ROUND(MinIn/@KBPS,2),ROUND(MinOut/@KBPS,2),ROUND(VolumeIn/@KBPS,2),ROUND(VolumeOut/@KBPS,2) from PEER_LINK_UTIL;



END | 
  
DROP PROCEDURE IF EXISTS port_threshold; 
  
CREATE PROCEDURE port_threshold()
BEGIN

DECLARE iIndex INTEGER DEFAULT 1;
DECLARE iPortID BIGINT(20) DEFAULT 0;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC1_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC2_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC3_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC4_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_TRAFFIC_TBL_B;
DROP TEMPORARY TABLE IF EXISTS minRowForPort;

CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL (indexColumn INTEGER primary key auto_increment not null,PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC_TBL_B (PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP,index(PortID));
CREATE TEMPORARY TABLE TEMP_TRAFFIC1_TBL (PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC2_TBL (PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC3_TBL (PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC4_TBL (PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP);
CREATE TEMPORARY TABLE TEMP_TRAFFIC (PortID BIGINT(20),RcvOctets BIGINT(20),TxOctets BIGINT(20),Time_1 TIMESTAMP,index(PortID),index(Time_1));
CREATE TEMPORARY TABLE minRowForPort(PortID INTEGER, minRow INTEGER);

CREATE INDEX entityIndex1 ON TEMP_TRAFFIC1_TBL(PortID);
CREATE INDEX entityIndex1 ON TEMP_TRAFFIC2_TBL(PortID);
CREATE INDEX entityIndex1 ON TEMP_TRAFFIC3_TBL(PortID);
CREATE INDEX entityIndex1 ON TEMP_TRAFFIC4_TBL(PortID);
CREATE INDEX entityIndex1 ON TEMP_TRAFFIC_TBL(PortID);
CREATE INDEX entityIndex2 ON TEMP_TRAFFIC_TBL(indexColumn);
CREATE INDEX entityIndex1 ON minRowForPort(minRow);
CREATE INDEX entityIndex2 ON minRowForPort(PortID);

select now() into @tempTime;
set @tempTime1 = TIMESTAMPADD(MINUTE,-30,@tempTime);

drop temporary table if exists vlanprt_tbl;
create temporary table vlanprt_tbl(index(PrtID),index (NodeID,IfIndex)) as select * from VLANPRT_TBL where class = 'A' or class = 'B';

INSERT INTO TEMP_TRAFFIC(PortID,RcvOctets,TxOctets,Time_1) SELECT PortID,RcvOctets,TxOctets,Time_1 from ROUTERTRAFFIC_VLANPRT_SCALE1_TBL a JOIN vlanprt_tbl b ON a.PortID = b.PrtID and   Time_1 > @tempTime1 and Time_1 < @tempTime ;

INSERT INTO TEMP_TRAFFIC_TBL(PortID,RcvOctets,TxOctets,Time_1) SELECT PortID,RcvOctets,TxOctets,Time_1 from TEMP_TRAFFIC a JOIN vlanprt_tbl b ON a.PortID = b.PrtID and b.Class = 'A' order by PortID,Time_1 desc;

drop temporary table if exists lastEntry;
create temporary table lastEntry (index (PortID,Time_1)) as select PortID,max(Time_1) as Time_1 from TEMP_TRAFFIC  group by PortID;

INSERT INTO TEMP_TRAFFIC_TBL_B(PortID,RcvOctets,TxOctets,Time_1)  SELECT a.PortID,a.RcvOctets,a.TxOctets,a.Time_1 from TEMP_TRAFFIC a JOIN lastEntry d  ON  a.PortID = d.PortID and a.Time_1=d.Time_1 ;

INSERT INTO minRowForPort SELECT PortID,min(indexColumn) FROM TEMP_TRAFFIC_TBL GROUP BY PortID;

insert into TEMP_TRAFFIC1_TBL select a.PortID,a.RcvOctets,a.TxOctets,Time_1 from TEMP_TRAFFIC_TBL a JOIN minRowForPort b where a.indexColumn = b.minRow;
insert into TEMP_TRAFFIC2_TBL select a.PortID,a.RcvOctets,a.TxOctets,Time_1 from TEMP_TRAFFIC_TBL a JOIN minRowForPort b where a.PortID = b.PortID and a.indexColumn = b.minRow+1;
insert into TEMP_TRAFFIC3_TBL select a.PortID,a.RcvOctets,a.TxOctets,Time_1 from TEMP_TRAFFIC_TBL a JOIN minRowForPort b where a.PortID = b.PortID and a.indexColumn = b.minRow+2;
insert into TEMP_TRAFFIC4_TBL select a.PortID,a.RcvOctets,a.TxOctets,Time_1 from TEMP_TRAFFIC_TBL a JOIN minRowForPort b where a.PortID = b.PortID and a.indexColumn = b.minRow+3;

DROP TEMPORARY TABLE IF EXISTS SUDDEN_HIKE;
CREATE TEMPORARY TABLE SUDDEN_HIKE
	select a.PortID,If((  
(ABS(a.RcvOctets-b.RcvOctets) > suddenTh*b.RcvOctets/100) or (ABS(a.TxOctets-b.TxOctets) > suddenTh*b.TxOctets/100)  OR
(ABS(a.RcvOctets-c.RcvOctets) > suddenTh*c.RcvOctets/100) or (ABS(a.TxOctets-c.TxOctets) > suddenTh*c.TxOctets/100)  OR
(ABS(a.RcvOctets-d.RcvOctets) > suddenTh*d.RcvOctets/100) or (ABS(a.TxOctets-d.TxOctets) > suddenTh*d.TxOctets/100)),1,0) as sudden_flag,
if(((ABS(a.RcvOctets-b.RcvOctets)> suddenTh*b.RcvOctets/100) OR 
(ABS(a.RcvOctets-c.RcvOctets)> suddenTh*c.RcvOctets/100) OR 
(ABS(a.RcvOctets-d.RcvOctets)> suddenTh*d.RcvOctets/100)),1,0) as InSudden,
If(((ABS(a.TxOctets-b.TxOctets)> suddenTh*b.TxOctets/100) OR 
(ABS(a.TxOctets-c.TxOctets)> suddenTh*c.TxOctets/100) OR 
(ABS(a.TxOctets-d.TxOctets)> suddenTh*d.TxOctets/100)),1,0) as OutSudden,
if(b.RcvOctets=0,(a.RcvOctets-b.RcvOctets),(a.RcvOctets-b.RcvOctets)*100/b.RcvOctets) as In_T_T1,
if(c.RcvOctets=0,(a.RcvOctets-c.RcvOctets),(a.RcvOctets-c.RcvOctets)*100/c.RcvOctets) as In_T_T2,
if(d.RcvOctets=0,(a.RcvOctets-d.RcvOctets),(a.RcvOctets-d.RcvOctets)*100/d.RcvOctets) as In_T_T3,
if(b.TxOctets=0,(a.TxOctets-b.TxOctets),(a.TxOctets-b.TxOctets)*100/b.TxOctets) as Out_T_T1,
if(c.TxOctets=0,(a.TxOctets-c.TxOctets),(a.TxOctets-c.TxOctets)*100/c.TxOctets) as Out_T_T2,
if(d.TxOctets=0,(a.TxOctets-d.TxOctets),(a.TxOctets-d.TxOctets)*100/d.TxOctets) as Out_T_T3
	from TEMP_TRAFFIC1_TBL a JOIN TEMP_TRAFFIC2_TBL b on a.PortID = b.PortID JOIN TEMP_TRAFFIC3_TBL c ON a.PortID = c.PortID JOIN TEMP_TRAFFIC4_TBL d ON a.PortID = d.PortID JOIN PORT_THRESHOLD_TBL P ON a.PortID = P.PortID;
create index i1 on SUDDEN_HIKE(PortID);


DROP TEMPORARY TABLE IF EXISTS nodeif;
CREATE TEMPORARY TABLE nodeif(index (NodeNumber,IfIndex)) as select NodeNumber,IfIndex,IfDescr,IfSpeed from NODEIF_TBL where ifDescr not like '%Tunnel%' and ifDescr not like '%tunnel%'; 
DROP TEMPORARY TABLE IF EXISTS vlanprt;
CREATE TEMPORARY TABLE vlanprt(index (portID)) 
	select PrtID as PortID,b.NodeName,c.IfDescr,c.IfSpeed,d.class from NODE_TBL b,nodeif c,vlanprt_tbl d
	WHERE  d.NodeId = b.NodeNumber
        and b.NodeNumber = c.NodeNumber and d.IfIndex = c.IfIndex
	and c.IfSpeed != 0 and (d.class = 'A' or d.class = 'B');

CREATE TABLE IF NOT EXISTS THRESHOLD_TBL (PortID BIGINT(20),NodeName VARCHAR(100),IfDescr VARCHAR(100),Bandwidth INTEGER,Abs_Threshold VARCHAR(4),Abs_Flag INTEGER,Cust_Threshold VARCHAR(4),Cus_Flag INTEGER,SuddenThresh VARCHAR(5),Abs_Flag2 INTEGER,In_T_T1 FLOAT DEFAULT 0,In_T_T2 FLOAT DEFAULT 0,In_T_T3 FLOAT DEFAULT 0,Out_T_T1 FLOAT  DEFAULT 0, Out_T_T2 FLOAT DEFAULT 0, Out_T_T3 FLOAT DEFAULT 0,InTraffic_KBPS FLOAT,OutTraffic_KBPS FLOAT,InUtilization FLOAT,InUtilFlag INTEGER,OutUtilization FLOAT,OutUtilFlag INTEGER,Time_1 TIMESTAMP,class VARCHAR(5),unique(NodeName,IfDescr,Time_1));

insert ignore into THRESHOLD_TBL(PortID,NodeName,IfDescr,Bandwidth,Abs_Threshold,Abs_Flag,Cust_Threshold,Cus_Flag,SuddenThresh,Abs_Flag2,In_T_T1,In_T_T2,In_T_T3,Out_T_T1,Out_T_T2,Out_T_T3,InTraffic_KBPS,OutTraffic_KBPS,InUtilization,InUtilFlag,OutUtilization,OutUtilFlag,Time_1,class)
select a.PortID,b.NodeName,b.IfDescr,b.IfSpeed,
AbsoluteTh,if(((RcvOctets/(b.IfSpeed*10)> AbsoluteTh or TxOctets/(b.IfSpeed*10)> AbsoluteTh)),1,0),
CustomerTh,if(((RcvOctets/(b.IfSpeed*10)> CustomerTh or TxOctets/(b.IfSpeed*10)> CustomerTh)),1,0),
SuddenTh,if(S.sudden_flag IS NULL,'NA',S.sudden_flag) as sudden_flag,
if(In_T_T1 IS NULL,0,ROUND(In_T_T1,2)),if(In_T_T2 IS NULL,0,ROUND(In_T_T2,2)),if(In_T_T3 IS NULL,0,ROUND(In_T_T3,2)),
if(Out_T_T1 IS NULL,0,ROUND(Out_T_T1,2)),if(Out_T_T2 IS NULL,0,ROUND(Out_T_T2,2)),if(Out_T_T3 IS NULL,0,ROUND(Out_T_T3,2)),
ROUND((RcvOctets/1000),2),ROUND((TxOctets/1000),2),
ROUND((RcvOctets/(b.IfSpeed*10)),2) as inUtil,if((RcvOctets/(b.IfSpeed*10)> AbsoluteTh) OR (RcvOctets/(b.IfSpeed*10)> CustomerTh) OR 
					(if(InSudden IS NULL,0,((RcvOctets/(b.IfSpeed*10) >25) AND InSudden))),1,0) as inUtilFlag,
ROUND((TxOctets/(b.IfSpeed*10)),2) as outUtil,if((TxOctets/(b.IfSpeed*10)> AbsoluteTh) OR (TxOctets/(b.IfSpeed*10)> CustomerTh) OR
					(if(OutSudden IS NULL,0,((TxOctets/(b.IfSpeed*10) >25) AND OutSudden))),1,0) as outUtilFlag,Time_1,class
        from TEMP_TRAFFIC_TBL_B a JOIN PORT_THRESHOLD_TBL L ON a.PortID = L.PortID 
	JOIN vlanprt b ON a.PortID= b.PortID 
	LEFT JOIN SUDDEN_HIKE S ON a.PortID = S.PortID
        group by a.PortID 
	having ((inUtil > LEAST(AbsoluteTh,CustomerTh) OR outUtil > LEAST(AbsoluteTh,CustomerTh)) OR ((sudden_flag = 1) AND (inUtilFlag = 1 OR outUtilFlag = 1)))
	order by Time_1 desc; 
END | 
 

 DROP PROCEDURE IF EXISTS priv_peer; 
 CREATE PROCEDURE priv_peer(startTime TIMESTAMP,endTime TIMESTAMP)
BEGIN

SET @StartTime=''; SET @StartTimeFinal='';
SET @EndTime=''; SET @EndTimeFinal='';
SET @filenameTime=''; SET @filenameTimeFinal='';

SET @StartTime=DATE_FORMAT(StartTime,'%m/%d/%Y %H:%i:%s:%f');
SET @StartTimeFinal=SUBSTRING(@StartTime,1,CHAR_LENGTH(@StartTime)-4);

SET @EndTime=DATE_FORMAT(EndTime,'%m/%d/%Y %H:%i:%s:%f');
SET @EndTimeFinal=SUBSTRING(@EndTime,1,CHAR_LENGTH(@EndTime)-4);

SET @filenameTime=DATE_FORMAT(EndTime,'%m_%d_%Y-%H_%i_%s_%f');
SET @filenameTimeFinal=SUBSTRING(@filenameTime,1,CHAR_LENGTH(@filenameTime)-4);

SET @FileName=CONCAT(CONCAT('IPPMS_Private_Peering_Link_Util_',@filenameTimeFinal),'.csv');


DROP TEMPORARY TABLE IF EXISTS PRIVATE_PEER;
CREATE TEMPORARY TABLE IF NOT EXISTS PRIVATE_PEER(
        STARTTIME       VARCHAR(50),
        ENDTIME         VARCHAR(50),
        PEERING_NE_FUNCTION     VARCHAR(100),
        PEERING_DESTINATION     VARCHAR(100),
        INGRESS_UTILIZATION     VARCHAR(50),
        EGRESS_UTLIZATION       VARCHAR(50));

INSERT INTO PRIVATE_PEER values ('STARTTIME','ENDTIME','PEERING_NE_FUNCTION','PEERING_DESTINATION','INGRESS_UTILIZATION','EGRESS_UTLIZATION');
INSERT INTO PRIVATE_PEER
select @StartTimeFinal,@EndTimeFinal,
        QUOTE(NEFunction),QUOTE(interfaceDest),ROUND(SUM(RcvOctets)/(1000*1000*1000),2),ROUND(SUM(TxOctets)/(1000*1000*1000),2)
        from PEERING_VPN p, ROUTERTRAFFIC_VLANPRT_SCALE1_TBL r
        where p.LinkID = r.PortID
        and r.Time_1 > startTime
        and r.Time_1 <= endTime
        group by interfaceDest;


set @Query = CONCAT("select * from PRIVATE_PEER into outfile \'/export/home/anec/Files_for_ANEC/",@FileName,"\' FIELDS TERMINATED BY ',' Lines terminated by '\n'") ;



PREPARE stmt1 from @Query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 

 DROP PROCEDURE IF EXISTS vpndelay; 
 CREATE PROCEDURE vpndelay()
BEGIN

drop table if exists TEMP_VPN_MAP;

create table if not exists TEMP_VPN_MAP(ID integer AUTO_INCREMENT PRIMARY KEY,NodeNumber integer,NodeIfIpaddress varchar(128),NodeIpAddres varchar(128),vpnid integer,NodeName varchar(128),vpnname varchar(128));

insert into TEMP_VPN_MAP(NodeNumber,NodeIfIpaddress,NodeIpAddres,vpnid,NodeName,vpnname) select b.TerNodeNumber,b.TerIfIPAddress,c.NodeId,a.vpnId,c.Nodename,d.vpnname from MAP_VPN_NODE_TBL a,LINK_TBL b,NODE_TBL c,VPN_TBL d where a.VpnNode = b.OriNodeNumber && a.VpnIfIndex = b.OriIfIndex && b.TerNodeNumber=c.Nodenumber && d.vpnid=a.vpnid;

create table if not exists TEMP_DESTINATION (ID integer AUTO_INCREMENT PRIMARY KEY,dest varchar(128));

insert into  TEMP_DESTINATION (dest) select distinct destinationip from VPN_DELAY_TABLE ;

 create table if not exists TEMP_SOURCE(ID integer AUTO_INCREMENT PRIMARY KEY,dest varchar(128));

insert into TEMP_SOURCE (dest) select distinct sourceip from VPN_DELAY_TABLE ;


create table if not exists FINAL_SOURCELIST (ID integer AUTO_INCREMENT PRIMARY KEY,NodeName varchar(128),IpAddress varchar(128),VpnName varchar(128));


insert into FINAL_SOURCELIST (NodeName,IpAddress,VpnName) select b.NodeName,a.dest,b.vpnname from TEMP_SOURCE a,TEMP_VPN_MAP b where a.dest = b.NodeIpAddres ;

create table if not exists FINAL_DESTINATIONLIST (ID integer AUTO_INCREMENT PRIMARY KEY,NodeName varchar(128),IpAddress varchar(128),VpnName varchar(128));

 insert into FINAL_DESTINATIONLIST (NodeName,IpAddress,VpnName) select b.NodeName,a.dest,b.vpnname from TEMP_DESTINATION a,TEMP_VPN_MAP b where a.dest = b.NodeIfIpAddress ;


insert into FINAL_DESTINATIONLIST (NodeName,IpAddress,VpnName) select b.NodeName,a.dest,b.vpnname from TEMP_DESTINATION a,TEMP_VPN_MAP b where a.dest = b.NodeIpAddres ;



END | 
 

 DROP PROCEDURE IF EXISTS webProbe; 
 CREATE PROCEDURE webProbe(inUrlName varchar(50000),inLocation varchar(50000),startTime timestamp, endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS data1;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_PROBE_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_PROBE_TBL LIKE WEB_PROBE_TBL;

SET @where=' where 1';
IF inUrlName!='ALL'
THEN
SET @where=CONCAT(@where,' AND Website in ( ',inUrlName,')');
END IF;

IF inLocation!="ALL"
THEN
SET @where=CONCAT(@where, ' AND SourceLocation in (',inLocation,')');
END IF;
SET @query=CONCAT('INSERT INTO TEMP_WEB_PROBE_TBL SELECT * from WEB_PROBE_TBL',@where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


CREATE TEMPORARY TABLE data1 as SELECT b.NameOrTag,b.SourceLocation,HostedLocation,a.UrlName,ROUND(AVG(TcpConnectTime),2) as TcpConnectTime_1,TimeStamp from WEB_STATS_TBL a, TEMP_WEB_PROBE_TBL b,URL150 c  where a.UrlName=b.Website and a.NodeIP = b.SourceIP and a.UrlName = c.URL and TimeStamp between startTime  and endTime GROUP BY a.UrlName,NameOrTag,HOUR(TimeStamp) order by Timestamp desc ;


set session group_concat_max_len=1000000;
SELECT NameOrTag,SourceLocation,HostedLocation,UrlName,GROUP_CONCAT(CONCAT(HOUR(TIMEDIFF(TimeStamp,StartTime)),'--',TcpConnectTime_1)) from data1  group by NameOrTag,SourceLocation,HostedLocation,UrlName ;
END | 
 

 DROP PROCEDURE IF EXISTS webProbeDns; 
 CREATE PROCEDURE webProbeDns(inUrlName varchar(50000),inLocation varchar(50000),startTime timestamp, endTime timestamp)
BEGIN

DROP TEMPORARY TABLE IF EXISTS data1;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_PROBE_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_PROBE_TBL LIKE WEB_PROBE_TBL;

SET @where=' where 1';
IF inUrlName!='ALL'
THEN
SET @where=CONCAT(@where,' AND Website in ( ',inUrlName,')');
END IF;

IF inLocation!="ALL"
THEN
SET @where=CONCAT(@where, ' AND SourceLocation in (',inLocation,')');
END IF;
SET @query=CONCAT('INSERT INTO TEMP_WEB_PROBE_TBL SELECT * from WEB_PROBE_TBL',@where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


CREATE TEMPORARY TABLE data1 as SELECT b.NameOrTag,b.SourceLocation,HostedLocation,a.UrlName,ROUND(AVG(DnsLookupTime),2) as DnsLookupTime_1,TimeStamp from WEB_STATS_TBL a, TEMP_WEB_PROBE_TBL b,URL150 c  where a.UrlName=b.Website and a.NodeIP = b.SourceIP and a.UrlName = c.URL and TimeStamp between startTime  and endTime GROUP BY a.UrlName,NameOrTag,HOUR(TimeStamp) order by Timestamp desc ;


set session group_concat_max_len=1000000;
SELECT NameOrTag,SourceLocation,HostedLocation,UrlName,GROUP_CONCAT(CONCAT(HOUR(TIMEDIFF(TimeStamp,StartTime)),'--',DnsLookupTime_1)) from data1  group by NameOrTag,SourceLocation,HostedLocation,UrlName ;
END | 
 

 DROP PROCEDURE IF EXISTS WebUsage; 
 CREATE PROCEDURE WebUsage(flag INTEGER, StartTime TimeStamp, EndTime TimeStamp)
BEGIN
DECLARE a INTEGER;
DECLARE cArea VARCHAR(50);
DECLARE UrlList VARCHAR(10);
SET UrlList="ALL";
IF(flag=1)
THEN
SET a=1;
DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_URL150_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(500),
                        Time_1 VARCHAR(50)
                        );

CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);

CREATE TEMPORARY TABLE TEMP_URL150_TBL LIKE URL150;
SET @where=' where 1';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;
SET @query=CONCAT('INSERT INTO  TEMP_URL150_TBL SELECT * from URL150 ', @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(rtt_avg,2)),ROUND(100-Success_rate),b.TimeStamp
FROM TEMP_URL150_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.NodeIp=c.NodeID and b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime ;



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from URL150 a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag varchar(500),Time_1 Timestamp);
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,NameOrTag,ws.TimeStamp
FROM WEB_STATS_TBL ws, TEMP_URL150_TBL f, WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and Timestamp>=StartTime AND Timestamp<EndTime;

        INSERT INTO WEB_USAGE_TBL
        SELECT StartTime, EndTime, b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber;

ELSEIF flag=2 THEN

SET a=1;

DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_URL150_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(500),
                        Time_1 VARCHAR(100)
                        );
CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);
CREATE TEMPORARY TABLE TEMP_URL150_TBL LIKE URL150;
SET @where=' where 1';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;

SET @query=CONCAT('INSERT INTO  TEMP_URL150_TBL SELECT * from URL150 ', @where);
PREPARE stmt1 from @query;

EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(AVG(rtt_avg),2)),ROUND(AVG(100-Success_rate)),b.TimeStamp
FROM TEMP_URL150_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime group by a.URL,b.NodeIp,HOUR(TimeStamp);



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from TEMP_URL150_TBL a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp DEFAULT '0000-00-00 00:00:00');
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation, AVG(TCPConnectTime), AVG(HTTPGetTime), AVG(DNSLookUpTime), AVG(TCPConnectTime+HTTPGetTime+DNSLookUpTime),NameOrTag,ws.Timestamp
FROM WEB_STATS_TBL ws, TEMP_URL150_TBL f, WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and Timestamp>=StartTime AND Timestamp<EndTime
GROUP BY URLName,NodeNo,HOUR(TimeStamp);

        INSERT INTO WEB_USAGE_TBL
        SELECT CONCAT(DATE(b.Time_1),' ',IF(HOUR(b.Time_1)>=10,HOUR(b.Time_1),CONCAT(0,HOUR(b.Time_1))),':00:00'),CONCAT(DATE(TIMESTAMPADD(HOUR,1,b.Time_1)),' ',IF(HOUR(TIMESTAMPADD(HOUR,1,b.Time_1))>=10,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)),CONCAT(0,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)))),':00:00'), b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,'N/A'
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber group by b.NodeNumber,b.URL,HOUR(b.Time_1);

END IF;

UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;

SELECT * FROM WEB_USAGE_TBL; 

END | 
 

 DROP PROCEDURE IF EXISTS WebUsage25; 
 CREATE PROCEDURE WebUsage25(flag INTEGER, StartTime TimeStamp, EndTime TimeStamp)
BEGIN
DECLARE a INTEGER;
DECLARE cArea VARCHAR(50);
DECLARE UrlList VARCHAR(10);
SET UrlList="ALL";
IF(flag=1)
THEN
SET a=1;
DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_Fav25Sites_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(100),
                        Time_1 VARCHAR(50)
                        );

CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);
CREATE TEMPORARY TABLE TEMP_Fav25Sites_TBL LIKE Fav25Sites;
SET @where=' where 1 ';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;
SET @query=CONCAT('INSERT INTO  TEMP_Fav25Sites_TBL SELECT * from Fav25Sites ', @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(rtt_avg,2)),ROUND(100-Success_rate),b.TimeStamp
FROM TEMP_Fav25Sites_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.NodeIp=c.NodeID and b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime ;



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from TEMP_Fav25Sites_TBL a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp);
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,NameOrTag,ws.TimeStamp
FROM WEB_STATS_TBL ws, TEMP_Fav25Sites_TBL f, WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and  Timestamp>=StartTime AND Timestamp<EndTime;

        INSERT INTO WEB_USAGE_TBL
        SELECT StartTime, EndTime, b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber;

ELSEIF flag=2 THEN

SET a=1;

DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_Fav25Sites_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(500),
                        Time_1 VARCHAR(10)
                        );
CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP
);

CREATE TEMPORARY TABLE TEMP_Fav25Sites_TBL LIKE Fav25Sites;
SET @where=' where 1';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;
SET @query=CONCAT('INSERT INTO  TEMP_Fav25Sites_TBL SELECT * from Fav25Sites ', @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(AVG(rtt_avg),2)),ROUND(AVG(100-Success_rate)),b.TimeStamp
FROM TEMP_Fav25Sites_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime group by a.URL,b.NodeIp,HOUR(TimeStamp);



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from TEMP_Fav25Sites_TBL a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp DEFAULT '0000-00-00 00:00:00');
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation, AVG(TCPConnectTime), AVG(HTTPGetTime), AVG(DNSLookUpTime), AVG(TCPConnectTime+HTTPGetTime+DNSLookUpTime),NameOrTag,ws.Timestamp
FROM WEB_STATS_TBL ws, TEMP_Fav25Sites_TBL f,WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and Timestamp>=StartTime AND Timestamp<EndTime
GROUP BY URLName,NodeNo,HOUR(TimeStamp);

        INSERT INTO WEB_USAGE_TBL
        SELECT CONCAT(DATE(b.Time_1),' ',IF(HOUR(b.Time_1)>=10,HOUR(b.Time_1),CONCAT(0,HOUR(b.Time_1))),':00:00'),CONCAT(DATE(TIMESTAMPADD(HOUR,1,b.Time_1)),' ',IF(HOUR(TIMESTAMPADD(HOUR,1,b.Time_1))>=10,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)),CONCAT(0,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)))),':00:00'), b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,'N/A'
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber group by b.NodeNumber,b.URL,HOUR(b.Time_1);

END IF;

UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;

SELECT * FROM WEB_USAGE_TBL; 

END | 
 

 DROP PROCEDURE IF EXISTS WebUsage25_realTime; 
 CREATE PROCEDURE WebUsage25_realTime()
BEGIN

DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(100),
                        Time_1 VARCHAR(20)
                        );
CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP
);
select Now() into @tempTime;
set @tempTime = TIMESTAMPADD(MINUTE,-30,@tempTime); 

drop temporary table if exists lastEntry;
create temporary table lastEntry (index (URLName,NodeIp,TimeStamp)) as select urlName,NodeIP,max(timeStamp) as timeStamp from WEB_PING_STATS_TBL where timeStamp > @tempTime group by urlName,NodeIP;

drop TEMPORARY table if exists pingJoin;
create TEMPORARY table pingJoin(index (urlName,NodeIp,timeStamp)) as select d.urlName,d.NodeIp,d.timeStamp from Fav25Sites a,NODE_TBL c,lastEntry d where a.url = CONCAT("http://",d.urlName) and d.NodeIp=c.NodeID; 

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URLName,a.NodeIp,IF(Success_rate=0,'N','Y'),IF(Success_rate=0,'Y','N'),IF(rtt_avg=-1,'N/A',ROUND(rtt_avg,2)),ROUND(100-Success_rate),a.TimeStamp FROM WEB_PING_STATS_TBL a JOIN pingJoin b where a.TimeStamp > @tempTime and a.NodeIp=b.NodeIp and a.UrlName like b.UrlName and a.TimeStamp = b.TimeStamp;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from Fav25Sites a where a.Url not in (select CONCAT("http://",UrlName) from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber,a.Time_1 = a.Time_1 where a.NodeIp=b.NodeID;
 
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,NodeIP VARCHAR(20),URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp);

drop temporary table if exists lastEntry1 ;
create temporary table lastEntry1(index(URLName,NodeIp,Timestamp)) as select URLName,NodeIp,max(Timestamp) as Timestamp from WEB_STATS_TBL where Timestamp > @tempTime group by URLName,NodeIp;

drop TEMPORARY table if exists webStatsJoin;
create temporary table webStatsJoin as select l.URLName,l.NodeIp,l.Timestamp,f.HostedLocation,wp.NameOrTag from Fav25Sites f ,lastEntry1 l, WEB_PROBE_TBL wp where f.URL = l.UrlName and wp.SourceIp=l.NodeIp and wp.Website = l.URLName;

set session group_concat_max_len=1000000;
select GROUP_CONCAT(CONCAT('"',l.URLName,'|',l.NodeIp,'|',l.Timestamp,'"')) into @webIp from webStatsJoin l;

set @query = CONCAT("INSERT INTO TEMP_WEB_STATS_TBL(NodeNumber,NodeIp,URL,TCPGetTime,HTTPGetTime,DNSLookUpTime,PageLoadTime,Time_1)
SELECT ws.NodeNo,ws.NodeIP,ws.URLName,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,ws.TimeStamp
FROM WEB_STATS_TBL ws WHERE ws.TimeStamp > '",@tempTime,"' and CONCAT(ws.URLName,'|',ws.NodeIp,'|',ws.Timestamp) in (",@webIp,")");
PREPARE statement1 from @query;
EXECUTE statement1;
DEALLOCATE PREPARE statement1;

update TEMP_WEB_STATS_TBL a join webStatsJoin b on a.URL = b.urlName and a.NodeIP = b.NodeIP set a.HostedLocation = b.HostedLocation,a.NameOrTag = b.NameOrTag, a.Time_1 = a.Time_1;
INSERT INTO WEB_USAGE_TBL 
	SELECT b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber group by b.URL,NodeName;

UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;

SELECT * FROM WEB_USAGE_TBL ;

END | 
 

 DROP PROCEDURE IF EXISTS WebUsage25_reporting; 
 CREATE PROCEDURE WebUsage25_reporting(flag INTEGER, StartTime TimeStamp, EndTime TimeStamp)
BEGIN
DECLARE a INTEGER;
DECLARE cArea VARCHAR(50);
DECLARE UrlList VARCHAR(10);
SET UrlList="ALL";
IF(flag=1)
THEN
SET a=1;
DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_Fav25Sites_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(100),
                        Time_1 VARCHAR(50)
                        );

CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);
CREATE TEMPORARY TABLE TEMP_Fav25Sites_TBL LIKE Fav25Sites;
SET @where=' where 1 ';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;
SET @query=CONCAT('INSERT INTO  TEMP_Fav25Sites_TBL SELECT * from Fav25Sites ', @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(rtt_avg,2)),ROUND(100-Success_rate),b.TimeStamp
FROM TEMP_Fav25Sites_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.NodeIp=c.NodeID and b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime ;



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from TEMP_Fav25Sites_TBL a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp);
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,NameOrTag,ws.TimeStamp
FROM WEB_STATS_TBL ws, TEMP_Fav25Sites_TBL f, WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and  Timestamp>=StartTime AND Timestamp<EndTime;

INSERT INTO WEB_USAGE_TBL
SELECT 'Start Time','End Time','URL','Hosted Location','Availibality','Timeout','Latency','TCPGetTime','HTTPGetTime','DNSLookUpTime','PageLoadTime','PacketLoss','Score','Instance Name','TimeStamp';

        INSERT INTO WEB_USAGE_TBL
        SELECT StartTime, EndTime, b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber;

ELSEIF flag=2 THEN

SET a=1;

DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_Fav25Sites_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(500),
                        Time_1 VARCHAR(100)
                        );
CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP
);

CREATE TEMPORARY TABLE TEMP_Fav25Sites_TBL LIKE Fav25Sites;
SET @where=' where 1';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;
SET @query=CONCAT('INSERT INTO  TEMP_Fav25Sites_TBL SELECT * from Fav25Sites ', @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(AVG(rtt_avg),2)),ROUND(AVG(100-Success_rate)),b.TimeStamp
FROM TEMP_Fav25Sites_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime group by a.URL,b.NodeIp,HOUR(TimeStamp);



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from TEMP_Fav25Sites_TBL a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp DEFAULT '0000-00-00 00:00:00');
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation, AVG(TCPConnectTime), AVG(HTTPGetTime), AVG(DNSLookUpTime), AVG(TCPConnectTime+HTTPGetTime+DNSLookUpTime),NameOrTag,ws.Timestamp
FROM WEB_STATS_TBL ws, TEMP_Fav25Sites_TBL f,WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and Timestamp>=StartTime AND Timestamp<EndTime
GROUP BY URLName,NodeNo,HOUR(TimeStamp);

INSERT INTO WEB_USAGE_TBL
SELECT 'Start Time','End Time','URL','Hosted Location','Availibality','Timeout','Latency','TCPGetTime','HTTPGetTime','DNSLookUpTime','PageLoadTime','PacketLoss','Score','Instance Name','TimeStamp';

        INSERT INTO WEB_USAGE_TBL
        SELECT CONCAT(DATE(b.Time_1),' ',IF(HOUR(b.Time_1)>=10,HOUR(b.Time_1),CONCAT(0,HOUR(b.Time_1))),':00:00'),CONCAT(DATE(TIMESTAMPADD(HOUR,1,b.Time_1)),' ',IF(HOUR(TIMESTAMPADD(HOUR,1,b.Time_1))>=10,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)),CONCAT(0,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)))),':00:00'), b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,'N/A'
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber group by b.NodeNumber,b.URL,HOUR(b.Time_1);

END IF;

UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;

SET @fileName=CONCAT(@dir_name,'/URL25.csv');
SET @query=CONCAT('SELECT * FROM WEB_USAGE_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS WebUsage_reporting; 
 CREATE PROCEDURE WebUsage_reporting(flag INTEGER, StartTime TimeStamp, EndTime TimeStamp)
BEGIN
DECLARE a INTEGER;
DECLARE cArea VARCHAR(50);
DECLARE UrlList VARCHAR(10);
SET UrlList="ALL";
IF(flag=1)
THEN
SET a=1;
DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_URL150_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(500),
                        Time_1 VARCHAR(50)
                        );

CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);

CREATE TEMPORARY TABLE TEMP_URL150_TBL LIKE URL150;
SET @where=' where 1';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;
SET @query=CONCAT('INSERT INTO  TEMP_URL150_TBL SELECT * from URL150 ', @where);
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(rtt_avg,2)),ROUND(100-Success_rate),b.TimeStamp
FROM TEMP_URL150_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.NodeIp=c.NodeID and b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime ;



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from URL150 a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag varchar(500),Time_1 Timestamp);
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,NameOrTag,ws.TimeStamp
FROM WEB_STATS_TBL ws, TEMP_URL150_TBL f, WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and Timestamp>=StartTime AND Timestamp<EndTime;

INSERT INTO WEB_USAGE_TBL
SELECT 'Start Time','End Time','URL','Hosted Location','Availibality','Timeout','Latency','TCPGetTime','HTTPGetTime','DNSLookUpTime','PageLoadTime','PacketLoss','Score','Instance Name','TimeStamp';

        INSERT INTO WEB_USAGE_TBL
        SELECT StartTime, EndTime, b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber;


ELSEIF flag=2 THEN

SET a=1;

DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_URL150_TBL;

CREATE TEMPORARY TABLE WEB_USAGE_TBL(StartTime VARCHAR(100),
                        EndTime VARCHAR(100),
                        URL VARCHAR(200),
                        HostedLocation VARCHAR(100),
                        Availibality VARCHAR(100),
                        Timeout VARCHAR(100),
                        Latency VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        PacketLoss VARCHAR(100),
                        Score VARCHAR(100),
                        InstanceName VARCHAR(500),
                        Time_1 VARCHAR(100)
                        );
CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Availability VARCHAR(10),
			TimeOut VARCHAR(10),
			Latency VARCHAR(100),
			PacketLoss VARCHAR(100),
			Time_1 TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);
CREATE TEMPORARY TABLE TEMP_URL150_TBL LIKE URL150;
SET @where=' where 1';
IF UrlList != 'ALL'
THEN
SET @where=CONCAT(@where,' and Url in (',UrlList,')');
END IF;

SET @query=CONCAT('INSERT INTO  TEMP_URL150_TBL SELECT * from URL150 ', @where);
PREPARE stmt1 from @query;

EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,Success_rate,(100-Success_rate),IF(rtt_avg=-1,'N/A',ROUND(AVG(rtt_avg),2)),ROUND(AVG(100-Success_rate)),b.TimeStamp
FROM TEMP_URL150_TBL a, WEB_PING_STATS_TBL b,NODE_TBL c where b.UrlName like a.Url and b.TimeStamp>StartTime and b.TimeStamp<EndTime group by a.URL,b.NodeIp,HOUR(TimeStamp);



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from TEMP_URL150_TBL a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,NameOrTag VARCHAR(500),Time_1 Timestamp DEFAULT '0000-00-00 00:00:00');
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation, AVG(TCPConnectTime), AVG(HTTPGetTime), AVG(DNSLookUpTime), AVG(TCPConnectTime+HTTPGetTime+DNSLookUpTime),NameOrTag,ws.Timestamp
FROM WEB_STATS_TBL ws, TEMP_URL150_TBL f, WEB_PROBE_TBL wp
WHERE ws.URLName like f.URL and wp.SourceIp=ws.NodeIp and wp.Website=ws.UrlName and Timestamp>=StartTime AND Timestamp<EndTime
GROUP BY URLName,NodeNo,HOUR(TimeStamp);

INSERT INTO WEB_USAGE_TBL
SELECT 'Start Time','End Time','URL','Hosted Location','Availibality','Timeout','Latency','TCPGetTime','HTTPGetTime','DNSLookUpTime','PageLoadTime','PacketLoss','Score','Instance Name','TimeStamp';

        INSERT INTO WEB_USAGE_TBL
        SELECT CONCAT(DATE(b.Time_1),' ',IF(HOUR(b.Time_1)>=10,HOUR(b.Time_1),CONCAT(0,HOUR(b.Time_1))),':00:00'),CONCAT(DATE(TIMESTAMPADD(HOUR,1,b.Time_1)),' ',IF(HOUR(TIMESTAMPADD(HOUR,1,b.Time_1))>=10,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)),CONCAT(0,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)))),':00:00'), b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NameOrTag,'N/A'
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber group by b.NodeNumber,b.URL,HOUR(b.Time_1);

END IF;

UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;

SET @fileName=CONCAT(@dir_name,'/URL150.csv');
SET @query=CONCAT('SELECT * FROM WEB_USAGE_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \'^\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

END | 
 

 DROP PROCEDURE IF EXISTS web_threshold; 
 CREATE PROCEDURE web_threshold()
BEGIN

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;

CREATE TABLE IF NOT EXISTS WEB_THRESHOLD_TBL(
                        URL VARCHAR(200),
                        NodeName VARCHAR(100),
                        HostedLocation VARCHAR(100),
			AcceptableTh VARCHAR(100),
                        Latency VARCHAR(100),
                        LatencyFlag VARCHAR(100),
                        TCPGetTime VARCHAR(100),
                        TCPGetTimeFlag VARCHAR(100),
                        HTTPGetTime VARCHAR(100),
                        DNSLookUpTime VARCHAR(100),
                        PageLoadTime VARCHAR(100),
                        Time_1 VARCHAR(20),
			unique(URL,NodeName,Time_1)
                        );
CREATE TEMPORARY TABLE TEMP_WEB_LATENCY_TBL
(
			URL VARCHAR(200),
			NodeNumber int,
			NodeIp varchar(64),
			Latency VARCHAR(100),
			Time_1 TIMESTAMP
);

drop temporary table if exists lastEntry;
create temporary table lastEntry (index (URLName,NodeIp,TimeStamp)) as select urlName,NodeIP,max(timeStamp) as timeStamp from WEB_PING_STATS_TBL group by urlName,NodeIP;
INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Latency, Time_1)
SELECT distinct a.URL,b.NodeIp,IF(rtt_avg=-1,'N/A',ROUND(rtt_avg,2)),b.TimeStamp
FROM Fav25Sites a, WEB_PING_STATS_TBL b,NODE_TBL c,lastEntry d where b.NodeIp=c.NodeID and b.UrlName like a.Url and b.NodeIP = d.NodeIP and b.urlName = d.urlName and b.TimeStamp = d.TimeStamp;


INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Latency, Time_1)
SELECT distinct a.URL,'N/A','0000-00-00' from Fav25Sites a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,NodeIP VARCHAR(20),URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,Time_1 Timestamp,index(URL,NodeIP));

drop temporary table if exists lastEntry1 ;
create temporary table lastEntry1(index(URLName,NodeNo,Timestamp)) as select URLName,NodeNo,max(Timestamp) as Timestamp from WEB_STATS_TBL group by URLName,NodeNo;
INSERT INTO TEMP_WEB_STATS_TBL
SELECT ws.NodeNo,ws.NodeIP,ws.URLName,HostedLocation,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,ws.TimeStamp
FROM WEB_STATS_TBL ws, Fav25Sites f,lastEntry1 l
WHERE ws.URLName like f.URL and ws.URLName= l.URLName and ws.Timestamp=l.Timestamp and ws.NodeNo = l.NodeNo;

INSERT IGNORE INTO WEB_THRESHOLD_TBL(URL,AcceptableTh,HostedLocation,Latency,LatencyFlag,TCPGetTime,TCPGetTimeFlag,HTTPGetTime,DNSLookUpTime,PageLoadTime,NodeName,Time_1) 
	SELECT b.URL,ThresholdAsPerLocation, HostedLocation, if(Latency IS NULL,'N/A',Latency),if(Latency > ThresholdAsPerLocation,1,0), b.TCPGetTime,if(TCPGetTime > ThresholdAsPerLocation,1,0), b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime,NameOrTag,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like d.Url and b.NodeNumber=d.NodeNumber JOIN WEB_PROBE_TBL ON SourceIp = b.NodeIp and WebSite like b.URL where Latency > ThresholdAsPerLocation or TCPGetTime > ThresholdAsPerLocation group by b.URL,NodeName;


END | 

\d ;
GRANT EXECUTE ON PROCEDURE Vegayan.maxIn24 TO 'webclient'@'10.227.244.74';

GRANT EXECUTE ON PROCEDURE Vegayan.maxIn24WithTime TO 'webclient'@'10.227.244.74';


GRANT EXECUTE ON PROCEDURE Vegayan.6_6_2_LspUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_3_TempUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_4_BufferUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_5_CpuUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_7_unusedServicePolicy TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_8_unusedVrf TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_10_CustomerUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.95percentile TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.maxIn24WithPortId TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.mpls_reporting_6_4_5_classOfService TO  'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_4_6_Latency TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.6_6_6_StorageUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.WebUsage TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.WebUsage25 TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.WebUsage25_realTime TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.webProbe TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.webProbeDns TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.mpls_web_reporting_6_4_3_Pearing_Link_Util TO  'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.mpls_web_6_4_1_PortUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.mpls_web_6_4_3_Pearing_Link_Util TO  'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.mpls_web_6_6_1_VRFUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.mpls_web_6_6_9_PortUtil TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.CustomerTrafFilter TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.getLatestThreshold TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.getLatestWebThreshold TO 'webclient'@'10.227.244.74';
GRANT EXECUTE ON PROCEDURE Vegayan.getHistoricThreshold TO 'webclient'@'10.227.244.74';



