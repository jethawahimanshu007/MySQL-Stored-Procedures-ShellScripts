DROP PROCEDURE IF EXISTS WebUsage25; 
DELIMITER |
CREATE PROCEDURE WebUsage25(flag INTEGER, StartTime TimeStamp, EndTime TimeStamp)
BEGIN
DECLARE a INTEGER;
DECLARE cArea VARCHAR(50);
IF(flag=1)
THEN
SET a=1;
DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;


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
                        NodeName VARCHAR(100),
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

INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,IF(Success_rate=0,'N','Y'),IF(Success_rate=0,'Y','N'),ROUND(rtt_avg,2),ROUND(100-Success_rate),b.TimeStamp
FROM Fav25Sites a, WEB_PING_STATS_TBL b,NODE_TBL c where b.NodeIp=c.NodeID and b.UrlName like CONCAT('%',a.Url,'%') and b.TimeStamp>StartTime and b.TimeStamp<EndTime ;



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from Fav25Sites a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,Time_1 Timestamp);
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation,TCPConnectTime, HTTPGetTime, DNSLookUpTime, TCPConnectTime+HTTPGetTime+DNSLookUpTime,ws.TimeStamp
FROM WEB_STATS_TBL ws, Fav25Sites f
WHERE ws.URLName like CONCAT('%',f.URL,'%') and Timestamp>=StartTime AND Timestamp<EndTime;

        INSERT INTO WEB_USAGE_TBL
        SELECT StartTime, EndTime, b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NodeName,b.Time_1
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like CONCAT('%',d.Url,'%') and b.NodeNumber=d.NodeNumber;

UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;

SELECT * FROM WEB_USAGE_TBL /*into outfile '/tmp/Hourly.csv' fields terminated by ','*/;

ELSEIF flag=2 THEN

SET a=1;

DROP TEMPORARY TABLE IF EXISTS WEB_USAGE_TBL;
DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_LATENCY_TBL;

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
                        NodeName VARCHAR(100),
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
INSERT INTO TEMP_WEB_LATENCY_TBL (URL,NodeIp,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,b.NodeIp,IF(AVG(Success_rate)=0,'N','Y'),IF(AVG(Success_rate)=0,'Y','N'),ROUND(AVG(rtt_avg),2),ROUND(AVG(100-Success_rate)),b.TimeStamp
FROM Fav25Sites a, WEB_PING_STATS_TBL b,NODE_TBL c where b.UrlName like CONCAT('%',a.Url,'%') and b.TimeStamp>StartTime and b.TimeStamp<EndTime group by a.URL,b.NodeIp,HOUR(TimeStamp);



INSERT INTO TEMP_WEB_LATENCY_TBL (URL,Availability, TimeOut, Latency, PacketLoss,Time_1)
SELECT distinct a.URL,'N/A','N/A','N/A','N/A','0000-00-00' from Fav25Sites a where a.Url not in (select UrlName from WEB_PING_STATS_TBL );

UPDATE TEMP_WEB_LATENCY_TBL a,NODE_TBL b set a.NodeNumber=b.NodeNumber where a.NodeIp=b.NodeID;

DROP TEMPORARY TABLE IF EXISTS TEMP_WEB_STATS_TBL;
CREATE TEMPORARY TABLE TEMP_WEB_STATS_TBL(NodeNumber INTEGER,URL VARCHAR(200), HostedLocation VARCHAR(256),TCPGetTime INTEGER, HTTPGetTime INTEGER, DNSLookUpTime INTEGER, PageLoadTime INTEGER,Time_1 Timestamp DEFAULT '0000-00-00 00:00:00');
INSERT INTO TEMP_WEB_STATS_TBL
SELECT NodeNo,URLName,HostedLocation, AVG(TCPConnectTime), AVG(HTTPGetTime), AVG(DNSLookUpTime), AVG(TCPConnectTime+HTTPGetTime+DNSLookUpTime),ws.Timestamp
FROM WEB_STATS_TBL ws, Fav25Sites f
WHERE ws.URLName like CONCAT('%',f.URL,'%') and Timestamp>=StartTime AND Timestamp<EndTime
GROUP BY URLName,NodeNo,HOUR(TimeStamp);

        INSERT INTO WEB_USAGE_TBL
        SELECT CONCAT(DATE(b.Time_1),' ',IF(HOUR(b.Time_1)>=10,HOUR(b.Time_1),CONCAT(0,HOUR(b.Time_1))),':00:00'),CONCAT(DATE(TIMESTAMPADD(HOUR,1,b.Time_1)),' ',IF(HOUR(TIMESTAMPADD(HOUR,1,b.Time_1))>=10,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)),CONCAT(0,HOUR(TIMESTAMPADD(HOUR,1,b.Time_1)))),':00:00'), b.URL, HostedLocation, d.Availability, TimeOut, Latency, b.TCPGetTime, b.HTTPGetTime, b.DNSLookUpTime, b.PageLoadTime, PacketLoss, '',NodeName,'N/A'
        FROM TEMP_WEB_STATS_TBL b LEFT JOIN NODE_TBL c ON b.NodeNumber = c.NodeNumber LEFT JOIN TEMP_WEB_LATENCY_TBL d ON b.URL like CONCAT('%',d.Url,'%') and b.NodeNumber=d.NodeNumber group by b.NodeNumber,b.URL,HOUR(b.Time_1);


UPDATE WEB_USAGE_TBL set Availibality ='N/A' where Availibality is NULL;
UPDATE WEB_USAGE_TBL set Timeout ='N/A' where TimeOut is NULL;
UPDATE WEB_USAGE_TBL set Latency ='N/A' where Latency is NULL;
UPDATE WEB_USAGE_TBL set PacketLoss ='N/A' where PacketLoss is NULL;
/*
SET @fileName=CONCAT(@dir_name,'/URL25_',DATE(startTime),'.csv');
SET @query=CONCAT('SELECT * FROM WEB_USAGE_TBL INTO OUTFILE \'',@fileName,'\' fields terminated by \',\'');
PREPARE stmt1 from @query;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
*/
SELECT * FROM WEB_USAGE_TBL /* into outfile '/tmp/150WebURL.csv' fields terminated by ','*/;

END IF;
END |   
DELIMITER ;
