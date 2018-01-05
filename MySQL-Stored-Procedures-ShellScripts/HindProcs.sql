/*Router CPU Util Pie chart*/
DROP PROCEDURE IF EXISTS cpuUTIL;
DELIMITER $$
CREATE PROCEDURE cpuUTIL()
	BEGIN
		DECLARE R1,R2,R3,R4,R5 INT DEFAULT 0;
		DECLARE i,j,k INT DEFAULT 0;
		DECLARE R INT DEFAULT 0;
		DECLARE MAX_TIME INT DEFAULT 0;
		DECLARE MxNodeCount,MnNodeCount INT DEFAULT 0;
		SET i=0;
		SET j=0;
		SET k=j+10;
		
		SELECT MAX(NodeNumber) INTO MxNodeCount from NODE_TBL;
		SELECT MIN(NodeNumber) INTO MnNodeCount from NODE_TBL;
		SELECT MAX(time_to_sec(TimeStamp)) INTO MAX_TIME from CPU_STATS_TBL WHERE CpuUtil<>0;

		DROP TABLE IF EXISTS TEMP_CPU_DASH;
		CREATE TABLE TEMP_CPU_DASH (NodeNo SMALLINT,CpuUtil INT);
		WHILE MnNodeCount <= MxNodeCount
		DO
			INSERT TEMP_CPU_DASH(NodeNo,cpuUtil) select NodeNo,CpuUtil from CPU_STATS_TBL WHERE time_to_sec(TimeStamp)>=(MAX_TIME-1800) AND  NodeNo=MnNodeCount and CpuUtil<>0 order by CpuUtil desc limit 2;
			SET MnNodeCount=MnNodeCount+1;
		END WHILE;


		WHILE i < 5 DO
			SELECT count(cpuUtil) INTO R FROM (SELECT CAST(SUM(CpuUtil)/2 AS unsigned) AS cpuUtil,NodeNo FROM TEMP_CPU_DASH GROUP BY NodeNo) AS T WHERE cpuUtil between j and k;
			CASE 
				WHEN j=0 THEN
					SET R1=R;
					SET j=1;
				WHEN j=11 THEN
					SET R2=R;
				WHEN j=21 THEN
					SET R3=R;
				WHEN j=31 THEN
					SET R4=R;
					SET k=90;
				WHEN j=41 THEN
					SET R5=R;
			END CASE;
			SET j=j+10;
			SET k=k+10;
			SET i = i + 1;
		END WHILE;
		SELECT R1,R2,R3,R4,R5;
	
	END$$
DELIMITER ;

/*Router Memory Util Pie chart*/
DROP PROCEDURE IF EXISTS memUTIL;
DELIMITER $$
CREATE PROCEDURE memUTIL()
        BEGIN
                DECLARE R1,R2,R3,R4,R5 INT DEFAULT 0;
                DECLARE i,j,k INT DEFAULT 0;
                DECLARE R INT DEFAULT 0;
		DECLARE MAX_TIME INT DEFAULT 0;
                SET i=0;
                SET j=0;
                SET k=j+10;

		select MAX(time_to_sec(TimeStamp)) INTO MAX_TIME from STORAGE_STATS_TBL;
                WHILE i < 5 DO
			SELECT COUNT(memUtil) INTO R FROM (SELECT COUNT(NodeNo),NodeNo,(sum(StorageUsed)/sum(StorageSize) * 100) AS memUtil FROM STORAGE_STATS_TBL WHERE time_to_sec(TimeStamp)>=(MAX_TIME-1800) GROUP BY NodeNo) AS T where memUtil BETWEEN j AND k;
                        CASE
                                WHEN j=0 THEN
                                        SET R1=R;
                                        SET j=1;
                                WHEN j=11 THEN
                                        SET R2=R;
                                WHEN j=21 THEN
                                        SET R3=R;
                                WHEN j=31 THEN
                                        SET R4=R;
					SET k=90;
                                WHEN j=41 THEN
                                        SET R5=R;
                        END CASE;
                        SET j=j+10;
                        SET k=k+10;
                        SET i = i + 1;
                END WHILE;
                SELECT R1,R2,R3,R4,R5;
        END$$
DELIMITER ;

/*Router Temp Util Pie chart*/
DROP PROCEDURE IF EXISTS tempUTIL;
DELIMITER $$
CREATE PROCEDURE tempUTIL()
        BEGIN
                DECLARE R1,R2,R3,R4,R5 INT DEFAULT 0;
                DECLARE i,j,k INT DEFAULT 0;
                DECLARE R INT DEFAULT 0;
		DECLARE MAX_TIME INT DEFAULT 0;
		DECLARE MxNodeCount,MnNodeCount INT DEFAULT 0;
                SET i=0;
                SET j=0;
                SET k=j+10;

		SELECT MAX(NodeNumber) INTO MxNodeCount from NODE_TBL;
		SELECT MIN(NodeNumber) INTO MnNodeCount from NODE_TBL;
		SELECT MAX(time_to_sec(TimeStamp)) INTO MAX_TIME from TEMP_STATS_TBL WHERE TempValue<>0;

		DROP TABLE IF EXISTS TEMP_TEMP_DASH;
		CREATE TABLE TEMP_TEMP_DASH (NodeNo SMALLINT,TempValue INT);
		WHILE MnNodeCount <= MxNodeCount
		DO
			INSERT TEMP_TEMP_DASH(NodeNo,TempValue) select NodeNo,TempValue from TEMP_STATS_TBL WHERE time_to_sec(TimeStamp)>=(MAX_TIME-1800) AND  NodeNo=MnNodeCount and TempValue<>0 order by TempValue desc limit 2;
			SET MnNodeCount=MnNodeCount+1;
		END WHILE;

                WHILE i < 5 DO
			SELECT COUNT(tempUtil) INTO R FROM (SELECT COUNT(NodeNo),NodeNo,CAST(sum(TempValue)/2 AS UNSIGNED) AS tempUtil FROM TEMP_TEMP_DASH GROUP BY NodeNo) AS T WHERE tempUtil BETWEEN j and k;
                        CASE
                                WHEN j=0 THEN
                                        SET R1=R;
                                        SET j=1;
                                WHEN j=11 THEN
                                        SET R2=R;
                                WHEN j=21 THEN
                                        SET R3=R;
                                WHEN j=31 THEN
                                        SET R4=R;
					SET k=90;
                                WHEN j=41 THEN
                                        SET R5=R;
                        END CASE;
                        SET j=j+10;
                        SET k=k+10;
                        SET i = i + 1;
                END WHILE;
                SELECT R1,R2,R3,R4,R5;
        END$$
DELIMITER ;

/*Top N links*/
DROP PROCEDURE IF EXISTS spTopNLinksMod;
DELIMITER $$
CREATE PROCEDURE spTopNLinksMod()
BEGIN

DECLARE iIndex INTEGER DEFAULT 1;
DECLARE iNoofLinks INTEGER DEFAULT 0;
DECLARE iCount INTEGER DEFAULT 0;

/* 1 Kbps = 1000 bps */
DECLARE KBPS INTEGER DEFAULT 1000;

SELECT COUNT(*) INTO iCount
FROM ROUTERTRAFFIC_LINK_SCALE1_TBL;

/* If there is no previous traffic data, then given first five links as Top N Links */
IF(iCount = 0) THEN
        SELECT T3.LinkName,0
        FROM LINK_TBL T1, NODE_TBL T2,TEMP_LINK_NAME T3
        WHERE T1.OriNodeNumber = T2.NodeNumber
                AND T1.Status = 1 AND T1.LinkID=T3.LinkID
        ORDER BY T1.LinkID
        ;        /* 5 - Top 5 Links */

/* Otherwise, compute Top N Links based on Traffic Data of last 1 Hr */
ELSE
        SELECT MAX(LinkID) INTO iNoofLinks
        FROM LINK_TBL;

        DROP TABLE IF EXISTS TOPTEN_LINKS_TBL;
        CREATE TABLE IF NOT EXISTS TOPTEN_LINKS_TBL(LinkID        SMALLINT UNSIGNED,
                                                    IfHCOutOctets BIGINT UNSIGNED);
	 WHILE(iIndex <= iNoofLinks)
        DO
                /* Get Latest 12 Entries - Last 1 Hr data */
                INSERT TOPTEN_LINKS_TBL(LinkID, IfHCOutOctets)
                        SELECT LinkID, IfHCOutOctets
                        FROM ROUTERTRAFFIC_LINK_SCALE1_TBL
                        WHERE LinkID = iIndex
                        ORDER BY Time_1 DESC
                        /* Per Hour, 5 Min Entries will be 12 */
                        ;        /* 12 Entries - 1 Hr Data */

                SET iIndex = iIndex + 1;
        END WHILE;

        /* Get Top 5 Entries based on Utilization in Last 1 Hr */
        SELECT T4.LinkName, T1.TotalBW, (AVG(T3.IfHCOutOctets)/10)/(T1.TotalBW) AS OctetRate
        FROM LINK_TBL T1, NODE_TBL T2, TOPTEN_LINKS_TBL T3, TEMP_LINK_NAME T4
        WHERE T2.NodeNumber = T1.OriNodeNumber
                AND T1.LinkID = T3.LinkID
                AND T1.Status = 1 AND T1.LinkID = T4.LinkID
        GROUP BY T1.LinkID
        /* IfHCOutOctets in bps and TotalBW in kbps */
        ORDER BY OctetRate DESC
        ;        /* 5 - Top 5 Links */

        /* Drop Temporary Tables */
        /*DROP TABLE IF EXISTS TOPTEN_LINKS_TBL;*/
END IF;

END$$            /* End of spTopNLinks() */
DELIMITER ;

DROP PROCEDURE IF EXISTS spTopNLinksMod_1;
DELIMITER $$
CREATE PROCEDURE spTopNLinksMod_1()
BEGIN

DECLARE iIndex INTEGER DEFAULT 1;
DECLARE iNoofLinks INTEGER DEFAULT 0;
DECLARE iCount INTEGER DEFAULT 0;

/* 1 Kbps = 1000 bps */
DECLARE KBPS INTEGER DEFAULT 1000;

SELECT COUNT(*) INTO iCount
FROM ROUTERTRAFFIC_LINK_SCALE1_TBL;

/* If there is no previous traffic data, then given first five links as Top N Links */
        SELECT MAX(LinkID) INTO iNoofLinks
        FROM LINK_TBL;

        DROP TABLE IF EXISTS TOPTEN_LINKS_TBL;
        CREATE TABLE IF NOT EXISTS TOPTEN_LINKS_TBL(LinkID        SMALLINT UNSIGNED,
                                                    IfHCOutOctets BIGINT UNSIGNED);
	 WHILE(iIndex <= iNoofLinks)
        DO
                /* Get Latest 12 Entries - Last 1 Hr data */
                INSERT TOPTEN_LINKS_TBL(LinkID, IfHCOutOctets)
                        SELECT LinkID, IfHCOutOctets
                        FROM ROUTERTRAFFIC_LINK_SCALE1_TBL
                        WHERE LinkID = iIndex
                        ORDER BY Time_1 DESC
                        /* Per Hour, 5 Min Entries will be 12 */
                        LIMIT 10;        /* 12 Entries - 1 Hr Data */

                SET iIndex = iIndex + 1;
        END WHILE;

END$$            /* End of spTopNLinks() */
DELIMITER ;
/*No of congested links is ranges 0-20..*/
/* TOPTEN_LINLS_TBL is persisted from above procedure and dropped in this procedure (congestion link bar chart)*/
DROP PROCEDURE IF EXISTS congestionPlot;
DELIMITER $$
CREATE PROCEDURE congestionPlot()
        BEGIN
                DECLARE R1,R2,R3,R4,R5 INT DEFAULT 0;
                DECLARE i,j,k INT DEFAULT 0;
		DECLARE iCount INT DEFAULT 0;
                DECLARE R INT DEFAULT 0;
                SET i=1;
                SET j=0;
                SET k=j+20;

		SELECT COUNT(*) INTO iCount FROM ROUTERTRAFFIC_LINK_SCALE1_TBL;
                /* If there is no previous traffic data, then given first five links as Top N Links */
                IF(iCount = 0) THEN
                        /*SELECT T1.LinkID, T1.OriIfIPAddress, T2.NodeID, T1.OriIfIndex,T3.LinkName
                        FROM LINK_TBL T1, NODE_TBL T2,TEMP_LINK_NAME T3
                        WHERE T1.OriNodeNumber = T2.NodeNumber
                        AND T1.Status = 1 AND T1.ConnectionType = "PE-CPE" and T1.LinkID=T3.LinkID
                        ORDER BY T1.LinkID
                        LIMIT 10;*/        /* 5 - Top 5 Links */
        	        SELECT R1,R2,R3,R4,R5;
                ELSE
                        call spTopNLinksMod_1();
	                WHILE i < 5 DO
				SELECT count(cnt) INTO R FROM (SELECT round((avg(IfHCOutOctets)/10)/T1.TotalBW) as cnt,T1.LinkID FROM LINK_TBL T1,TOPTEN_LINKS_TBL T2 where T1.LinkID=T2.LinkID group by T2.LinkID)as T where T.cnt BETWEEN j AND k;
        	                CASE
        	                        WHEN j=0 THEN
        	                                SET R1=R;
        	                                SET j=1;
        	                        WHEN j=21 THEN
        	                                SET R2=R;
        	                        WHEN j=41 THEN
        	                                SET R3=R;
        	                        WHEN j=61 THEN
        	                                SET R4=R;
        	                        WHEN j=81 THEN
        	                                SET R5=R;
        	                END CASE;
        	                SET j=j+20;
        	                SET k=j+19;
        	                SET i = i + 1;
        	        END WHILE;
		        DROP TABLE IF EXISTS TOPTEN_LINKS_TBL;
        	        SELECT R1,R2,R3,R4,R5;
		END IF;
END$$
DELIMITER ;

drop procedure if exists spHindLinkName;
\d $$

CREATE PROCEDURE spHindLinkName()
BEGIN

DECLARE iIndex INTEGER DEFAULT 0;
DECLARE iNodeCount INTEGER DEFAULT 0;
DECLARE Total INTEGER DEFAULT 0;
	

drop table if exists TEMP_LINK_NAME;
CREATE TABLE IF NOT EXISTS TEMP_LINK_NAME(Id SMALLINT AUTO_INCREMENT PRIMARY KEY,LinkID SMALLINT,OriNodeNumber SMALLINT ,OriNodeName VARCHAR(128),OriIfIndex SMALLINT,OriIfDescr VARCHAR(128),OriIfIPAddress VARCHAR(15),TerNodeNumber SMALLINT ,TerNodeName VARCHAR(128),TerIfIndex SMALLINT,TerIfDescr VARCHAR(128),TerIfIPAddress VARCHAR(15),LinkName VARCHAR(128));

insert into TEMP_LINK_NAME(LinkID, OriNodeNumber,OriIfIndex,OriIfIPAddress,TerNodeNumber,TerIfIndex,TerIfIPAddress)
SELECT LinkID, OriNodeNumber,OriIfIndex,OriIfIPAddress,TerNodeNumber,TerIfIndex,TerIfIPAddress
FROM  LINK_TBL ;
       

update TEMP_LINK_NAME  set TEMP_LINK_NAME.OriIfDescr ='NA' where OriIfDescr is NULL;
update TEMP_LINK_NAME  set TEMP_LINK_NAME.TerIfDescr ='NA' where TerIfDescr  is NULL;
/*inserting the linkDetails in the TEMP_LINK_NAME*/

update TEMP_LINK_NAME JOIN NODE_TBL
on TEMP_LINK_NAME.OriNodeNumber = NODE_TBL.NodeNumber
set TEMP_LINK_NAME.OriNodeName = NODE_TBL.NodeName;

update TEMP_LINK_NAME JOIN NODE_TBL
on TEMP_LINK_NAME.TerNodeNumber = NODE_TBL.NodeNumber
set TEMP_LINK_NAME.TerNodeName = NODE_TBL.NodeName;

update TEMP_LINK_NAME JOIN NODEIF_TBL
on TEMP_LINK_NAME.OriNodeNumber = NODEIF_TBL.NodeNumber
	and TEMP_LINK_NAME.OriIfIndex = NODEIF_TBL.IfIndex
set TEMP_LINK_NAME.OriIfDescr = NODEIF_TBL.IfDescr;


update TEMP_LINK_NAME JOIN NODEIF_TBL
on TEMP_LINK_NAME.TerNodeNumber = NODEIF_TBL.NodeNumber
	and TEMP_LINK_NAME.TerIfIndex = NODEIF_TBL.IfIndex
set TEMP_LINK_NAME.TerIfDescr = NODEIF_TBL.IfDescr;

update TEMP_LINK_NAME
	set LinkName =  concat(OriNodeName,' - ', TerNodeName);

	/*UPDATE TEMP_LINK_NAME, NODE_TBL
	SET LinkName = concat(LinkName, '(OUT)')
	WHERE TEMP_LINK_NAME.OriNodeNumber = NODE_TBL.NodeNumber;
		

	UPDATE TEMP_LINK_NAME, NODE_TBL
	SET LinkName = concat(LinkName, '(IN)')
	WHERE TEMP_LINK_NAME.TerNodeNumber = NODE_TBL.NodeNumber;*/
		


/*select LinkName from TEMP_LINK_NAME into outfile '/tmp/linkName.txt' fields terminated by ',' lines terminated by '\n'; 
select LinkName from TEMP_LINK_NAME ;
*/


select count(*) into iNodecount from TEMP_LINK_NAME ;

alter table TEMP_LINK_NAME add corespondinglinkid smallint;
alter table TEMP_LINK_NAME add correspondinglinkname varchar(128);

while(iIndex <= iNodecount)
DO

select orinodenumber,OriIfIndex,ternodenumber,TerIfIndex into
@orinodenumber,@OriIfIndex,@ternodenumber,@TerIfIndex from
TEMP_LINK_NAME where ID = iIndex;


select linkid,linkname into @linkid,@linkname from TEMP_LINK_NAME
where ternodenumber=@orinodenumber and terIfIndex= @OriIfIndex and
orinodenumber=@ternodenumber and oriIfIndex=@TerIfIndex limit 1;

update TEMP_LINK_NAME set corespondinglinkid=@linkid where ID = iIndex;
update TEMP_LINK_NAME set correspondinglinkname=@linkname where ID = iIndex;


set iIndex = iIndex + 1;

end while;



END$$

\d;