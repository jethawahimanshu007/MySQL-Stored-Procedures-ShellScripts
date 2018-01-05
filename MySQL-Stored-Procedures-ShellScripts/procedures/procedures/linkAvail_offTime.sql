delimiter |
DROP PROCEDURE IF EXISTS getLinkAvail|
CREATE PROCEDURE getLinkAvail( IN p_LinkId INTEGER,IN p_StartTime VARCHAR(20), IN p_EndTime VARCHAR(20) )
BEGIN
	DECLARE downTime VARCHAR(20);
        DECLARE upTime VARCHAR(20);
        DECLARE totalDownTime INTEGER default 0;
        DECLARE iTimeSpan INTEGER DEFAULT 0;
        DECLARE fAvailability FLOAT(12,4) DEFAULT NULL;
        DECLARE iCount INTEGER DEFAULT 0;
	DECLARE iNoOfOutage INTEGER DEFAULT 0;
	DECLARE iOperStatus VARCHAR(5) DEFAULT NULL;
	DECLARE iCount1 INTEGER DEFAULT 0;
	DECLARE iScheduled_Off_Count INTEGER DEFAULT 0;
	DECLARE LinkFromTime TIMESTAMP DEFAULT NULL;
	DECLARE LinkToTime TIMESTAMP DEFAULT NULL;
	DECLARE iOff_Count INTEGER DEFAULT 0;
	DECLARE iOriNodeNumber INTEGER DEFAULT 0;
	DECLARE OriFromTime TIME DEFAULT NULL;
	DECLARE OriToTime TIME DEFAULT NULL;
	DECLARE iTerNodeNumber INTEGER DEFAULT 0;
	DECLARE TerFromTime TIME DEFAULT NULL;
	DECLARE TerToTime TIME DEFAULT NULL;
	DECLARE iFlag_NWH INTEGER DEFAULT 0; /*Non Working Hour Flag*/
	DECLARE iFlag_SDT Integer DEFAULT 0; /*Scheduled Down Time Flag*/
	DECLARE NWH_Time VARCHAR(100) DEFAULT '';
	DECLARE SDT_Time VARCHAR(100) DEFAULT '';

        DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL,SCHED_TIME_OFF;
	DROP TABLE IF EXISTS LINK_T_TBL;
	CREATE TABLE LINK_T_TBL(LinkID Integer,
				rcvd_time_1 TIMESTAMP,
				Operstatus VARCHAR(10),
				trapType INTEGER);
	INSERT INTO LINK_T_TBL select LinkID,rcvd_time_1,Operstatus,trapType from TRAP_TBL where trapType = 2; 
        CREATE TEMPORARY TABLE TRAP_DOWN_TBL (
                LinkId INTEGER UNSIGNED,
                OperDownStatus VARCHAR(10) NOT NULL,
                rcvd_Time_1 TIMESTAMP );

        CREATE TEMPORARY TABLE TRAP_UP_TBL (
                LinkId INTEGER UNSIGNED,
                OperUpStatus VARCHAR(10) NOT NULL,
                rcvd_Time_1 TIMESTAMP );

	CREATE TEMPORARY TABLE SCHED_TIME_OFF LIKE SCHEDULED_DOWN_TIME;
	
        INSERT INTO TRAP_DOWN_TBL
                SELECT LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL
                        WHERE LinkID = p_LinkId
			AND trapType = 2      /* trapType 2 is TrapLink */
                        AND rcvd_Time_1 > p_StartTime
                        AND rcvd_Time_1 <= p_EndTime
                        AND OperStatus = 'Down'
                        ORDER BY rcvd_time_1;
        INSERT INTO TRAP_UP_TBL
                SELECT LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL
                        WHERE LinkID = p_LinkId
			AND trapType = 2      /* trapType 2 is TrapLink*/
                        AND rcvd_Time_1 > p_StartTime
                        AND rcvd_Time_1 <= p_EndTime
                        AND OperStatus = 'Up'
                        ORDER BY rcvd_time_1;

	SELECT OriNodeNumber,TerNodeNumber into iOriNodeNumber,iTerNodeNumber from LINK_TBL where LinkID = p_LinkId;

        select count(*) into iOff_Count from NODE_NON_WORKING_HOUR 
		where nodeid IN (select Nodeid from NODE_TBL where NodeNumber = iOriNodeNumber OR NodeNumber = iTerNodeNumber);

	IF(iOff_Count != 0)
        THEN
		SET iFlag_NWH = 1;
                select FromTime,ToTime into OriFromTime,OriToTime from NODE_NON_WORKING_HOUR 
                        where NodeID = (select nodeId from NODE_TBL where NodeNumber = iOriNodeNumber);
                select FromTime,ToTime into TerFromTime,TerToTime from NODE_NON_WORKING_HOUR 
                        where NodeID = (select NodeID from NODE_TBL where NodeNumber = iTerNodeNumber);
		IF(OriFromTime IS NOT NULL AND OriToTime IS NOT NULL)
		THEN
			SELECT CONCAT(NWH_Time,"(",OriFromTime,"-",OriToTime,")") into NWH_Time;
                	DELETE FROM TRAP_DOWN_TBL where TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
                	DELETE FROM TRAP_UP_TBL where TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
		END IF;
                IF(TerFromTime IS NOT NULL AND TerToTime IS NOT NULL)
                THEN
                        SELECT CONCAT(NWH_Time,"(",TerFromTime,"-",TerToTime,")") into NWH_Time;
                        DELETE FROM TRAP_DOWN_TBL where TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
                        DELETE FROM TRAP_UP_TBL where TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
                END IF;

        END IF;

	INSERT INTO SCHED_TIME_OFF (SELECT * from SCHEDULED_DOWN_TIME where LinkID = p_LinkId);
        SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF;
        WHILE(iScheduled_Off_Count != 0)
        DO
		SET iFlag_SDT = 1;
                select FromTime,ToTime into LinkFromTime,LinkToTime from SCHED_TIME_OFF limit 1;
                IF(LinkFromTime IS NOT NULL AND LinkToTime IS NOT NULL)
                THEN
                        SELECT CONCAT(SDT_Time,"(",LinkFromTime," - ",LinkToTime,")") into SDT_Time;
                        DELETE FROM TRAP_DOWN_TBL where rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
                        DELETE FROM TRAP_UP_TBL where rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
                END IF;

		DELETE FROM SCHED_TIME_OFF limit 1;
		SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF;
        END WHILE;


        SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
        SET iTimeSpan = TIMESTAMPDIFF(SECOND, p_StartTime, p_EndTime);

	IF(iCount = 0)
	THEN
		SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL limit 1;
		IF(upTime IS NULL)
		THEN
			SELECT OperStatus into iOperStatus from LINK_T_TBL 
				WHERE LinkID = p_LinkId
                        	AND trapType = 2      /* trapType 2 is TrapLink*/
                        	ORDER BY rcvd_time_1 desc limit 1;
			IF(iOperStatus = 'Down')
			THEN
				SET iNoOfOutage = iNoOfOutage + 1;
				SET totalDownTime = iTimeSpan;
			END IF;
		ELSE
                        SET iNoOfOutage = iNoOfOutage + 1;
                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, p_StartTime, upTime);
		END IF;
	ELSE
		SELECT rcvd_Time_1 INTO downTime FROM TRAP_DOWN_TBL limit 1;
                SET upTime = NULL;
                SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL where rcvd_Time_1 < downTime order by rcvd_Time_1 limit 1;
                IF(upTime IS NOT NULL)
                THEN
                	SET iNoOfOutage = iNoOfOutage + 1;
                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, p_StartTime, upTime);
                        DELETE FROM TRAP_UP_TBL where rcvd_Time_1 <= downTime;
                END IF;
	        WHILE(iCount != 0)
        	DO
			SET downTime = NULL;
                	SELECT rcvd_Time_1 INTO downTime FROM TRAP_DOWN_TBL limit 1;
			SET upTime = NULL;	
                        DELETE FROM TRAP_UP_TBL where rcvd_Time_1 < downTime;
			SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL limit 1;

                	IF(upTime IS NULL)
                	THEN
				SET iNoOfOutage = iNoOfOutage + 1;
	                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, p_EndTime);
        	                DELETE FROM TRAP_DOWN_TBL where rcvd_Time_1 = downTime;
                	ELSE
				SET iNoOfOutage = iNoOfOutage + 1;
	                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, upTime);
        	                DELETE FROM TRAP_DOWN_TBL where rcvd_Time_1 <= upTime;
	                END IF;
        	        SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
	        END WHILE;
	END IF;
	
	IF(iTimeSpan IS NULL)
                THEN SET iTimeSpan = 1;
        END IF;
        SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
	IF(NWH_Time = "")
                THEN SET NWH_Time = 'NA';
        END IF;

        IF(SDT_Time = "")
                THEN SET SDT_Time = 'NA';
        END IF;
     	
	SELECT p_LinkId,LinkName,iNoOfOutage,totalDownTime,fAvailability,NWH_Time,SDT_Time,CONCAT(p_StartTime,'-',p_EndTime) as Time
		from TEMP_LINK_NAME
		where LinkID = p_LinkId limit 1;

        DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL,SCHED_TIME_OFF;
END |
DROP PROCEDURE IF EXISTS getLinkAvail_Last24Hour|
CREATE PROCEDURE getLinkAvail_Last24Hour( IN p_LinkId INTEGER)
BEGIN
	DECLARE p_StartTime VARCHAR(20) default NULL;
	DECLARE p_EndTime VARCHAR(20) default NULL;
        DECLARE downTime VARCHAR(20) default NULL;
        DECLARE upTime VARCHAR(20) default NULL;
        DECLARE totalDownTime INTEGER default 0;
        DECLARE iTimeSpan INTEGER DEFAULT 0;
        DECLARE fAvailability FLOAT(12,4) DEFAULT NULL;
        DECLARE iCount INTEGER DEFAULT 0;
	DECLARE iNoOfOutage INTEGER DEFAULT 0;
        DECLARE iOperStatus VARCHAR(5) DEFAULT NULL;
        DECLARE iCount1 INTEGER DEFAULT 0;
        DECLARE iScheduled_Off_Count INTEGER DEFAULT 0;
        DECLARE LinkFromTime TIMESTAMP DEFAULT NULL;
        DECLARE LinkToTime TIMESTAMP DEFAULT NULL;
        DECLARE iOff_Count INTEGER DEFAULT 0;
        DECLARE iOriNodeNumber INTEGER DEFAULT 0;
        DECLARE OriFromTime TIME DEFAULT NULL;
        DECLARE OriToTime TIME DEFAULT NULL;
        DECLARE iTerNodeNumber INTEGER DEFAULT 0;
        DECLARE TerFromTime TIME DEFAULT NULL;
        DECLARE TerToTime TIME DEFAULT NULL;
        DECLARE iFlag_NWH INTEGER DEFAULT 0; /*Non Working Hour Flag*/
        DECLARE iFlag_SDT Integer DEFAULT 0; /*Scheduled Down Time Flag*/
        DECLARE NWH_Time VARCHAR(100) DEFAULT '';
        DECLARE SDT_Time VARCHAR(100) DEFAULT '';

        DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL,SCHED_TIME_OFF;
	DROP TABLE IF EXISTS LINK_T_TBL;
	CREATE TABLE LINK_T_TBL(LinkID Integer,
				rcvd_time_1 TIMESTAMP,
				Operstatus VARCHAR(10),
				trapType INTEGER);
	INSERT INTO LINK_T_TBL select LinkID,rcvd_time_1,Operstatus,trapType from TRAP_TBL where trapType = 2; 
        CREATE TEMPORARY TABLE TRAP_DOWN_TBL (
                LinkId INTEGER UNSIGNED,
                OperDownStatus VARCHAR(10) NOT NULL,
                rcvd_Time_1 TIMESTAMP );

        CREATE TEMPORARY TABLE TRAP_UP_TBL (
                LinkId INTEGER UNSIGNED,
                OperUpStatus VARCHAR(10) NOT NULL,
                rcvd_Time_1 TIMESTAMP );

	CREATE TEMPORARY TABLE SCHED_TIME_OFF LIKE SCHEDULED_DOWN_TIME;

	SELECT rcvd_Time_1 into p_EndTime from TRAP_TBL where trapid = (SELECT max(trapid) from TRAP_TBL);
	/*SET p_EndTime = NOW();*/
	SET p_StartTime = DATE_SUB(p_EndTime,INTERVAL 1 DAY);
	
        INSERT INTO TRAP_DOWN_TBL
                SELECT LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL
                        WHERE LinkID = p_LinkId
                        AND trapType = 2      /* trapType 2 is TrapLink */
                        AND rcvd_Time_1 > p_StartTime
			AND rcvd_Time_1 <= p_EndTime
			AND OperStatus = 'Down'
                        ORDER BY rcvd_time_1;
        INSERT INTO TRAP_UP_TBL
                SELECT LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL
                        WHERE LinkID = p_LinkId
                        AND trapType = 2      /* trapType 2 is TrapLink */
                        AND rcvd_Time_1 > p_StartTime
                        AND rcvd_Time_1 <= p_EndTime
                        AND OperStatus = 'Up'
                        ORDER BY rcvd_time_1;


        SELECT OriNodeNumber,TerNodeNumber into iOriNodeNumber,iTerNodeNumber from LINK_TBL where LinkID = p_LinkId;

        select count(*) into iOff_Count from NODE_NON_WORKING_HOUR 
		where nodeid IN (select Nodeid from NODE_TBL where NodeNumber = iOriNodeNumber OR NodeNumber = iTerNodeNumber);

        IF(iOff_Count != 0)
        THEN
		SET iFlag_NWH = 1;
                select FromTime,ToTime into OriFromTime,OriToTime from NODE_NON_WORKING_HOUR
                        where NodeID = (select NodeID from NODE_TBL where NodeNumber = iOriNodeNumber);
                select FromTime,ToTime into TerFromTime,TerToTime from NODE_NON_WORKING_HOUR
                        where NodeID = (select NodeID from NODE_TBL where NodeNumber = iTerNodeNumber);
	
		IF(OriFromTime IS NOT NULL AND OriToTime IS NOT NULL)
                THEN
                        SELECT CONCAT(NWH_Time,"(",OriFromTime,"-",OriToTime,")") into NWH_Time from dual;
                        DELETE FROM TRAP_DOWN_TBL where TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
                        DELETE FROM TRAP_UP_TBL where TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
                END IF;
                IF(TerFromTime IS NOT NULL AND TerToTime IS NOT NULL)
                THEN
                        SELECT CONCAT(NWH_Time,"(",TerFromTime,"-",TerToTime,")") into NWH_Time;
                        DELETE FROM TRAP_DOWN_TBL where TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
                        DELETE FROM TRAP_UP_TBL where TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
                END IF;

        END IF;

        INSERT INTO SCHED_TIME_OFF (SELECT * from SCHEDULED_DOWN_TIME where LinkID = p_LinkId);
        SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF;
        WHILE(iScheduled_Off_Count != 0)
        DO
                SET iFlag_SDT = 1;
                select FromTime,ToTime into LinkFromTime,LinkToTime from SCHED_TIME_OFF limit 1;
                IF(LinkFromTime IS NOT NULL AND LinkToTime IS NOT NULL)
                THEN
                        SELECT CONCAT(SDT_Time,"(",LinkFromTime," - ",LinkToTime,")") into SDT_Time from dual;
                        DELETE FROM TRAP_DOWN_TBL where rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
                        DELETE FROM TRAP_UP_TBL where rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
                END IF;

                DELETE FROM SCHED_TIME_OFF limit 1;
                SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF;
        END WHILE;

        SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
        SET iTimeSpan = TIMESTAMPDIFF(SECOND, p_StartTime, p_EndTime);

	IF(iCount = 0)
        THEN
                SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL limit 1;
                IF(upTime IS NULL)
                THEN
                        SELECT OperStatus into iOperStatus from LINK_T_TBL
                                WHERE LinkID = p_LinkId
                                AND trapType = 2      /* trapType 2 is TrapLink*/
                                ORDER BY rcvd_time_1 desc limit 1;
                        IF(iOperStatus = 'Down')
			THEN
                                SET iNoOfOutage = iNoOfOutage + 1;
                                SET totalDownTime = iTimeSpan;
                        END IF;
                ELSE
                        SET iNoOfOutage = iNoOfOutage + 1;
                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, p_StartTime, upTime);
                END IF;
        ELSE

        	SELECT rcvd_Time_1 INTO downTime FROM TRAP_DOWN_TBL limit 1;
		SET upTime = NULL;
                SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL where rcvd_Time_1 < downTime order by rcvd_Time_1 limit 1;
                IF(upTime IS NOT NULL)
                THEN
                	SET iNoOfOutage = iNoOfOutage + 1;
                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, p_StartTime, upTime);
                        DELETE FROM TRAP_UP_TBL where rcvd_Time_1 < downTime;
                END IF;
        	ruchi: WHILE(iCount != 0)
	        DO
			SET downTime = NULL;
			SET upTime = NULL;
			SELECT rcvd_Time_1 INTO downTime FROM TRAP_DOWN_TBL limit 1;
			DELETE FROM TRAP_UP_TBL where rcvd_Time_1 < downTime;
	                SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL limit 1;
	
        	        IF(upTime IS NULL)
                	THEN
				SET iNoOfOutage = iNoOfOutage + 1;
        	                SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, p_EndTime);
                	        DELETE FROM TRAP_DOWN_TBL limit 1;
				Leave ruchi;
	                ELSE
				SET iNoOfOutage = iNoOfOutage + 1;
                	        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, upTime);
	                        DELETE FROM TRAP_DOWN_TBL where rcvd_Time_1 <= upTime;
                	END IF;
	                SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
        	END WHILE;
	END IF;
	
	IF(iTimeSpan IS NULL)
                THEN SET iTimeSpan = 1;
        END IF;
        SET fAvailability = (100 - ((totalDownTime/iTimeSpan)*100));
	
        IF(NWH_Time = "" )
                THEN SET NWH_Time = 'NA';
        END IF;

        IF(SDT_Time = "")
                THEN SET SDT_Time = 'NA';
        END IF;
	SELECT p_LinkId,LinkName,iNoOfOutage,totalDownTime,fAvailability,NWH_Time,SDT_Time,CONCAT(p_StartTime,'-',p_EndTime) as Time
		from TEMP_LINK_NAME
		where LinkID = p_LinkId limit 1;

        DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL,SCHED_TIME_OFF;

END |

delimiter ;

