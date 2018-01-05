
delimiter |
DROP PROCEDURE IF EXISTS multiLinkAvailability ;
CREATE PROCEDURE multiLinkAvailability(IN p_StartTime VARCHAR(20), IN p_EndTime VARCHAR(20))
BEGIN
	DECLARE downTime VARCHAR(20) DEFAULT NULL;
	DECLARE upTime VARCHAR(20) DEFAULT NULL;
	DECLARE totalDownTime INTEGER default 0; 
	DECLARE iLinkID INTEGER UNSIGNED DEFAULT NULL;
	DECLARE iNextLinkID INTEGER DEFAULT 0;
	DECLARE iTimeSpan INTEGER DEFAULT 0;
	DECLARE fAvailability FLOAT(12,4) DEFAULT NULL;
	DECLARE iOperStatus VARCHAR(5) DEFAULT NULL;
	DECLARE iCount INTEGER DEFAULT 0;
	DECLARE iNoOfOutage INTEGER DEFAULT 0;
	DECLARE ifirstUpFlag INTEGER DEFAULT 1;
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
	DECLARE p_LinkID INTEGER DEFAULT 0;
	DECLARE cSDT_Time VARCHAR(100) DEFAULT "";
	DECLARE cNWH_Time VARCHAR(100) DEFAULT "";
	
	DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL, LINK_AVAIL_TBL,SCHED_TIME_OFF;
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

	CREATE TEMPORARY TABLE LINK_AVAIL_TBL (
		LinkID INTEGER(10) NOT NULL,
		NoOfOutage INTEGER DEFAULT 0,
		TotalOutage INTEGER DEFAULT 0,
		Availabilty Float(12,4) DEFAULT NULL,
		NWH_Time VARCHAR(100),
		SDT_Time VARCHAR(100),
		Time VARCHAR(100));

	CREATE TEMPORARY TABLE SCHED_TIME_OFF LIKE SCHEDULED_DOWN_TIME;

	INSERT INTO TRAP_DOWN_TBL
		SELECT T.LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL T,TEMP_LINK L
			WHERE trapType = 2 	/* trapType 2 is TrapLink */
			AND T.LinkID = L.LinkID
			AND rcvd_Time_1 > p_StartTime
			AND rcvd_Time_1 <= p_EndTime
			AND OperStatus = 'Down'
			ORDER BY LinkID,rcvd_time_1;

	INSERT INTO TRAP_UP_TBL
		SELECT T.LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL T,TEMP_LINK L
	        	WHERE trapType = 2 	/* trapType 2 is TrapLink*/
			AND T.LinkID = L.LinkID
		        AND rcvd_Time_1 > p_StartTime
        		AND rcvd_Time_1 <= p_EndTime
			AND OperStatus = 'Up'
        		ORDER BY LinkID,rcvd_time_1;

        INSERT INTO LINK_AVAIL_TBL(LinkID,Time)
                SELECT LinkID,CONCAT(p_StartTime,'-',p_EndTime) from TEMP_LINK;

	SELECT count(*) into iCount from TEMP_LINK;
	while(iCount != 0)
	DO
		select LinkID into p_LinkID from TEMP_LINK limit 1;
		
		SELECT OriNodeNumber,TerNodeNumber into iOriNodeNumber,iTerNodeNumber from LINK_TBL where LinkID = p_LinkId limit 1;

        	select count(*) into iOff_Count from NODE_NON_WORKING_HOUR
                	where nodeid IN (select Nodeid from NODE_TBL where NodeNumber = iOriNodeNumber OR NodeNumber = iTerNodeNumber);

	        IF(iOff_Count != 0)
        	THEN
                	SET iFlag_NWH = 1;
	                select FromTime,ToTime into OriFromTime,OriToTime from NODE_NON_WORKING_HOUR
        	                where NodeID = (select NodeId from NODE_TBL where NodeNumber = iOriNodeNumber) limit 1;
                	select FromTime,ToTime into TerFromTime,TerToTime from NODE_NON_WORKING_HOUR
                        	where NodeID = (select NodeId from NODE_TBL where NodeNumber = iTerNodeNumber) limit 1;
	                IF(OriFromTime IS NOT NULL AND OriToTime IS NOT NULL)
	                THEN
        	                SELECT CONCAT(cNWH_Time,"(",OriFromTime,"-",OriToTime,")") into cNWH_Time;
                	        DELETE FROM TRAP_DOWN_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
                        	DELETE FROM TRAP_UP_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
	                END IF;
        	        IF(TerFromTime IS NOT NULL AND TerToTime IS NOT NULL)
                	THEN
                        	SELECT CONCAT(cNWH_Time,"(",TerFromTime,"-",TerToTime,")") into cNWH_Time;
	                        DELETE FROM TRAP_DOWN_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
        	                DELETE FROM TRAP_UP_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
                	END IF;
        	END IF;

		INSERT INTO SCHED_TIME_OFF (SELECT * from SCHEDULED_DOWN_TIME where LinkID = p_LinkId);
	        SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF ;
       	        WHILE(iScheduled_Off_Count != 0)
	        DO
        	        SET iFlag_SDT = 1;
                	select FromTime,ToTime into LinkFromTime,LinkToTime from SCHED_TIME_OFF limit 1;
	                IF(LinkFromTime IS NOT NULL AND LinkToTime IS NOT NULL)
        	        THEN
                	        SELECT CONCAT(cSDT_Time,"(",LinkFromTime," - ",LinkToTime,")") into cSDT_Time;
                        	DELETE FROM TRAP_DOWN_TBL where LinkID = p_LinkId and rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
	                        DELETE FROM TRAP_UP_TBL where LinkID = p_LinkId and rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
        	        END IF;
	
        	        DELETE FROM SCHED_TIME_OFF limit 1;
                	SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF;
	        END WHILE;

                IF(cNWH_Time = "")
                        THEN SET cNWH_Time = 'NA';
                END IF;

                IF(cSDT_Time = "")
                        THEN SET cSDT_Time = 'NA';
                END IF;
	
		UPDATE LINK_AVAIL_TBL
	                SET NWH_Time = cNWH_Time,
			SDT_Time = cSDT_Time
			WHERE LinkID = p_LinkId;

		SET LinkFromTime = NULL;
		SET LinkToTime = NULL;
		SET OriFromTime = NULL;
		SET OriToTime = NULL;
		SET TerFromTime = NULL;
		SET TerToTime = NULL;
                SET iFlag_NWH = 0;
                SET iScheduled_Off_Count = 0;
                SET iOff_Count = 0;
		SET cNWH_Time = "";
		SET cSDT_Time = "";

		DELETE from TEMP_LINK limit 1;
		SELECT count(*) into iCount from TEMP_LINK;
	END WHILE;

	SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
	SET iTimeSpan = TIMESTAMPDIFF(SECOND, p_StartTime, p_EndTime); 

     IF(iCount = 0)
     THEN
		SET iCount = 1;
		while(iCount != 0)
		DO
			SET upTime = NULL;
	                SELECT LinkID,rcvd_Time_1 INTO iLinkID,upTime FROM TRAP_UP_TBL limit 1;
        	        IF(upTime IS NULL)
                	THEN
				SELECT OperStatus into iOperStatus from LINK_T_TBL 
				WHERE LinkID = iLinkID
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
	
			SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
                	UPDATE LINK_AVAIL_TBL
                                SET Availabilty=fAvailability,
                                NoOfOutage=iNoOfOutage,
                                TotalOutage=totalDownTime
                                WHERE LinkID = iLinkID;
		
			DELETE FROM TRAP_UP_TBL where LinkID = iLinkId;
			SELECT count(*) into iCount FROM TRAP_UP_TBL;
		END WHILE;

     ELSE	
	WHILE(iCount != 0)
	DO
		SET iLinkID = NULL;
		SET downTime = NULL;
		SET iNextLinkID = 0;
		SELECT LinkID,rcvd_Time_1 INTO iLinkID,downTime FROM TRAP_DOWN_TBL limit 1;
		SET upTime = NULL;
		IF(ifirstUpFlag = 0)
		THEN
			DELETE FROM TRAP_UP_TBL where LinkID = iLinkID and rcvd_Time_1 <= downTime;
		ELSE
			SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL where LinkID = iLinkID and rcvd_Time_1 < downTime order by rcvd_Time_1 limit 1;
                	IF(upTime IS NOT NULL)
                	THEN
                		SET iNoOfOutage = iNoOfOutage + 1;
	                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, p_StartTime, upTime);
        	                DELETE FROM TRAP_UP_TBL where LinkID = iLinkID and rcvd_Time_1 <= downTime;
                	END IF;
			SET ifirstUpFlag = 0;
		END IF;
                SET upTime = NULL;
		SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL where LinkID = iLinkID limit 1;

		IF(upTime IS NULL)
		THEN
			SET iNoOfOutage = iNoOfOutage + 1;
			SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, p_EndTime);
			DELETE FROM TRAP_DOWN_TBL where LinkID = iLinkID and rcvd_Time_1 = downTime;
		ELSE 
			SET iNoOfOutage = iNoOfOutage + 1;
			SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, upTime);

			DELETE FROM TRAP_DOWN_TBL where LinkID = iLinkID and rcvd_Time_1 <= upTime;
		END IF;
		SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
		SELECT LinkID INTO iNextLinkID FROM TRAP_DOWN_TBL limit 1;
		IF(iLinkID != iNextLinkID)
                THEN
                        SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
        		UPDATE LINK_AVAIL_TBL 
				SET Availabilty=fAvailability,
				NoOfOutage=iNoOfOutage,
				TotalOutage=totalDownTime 
				WHERE LinkID = iLinkID;
                        SET fAvailability = NULL;
                        SET totalDownTime = 0;
			SET iNoOfOutage = 0;
			DELETE FROM TRAP_DOWN_TBL where LinkID = iLinkID;
			DELETE FROM TRAP_UP_TBL where LinkID = iLinkID;
                END IF;
	END WHILE;
		
	IF(iLinkID != NULL) THEN	
		SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
        	UPDATE LINK_AVAIL_TBL 
                                SET Availabilty=fAvailability,
                                NoOfOutage=iNoOfOutage,
                                TotalOutage=totalDownTime 
                                WHERE LinkID = iLinkID;	
	END IF;
     END IF;
	select count(*) into iCount from LINK_AVAIL_TBL where Availabilty IS NULL;
        while(iCount != 0)
                DO
			SET iNoOfOutage = 0;
			SET totalDownTime = 0;
			SET iLinkID = 0;
			set fAvailability = 0;
			SET iOperStatus = NULL;
			SET downTime = NULL;
                        SELECT LinkID into iLinkID from LINK_AVAIL_TBL where Availabilty IS NULL limit 1;

                        SELECT OperStatus,rcvd_time_1 into iOperStatus,downTime from LINK_T_TBL
                                WHERE LinkID = iLinkId
                                AND trapType = 2      /* trapType 2 is TrapLink*/
                                ORDER BY rcvd_time_1 desc limit 1;

                        IF(iOperStatus = 'Down')
                                THEN
                                        SET iNoOfOutage = iNoOfOutage + 1;
					IF(downTime <= p_StartTime)
					THEN
                                        	SET totalDownTime = iTimeSpan;
					ELSE
						SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, p_EndTime);
					END IF;
                                END IF;
                        SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
                        UPDATE LINK_AVAIL_TBL
                        SET Availabilty=fAvailability,
                        NoOfOutage=iNoOfOutage,
                        TotalOutage=totalDownTime
                        WHERE LinkID = iLinkID;
                SET iCount = 0;
		select count(*) into iCount from LINK_AVAIL_TBL where Availabilty IS NULL;
        END WHILE;
	SELECT a.LinkID,a.LinkName,NoOfOutage,TotalOutage,Availabilty,NWH_Time,SDT_Time,Time FROM LINK_AVAIL_TBL b,TEMP_LINK_NAME a where a.LinkID = b.LinkID;
	
	DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL,LINK_AVAIL_TBL,SCHED_TIME_OFF;
	DROP TABLE IF EXISTS TEMP_LINK;

END |
DROP PROCEDURE IF EXISTS multiLinkAvail_Last24Hour ;
CREATE PROCEDURE multiLinkAvail_Last24Hour()
BEGIN
	DECLARE p_StartTime VARCHAR(20) DEFAULT NULL;
	DECLARE p_EndTime VARCHAR(20) DEFAULT NULL;
        DECLARE downTime VARCHAR(20) DEFAULT NULL;
        DECLARE upTime VARCHAR(20) DEFAULT NULL;
        DECLARE totalDownTime INTEGER default 0;
        DECLARE iLinkID INTEGER UNSIGNED DEFAULT NULL;
        DECLARE iNextLinkID INTEGER default 0;
        DECLARE iTimeSpan INTEGER DEFAULT 0;
        DECLARE fAvailability FLOAT(12,4) DEFAULT NULL;
	DECLARE iOperStatus VARCHAR(5) DEFAULT NULL;
        DECLARE iCount INTEGER DEFAULT 0;
	DECLARE iNoOfOutage INTEGER DEFAULT 0;
	DECLARE ifirstUpFlag INTEGER DEFAULT 1;
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
	DECLARE p_LinkID INTEGER DEFAULT 0;
        DECLARE cSDT_Time VARCHAR(100) DEFAULT "";
        DECLARE cNWH_Time VARCHAR(100) DEFAULT "";
	

        DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL, LINK_AVAIL_TBL,SCHED_TIME_OFF;
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

        CREATE TEMPORARY TABLE LINK_AVAIL_TBL (
                LinkID INTEGER(10) NOT NULL,
		NoOfOutage INTEGER DEFAULT 0,
                TotalOutage INTEGER DEFAULT 0,
                Availabilty Float(12,4) DEFAULT NULL,
		NWH_Time VARCHAR(100),
                SDT_Time VARCHAR(100),
		Time VARCHAR(100));

	CREATE TEMPORARY TABLE SCHED_TIME_OFF LIKE SCHEDULED_DOWN_TIME;

	SELECT rcvd_Time_1 into p_EndTime from TRAP_TBL where trapid = (SELECT max(trapid) from TRAP_TBL) limit 1;
	SET p_StartTime = DATE_SUB(p_EndTime, INTERVAL 1 DAY);

        INSERT INTO TRAP_DOWN_TBL
                SELECT T.LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL T,TEMP_LINK L
                        WHERE trapType = 2      /* trapType 2 is TrapLink */
			AND T.LinkID = L.LinkID
                        AND rcvd_Time_1 > p_StartTime
                        AND rcvd_Time_1 <= p_EndTime
                        AND OperStatus = 'Down'
                        ORDER BY LinkID,rcvd_time_1;
        INSERT INTO TRAP_UP_TBL
                SELECT T.LinkId,OperStatus,rcvd_Time_1 FROM LINK_T_TBL T,TEMP_LINK L
                        WHERE trapType = 2      /* trapType 2 is TrapLink*/
			AND T.LinkID = L.LinkID
                        AND rcvd_Time_1 > p_StartTime
                        AND rcvd_Time_1 <= p_EndTime
                        AND OperStatus = 'Up'
                        ORDER BY LinkID,rcvd_time_1;

        INSERT INTO LINK_AVAIL_TBL(LinkID,Time)
                SELECT LinkID,CONCAT(p_StartTime,'-',p_EndTime) from TEMP_LINK;

        SELECT count(*) into iCount from TEMP_LINK;
        while(iCount != 0)
        DO
                select LinkID into p_LinkID from TEMP_LINK limit 1;

                SELECT OriNodeNumber,TerNodeNumber into iOriNodeNumber,iTerNodeNumber from LINK_TBL where LinkID = p_LinkId limit 1;

                select count(*) into iOff_Count from NODE_NON_WORKING_HOUR
                        where nodeid IN (select Nodeid from NODE_TBL where NodeNumber = iOriNodeNumber OR NodeNumber = iTerNodeNumber);

                IF(iOff_Count != 0)
                THEN
                        SET iFlag_NWH = 1;
                        select FromTime,ToTime into OriFromTime,OriToTime from NODE_NON_WORKING_HOUR
                                where NodeID = (select NodeId from NODE_TBL where NodeNumber = iOriNodeNumber) limit 1;
                        select FromTime,ToTime into TerFromTime,TerToTime from NODE_NON_WORKING_HOUR
                                where NodeID = (select NodeId from NODE_TBL where NodeNumber = iTerNodeNumber) limit 1;
                        IF(OriFromTime IS NOT NULL AND OriToTime IS NOT NULL)
	                THEN
        	                SELECT CONCAT(cNWH_Time,"(",OriFromTime,"-",OriToTime,")") into cNWH_Time;
                	        DELETE FROM TRAP_DOWN_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
                                DELETE FROM TRAP_UP_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= OriFromTime and TIME(rcvd_Time_1) <= OriToTime;
                        END IF;
                        IF(TerFromTime IS NOT NULL AND TerToTime IS NOT NULL)
                        THEN
                                SELECT CONCAT(cNWH_Time,"(",TerFromTime,"-",TerToTime,")") into cNWH_Time;
                                DELETE FROM TRAP_DOWN_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime; 
                                DELETE FROM TRAP_UP_TBL where LinkID = p_LinkId and TIME(rcvd_Time_1) >= TerFromTime and TIME(rcvd_Time_1) <= TerToTime;
                        END IF;
                END IF;

                INSERT INTO SCHED_TIME_OFF (SELECT * from SCHEDULED_DOWN_TIME where LinkID = p_LinkId);
                SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF ;
                WHILE(iScheduled_Off_Count != 0)
                DO
                        SET iFlag_SDT = 1;
                        select FromTime,ToTime into LinkFromTime,LinkToTime from SCHED_TIME_OFF limit 1;
                        IF(LinkFromTime IS NOT NULL AND LinkToTime IS NOT NULL)
                        THEN
                                SELECT CONCAT(cSDT_Time,"(",LinkFromTime," - ",LinkToTime,")") into cSDT_Time;
                                DELETE FROM TRAP_DOWN_TBL where LinkID = p_LinkId and rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime limit 1;
                                DELETE FROM TRAP_UP_TBL where LinkID = p_LinkId and rcvd_Time_1 >= LinkFromTime and rcvd_Time_1 <= LinkToTime;
                        END IF;

                        DELETE FROM SCHED_TIME_OFF limit 1;
                        SELECT count(*) into iScheduled_Off_Count from SCHED_TIME_OFF;
                END WHILE;
		
	        IF(cNWH_Time  = "")
	                THEN SET cNWH_Time = 'NA';
        	END IF;

	        IF(cSDT_Time = "")
        	        THEN SET cSDT_Time = 'NA';
	        END IF;

                UPDATE LINK_AVAIL_TBL
                        SET NWH_Time = cNWH_Time,
                        SDT_Time = cSDT_Time
                        WHERE LinkID = p_LinkId;

                SET LinkFromTime = NULL;
                SET LinkToTime = NULL;
                SET OriFromTime = NULL;
                SET OriToTime = NULL;
                SET TerFromTime = NULL;
                SET TerToTime = NULL;
		SET cSDT_Time = "";
		SET cNWH_Time = "";
		SET iFlag_NWH = 0;
		SET iScheduled_Off_Count = 0;
		SET iOff_Count = 0;

                DELETE from TEMP_LINK limit 1;
                SELECT count(*) into iCount from TEMP_LINK;
        END WHILE;

        SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
        SET iTimeSpan = TIMESTAMPDIFF(SECOND, p_StartTime, p_EndTime);
     IF(iCount = 0)
     THEN
                SET iCount = 1;
                while(iCount != 0)
                DO
			SET upTime = NULL;
                        SELECT LinkID,rcvd_Time_1 INTO iLinkID,upTime FROM TRAP_UP_TBL limit 1;
                        IF(upTime IS NULL)
                        THEN
				SELECT OperStatus into iOperStatus from LINK_T_TBL 
				WHERE LinkID = iLinkID
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

                        SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
                        UPDATE LINK_AVAIL_TBL
                                SET Availabilty=fAvailability,
                                NoOfOutage=iNoOfOutage,
                                TotalOutage=totalDownTime
                                WHERE LinkID = iLinkID;

                        DELETE FROM TRAP_UP_TBL where LinkID = iLinkId;
                        SELECT count(*) into iCount FROM TRAP_UP_TBL;
                END WHILE;

     ELSE

        WHILE(iCount != 0)
        DO
		SET iLinkID = NULL;
		SET downTime = NULL;
		SET iNextLinkID = 0;
                SELECT LinkID,rcvd_Time_1 INTO iLinkID,downTime FROM TRAP_DOWN_TBL limit 1;
		SET upTime = NULL;
		IF(ifirstUpFlag = 0)
		THEN 
			DELETE FROM TRAP_UP_TBL where LinkID = iLinkID and rcvd_Time_1 <= downTime;
		ELSE
			SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL where LinkID = iLinkID and rcvd_Time_1 < downTime order by rcvd_Time_1 limit 1;
                	IF(upTime IS NOT NULL)
                	THEN
                       		SET iNoOfOutage = iNoOfOutage + 1;
	                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, p_StartTime, upTime);
        	                DELETE FROM TRAP_UP_TBL where LinkID = iLinkID and rcvd_Time_1 <= downTime;
                	END IF;
			SET ifirstUpFlag = 0;
		END IF;
                SET upTime = NULL;
                SELECT rcvd_Time_1 INTO upTime FROM TRAP_UP_TBL where LinkID = iLinkID limit 1;

                IF(upTime IS NULL)
                THEN
			SET iNoOfOutage = iNoOfOutage + 1;
                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, p_EndTime);
                        DELETE FROM TRAP_DOWN_TBL where LinkID = iLinkID;
                ELSE
			SET iNoOfOutage = iNoOfOutage + 1;
                        SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, upTime);
                        DELETE FROM TRAP_DOWN_TBL where LinkID = iLinkID and rcvd_Time_1 <= upTime;
                END IF;
                SELECT count(*) into iCount FROM TRAP_DOWN_TBL;
                SELECT LinkID INTO iNextLinkID FROM TRAP_DOWN_TBL limit 1;
                IF(iLinkID != iNextLinkID)
                THEN
                        SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
                	UPDATE LINK_AVAIL_TBL
                                SET Availabilty=fAvailability,
                                NoOfOutage=iNoOfOutage,
                                TotalOutage=totalDownTime
                                WHERE LinkID = iLinkID;
	
		        SET fAvailability = NULL;
                        SET totalDownTime = 0;
                	SET iNoOfOutage = 0;
			DELETE FROM TRAP_DOWN_TBL where LinkID = iLinkID;
			DELETE FROM TRAP_UP_TBL where LinkID = iLinkID;
		END IF;
        END WHILE;
    
        IF(iLinkID != NULL) THEN
                SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
		UPDATE LINK_AVAIL_TBL
			SET Availabilty=fAvailability,
                        NoOfOutage=iNoOfOutage,
                        TotalOutage=totalDownTime
                        WHERE LinkID = iLinkID;

	END IF;
    END IF;
	SET iCount = 0;
	select count(*) into iCount from LINK_AVAIL_TBL where Availabilty IS NULL;
        while(iCount != 0)
                DO
			SET iNoOfOutage = 0;
			SET totalDownTime = 0;
			SET iLinkID = 0;
			SET fAvailability = 0;
			SET iOperStatus = NULL;
			SET downTime = NULL;
                        SELECT LinkID into iLinkID from LINK_AVAIL_TBL where Availabilty IS NULL limit 1;

                        SELECT OperStatus,rcvd_time_1 into iOperStatus,downTime from LINK_T_TBL
                                WHERE LinkID = iLinkId
                                AND trapType = 2      /* trapType 2 is TrapLink*/
                                ORDER BY rcvd_time_1 desc limit 1;

                        IF(iOperStatus = 'Down')
                                THEN
                                        SET iNoOfOutage = iNoOfOutage + 1;
					IF(downTime <= p_StartTime)
                                        THEN
                                                SET totalDownTime = iTimeSpan;
                                        ELSE
                                                SET totalDownTime = totalDownTime + TIMESTAMPDIFF(SECOND, downTime, p_EndTime);
                                        END IF;
                                END IF;
                        SET fAvailability = (100 - (totalDownTime/iTimeSpan)*100);
                        UPDATE LINK_AVAIL_TBL
                        SET Availabilty=fAvailability,
                        NoOfOutage=iNoOfOutage,
                        TotalOutage=totalDownTime
                        WHERE LinkID = iLinkID;
		SET iCount = 0;
                select count(*) into iCount from LINK_AVAIL_TBL where Availabilty IS NULL;
        END WHILE;

        SELECT a.LinkID,a.LinkName,NoOfOutage,TotalOutage, Availabilty,NWH_Time,SDT_Time,Time FROM LINK_AVAIL_TBL b,TEMP_LINK_NAME a where a.LinkID = b.LinkID;

        DROP TEMPORARY TABLE IF EXISTS TRAP_DOWN_TBL, TRAP_UP_TBL,LINK_AVAIL_TBL,SCHED_TIME_OFF ;
	DROP TABLE IF EXISTS TEMP_LINK;
END |
delimiter ;
