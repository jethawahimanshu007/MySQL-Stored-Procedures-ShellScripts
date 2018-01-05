DROP PROCEDURE IF EXISTS addNonWorkingHour;
delimiter |

CREATE PROCEDURE addNonWorkingHour(p_NodeIP VARCHAR(50),p_FromTime TIME,p_ToTime TIME)
BEGIN

	DECLARE iCount INTEGER DEFAULT 0;
	
	SELECT count(*) into iCount from NODE_NON_WORKING_HOUR where NodeID = p_NodeIP;

	IF(iCount = 0)
	THEN 
		INSERT INTO NODE_NON_WORKING_HOUR VALUES(p_NodeIP,p_FromTime,p_ToTime);
	ELSE
		UPDATE NODE_NON_WORKING_HOUR
			SET FromTime = p_FromTime,
			ToTime = p_ToTime
			WHERE NodeID = p_NodeIP;
	END IF;
END |
delimiter |
