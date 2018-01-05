DELIMITER |

CREATE PROCEDURE postorder3(in data1 int)
BEGIN
	
	DECLARE flag int;
	DECLARE temp int;
	DECLARE temp1 int;
	DECLARE ID int;
	 SET @@SESSION.max_sp_recursion_depth = 255; 
	SET ID=(SELECT NodeId from node where data=data1);
	if ID is NULL then
	SET flag=1;
	END if;
	
	if flag is NULL then
	
	SET temp=(select NodeIdL from node where NodeId=ID);	
	SET temp1=(SELECT data from node where NodeId=temp);
	call postorder3(temp1);
	
	SET temp=(select NodeIdR from node where NodeId=ID);
	SET temp1=(SELECT data from node where NodeId=temp);
	call postorder3(temp1);
	select data from node where NodeId=ID;
	
	end if;
	
	
END |
DELIMITER ;



