DELIMITER |

CREATE PROCEDURE postorder(in ID int)
BEGIN
	
	DECLARE flag int;
	DECLARE temp int;
	 SET @@SESSION.max_sp_recursion_depth = 255; 
	if ID is NULL then
	SET flag=1;
	END if;
	
	if flag is NULL then
	
	SET temp=(select NodeIdL from node where NodeId=ID);	
	
	call postorder(temp);
	
	SET temp=(select NodeIdR from node where NodeId=ID);
	call postorder(temp);
	select data from node where NodeId=ID;
	end if;
	
	
END |
DELIMITER ;



