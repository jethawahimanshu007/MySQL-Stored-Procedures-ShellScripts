DELIMITER |

CREATE PROCEDURE preorder(in ID int)
BEGIN
	
	DECLARE flag int;
	DECLARE temp int;
	 SET @@SESSION.max_sp_recursion_depth = 255; 
	if ID is NULL then
	SET flag=1;
	END if;
	
	if flag is NULL then
	select data from node where NodeId=ID;
	SET temp=(select NodeIdL from node where NodeId=ID);	
	
	call preorder(temp);
	
	SET temp=(select NodeIdR from node where NodeId=ID);
	call preorder(temp);
	end if;
	
	
END |
DELIMITER ;



