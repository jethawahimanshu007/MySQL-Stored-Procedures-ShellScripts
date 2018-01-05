drop procedure if exists spDiffServGroupLink;

\d |
CREATE PROCEDURE spDiffServGroupLink_24Hr()
BEGIN
    	DECLARE iCount INTEGER DEFAULT 0;
    	DECLARE iLinkID INTEGER DEFAULT 0;
    	DECLARE iLinks INTEGER DEFAULT 0;
	DECLARE p_StartTime VARCHAR(20) DEFAULT NULL;
	DECLARE p_EndTime VARCHAR(20) DEFAULT NULL;

/* this is for testing */
	DROP TABLE IF EXISTS GROUP_LINKDIFF_TBL;
	CREATE TABLE GROUP_LINKDIFF_TBL(LinkId INTEGER UNSIGNED);

	INSERT INTO GROUP_LINKDIFF_TBL (LinkId) values (3104);
	INSERT INTO GROUP_LINKDIFF_TBL (LinkId) values (3105);

/* remove up to this line */
	SELECT max(Time_1) into p_EndTime from ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL;
	SET p_StartTime = DATE_SUB(p_EndTime, INTERVAL 1 DAY);

select f.LinkName, f.TerNodeName, g.TotalBW/1000, f.OriIfDescr, f.OriNodeName,a.CosFCName, avg(TxOctets), max(TxOctets),CONCAT(p_StartTime,'-',p_EndTime) as Time
from COSFC_TBL a, COSQSTAT_TBL b,ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL c, NODE_TBL d , NODEIF_TBL e, TEMP_LINK_NAME f, LINK_TBL g, GROUP_LINKDIFF_TBL h
where  a.NodeNumber = b.NodeNumber and a.CosQNumber =b.QNumber and
b.QSTID =c.DiffID and a.NodeNumber = d.NodeNumber and
e.IfIndex = b.IfIndex and b.NodeNumber = e.NodeNumber and
f.OriNodeNumber = b.NodeNumber and f.OriIfIndex = b.IfIndex and g.LinkId = f.LinkId  and
Time_1 > p_StartTime and Time_1 < p_EndTime
and g.LinkID = h.LinkID
group by c.DiffID order by a.NodeNumber , b.IfIndex  ;

END |
