drop procedure if exists spDiffServGroupLink;

\d |
CREATE PROCEDURE spDiffServGroupLink(IN startTime TIMESTAMP, IN endTime TIMESTAMP)
   BEGIN
    	DECLARE iCount INTEGER DEFAULT 0;
    	DECLARE iLinkID INTEGER DEFAULT 0;
    	DECLARE iLinks INTEGER DEFAULT 0;


select f.LinkID,f.LinkName, f.TerNodeName, g.TotalBW/1000, f.OriIfDescr, f.OriNodeName,a.CosFCName, avg(TxOctets), max(TxOctets)
from COSFC_TBL a, COSQSTAT_TBL b,ROUTERTRAFFIC_DIFFSERV_SCALE1_TBL c, NODE_TBL d , NODEIF_TBL e, TEMP_LINK_NAME f, LINK_TBL g, GROUP_LINKDIFF_TBL h
where  a.NodeNumber = b.NodeNumber and a.CosQNumber =b.QNumber and
b.QSTID =c.DiffID and a.NodeNumber = d.NodeNumber and
e.IfIndex = b.IfIndex and b.NodeNumber = e.NodeNumber and
f.OriNodeNumber = b.NodeNumber and f.OriIfIndex = b.IfIndex and g.LinkId = f.LinkId  and
Time_1 > startTime and Time_1 < endTime
and g.LinkID = h.LinkID
group by c.DiffID order by a.NodeNumber , b.IfIndex  ;
DROP TABLE IF EXISTS GROUP_LINKDIFF_TBL;

END |
