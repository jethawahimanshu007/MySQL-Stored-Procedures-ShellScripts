drop procedure if exists spVodaLinkName;
\d |

CREATE PROCEDURE spVodaLinkName()
BEGIN

DECLARE iIndex INTEGER DEFAULT 0;
DECLARE iNodeCount INTEGER DEFAULT 0;
DECLARE Total INTEGER DEFAULT 0;
	

drop table if exists TEMP_LINK_NAME;
CREATE TABLE IF NOT EXISTS TEMP_LINK_NAME(
                              Id        SMALLINT AUTO_INCREMENT PRIMARY KEY,
  	                        LinkID          SMALLINT ,
					OriNodeNumber SMALLINT ,
					OriNodeName        VARCHAR(128),
					OriIfIndex SMALLINT,
					OriIfDescr         VARCHAR(128),
					OriIfIPAddress     VARCHAR(15),
					TerNodeNumber SMALLINT ,
                                        TerNodeName        VARCHAR(128),
					TerIfIndex SMALLINT,
					TerIfDescr         VARCHAR(128),
					TerIfIPAddress     VARCHAR(15),
					LinkName	VARCHAR(128),
                                        ConnectionType SMALLINT );

insert into TEMP_LINK_NAME(LinkID, OriNodeNumber,OriIfIndex,OriIfIPAddress,TerNodeNumber,TerIfIndex,TerIfIPAddress,ConnectionType)
SELECT LinkID, OriNodeNumber,OriIfIndex,OriIfIPAddress,TerNodeNumber,TerIfIndex,TerIfIPAddress,ConnectionType 
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
	set LinkName =  concat(OriNodeName,':',OriIfDescr,' - ', TerNodeName,':',TerIfDescr);

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
TEMP_LINK_NAME where ID = iIndex limit 1;


select linkid,linkname into @linkid,@linkname from TEMP_LINK_NAME
where ternodenumber=@orinodenumber and terIfIndex= @OriIfIndex and
orinodenumber=@ternodenumber and oriIfIndex=@TerIfIndex limit 1;

update TEMP_LINK_NAME set corespondinglinkid=@linkid where ID = iIndex;
update TEMP_LINK_NAME set correspondinglinkname=@linkname where ID = iIndex;


set iIndex = iIndex + 1;

end while;



END|

\d;
