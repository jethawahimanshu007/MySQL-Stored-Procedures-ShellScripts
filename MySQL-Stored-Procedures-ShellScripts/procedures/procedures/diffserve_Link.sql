drop procedure if exists diffServeLink;

\d |
CREATE PROCEDURE diffServeLink()
BEGIN
        DECLARE iCount INTEGER DEFAULT 0;
        DECLARE iLinkID INTEGER DEFAULT 0;
        DECLARE iLinks INTEGER DEFAULT 0;

        DROP TABLE IF EXISTS LINKDIFF_TBL;
        CREATE TABLE LINKDIFF_TBL(LinkId INTEGER UNSIGNED,
				   LinkName VARCHAR(100),
				   CosFCName varchar(255),
				   DiffID INTEGER);

Insert into LINKDIFF_TBL
select f.LinkId, f.LinkName,a.CosFCName,b.QSTID as DiffID
from COSFC_TBL a, COSQSTAT_TBL b,TEMP_LINK_NAME f, LINK_TBL g
where  a.NodeNumber = b.NodeNumber and a.CosQNumber =b.QNumber and
f.OriNodeNumber = b.NodeNumber and f.OriIfIndex = b.IfIndex and g.LinkId = f.LinkId;

select * from LINKDIFF_TBL;

END |

