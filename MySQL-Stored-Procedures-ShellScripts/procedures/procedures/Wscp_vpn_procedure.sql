delimiter |
DROP PROCEDURE IF EXISTS getLinkInfoOnVpn|
CREATE PROCEDURE getLinkInfoOnVpn( IN p_VPNName VARCHAR(128) )
BEGIN
DROP TABLE IF EXISTS TEMP_VPN_MAP;
CREATE TABLE TEMP_VPN_MAP(OriNodeNumber INTEGER(11) DEFAULT NULL,
	OriIfIndex INTEGER(11) default NULL, 
	TerNodeNumber INTEGER(11) default NULL, 
	TerIfIndex INTEGER(11) default NULL, 
	LinkID INTEGER(11) default NULL, 
	vpniD INTEGER(11) default NULL 
	); 
INSERT INTO TEMP_VPN_MAP (
	SELECT OriNodeNumber, OriIfIndex, TerNodeNumber, TerIfIndex, l.LinkID, v.vpniD
	FROM VPN_IFINDEX_TBL vii, VPN_TBL v,LINK_TBL l
	WHERE  	v.VPNName = p_VPNName AND
 		v.VpnID = vii.VpnID AND
		vii.VPNNode = l.OriNodeNumber AND
		vii.VpnIfIndex = l.OriIfIndex);
SELECT Ori.NodeName, O.IfDescr, Ter.NodeName, T.IfDescr, LinkID
	FROM TEMP_VPN_MAP tv, NODE_TBL Ori, NODE_TBL Ter, NODEIF_TBL O, NODEIF_TBL T
	WHERE 	tv.OriNodeNumber = Ori.NodeNumber AND 
		tv.TerNodeNumber = Ter.NodeNumber AND 
		tv.OriNodeNumber = O.NodeNumber AND
		tv.OriIfIndex = O.IfIndex AND
		tv.TerNodeNumber = T.NodeNumber AND
		tv.TerIfIndex = T.IfIndex;

END|
DROP PROCEDURE IF EXISTS getTrapsOnVpn|
CREATE PROCEDURE getTrapsOnVpn( IN p_VPNName VARCHAR(128) )
BEGIN
DROP TABLE IF EXISTS TEMP_VPN_MAP;
CREATE TABLE TEMP_VPN_MAP
        (LinkID INTEGER(11) default NULL
        );
INSERT INTO TEMP_VPN_MAP (
        SELECT l.LinkID
        FROM VPN_IFINDEX_TBL vii, VPN_TBL v,LINK_TBL l
        WHERE   v.VPNName = p_VPNName AND
                v.VpnID = vii.VpnID AND
                vii.VPNNode = l.OriNodeNumber AND
                vii.VpnIfIndex = l.OriIfIndex);
SELECT trapID,trapType,SenderIP,NodeName,NodeType,Status,generated_Time,rcvd_Time_1,ack_Time_1, \
	ack_message,AckUserName,clear_Time_1,clear_message,ClearUserName,LspName,PathName,IfIndex, \
	AdminStatus,OperStatus,t.LinkId,OSPFStatus,OSPFRouterIP,OSPFIfIP,OSPFAddrLessIf,OSPFNbrRtrIP, \
	OSPFNbrIfIP,AlarmRiseThreshold,AlarmFallThreshold,AlarmValue,AlarmMonitorOid,BgpErrDesc, \
	BgpState,MiscTrapDesc,RelatedTrapId,TrapSeverity,trapSource,AlarmEntity,RttMonResult,RttMonTargetIP 
	FROM TRAP_TBL t, TEMP_VPN_MAP tv 
	WHERE t.LinkID = tv.LinkID;
END|
DROP PROCEDURE IF EXISTS get2HrTrapsOnVpn|
CREATE PROCEDURE get2HrTrapsOnVpn( IN p_VPNName VARCHAR(128) )
BEGIN
DROP TABLE IF EXISTS TEMP_VPN_MAP;
CREATE TABLE TEMP_VPN_MAP
        (LinkID INTEGER(11) default NULL
        );
SELECT DATE_SUB(rcvd_Time_1, INTERVAL 2 HOUR) into @Time from TRAP_TBL order by rcvd_Time_1 desc limit 1;
INSERT INTO TEMP_VPN_MAP (
        SELECT l.LinkID
        FROM VPN_IFINDEX_TBL vii, VPN_TBL v,LINK_TBL l
        WHERE   v.VPNName = p_VPNName AND
                v.VpnID = vii.VpnID AND
                vii.VPNNode = l.OriNodeNumber AND
                vii.VpnIfIndex = l.OriIfIndex);
SELECT trapID,trapType,SenderIP,NodeName,NodeType,Status,generated_Time,rcvd_Time_1,ack_Time_1, \
        ack_message,AckUserName,clear_Time_1,clear_message,ClearUserName,LspName,PathName,IfIndex, \
        AdminStatus,OperStatus,t.LinkId,OSPFStatus,OSPFRouterIP,OSPFIfIP,OSPFAddrLessIf,OSPFNbrRtrIP, \
        OSPFNbrIfIP,AlarmRiseThreshold,AlarmFallThreshold,AlarmValue,AlarmMonitorOid,BgpErrDesc, \
        BgpState,MiscTrapDesc,RelatedTrapId,TrapSeverity,trapSource,AlarmEntity,RttMonResult,RttMonTargetIP
        FROM TRAP_TBL t, TEMP_VPN_MAP tv
        WHERE t.LinkID = tv.LinkID
		AND rcvd_Time_1 > @Time;
END|
DROP PROCEDURE IF EXISTS get24HrTrapsOnVpn|
CREATE PROCEDURE get24HrTrapsOnVpn( IN p_VPNName VARCHAR(128) )
BEGIN
DROP TABLE IF EXISTS TEMP_VPN_MAP;
CREATE TABLE TEMP_VPN_MAP
        (LinkID INTEGER(11) default NULL
        );
SELECT DATE_SUB(rcvd_Time_1, INTERVAL 24 HOUR) into @Time from TRAP_TBL order by rcvd_Time_1 desc limit 1;
INSERT INTO TEMP_VPN_MAP (
        SELECT l.LinkID
        FROM VPN_IFINDEX_TBL vii, VPN_TBL v,LINK_TBL l
        WHERE   v.VPNName = p_VPNName AND
                v.VpnID = vii.VpnID AND
                vii.VPNNode = l.OriNodeNumber AND
                vii.VpnIfIndex = l.OriIfIndex);
SELECT trapID,trapType,SenderIP,NodeName,NodeType,Status,generated_Time,rcvd_Time_1,ack_Time_1, \
        ack_message,AckUserName,clear_Time_1,clear_message,ClearUserName,LspName,PathName,IfIndex, \
        AdminStatus,OperStatus,t.LinkId,OSPFStatus,OSPFRouterIP,OSPFIfIP,OSPFAddrLessIf,OSPFNbrRtrIP, \
        OSPFNbrIfIP,AlarmRiseThreshold,AlarmFallThreshold,AlarmValue,AlarmMonitorOid,BgpErrDesc, \
        BgpState,MiscTrapDesc,RelatedTrapId,TrapSeverity,trapSource,AlarmEntity,RttMonResult,RttMonTargetIP
        FROM TRAP_TBL t, TEMP_VPN_MAP tv
        WHERE t.LinkID = tv.LinkID
                AND rcvd_Time_1 > @Time;
END|


delimiter ;
