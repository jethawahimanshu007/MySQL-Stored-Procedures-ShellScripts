drop procedure if exists vpndelay;
\d |

CREATE PROCEDURE vpndelay()
BEGIN

drop table if exists TEMP_VPN_MAP;

create table if not exists TEMP_VPN_MAP(ID integer AUTO_INCREMENT PRIMARY KEY,NodeNumber integer,NodeIfIpaddress varchar(128),NodeIpAddres varchar(128),vpnid integer,NodeName varchar(128),vpnname varchar(128));

insert into TEMP_VPN_MAP(NodeNumber,NodeIfIpaddress,NodeIpAddres,vpnid,NodeName,vpnname) select b.TerNodeNumber,b.TerIfIPAddress,c.NodeId,a.vpnId,c.Nodename,d.vpnname from MAP_VPN_NODE_TBL a,LINK_TBL b,NODE_TBL c,VPN_TBL d where a.VpnNode = b.OriNodeNumber && a.VpnIfIndex = b.OriIfIndex && b.TerNodeNumber=c.Nodenumber && d.vpnid=a.vpnid;

create table if not exists TEMP_DESTINATION (ID integer AUTO_INCREMENT PRIMARY KEY,dest varchar(128));

insert into  TEMP_DESTINATION (dest) select distinct destinationip from VPN_DELAY_TABLE ;

 create table if not exists TEMP_SOURCE(ID integer AUTO_INCREMENT PRIMARY KEY,dest varchar(128));

insert into TEMP_SOURCE (dest) select distinct sourceip from VPN_DELAY_TABLE ;


create table if not exists FINAL_SOURCELIST (ID integer AUTO_INCREMENT PRIMARY KEY,NodeName varchar(128),IpAddress varchar(128),VpnName varchar(128));


insert into FINAL_SOURCELIST (NodeName,IpAddress,VpnName) select b.NodeName,a.dest,b.vpnname from TEMP_SOURCE a,TEMP_VPN_MAP b where a.dest = b.NodeIpAddres ;

create table if not exists FINAL_DESTINATIONLIST (ID integer AUTO_INCREMENT PRIMARY KEY,NodeName varchar(128),IpAddress varchar(128),VpnName varchar(128));

 insert into FINAL_DESTINATIONLIST (NodeName,IpAddress,VpnName) select b.NodeName,a.dest,b.vpnname from TEMP_DESTINATION a,TEMP_VPN_MAP b where a.dest = b.NodeIfIpAddress ;


insert into FINAL_DESTINATIONLIST (NodeName,IpAddress,VpnName) select b.NodeName,a.dest,b.vpnname from TEMP_DESTINATION a,TEMP_VPN_MAP b where a.dest = b.NodeIpAddres ;



END|

\d;

