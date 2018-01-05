#!/bin/bash

#Set the value for before 

before=1

beforeFinal=$(($before*-1))
current=$(($beforeFinal+1))


currentTimeStamp=$(/usr/local/mysql/bin/mysql -u root -pokhla3 --batch -e "SELECT IF(TIMESTAMPADD(day, $current,(NOW()) like '%00:00:00%'),TIMESTAMPADD(day,$current,NOW()),CONCAT(date(TIMESTAMPADD(day,$current,NOW())),' 00:00:00'))" | tail +2)
previousTimeStamp=$(/usr/local/mysql/bin/mysql -u root -pokhla3 --batch -e "select IF((TIMESTAMPADD(day,$beforeFinal,now()) like '%00:00:00%'),TIMESTAMPADD(day,$beforeFinal,now()),CONCAT(date(TIMESTAMPADD(day,$beforeFinal,now())),' 00:00:00')) " | tail +2)

cd /export/home/vegayan/mpls
dir_nameDate=$(/usr/local/mysql/bin/mysql -u root -pokhla3 --batch  -e "select DATE(IF((TIMESTAMPADD(day,$beforeFinal,now()) like '%00:00:00%'),TIMESTAMPADD(day,$beforeFinal,now()),CONCAT(date(TIMESTAMPADD(day,$beforeFinal,now())),' 00:00:00')))" | tail +2)
rm -rf $dir_nameDate
mkdir $dir_nameDate
chmod -R 777 $dir_nameDate
cd /export/home/vegayan/mplsFinalReports
rm -rf $dir_nameDate
mkdir $dir_nameDate
chmod -R 777 $dir_nameDate
dir_name=/export/home/vegayan/mpls/$dir_nameDate

cd /export/home/vegayan/mplsForWindows
rm -rf 6_4_*
rm -rf 6_6_*
rm -rf URL_*

echo "MPLS" $previousTimeStamp $currentTimeStamp



/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_1_PortUtil(\"\'AESI-DOM\'\",'ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_Domestic_Backbone_Service_wise_Link_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_Domestic_Backbone_Service_wise_Link_Utilization.csv /export/home/vegayan/mplsForWindows/6_4_1.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_2_PortUtil(\"\'AESI-IN\'\",'ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_International_Backbone_Service_wise_Link_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_International_Backbone_Service_wise_Link_Utilization.csv /export/home/vegayan/mplsForWindows/6_4_2.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_3_Pearing_Link_Util('ALL','ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_Peering_Link_Utilization_Report.csv  /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_Peering_Link_Utilization_Report.csv /export/home/vegayan/mplsForWindows/6_4_3.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_5_classOfService_24('ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_IPMPLS_CORE_NETWORK_CLASS_OF_SERVICE_TRAFFIC.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_IPMPLS_CORE_NETWORK_CLASS_OF_SERVICE_TRAFFIC.csv /export/home/vegayan/mplsForWindows/6_4_5.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_6_Latency('ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_Latency_Jitter_PacketDrops.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/NWP_Latency_Jitter_PacketDrops.csv /export/home/vegayan/mplsForWindows/6_4_6.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_1_VRFUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_VRF_UTILIZATION.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_VRF_UTILIZATION.csv /export/home/vegayan/mplsForWindows/6_6_1.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_2_LspUtil('ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_LSP_Path_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_LSP_Path_Utilization.csv /export/home/vegayan/mplsForWindows/6_6_2.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_3_TempUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_Temperature.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_Temperature.csv /export/home/vegayan/mplsForWindows/6_6_3.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_4_BufferUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_Buffer_Report.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_Buffer_Report.csv /export/home/vegayan/mplsForWindows/6_6_4.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_5_CpuUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_CPU_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_CPU_Utilization.csv /export/home/vegayan/mplsForWindows/6_6_5.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_6_StorageUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_Storage_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Router_Storage_Utilization.csv /export/home/vegayan/mplsForWindows/6_6_6.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_7_unusedServicePolicy('ALL','ALL','ALL')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Unused_Service_Policy.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Unused_Service_Policy.csv /export/home/vegayan/mplsForWindows/6_6_7.csv

#/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_8_unusedVrf('ALL','ALL','ALL')"
#cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Unused_VRF.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
#cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_Unused_VRF.csv /export/home/vegayan/mplsForWindows/6_6_8.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_9_PortUtil('ALL','ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_RouterWise_Interface_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_RouterWise_Interface_Utilization.csv /export/home/vegayan/mplsForWindows/6_6_9.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_10_CustomerUtil('ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_CityWise_Customer_Utilization.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/MPLS_CityWise_Customer_Utilization.csv  /export/home/vegayan/mplsForWindows/6_6_10.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call WebUsage25_reporting(2,'$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/URL25_*.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/URL25_*.csv  /export/home/vegayan/mplsForWindows/URL25.csv

/usr/local/mysql/bin/mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call  WebUsage_reporting(2,'$previousTimeStamp','$currentTimeStamp')"
cp /export/home/vegayan/mpls/$dir_nameDate/URL150_*.csv /export/home/vegayan/mplsFinalReports/$dir_nameDate
cp /export/home/vegayan/mpls/$dir_nameDate/URL150_*.csv  /export/home/vegayan/mplsForWindows/URL150.csv


