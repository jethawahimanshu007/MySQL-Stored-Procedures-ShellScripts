#Set the value for before 

before=1

beforeFinal=$before*-1
current=$beforeFinal+1

currentTimeStamp=$(mysql -u root -pokhla3 --batch -e "SELECT IF(TIMESTAMPADD(day, $current,(NOW()) like '%00:00:00%'),TIMESTAMPADD(day,$current,NOW()),CONCAT(date(TIMESTAMPADD(day,$current,NOW())),' 00:00:00'))" | tail +2)
previousTimeStamp=$(mysql -u root -pokhla3 --batch -e "select IF((TIMESTAMPADD(day,$beforeFinal,now()) like '%00:00:00%'),TIMESTAMPADD(day,$beforeFinal,now()),CONCAT(date(TIMESTAMPADD(day,$beforeFinal,now())),' 00:00:00')) " | tail +2)
mkdir /home/Vegayan/BRD_REPORTS
cd /home/Vegayan/BRD_REPORTS
dir_nameDate=$(mysql -u root -pokhla3 --batch  -e "select DATE(IF((TIMESTAMPADD(day,$beforeFinal,now()) like '%00:00:00%'),TIMESTAMPADD(day,$beforeFinal,now()),CONCAT(date(TIMESTAMPADD(day,$beforeFinal,now())),' 00:00:00')))" | tail +2)
rm -rf $dir_nameDate
mkdir $dir_nameDate
chmod -R 777 $dir_nameDate
dir_name=/home/Vegayan/BRD_REPORTS/$dir_nameDate
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_1_PortUtil("'AESI-DOM'",'ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_2_PortUtil("'AESI-IN'",'ALL','ALL','ALL','ALL',
'$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_3_Pearing_Link_Util('ALL','ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_5_classOfService('ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_4_6_Latency('ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_1_VRFUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_2_LspUtil('ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_3_TempUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_4_BufferUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_5_CpuUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_6_StorageUtil('ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_7_unusedServicePolicy('ALL','ALL','ALL')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_8_unusedVrf('ALL','ALL','ALL')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_9_PortUtil('ALL','ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"
mysql -u root  -pokhla3 Vegayan  -e "select '$dir_name' into @dir_name;call mpls_reporting_6_6_10_CustomerUtil('ALL','ALL','ALL','ALL','$previousTimeStamp','$currentTimeStamp')"

