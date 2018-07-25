SQOOP_OUT=`sqoop eval \
--connect \
jdbc:oracle:thin:@10.20.79.120:1521:frrdp \
--username \
frr_usr \
--password \
frr_usr \
-e "select (case when PROCESS_COUNT>=LOOKUP_COUNT then 'Y' ELSE 'N' END) Status from(select (select count(*) from frr_subs_processed where subdate =trunc(sysdate) and timeid<=6) PROCESS_COUNT,(select sum(file_cnt) from frr_subs.frr_subs_filename_dtl  where weekday =to_char(sysdate,'DY') and timeid<=6) LOOKUP_COUNT from dual)"`

STATUS_FLAG=`echo $SQOOP_OUT | awk -F"|" '{print($4)}' | tr -d '[:space:]'`

echo "STATUS=$STATUS_FLAG"

