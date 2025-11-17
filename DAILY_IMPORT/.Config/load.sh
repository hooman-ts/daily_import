#!/bin/bash
SyncRemoteDB() {
echo `date` " - Loading Transactions to $REMOTE_DBNAME..."
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$REMOTE_TNSNAME << EOF
exec shaparak.pel_debittrans;
exit
EOF" &
PID_PEL_DEB=$!
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$REMOTE_TNSNAME << EOF
exec shaparak.pel_batchtrans;
exit
EOF" &
PID_PEL_BAT=$!
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$REMOTE_TNSNAME << EOF
exec shaparak.pel_deposittrans;
exit
EOF" &
PID_PEL_DEP=$!
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$REMOTE_TNSNAME << EOF
exec shaparak.pel_other;
exit
EOF" &
PID_PEL_OTH=$!
wait $PID_PEL_OTH
CheckError 7
wait $PID_PEL_DEP
CheckError 7
wait $PID_PEL_DEB
CheckError 7
wait $PID_PEL_BAT
CheckError 7

echo -e "\n"`date` " - Synchronization process for $REMOTE_DBNAME is completed. Make sure that each procedure is completed successfully."
#Postprocess
/home/script/DAO_Provinces_daily.sh & #Update Acceptor counts in provinces and cities that used by DAO
/home/script/DAO_Counties_daily.sh &
/home/script/DAO_Provinces.sh "$SHAMSI8" &
/home/script/DAO_Counties.sh  "$SHAMSI8" &
}

CompareDB() {
echo `date` " - Comparing count of transactions for $SHAMSI6..."
LOCAL_CNT=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
set pagesize 0;
set wrap off;
set linesize 10000;
select count(*) from shaparak.revtrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.ratrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.errortrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.deposittrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.debittrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.batchtrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.vouchers where batchdate='"$SHAMSI8"';
select count(*) from shaparak.batchvoucher where batchdate='"$SHAMSI8"';
exit
EOF"`

REMOTE_CNT=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$REMOTE_TNSNAME << EOF
set pagesize 0;
set wrap off;
set linesize 10000;
select count(*) from shaparak.revtrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.ratrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.errortrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.deposittrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.debittrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.batchtrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
select count(*) from shaparak.vouchers where batchdate='"$SHAMSI8"';
select count(*) from shaparak.batchvoucher where batchdate='"$SHAMSI8"';
exit
EOF"`

TITLE="REVTRANS RATRANS ERRORTRANS DEPOSITTRANS DEBITTRANS BATCHTRANS VOUCHERS BATCHVOUCHER"
#Convert persian date to gregorian
MILADI8=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
set pagesize 0;
select to_char(to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian'),'yyyymmdd') from dual;
exit
EOF"`
CNTLOG="/tmp/count-$MILADI8.log"
echo "SERVER" $TITLE  > $CNTLOG
echo $LOCAL_DBNAME $LOCAL_CNT >> $CNTLOG
echo $REMOTE_DBNAME $REMOTE_CNT >> $CNTLOG
column -t -s ' ' $CNTLOG
if [ "$LOCAL_CNT" == "$REMOTE_CNT" ]; then
   echo "Count of transactions is equal in both databases." >> $CNTLOG
else
   echo "Count of transactions is different in both databases." >> $CNTLOG
fi
logger -t "$LOCAL_DBNAME" -p local5.warning `tail -1 $CNTLOG`
ARRAY_TBL=(${TITLE// / })
ARRAY_LOCAL=(${LOCAL_CNT// / })
ARRAY_REMOTE=(${REMOTE_CNT// / })
for i in {0..7}
do
   if [ "${ARRAY_LOCAL[i]}" == 0 ]; then
      echo "Zero record imported on table ${ARRAY_TBL[i]}" >> $CNTLOG
      logger -t "$LOCAL_DBNAME" -p local5.warning `tail -1 $CNTLOG`
      echo `date` " - `tail -1 $CNTLOG`"
   fi
   if [ "${ARRAY_LOCAL[i]}" != "${ARRAY_REMOTE[i]}" ]; then
      echo "Count of transactions on table ${ARRAY_TBL[i]} is different." >> $CNTLOG
      logger -t "$LOCAL_DBNAME" -p local5.warning `tail -1 $CNTLOG` 
      echo `date` " - `tail -1 $CNTLOG`"
   fi
done
mv -f $CNTLOG ./Logs/"$SHAMSI6" > /dev/null 2>&1
#printf "%-15s %-15s %-15s\n" $TITLE $LOCAL_CNT $REMOTE_CNT


#Compare SHP Amount and PSP Amount
AMNT=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
set pagesize 0;
set wrap off;
set linesize 10000;
select trim(to_char(sum(shpamnt),'999999999999999999999999999999D99')) , trim(to_char(sum(pspamnt),'999999999999999999999999999999D99')) from shaparak.debittrans where filedate = to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian');
exit
EOF"`
SHPAMNT=`echo $AMNT | cut -d' ' -f1`
PSPAMNT=`echo $AMNT | cut -d' ' -f2`
if [ $SHPAMNT != $PSPAMNT ]; then
   echo "SHPAMNT and PSPAMNT have different values." >> $CNTLOG
   logger -t "$LOCAL_DBNAME" -p local5.warning `tail -1 $CNTLOG`
   echo `date` " - `tail -1 $CNTLOG`"
fi
}

RunCounter() {
if [ -f /tmp/load_temp_info.log ]; then
read TEMPDT TEMPCOUNTER TEMPEXISTS < /tmp/load_temp_info.log
   if [ "$TEMPDT" == "$SHAMSI8" ]; then
      TEMPCOUNTER=$((TEMPCOUNTER + 1))
      if [ "$1" -eq 1 ] && [ "$TEMPEXISTS" -eq 0 ]; then TEMPCOUNTER=1; fi
      if [ "$1" == 0 ]; then
         if [ "$TEMPCOUNTER" -ge 19 ]; then #After 3 hours from 6:00
            echo `date` " - The files are not available by ISC."
            logger -t "$LOCAL_DBNAME" -p local5.warning "The files are not available by ISC."
         fi
      else
         if [ "$TEMPCOUNTER" -ge 1 ]; then
            logger -t "$LOCAL_DBNAME" -p local5.warning "The files are ready. It is going to Start loading transactions to the database."
         fi
      fi
      echo $SHAMSI8 $TEMPCOUNTER $1 > /tmp/load_temp_info.log
   else
      echo $SHAMSI8 1 $1 > /tmp/load_temp_info.log
   fi
else
   echo $SHAMSI8 1 $1 > /tmp/load_temp_info.log
fi
}

CheckError() {
RES=`tail -$1 $LOGFILE | grep ORA-`
if [ "$RES" != "" ]; then
   logger -t "$LOCAL_DBNAME" -p local5.warning "There is an error in loading transactions."
fi
}

PashaImport() {
rm -rf ./pasha/*
umount -l "$MOUNT_POINT" > /dev/null 2>&1
for i in {2..6}
do
   mount.cifs -o ro,vers=2.0,username="$FTP_USER",password="$FTP_PASS",domain="$FTP_DOMAIN" "$PASHA_DIR" "$MOUNT_POINT"
   if [ $? != 0 ]; then
      echo `date` " - Cannot access to opdr-01. Trying $((6 - $i)) more time(s)..."
      sleep 60
      if [ "$i" == 5 ]; then echo `date` " - Cannot access to opdr-01"; logger -t "$LOCAL_DBNAME" -p local5.warning "Cannot access to opdr-01"; echo -e `date` " - Exit\n\n"; exit; fi
   else
      break
   fi
done

find /mnt -not -path "/mnt/Shaparak/*" -type f -name "*_$SHAMSI6.zip" -exec cp '{}' ./pasha/ \;
umount -l "$MOUNT_POINT" > /dev/null 2>&1
mkdir ./pasha > /dev/null 2>&1
chown -R oracle: ./pasha/
for FILE in ./pasha/*.zip; do
   unzip -j $FILE "202*.txt" "206*.txt" -d ./pasha/
done

for FILE in ./pasha/*.txt; do
   iconv -c -f UTF-16LE -t UTF-8 $FILE > $FILE.tmp
   mv $FILE.tmp $FILE
done
cd ./pasha
for FILE in 202*.txt; do
   echo "OPTIONS (DIRECT=FALSE)
         LOAD DATA
         INFILE \"$FILE\"
         INTO TABLE DAO.PASHA_202
         APPEND
         FIELDS TERMINATED BY '%'
         TRAILING NULLCOLS
         (ACQ_BANK,
         ACCEPTOR_NAME,
         RETURN_REASON,
         ADDITIONAL_DATA,
         AMOUNT1,
         CT,
         TRACENO,
         SHEBA,
         RCT,
         AMOUNT2,
         CYCLENO,
         FILE_NAME CONSTANT \"$FILE\")" > $FILE.ctl
   sch=`su oracle -c "sqlldr $DAO_USER/$DAO_PASS@$DAO_TNSNAME $FILE.ctl"`
done

for FILE in 206*.txt; do
   echo "OPTIONS (DIRECT=FALSE)
         LOAD DATA
         INFILE \"$FILE\"
         INTO TABLE DAO.PASHA_206
         APPEND
         FIELDS TERMINATED BY '%'
         TRAILING NULLCOLS
         (TYPE,
         SHEBA,
         ADDITIONAL_DATA,
         AMOUNT,
         TRACENO,
         CT,
         TRANDATE,
         CYCLENO,
         FILE_NAME CONSTANT \"$FILE\")" > $FILE.ctl
   sch=`su oracle -c "sqlldr $DAO_USER/$DAO_PASS@$DAO_TNSNAME $FILE.ctl"`
done
cd ../
grep -R -E -A1 -B1 "Rows successfully loaded|ORA" ./pasha/*.log
CheckError 5
}


######################################################## START POINT OF SCRIPT
umask 002
cd /Reports/Daily_Import
sed -i 's/ =/=/g'  ./.Config/load.cfg
sed -i 's/= /=/g'  ./.Config/load.cfg
sed -i 's/	=/=/g' ./.Config/load.cfg
sed -i 's/=	/=/g' ./.Config/load.cfg
source /home/oracle/.bash_profile
source ./.Config/load.cfg
VALIDATE_ZIP_FILES=`echo $VALIDATE_ZIP_FILES | tr '[:upper:]' '[:lower:]'` #convert input config value to lowercase
SYNC_REMOTE_DB=`echo $SYNC_REMOTE_DB | tr '[:upper:]' '[:lower:]'` #convert input config value to lowercase


ISRUN=`ps -ef | grep load.sh | grep -v grep | grep -v vi | grep -v load.log | grep -v "$$"` # $$ returns self PID
if [ "$ISRUN" != "" ]; then
#   echo ; echo
#   echo `date` " - Load script is already running."
#   echo -e `date` " - Exit\n\n";
   exit
fi

if [ "$1" != "" ]; then
   if [ ${#1} == 6 ]; then #yymmdd
      SHAMSI8=$CENTURY$1
      SHAMSI6=$1
   elif [ ${#1} == 8 ]; then #yyyymmdd
      SHAMSI8=$1
      SHAMSI6=${SHAMSI8:2:8}
   else
      echo `date` " - Wrong format of date. only 'yyyymmdd' and 'yymmdd' in persian calendar are acceptable."
      logger -t "$LOCAL_DBNAME" -p local5.warning "Wrong format of date. only 'yyyymmdd' and 'yymmdd' in persian calendar are acceptable."
      echo -e `date` " - Exit\n\n";
      exit
   fi
   MILADI8=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
   set pagesize 0;
   select to_char(to_date('"$SHAMSI8"','yyyymmdd','nls_calendar=persian'),'yyyymmdd') from dual;
   exit
EOF"`
else #no input
   MILADI8=`date +%Y%m%d -d "-1 day"`
   SHAMSI8=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
   set pagesize 0;
   select to_char(to_date('"$MILADI8"','yyyymmdd'),'yyyymmdd','nls_calendar=persian') from dual;
   exit
EOF"`
   SHAMSI6=${SHAMSI8:2:8}
fi

CNTLOG="./Logs/"$SHAMSI6"/count-"$MILADI8".log"
if [ -f "$CNTLOG" ]; then
   ISSYNC=`grep "equal" $CNTLOG`
   ISLOAD=`grep "Zero" $CNTLOG | grep -v RATRANS`
   if [ "$ISSYNC" != "" ] && [ "$ISLOAD" == "" ]; then
#      echo `date` " - Both databases are sync."
#      echo -e `date` " - Exit\n\n";
      exit
   fi
fi

echo `date` " - ################################# START #################################"
echo `date` " - Clearing..."
rm -rf ./BATCH ./Shaparak ./voucher ./bvoucher ./*.zip
if [ $? != 0 ]; then
   echo `date` " - Can not remove old files. Please delete 'BATCH' and 'Shaparak' zip files and directories manually."
   logger -t "$LOCAL_DBNAME" -p local5.warning "Can not remove old files. Please delete 'BATCH' and 'Shaparak' zip files and directories manually."
   echo -e `date` " - Exit\n\n"; exit
fi
umount -l "$MOUNT_POINT" > /dev/null 2>&1
for i in {2..6}
do
   mount.cifs -o ro,vers=2.0,username="$FTP_USER",password="$FTP_PASS",domain="$FTP_DOMAIN" "$FTP_DIR" "$MOUNT_POINT"
   if [ $? != 0 ]; then
      echo `date` " - Cannot access to opdr-01. Trying $((6 - $i)) more time(s)..."
      sleep 60
      if [ "$i" == 5 ]; then echo `date` " - Cannot access to opdr-01"; logger -t "$LOCAL_DBNAME" -p local5.warning "Cannot access to opdr-01"; echo -e `date` " - Exit\n\n"; exit; fi
   else
      break
   fi
done

BATCHFILE=""$MOUNT_POINT"/BATCH_"$SHAMSI6".zip"
SHAPFILE=""$MOUNT_POINT"/Shaparak_"$SHAMSI6".zip"

if [ ! -f "$BATCHFILE" ] || [ ! -f "$SHAPFILE" ]; then
   #echo `date` " - The files are not available by ISC."
   RunCounter 0
   umount -l "$MOUNT_POINT" > /dev/null 2>&1
   #if [ "$SYNC_REMOTE_DB" == "true" ]; then SyncRemoteDB; fi
   #CompareDB
   #echo -e `date` " - Exit\n\n"
   exit
else
   RunCounter 1
fi

#B=`echo $BATCHFILE | cut -d'/' -f3 | cut -d'.' -f1` # BATCH_yymmdd
#S=`echo $SHAPFILE  | cut -d'_' -f2 | cut -d'.' -f1` # yymmdd

VOU_ROWS=0; BVO_ROWS=0; DEB_ROWS=0; BAT_ROWS=0; DEP_ROWS=0; ERR_ROWS=0; RA_ROWS=0; REV_ROWS=0
if [ -f ./Logs/"$SHAMSI6"/voucher.log ];  then VOU_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/voucher.log  | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/bvoucher.log ]; then BVO_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/bvoucher.log | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/debit.log ];    then DEB_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/debit.log    | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/batch.log ];    then BAT_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/batch.log    | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/deposit.log ];  then DEP_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/deposit.log  | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/error.log ];    then ERR_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/error.log    | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/rev.log ];      then REV_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/rev.log      | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/ra.log ];       then RA_ROWS=`grep  "Rows successfully loaded" ./Logs/"$SHAMSI6"/ra.log       | awk {'print $1'}`; fi
if [ -f ./Logs/"$SHAMSI6"/ra.log.gz ];    then
				    gunzip ./Logs/"$SHAMSI6"/ra.log.gz
				    RA_ROWS=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/ra.log | awk {'print $1'}`
fi
gzip -9 ./Logs/"$SHAMSI6"/ra.log &
if [ "$VOU_ROWS" == "" ]; then VOU_ROWS=0; fi
if [ "$BVO_ROWS" == "" ]; then BVO_ROWS=0; fi
if [ "$DEB_ROWS" == "" ]; then DEB_ROWS=0; fi
if [ "$BAT_ROWS" == "" ]; then BAT_ROWS=0; fi
if [ "$DEP_ROWS" == "" ]; then DEP_ROWS=0; fi
if [ "$ERR_ROWS" == "" ]; then ERR_ROWS=0; fi
if [ "$REV_ROWS" == "" ]; then REV_ROWS=0; fi
if [ "$RA_ROWS"  == "" ]; then RA_ROWS=0;  fi
# Checking rows of all tables except RA, cause of holidays that has 0 rows.
if [ "$VOU_ROWS" != 0 ] && [ "$BVO_ROWS" != 0 ] && [ "$DEB_ROWS" != 0 ] && [ "$BAT_ROWS" != 0 ] && [ "$DEP_ROWS" != 0 ] && [ "$ERR_ROWS" != 0 ] && [ "$REV_ROWS" != 0 ]; then
   echo `date` " - All of the transactions for "$SHAMSI6" is already imported to the database."
   umount -l "$MOUNT_POINT" > /dev/null 2>&1
   if [ "$SYNC_REMOTE_DB" == "true" ]; then SyncRemoteDB; fi
   CompareDB
   echo -e `date` " - Exit\n\n"; exit
fi


echo `date` " - Copying files ("$BATCHFILE" , "$SHAPFILE") from opdr-01 to local server..."
cp "$BATCHFILE" . &
PID1=$!
cp "$SHAPFILE"  . &
PID2=$!
wait
if [ $? != 0 ]; then
   echo `date` " - Cannot copy files from opdr-01 to local server."
   umount -l "$MOUNT_POINT" > /dev/null 2>&1
   logger -t "$LOCAL_DBNAME" -p local5.warning "Cannot copy files from opdr-01 to $LOCAL_DBNAME"
   echo -e `date` " - Exit\n\n"; exit
else
   echo `date` " - The files copied successfully."
fi
umount -l "$MOUNT_POINT" > /dev/null 2>&1

if [ "$VALIDATE_ZIP_FILES" == "true" ]; then
   echo `date` " - Testing Batch zip file for health integrity..."
   echo `date` " - Testing Shaparak zip file for health integrity simultaneously..."
   zip --test ./BATCH*.zip > /dev/null 2>&1 &
   PID1=$!
   zip --test ./Shaparak*.zip > /dev/null 2>&1 &
   PID2=$!
   wait $PID1
   if [ $? != 0 ]; then echo `date` " - BATCH zip file is corrupted!"; logger -t "$LOCAL_DBNAME" -p local5.warning "BATCH zip file is corrupted!"; echo -e `date` " - Exit\n\n"; exit; fi
   echo `date` " - Test operation for Batch file is completed."
   wait $PID2
   if [ $? != 0 ]; then echo `date` " - Shaparak zip file is corrupted!"; logger -t "$LOCAL_DBNAME" -p local5.warning "Shaparak zip file is corrupted!"; echo -e `date` " - Exit\n\n"; exit; fi
   echo `date` " - Test operation for Shaparak file is completed."
fi

unzip -j ./BATCH*.zip "BATCH_"$SHAMSI6"/SHAP/*" -d ./BATCH &
PID1=$!
unzip -j ./Shaparak*.zip "$SHAMSI6/SHAP/*" -d ./Shaparak &
PID2=$!
wait $PID1
if [ $? != 0 ]; then echo `date` " - Cannot unzip Batch file."; logger -t "$LOCAL_DBNAME" -p local5.warning "Cannot unzip Batch file."; echo -e `date` " - Exit\n\n"; exit; fi
wait $PID2
if [ $? != 0 ]; then echo `date` " - Cannot unzip Shaparak file."; logger -t "$LOCAL_DBNAME" -p local5.warning "Cannot unzip Batch file."; echo -e `date` " - Exit\n\n"; exit; fi

unzip -j ./BATCH*.zip "BATCH_"$SHAMSI6"/*/*voucher.txt" -d ./bvoucher
rm -f ./bvoucher/581672000.voucher.txt #remove shaparak voucher

unzip -j ./Shaparak*.zip "$SHAMSI6/*/*voucher.txt" -d ./voucher
rm -f ./voucher/581672000.voucher.txt #remove shaparak voucher

cd ./bvoucher
for i in *.voucher.txt
do
   PSPIIN=`echo "$i" | cut -d'.' -f1`
   while read p; do
      LINE=$PSPIIN"|"$p
      echo $LINE >> ./bvoucher.all.txt
   done < "$i"
done

cd ../voucher
for i in *.voucher.txt
do
   PSPIIN=`echo "$i" | cut -d'.' -f1`
   while read p; do
      LINE=$PSPIIN"|"$p
      echo $LINE >> ./voucher.all.txt
   done < "$i"
done
cd ../


echo
echo `date` " - Creating partitions..."
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
set feed off;
exec shaparak.pr_cr_load_partition('BATCHTRANS$TABLE_SUFFIX','Y');
exec shaparak.pr_cr_load_partition('DEBITTRANS$TABLE_SUFFIX','Y');
exec shaparak.pr_cr_load_partition('DEPOSITTRANS$TABLE_SUFFIX','Y');
exec shaparak.pr_cr_load_partition('ERRORTRANS$TABLE_SUFFIX','N');
exec shaparak.pr_cr_load_partition('REVTRANS$TABLE_SUFFIX','N');
exec shaparak.pr_cr_load_partition('RATRANS$TABLE_SUFFIX','N');
exit
EOF"
echo `date` " - Creating partitions done."
CheckError 25
echo
if [ "$VOU_ROWS" -eq 0 ]; then
   echo `date` " - VOUCHERS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/voucher.ctl"` &
   PID1=$!
else
   echo `date` " - VOUCHERS has already $VOU_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$BVO_ROWS" -eq 0 ]; then
   echo `date` " - BATCHVOUCHER is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/bvoucher.ctl"` &
   PID2=$!
else
   echo `date` " - BATCHVOUCHER has already $BVO_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$DEB_ROWS" -eq 0 ]; then
   echo `date` " - DEBITTRANS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/debit.ctl"` &
   PID3=$!
else
   echo `date` " - DEBITTRANS has already $DEB_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$BAT_ROWS" -eq 0 ]; then
   echo `date` " - BATCHTRANS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/batch.ctl"` &
   PID4=$!
else
   echo `date` " - BATCHTRANS has already $BAT_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$DEP_ROWS" -eq 0 ]; then
   echo `date` " - DEPOSITTRANS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/deposit.ctl"` &
   PID5=$!
else
   echo `date` " - DEPOSITTRANS has already $DEP_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$ERR_ROWS" -eq 0 ]; then
   echo `date` " - ERRORTRANS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/error.ctl"` &
   PID6=$!
else
   echo `date` " - ERRORTRANS has already $ERR_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$REV_ROWS" -eq 0 ]; then
   echo `date` " - REVTRANS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/rev.ctl"` &
   PID7=$!
else
   echo `date` " - REVTRANS has already $REV_ROWS rows at $SHAMSI6. Please check it out."
fi

if [ "$RA_ROWS" -eq 0 ]; then
   echo `date` " - RATRANS is loading..."
   sch=`su oracle -c "sqlldr $DBUSER/$DBPASS@$LOCAL_TNSNAME ./.Config/ctl/ra.ctl"` &
   PID8=$!
else
   echo `date` " - RATRANS has already $RA_ROWS rows at $SHAMSI6. Please check it out."
fi

echo -e "\n"`date` " - Please wait approximately 90 minutes..."

wait $PID1 #voucher
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./voucher.log
CheckError 5
wait $PID2 #bvoucher
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./bvoucher.log
CheckError 5
wait $PID7 #rev
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./rev.log
CheckError 5
wait $PID6 #error
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./error.log
CheckError 5
wait $PID8 #ra
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./ra.log
CheckError 5
wait $PID3 #debit
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./debit.log
CheckError 5
wait $PID5 #deposit
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./deposit.log
CheckError 5
wait $PID4 #batch
echo `date`
grep -E -A1 -B1 "Rows successfully loaded|ORA" ./batch.log
CheckError 5
wait

echo -e "\n\n"`date` " - Transaction load is completed. Please check the above values."

# Insert ETL_LOG Record for DEBITTRANS
echo `date` " - Insert ETL_LOG Record:"
if [ -f ./Logs/"$SHAMSI6"/debit.log ]; then
   DEBIT_CNT=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/debit.log | awk {'print $1'}`
else
   DEBIT_CNT=`grep "Rows successfully loaded" ./debit.log | awk {'print $1'}`
fi

if [ -f ./Logs/"$SHAMSI6"/batch.log ]; then
   BATCH_CNT=`grep "Rows successfully loaded" ./Logs/"$SHAMSI6"/batch.log | awk {'print $1'}`
else
   BATCH_CNT=`grep "Rows successfully loaded" ./batch.log | awk {'print $1'}`
fi


if (( DEBIT_CNT > 0 )); then
   su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
   insert into ETL_LOG values(to_date('"$MILADI8"','yyyymmdd'),'DEBIT_TRANS','LOADED',sysdate,'ETL','10061');
   commit;
   exit
EOF"
else
   echo `date` " - No Record Inserted to ETL_LOG"
fi
CheckError 7

mkdir -p ./Logs/"$SHAMSI6" > /dev/null 2>&1
mv -f `ls *.log | grep -v load.log` ./Logs/"$SHAMSI6" > /dev/null 2>&1 #move all log files except main log (load.log)
gzip -9 ./Logs/"$SHAMSI6"/ra.log > /dev/null 2>&1 &

echo ""
echo `date` " - Rebuild indexes..."
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_rebuild('BATCHTRANS$TABLE_SUFFIX','Y');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_rebuild('DEBITTRANS$TABLE_SUFFIX','Y');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_rebuild('DEPOSITTRANS$TABLE_SUFFIX','Y');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_rebuild('ERRORTRANS$TABLE_SUFFIX','N');
exit
EOF" &
wait
CheckError 25
echo ""
echo `date` " - Update statistics..."
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_gather_stats('BATCHTRANS$TABLE_SUFFIX','Y');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_gather_stats('DEBITTRANS$TABLE_SUFFIX','Y');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_gather_stats('DEPOSITTRANS$TABLE_SUFFIX','Y');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_gather_stats('ERRORTRANS$TABLE_SUFFIX','N');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_gather_stats('REVTRANS$TABLE_SUFFIX','N');
exit
EOF" &
su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
exec shaparak.pr_load_gather_stats('RATRANS$TABLE_SUFFIX','N');
exit
EOF" &
wait
CheckError 25

#Run DAO Procedures
DEBIT_CNT=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
set head off;
select count(*) from shaparak.debittrans where filedate = to_date('"$MILADI8"','yyyymmdd');
exit
EOF"`

BATCH_CNT=`su oracle -c "sqlplus -s $DBUSER/$DBPASS@$LOCAL_TNSNAME << EOF
set head off;
select count(*) from shaparak.batchtrans where filedate = to_date('"$MILADI8"','yyyymmdd');
exit
EOF"`

if (( DEBIT_CNT > 0 )) && (( BATCH_CNT > 0 )) ; then
   su oracle -c "sqlplus -s $DAO_USER/$DAO_PASS@$DAO_TNSNAME << EOF
   set feed off;
   set echo off;
   exec DAO.import.all_proc_parallel(to_date("$MILADI8",'yyyymmdd'));
   exit
EOF" &
fi

if [ "$SYNC_REMOTE_DB" == "true" ]; then SyncRemoteDB; fi
CompareDB

#Checking DAO status
DAO_STATUS=''; DAO_DONE='0'; DAO_START='1'
DAO_ERR=`echo $DAO_STATUS | grep ERROR`
while [ "$DAO_ERR" == '' ] && [ "$DAO_DONE" != "$DAO_START" ]; do
   DAO_STATUS=`su oracle -c "sqlplus -s $DAO_USER/$DAO_PASS@$DAO_TNSNAME << EOF
   set head off;
   select count(*),status from dao.import_log where import_gdate = to_date('"$MILADI8"','yyyymmdd') group by status;
   exit
EOF"`

   DAO_START=`su oracle -c "sqlplus -s $DAO_USER/$DAO_PASS@$DAO_TNSNAME << EOF
   set head off;
   select count(*) from dao.import_log where import_gdate = to_date('"$MILADI8"','yyyymmdd') and status = 'START';
   exit
EOF"`

   DAO_DONE=`su oracle -c "sqlplus -s $DAO_USER/$DAO_PASS@$DAO_TNSNAME << EOF
   set head off;
   select count(*) from dao.import_log where import_gdate = to_date('"$MILADI8"','yyyymmdd') and status = 'DONE';
   exit
EOF"`

   DAO_ERR=`echo $DAO_STATUS | grep ERROR`
   if [ "$DAO_ERR" != '' ]; then
      logger -t "$LOCAL_DBNAME" -p local5.warning "Data importation to the DAO server has been failed!"
   fi
   sleep 60
done


#Pasha Files Import until 9AM
HOUR=`date +%H`

PASHA_CNT=`su oracle -c "sqlplus -s $DAO_USER/$DAO_PASS@$DAO_TNSNAME << EOF
set head off;
select count(*) from dao.PASHA_202 where file_date='"$SHAMSI6"';
exit
EOF"`
if (( PASHA_CNT == 0 )); then
   if [ "$HOUR" -ge 9 ]; then
      PashaImport > /dev/null 2>&1
   else
      while [ "$HOUR" -lt 9 ]; do
         sleep 300
         HOUR=`date +%H`
         if [ "$HOUR" -ge 9 ]; then
            PashaImport > /dev/null 2>&1
            break;
         fi
      done
   fi
fi

#Pasha Files Import until 12PM . It will be run if pasha files appeared after 9AM.
HOUR=`date +%H`

PASHA_CNT=`su oracle -c "sqlplus -s $DAO_USER/$DAO_PASS@$DAO_TNSNAME << EOF
set head off;
select count(*) from dao.PASHA_202 where file_date='"$SHAMSI6"';
exit
EOF"`
if (( PASHA_CNT == 0 )); then
   if [ "$HOUR" -ge 12 ]; then
      PashaImport > /dev/null 2>&1
   else
      while [ "$HOUR" -lt 12 ]; do
         sleep 300
         HOUR=`date +%H`
         if [ "$HOUR" -ge 12 ]; then
            PashaImport > /dev/null 2>&1
            break;
         fi
      done
   fi
fi


echo -e `date` " - Done.\n\n"
exit
