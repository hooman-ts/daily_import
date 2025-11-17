OPTIONS (DIRECT=TRUE)
UNRECOVERABLE LOAD DATA
  CHARACTERSET WE8MSWIN1252
INFILE '/Reports/Daily_Import/BATCH/581672000.acq.app'
INTO TABLE "SHAPARAK"."DEPOSITTRANS"
  APPEND
    WHEN "PRCODE" != 'RA'
    FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' 
    TRAILING NULLCOLS 
(
"FILEDATE" EXPRESSION "to_date(:SETTLEDATE,'yyyy/mm/dd','nls_calendar=persian')",
"BILLTYPE" EXPRESSION "case
  when  :\"PRCODE\"  = 'BP' and (case
  when substr(:\"TRANS_INFO\" ,0,1) = '_' then 'XX' || substr( :\"TRANS_INFO\" ,1,33)
  else :\"TRANS_INFO\" 
end) is not null then substr((case
  when substr(:\"TRANS_INFO\" ,0,1) = '_' then 'XX' || substr( :\"TRANS_INFO\" ,1,33)
  else :\"TRANS_INFO\" 
  end) ,1,2)
  else null
end",
"FILLER_ROW_" FILLER POSITION(1) DECIMAL EXTERNAL ,
"PSPIIN" DECIMAL EXTERNAL ,
"ACPTRID" CHAR(15) ,
"TERMID" CHAR(8) ,
"TRACEDATE" CHAR(10) ,
"TRACETIME" CHAR(8) ,
"TRACENO" DECIMAL EXTERNAL(6) ,
"RRN" CHAR(12) ,
"TERMTYPE" CHAR(3) ,
"PRCODE" CHAR(2) ,
"CARDISSUER" DECIMAL EXTERNAL(9) ,
"PAN" CHAR(19) "case
 when substr( :\"PAN\" ,1,3) = '989' then  substr(:\"PAN\",4,16) || '   '
 when substr( :\"PAN\" ,1,1) = '9' and substr( :\"PAN\" ,-1,1) = '0' then  substr(:\"PAN\",2,18) || '0' 
 when substr( :\"PAN\" ,1,1) = '9' and substr( :\"PAN\" ,-1,1) = ' ' then  substr(:\"PAN\",2,18) || ' ' 
  else :\"PAN\"   
end" ,
"PSPAMNT" DECIMAL EXTERNAL(12) ,
"SHPAMNT" DECIMAL EXTERNAL(12) ,
"ISSAMNT" DECIMAL EXTERNAL(12) ,
"SETTLEDATE" CHAR(10) ,
"SETTLETYPE" DECIMAL EXTERNAL(4) ,
"TRNSRCVDATE" CHAR(10) "TO_NUMBER(SUBSTR( :\"TRNSRCVDATE\", 0, 4)  || SUBSTR( :\"TRNSRCVDATE\", 6, 2) || SUBSTR( :\"TRNSRCVDATE\", 9, 2))" ,
"NETABBV" CHAR(3) ,
"PSPFEE" CHAR(12) ,
"ACPTRFEE" CHAR(12) ,
"SHPTRNSDATE" CHAR(10) ,
"SHPTRNSTIME" CHAR(8) ,
"TRANS_INFO" CHAR(38) "case
  when  :\"PRCODE\"  = 'BP' and (case
  when substr(:\"TRANS_INFO\" ,0,1) = '_' then 'XX' || substr( :\"TRANS_INFO\" ,1,33)
  else :\"TRANS_INFO\" 
end) is not null then substr((case
  when substr(:\"TRANS_INFO\" ,0,1) = '_' then 'XX' || substr( :\"TRANS_INFO\" ,1,33)
  else :\"TRANS_INFO\" 
end) ,3,33)
  when substr( :\"PAN\" ,1,3) = '989' then  '989'
  else null
end" ,
"FILLER_C25" FILLER CHAR(1) ,
"FILLER_C26" FILLER CHAR(1) ,
"FILLER_C27" FILLER CHAR(1) ,
"FILLER_C28" FILLER CHAR(1) 
)
