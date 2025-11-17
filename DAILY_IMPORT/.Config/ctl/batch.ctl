OPTIONS (DIRECT=TRUE)
UNRECOVERABLE LOAD DATA
  CHARACTERSET WE8MSWIN1252
  INFILE '/Reports/Daily_Import/BATCH/581672000.batch'
INTO TABLE "SHAPARAK"."BATCHTRANS"
  APPEND
    FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' 
    TRAILING NULLCOLS 
(
"FILEDATE" EXPRESSION "TO_DATE(:\"BATCHDATE\",'YYYY/MM/DD','NLS_CALENDAR=PERSIAN')",
"ORIG_PRCODE" EXPRESSION "case 
  when  :\"PRCODE\"  = 'RA' then substr((case 
  when substr(:\"ORIG_TRACE\" ,0,1) = '_' then 'XX' || substr( :\"ORIG_TRACE\" ,1,33) 
  else :\"ORIG_TRACE\"  
end) ,1,2) 
  else null 
end",
"BILLTYPE" EXPRESSION "case 
  when  :\"PRCODE\"  = 'BP' and (case 
  when substr(:\"ORIG_TRACE\" ,0,1) = '_' then 'XX' || substr( :\"ORIG_TRACE\" ,1,33) 
  else :\"ORIG_TRACE\"  
end) is not null then substr((case 
  when substr(:\"ORIG_TRACE\" ,0,1) = '_' then 'XX' || substr( :\"ORIG_TRACE\" ,1,33) 
  else :\"ORIG_TRACE\"  
end) ,1,2) 
  else null 
end",
"TRANS_INFO" EXPRESSION "case 
  when  :\"PRCODE\"  = 'BP' and (case 
  when substr(:\"ORIG_TRACE\" ,0,1) = '_' then 'XX' || substr( :\"ORIG_TRACE\" ,1,33) 
  else :\"ORIG_TRACE\"  
end) is not null then substr((case 
  when substr(:\"ORIG_TRACE\" ,0,1) = '_' then 'XX' || substr( :\"ORIG_TRACE\" ,1,33) 
  else :\"ORIG_TRACE\"  
end) ,3,33) 
  when substr( :\"PAN\" ,1,3) = '989' then  '989'
  when substr( :\"PAN\" ,1,1) = '9' then  '9'
  else null 
end",
"FILLER_ROW" FILLER POSITION(1) DECIMAL EXTERNAL ,
"PSPIIN" DECIMAL EXTERNAL(9) ,
"ACPTRID" CHAR(15) ,
"TRACECODE" DECIMAL EXTERNAL(6) ,
"LOCALDATE" CHAR(10) ,
"LOCALTIME" CHAR(8) ,
"TRNSRCVDATE" CHAR(10) ,
"SHEBA" CHAR(34) ,
"BATCHDATE" CHAR(10) ,
"BATCHTUPE" CHAR(1) ,
"CYCLENUM" DECIMAL EXTERNAL(1) ,
"TERMTYPE" CHAR(3) ,
"PRCODE" CHAR(2) ,
"CARDTYPE" CHAR(1) ,
"SHPAMNT" DECIMAL EXTERNAL(12) ,
"RRN" CHAR(12) ,
"BATCHFLAG" CHAR(1) ,
"ACPTRFEE" CHAR(12) ,
"PSPFEE" CHAR(12) ,
"PUREFEE" CHAR(12) ,
"FILLER_XFEE" FILLER CHAR(12) ,
"TERMID" CHAR(8) ,
"PAN" CHAR(19) "case 
  when substr( :\"PAN\" ,1,3) = '989' then  substr(:\"PAN\",4,16) || '   '
  when substr( :\"PAN\" ,1,1) = '9' and substr( :\"PAN\" ,-1,1) = '0' then  substr(:\"PAN\",2,18) || '0'  
  when substr( :\"PAN\" ,1,1) = '9' and substr( :\"PAN\" ,-1,1) = ' ' then  substr(:\"PAN\",2,18) || ' '  
  else :\"PAN\"    
end" ,
"ORIG_TRACE" CHAR(38) "case 
  when  :\"PRCODE\"  = 'RA' then to_number(REGEXP_SUBSTR ((case 
  when substr(:\"ORIG_TRACE\" ,0,1) = '_' then 'XX' || substr( :\"ORIG_TRACE\" ,1,33) 
  else :\"ORIG_TRACE\"  
end) , '[^_]+',1,2)) 
  else null 
end" ,
"FILLER_C22" FILLER CHAR(1) ,
"FILLER_C23" FILLER CHAR(1) ,
"FILLER_C24" FILLER CHAR(1) ,
"FILLER_C25" FILLER CHAR(1) 
)
