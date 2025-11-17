-- /****************************************************************************
--  
--  Generator Version           : 11.2.0.3.0
--  Created Date                : Wed Aug 01 11:02:39 IRDT 2018
--  Modified Date               : Wed Aug 01 11:02:39 IRDT 2018
--  Created By                  : owb
--  Modified By                 : owb
--  Generated Object Type       : SQL*Loader Control File
--  Generated Object Name       : "LOAD_BATCHVOUCHER"

--  Copyright (c) 2000, 2018, Oracle. All rights reserved.
-- ****************************************************************************/


OPTIONS (BINDSIZE=50000,ERRORS=0,ROWS=1000,READSIZE=65536)
LOAD DATA
  CHARACTERSET AL32UTF8
  INFILE '/Reports/Daily_Import/bvoucher/bvoucher.all.txt' 
INTO TABLE "SHAPARAK"."BATCHVOUCHER"
  APPEND
  REENABLE DISABLED_CONSTRAINTS
  FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' 
  TRAILING NULLCOLS 
(
"PSPIIN" POSITION(1) DECIMAL EXTERNAL(9) ,
"CARDTYPE" CHAR(1) ,
"PRCODE" CHAR(2) ,
"TERMTYPE" CHAR(3) ,
"TRNS_COUNT" DECIMAL EXTERNAL(10) ,
"AMNT" CHAR(17) ,
"ACPTR_PREFEE" CHAR(17) ,
"PSP_PREFEE" CHAR(17) ,
"ACPTR_TODAYFEE" CHAR(17) ,
"PSP_TODAYFEE" CHAR(17) ,
"ACPTR_FUTUREFEE" CHAR(17) ,
"PSP_FUTUREFEE" CHAR(17) ,
"BATCHDATE" DECIMAL EXTERNAL(8) ,
"FILLER_C14" FILLER CHAR(1) 
)
