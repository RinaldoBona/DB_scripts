--------------------------------------------------------
--  File creato - venerd�-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table SF_POLLER_CUST
--------------------------------------------------------

  CREATE TABLE "DBACD1CDB"."SF_POLLER_CUST" 
   (	"TIPO" CHAR(1), 
	"CUSTOMERID" VARCHAR2(10), 
	"NUM" NUMBER(9,0)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "TEMPZZ01"
  GRANT DELETE ON "DBACD1CDB"."SF_POLLER_CUST" TO "DBACD8ESB"
 
  GRANT INSERT ON "DBACD1CDB"."SF_POLLER_CUST" TO "DBACD8ESB"
 
  GRANT SELECT ON "DBACD1CDB"."SF_POLLER_CUST" TO "DBACD8ESB"
 
  GRANT UPDATE ON "DBACD1CDB"."SF_POLLER_CUST" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for Table SF_POLLER_LOG
--------------------------------------------------------

  CREATE TABLE "DBACD1CDB"."SF_POLLER_LOG" 
   (	"MSG_DATE" DATE, 
	"MSG_TEXT" VARCHAR2(4000)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "TEMPZZ01"
  GRANT DELETE ON "DBACD1CDB"."SF_POLLER_LOG" TO "DBACD8ESB"
 
  GRANT INSERT ON "DBACD1CDB"."SF_POLLER_LOG" TO "DBACD8ESB"
 
  GRANT SELECT ON "DBACD1CDB"."SF_POLLER_LOG" TO "DBACD8ESB"
 
  GRANT UPDATE ON "DBACD1CDB"."SF_POLLER_LOG" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for Table SF_POLLER_TEMP
--------------------------------------------------------

  CREATE TABLE "DBACD1CDB"."SF_POLLER_TEMP" 
   (	"RUN_DATE" DATE, 
	"PADRE" VARCHAR2(50), 
	"FIGLIO" VARCHAR2(50), 
	"TRNS_DATE" DATE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "TEMPZZ01"
  GRANT DELETE ON "DBACD1CDB"."SF_POLLER_TEMP" TO "DBACD8ESB"
 
  GRANT INSERT ON "DBACD1CDB"."SF_POLLER_TEMP" TO "DBACD8ESB"
 
  GRANT SELECT ON "DBACD1CDB"."SF_POLLER_TEMP" TO "DBACD8ESB"
 
  GRANT UPDATE ON "DBACD1CDB"."SF_POLLER_TEMP" TO "DBACD8ESB"