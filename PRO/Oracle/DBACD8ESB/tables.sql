--------------------------------------------------------
--  File creato - venerd�-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table TCD00_SFDC_INPUT
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" 
   (	"CD00_RUN_GUID" VARCHAR2(32) NOT NULL ENABLE, 
	"CD00_RUN_STAT" VARCHAR2(32) NOT NULL ENABLE, 
	"CD00_CRTN_DATE" DATE NOT NULL ENABLE, 
	"CD00_SEND_DATE" DATE, 
	"CD00_MODF_DATE" DATE NOT NULL ENABLE, 
	"CD00_DATA_TYP" VARCHAR2(20) NOT NULL ENABLE, 
	"CD00_DATA_VAL" VARCHAR2(200) NOT NULL ENABLE, 
	"CD00_OPER_VAL" VARCHAR2(2) NOT NULL ENABLE, 
	"CD00_SRC_COD" VARCHAR2(5), 
	 CONSTRAINT "TCD00_RUN_STAT" CHECK (CD00_RUN_STAT IN ('DRAFT', 'NEW', 'PENDING', 'QUEUED', 'DONE', 'FAILED')) ENABLE, 
	 CONSTRAINT "TCD00_DATA_TYP" CHECK (CD00_DATA_TYP IN ('SEDE', 'CUSTOMER', 'OPEC', 'RAGIONESOCIALE', 'DATAMODIFICA')) ENABLE, 
	 CONSTRAINT "TCD00_OPER_VAL" CHECK (CD00_OPER_VAL IN ('=', '!=', '>', '<', '>=', '<=')) ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DA01"
--------------------------------------------------------
--  DDL for Table TCD01_DC_LOG
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TCD01_DC_LOG" 
   (	"MSG_DATE" DATE, 
	"MSG_TEXT" VARCHAR2(4000)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DA01"
--------------------------------------------------------
--  DDL for Table TCD02_SFDC_DATA
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TCD02_SFDC_DATA" 
   (	"CD02_RUN_GUID" VARCHAR2(32), 
	"CD02_INS_DTA" DATE, 
	"ANNO_FONDAZIONE__C" VARCHAR2(200), 
	"ANNUALREVENUE" VARCHAR2(200), 
	"BILLINGCITY" VARCHAR2(200), 
	"BILLINGCOUNTRY" VARCHAR2(200), 
	"BILLINGSTATE" VARCHAR2(200), 
	"BILLINGSTREET" VARCHAR2(200), 
	"CAP__C" VARCHAR2(200), 
	"CELLULA__C" VARCHAR2(200), 
	"CODICE_COMUNE_IOL__C" VARCHAR2(200), 
	"CODICE_CUSTOMER__C" VARCHAR2(200), 
	"CODICE_FRAZIONE__C" VARCHAR2(200), 
	"CODICE_IPA__C" VARCHAR2(200), 
	"CODICE_PROVINCIA_REA__C" VARCHAR2(200), 
	"CODICE_SEDE__C" VARCHAR2(200), 
	"CODICE_STATO_CUSTOMER__C" VARCHAR2(200), 
	"CODICE_STATO_SEDE__C" VARCHAR2(200), 
	"CODICEFISCALE__C" VARCHAR2(200), 
	"COMUNE_IOL__C" VARCHAR2(200), 
	"COORD_ANAG__LATITUDE__S" VARCHAR2(200), 
	"COORD_ANAG__LONGITUDE__S" VARCHAR2(200), 
	"COORDINATE_MANUALI__C" VARCHAR2(200), 
	"DATO_FISC_EST_CERT__C" VARCHAR2(200), 
	"DUG__C" VARCHAR2(200), 
	"DUG_TOPONIMO__C" VARCHAR2(200), 
	"EMAIL_ARRICCHIMENTO__C" VARCHAR2(200), 
	"EMAIL_ARRICCHIMENTO_ID__C" VARCHAR2(200), 
	"EMAIL_BOZZE__C" VARCHAR2(200), 
	"EMAIL_BOZZE_ID__C" VARCHAR2(200), 
	"EMAIL_COMMERCIALE_IOL__C" VARCHAR2(200), 
	"EMAIL_COMMERCIALE_IOL_ID__C" VARCHAR2(200), 
	"EMAIL_FATTURAZIONE__C" VARCHAR2(200), 
	"EMAIL_FATTURAZIONE_ID__C" VARCHAR2(200), 
	"EMAIL_POST_VENDITA__C" VARCHAR2(200), 
	"EMAIL_POST_VENDITA_ID__C" VARCHAR2(200), 
	"ENTE_PUBBLICO__C" VARCHAR2(200), 
	"FATTURAZIONE_COD_FISC__C" VARCHAR2(200), 
	"FATT_ELETTR_OBBLIGATORIA__C" VARCHAR2(200), 
	"FRAZIONE__C" VARCHAR2(200), 
	"ID" VARCHAR2(200), 
	"INDUSTRY" VARCHAR2(200), 
	"INFO_TOPONIMO__C" VARCHAR2(200), 
	"INSEGNA__C" VARCHAR2(200), 
	"IOL_ANNOFATTURATO__C" VARCHAR2(200), 
	"IOL_ANNORIFDIPENDENTI__C" VARCHAR2(200), 
	"IOL_BILLINGCOUNTRYCOD__C" VARCHAR2(200), 
	"IOL_BUONOORDINE__C" VARCHAR2(200), 
	"IOL_CATEGORIA__C" VARCHAR2(200), 
	"IOL_CATEGORIAISTAT__C" VARCHAR2(200), 
	"IOL_CATEGORIAMERCEOLOGICA__C" VARCHAR2(200), 
	"IOL_CDBTRANSACTIONID__C" VARCHAR2(200), 
	"IOL_CLASSIFICAZIONEPMI__C" VARCHAR2(200), 
	"IOL_CODICEAREAELENCO__C" VARCHAR2(200), 
	"IOL_CODICECATEGORIAISTAT__C" VARCHAR2(200), 
	"IOL_CODCATEGMERCEOLOGICA__C" VARCHAR2(200), 
	"IOL_CODCATEGMERCMAXSPESA__C" VARCHAR2(200), 
	"IOL_CODICECUSTOMER_OLD__C" VARCHAR2(200), 
	"IOL_CODINDICCLIENTISPEC__C" VARCHAR2(200), 
	"IOL_CODICESEDE_OLD__C" VARCHAR2(200), 
	"IOL_COGNOMEPERSONAFISICA__C" VARCHAR2(200), 
	"IOL_CUSTCESSRIAT__C" VARCHAR2(200), 
	"IOL_CUSTOMERCOUNTRYCOD__C" VARCHAR2(200), 
	"IOL_EMAIL_ARR_PEC__C" VARCHAR2(200), 
	"IOL_EMAIL_BOZZE_PEC__C" VARCHAR2(200), 
	"IOL_EMAIL_COMM_IOL_PEC__C" VARCHAR2(200), 
	"IOL_EMAIL_FATTURAZIONE_PEC__C" VARCHAR2(200), 
	"IOL_EMAIL_POST_VENDITA_PEC__C" VARCHAR2(200), 
	"IOL_INDIRIZZOID__C" VARCHAR2(200), 
	"IOL_MERCATOAGGREGATO__C" VARCHAR2(200), 
	"IOL_NOMEPERSONAFISICA__C" VARCHAR2(200), 
	"IOL_NUMEROREA__C" VARCHAR2(200), 
	"IOL_OPECCONSEGNABILE__C" VARCHAR2(200), 
	"IOL_POTENZIALENIP__C" VARCHAR2(200), 
	"IOL_SEDECESSRIATT_C" VARCHAR2(200), 
	"IOL_SOTTOAREAMERCATO__C" VARCHAR2(200), 
	"IOL_SOTTOCLASSEDIPENDENTI__C" VARCHAR2(200), 
	"IOL_SOTTOCLASSEFATTURATO__C" VARCHAR2(200), 
	"IOL_TIPOMERCATO__C" VARCHAR2(200), 
	"LATITUDINE__C" VARCHAR2(200), 
	"LONGITUDINE__C" VARCHAR2(200), 
	"NAME" VARCHAR2(200), 
	"NAZIONE__C" VARCHAR2(200), 
	"NOTE_SUL_RECAPITO_FATTURA__C" VARCHAR2(200), 
	"NUMBEROFEMPLOYEES" VARCHAR2(200), 
	"NUMERO_CIVICO__C" VARCHAR2(200), 
	"OPEC__C" VARCHAR2(200), 
	"PARTITAIVA__C" VARCHAR2(200), 
	"PHONE" VARCHAR2(200), 
	"PRIMARIO1__C" VARCHAR2(200), 
	"PRIMARIO2__C" VARCHAR2(200), 
	"PRIMARIO3__C" VARCHAR2(200), 
	"PRIMARIO4__C" VARCHAR2(200), 
	"PRIMARIO5__C" VARCHAR2(200), 
	"PRIMARIO6__C" VARCHAR2(200), 
	"PRIMARIO7__C" VARCHAR2(200), 
	"PRIMARIO8__C" VARCHAR2(200), 
	"PROF_COORD_ANAGRAFICHE__C" VARCHAR2(200), 
	"PROVINCIA_IOL__C" VARCHAR2(200), 
	"SEDE_PRIMARIA_OPEC__C" VARCHAR2(200), 
	"TELEFONO_1_ID__C" VARCHAR2(200), 
	"TELEFONO_2_ID__C" VARCHAR2(200), 
	"TELEFONO_3_ID__C" VARCHAR2(200), 
	"TELEFONO_4_ID__C" VARCHAR2(200), 
	"TELEFONO_5_ID__C" VARCHAR2(200), 
	"TELEFONO_6_ID__C" VARCHAR2(200), 
	"TELEFONO_7_ID__C" VARCHAR2(200), 
	"TELEFONO_8_ID__C" VARCHAR2(200), 
	"TELEFONO2__C" VARCHAR2(200), 
	"TELEFONO3__C" VARCHAR2(200), 
	"TELEFONO4__C" VARCHAR2(200), 
	"TELEFONO5__C" VARCHAR2(200), 
	"TELEFONO6__C" VARCHAR2(200), 
	"TELEFONO7__C" VARCHAR2(200), 
	"TELEFONO8__C" VARCHAR2(200), 
	"TIPO_PERSONA_GIURIDICA__C" VARCHAR2(200), 
	"TIPO_SOCIETA_GIURIDICA__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO1__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO2__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO3__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO4__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO5__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO6__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO7__C" VARCHAR2(200), 
	"TIPOLOGIATELEFONO8__C" VARCHAR2(200), 
	"TOPONIMO__C" VARCHAR2(200), 
	"TYPE" VARCHAR2(200), 
	"TYPESOC" VARCHAR2(200), 
	"ULT_POS_AMMINSTRATIVA__C" VARCHAR2(200), 
	"URL_FANPAGE__C" VARCHAR2(200), 
	"URL_ID_FANPAGE__C" VARCHAR2(200), 
	"URL_ID_ISTITUZIONALE__C" VARCHAR2(200), 
	"URL_ID_PAGINE_BIANCHE__C" VARCHAR2(200), 
	"URL_ID_PAGINE_GIALLE__C" VARCHAR2(200), 
	"URL_PAGINE_BIANCHE__C" VARCHAR2(200), 
	"URL_PAGINE_GIALLE__C" VARCHAR2(200), 
	"WEBSITE" VARCHAR2(200)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DA01"
--------------------------------------------------------
--  DDL for Table TCD03_CDB_SEDI_SU_SALF
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" 
   (	"CD03_SEDE_VERS_COD" VARCHAR2(10), 
	"CD03_CUST_COD" VARCHAR2(10), 
	"CD03_SEDE_COD" VARCHAR2(8), 
	 CONSTRAINT "ICD03_CDB_SEDI_SALFPK" PRIMARY KEY ("CD03_SEDE_VERS_COD")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 104857600 NEXT 104857600 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD1DD02"  ENABLE
   ) PCTFREE 10 PCTUSED 0 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD1DD02" 
 

   COMMENT ON COLUMN "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF"."CD03_SEDE_VERS_COD" IS 'Codice Sede||Versione inviato a Salesforce da CDB in Primo Impianto o replica (stato record <> ''I'')'
  GRANT ALTER ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT DELETE ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT INDEX ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT INSERT ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT SELECT ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT UPDATE ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT REFERENCES ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT ON COMMIT REFRESH ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT QUERY REWRITE ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT DEBUG ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
 
  GRANT FLASHBACK ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" TO "DBACD1CDB"
--------------------------------------------------------
--  DDL for Table TMS00_CONFIG
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TMS00_CONFIG" 
   (	"MS00_KEY_COD" VARCHAR2(20) NOT NULL ENABLE, 
	"MS00_KEY_VAL" VARCHAR2(20) NOT NULL ENABLE, 
	"MS00_KEY_DES" VARCHAR2(200), 
	"MS00_SYS_DES" VARCHAR2(10) NOT NULL ENABLE, 
	"MS00_MODI_DTA" DATE DEFAULT SYSDATE NOT NULL ENABLE, 
	 CONSTRAINT "TMS00_CONFIG_PK" PRIMARY KEY ("MS00_KEY_COD")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IA01"  ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DA01"
  GRANT SELECT ON "DBACD8ESB"."TMS00_CONFIG" TO "DBACD1CDB"
--------------------------------------------------------
--  DDL for Table TMS01_LOG
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TMS01_LOG" 
   (	"MS01_LOG_DTA" DATE DEFAULT sysdate NOT NULL ENABLE, 
	"MS01_LOG_MSG" VARCHAR2(2000) NOT NULL ENABLE, 
	"MS01_USER_COD" VARCHAR2(20) NOT NULL ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DE01"
--------------------------------------------------------
--  DDL for Table TMS10_ESITI_CA
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TMS10_ESITI_CA" 
   (	"MS10_SYS_COD" VARCHAR2(10) DEFAULT 'CA' NOT NULL ENABLE, 
	"MS10_RAW_XID_DES" RAW(8) NOT NULL ENABLE, 
	"MS10_PROG_TRNS_DES" VARCHAR2(5) NOT NULL ENABLE, 
	"MS10_STAT_COD" CHAR(1) DEFAULT 'A' NOT NULL ENABLE, 
	"MS10_CUST_COD" VARCHAR2(10) NOT NULL ENABLE, 
	"MS10_PDR_COD" VARCHAR2(8), 
	"MS10_SEDE_COD" VARCHAR2(8) NOT NULL ENABLE, 
	"MS10_SEDE_PREC_COD" VARCHAR2(8), 
	"MS10_INS_DTA" DATE DEFAULT SYSDATE NOT NULL ENABLE, 
	"MS10_POLL_DTA" DATE, 
	"MS10_UPD_DTA" DATE, 
	"MS10_UPD_USER_COD" VARCHAR2(10), 
	"MS10_REPLY_TRX_ID" VARCHAR2(50), 
	"MS10_MULE_TRX_ID" VARCHAR2(50), 
	"MS10_MSG_DES" VARCHAR2(1000), 
	"MS10_CDB_INS_DTA" DATE NOT NULL ENABLE, 
	 CONSTRAINT "TMS10_SYS_COD" CHECK (MS10_SYS_COD IN ('CA')) ENABLE, 
	 CONSTRAINT "TMS10_UPD_USER_COD" CHECK (MS10_UPD_USER_COD IN ('ORACLE','CA','MULE-PREM','MULE-CLOUD')) ENABLE, 
	 CONSTRAINT "TMS10_STAT_COD" CHECK (MS10_STAT_COD IN ('A','D','I','P','Q','R','S','N','O','K')) ENABLE, 
	 CONSTRAINT "TMS10_ESITI_CA_PK" PRIMARY KEY ("MS10_SYS_COD", "MS10_RAW_XID_DES", "MS10_PROG_TRNS_DES")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IE01"  ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DG01"
  GRANT SELECT ON "DBACD8ESB"."TMS10_ESITI_CA" TO "DBACD1CDB"
--------------------------------------------------------
--  DDL for Table TMS11_STORICO_ESITI_CA
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" 
   (	"MS11_SYS_COD" VARCHAR2(10) NOT NULL ENABLE, 
	"MS11_RAW_XID_DES" RAW(8) NOT NULL ENABLE, 
	"MS11_PROG_TRNS_DES" VARCHAR2(5) NOT NULL ENABLE, 
	"MS11_OLD_STAT_COD" CHAR(1) NOT NULL ENABLE, 
	"MS11_NEW_STAT_COD" CHAR(1) NOT NULL ENABLE, 
	"MS11_INS_DTA" DATE DEFAULT SYSDATE NOT NULL ENABLE, 
	"MS11_USER_COD" VARCHAR2(10) NOT NULL ENABLE, 
	"MS11_REPLY_TRX_ID" VARCHAR2(50), 
	"MS11_MULE_TRX_ID" VARCHAR2(50), 
	"MS11_FORCE_FLG" NUMBER(*,0) DEFAULT 0 NOT NULL ENABLE, 
	 CONSTRAINT "TMS11_SYS_COD" CHECK (MS11_SYS_COD IN ('CA')) ENABLE, 
	 CONSTRAINT "TMS11_USER_COD" CHECK (MS11_USER_COD IN ('ORACLE','CA','MULE-PREM','MULE-CLOUD')) ENABLE, 
	 CONSTRAINT "TMS11_FORCE_FLG" CHECK (MS11_FORCE_FLG IN (0,1)) ENABLE, 
	 CONSTRAINT "TMS11_NEW_STAT_COD" CHECK (MS11_NEW_STAT_COD IN ('A','D','I','P','Q','R','S','N','O','K')) ENABLE, 
	 CONSTRAINT "TMS11_OLD_STAT_COD" CHECK (MS11_OLD_STAT_COD IN ('A','D','I','P','Q','R','S','N','O','K')) ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DG03"
--------------------------------------------------------
--  DDL for Table TMS12_POLLED_CA_TRX
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TMS12_POLLED_CA_TRX" 
   (	"MS12_POLL_GUID" VARCHAR2(32) NOT NULL ENABLE, 
	"MS12_TRX_COD" VARCHAR2(21) NOT NULL ENABLE, 
	"MS12_SEDE_COD" VARCHAR2(8) NOT NULL ENABLE, 
	"MS12_SEDE_PREC_COD" VARCHAR2(8), 
	"MS12_POLL_DTA" DATE NOT NULL ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DA01"
--------------------------------------------------------
--  DDL for Table TMS13_POLLED_CA_TK
--------------------------------------------------------

  CREATE TABLE "DBACD8ESB"."TMS13_POLLED_CA_TK" 
   (	"MS13_POLL_GUID" VARCHAR2(32) NOT NULL ENABLE, 
	"MS13_TK_NUM" NUMBER NOT NULL ENABLE, 
	"MS13_POLL_DTA" DATE NOT NULL ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8DA01"
--------------------------------------------------------
--  DDL for Index ICD02_SFDC_DATA_IDX01
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."ICD02_SFDC_DATA_IDX01" ON "DBACD8ESB"."TCD02_SFDC_DATA" ("CD02_RUN_GUID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "TEMPZZ01"
--------------------------------------------------------
--  DDL for Index ICD03_CDB_SEDI_SALFPK
--------------------------------------------------------

  CREATE UNIQUE INDEX "DBACD8ESB"."ICD03_CDB_SEDI_SALFPK" ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" ("CD03_SEDE_VERS_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 104857600 NEXT 104857600 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD1DD02"
--------------------------------------------------------
--  DDL for Index ICD03_CDB_SEDI_SALF_I02
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."ICD03_CDB_SEDI_SALF_I02" ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" ("CD03_SEDE_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 104857600 NEXT 104857600 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD1DD02"
--------------------------------------------------------
--  DDL for Index ICD03_CDB_SEDI_SALF_I03
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."ICD03_CDB_SEDI_SALF_I03" ON "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" ("CD03_CUST_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 104857600 NEXT 104857600 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD1DD02"
--------------------------------------------------------
--  DDL for Index TMS00_CONFIG_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "DBACD8ESB"."TMS00_CONFIG_PK" ON "DBACD8ESB"."TMS00_CONFIG" ("MS00_KEY_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IA01"
--------------------------------------------------------
--  DDL for Index TMS10_ESITI_CA_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "DBACD8ESB"."TMS10_ESITI_CA_PK" ON "DBACD8ESB"."TMS10_ESITI_CA" ("MS10_SYS_COD", "MS10_RAW_XID_DES", "MS10_PROG_TRNS_DES") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IE01"
--------------------------------------------------------
--  DDL for Index IMS10_ESITI_CA_IDX02
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."IMS10_ESITI_CA_IDX02" ON "DBACD8ESB"."TMS10_ESITI_CA" (RAWTOHEX("MS10_RAW_XID_DES")||"MS10_PROG_TRNS_DES") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8ID01"
--------------------------------------------------------
--  DDL for Index IMS10_ESITI_CA_IDX04
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."IMS10_ESITI_CA_IDX04" ON "DBACD8ESB"."TMS10_ESITI_CA" ("MS10_STAT_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IC01"
--------------------------------------------------------
--  DDL for Index IMS10_ESITI_CA_IDX03
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."IMS10_ESITI_CA_IDX03" ON "DBACD8ESB"."TMS10_ESITI_CA" ("MS10_CUST_COD", "MS10_PDR_COD", "MS10_SEDE_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IE01"
--------------------------------------------------------
--  DDL for Index IMS10_ESITI_CA_IDX05
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."IMS10_ESITI_CA_IDX05" ON "DBACD8ESB"."TMS10_ESITI_CA" ("MS10_RAW_XID_DES", "MS10_SEDE_COD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8ID01"
--------------------------------------------------------
--  DDL for Index IMS11_STORICO_ESITI_CA_IDX01
--------------------------------------------------------

  CREATE INDEX "DBACD8ESB"."IMS11_STORICO_ESITI_CA_IDX01" ON "DBACD8ESB"."TMS11_STORICO_ESITI_CA" ("MS11_RAW_XID_DES", "MS11_PROG_TRNS_DES") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8ID01"
--------------------------------------------------------
--  Constraints for Table TCD00_SFDC_INPUT
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_RUN_GUID" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_RUN_STAT" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_CRTN_DATE" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_MODF_DATE" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_DATA_TYP" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_DATA_VAL" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" MODIFY ("CD00_OPER_VAL" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" ADD CONSTRAINT "TCD00_DATA_TYP" CHECK (CD00_DATA_TYP IN ('SEDE', 'CUSTOMER', 'OPEC', 'RAGIONESOCIALE', 'DATAMODIFICA')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" ADD CONSTRAINT "TCD00_OPER_VAL" CHECK (CD00_OPER_VAL IN ('=', '!=', '>', '<', '>=', '<=')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TCD00_SFDC_INPUT" ADD CONSTRAINT "TCD00_RUN_STAT" CHECK (CD00_RUN_STAT IN ('DRAFT', 'NEW', 'PENDING', 'QUEUED', 'DONE', 'FAILED')) ENABLE
--------------------------------------------------------
--  Constraints for Table TCD03_CDB_SEDI_SU_SALF
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TCD03_CDB_SEDI_SU_SALF" ADD CONSTRAINT "ICD03_CDB_SEDI_SALFPK" PRIMARY KEY ("CD03_SEDE_VERS_COD")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 104857600 NEXT 104857600 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD1DD02"  ENABLE
--------------------------------------------------------
--  Constraints for Table TMS00_CONFIG
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TMS00_CONFIG" MODIFY ("MS00_KEY_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS00_CONFIG" MODIFY ("MS00_KEY_VAL" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS00_CONFIG" MODIFY ("MS00_SYS_DES" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS00_CONFIG" MODIFY ("MS00_MODI_DTA" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS00_CONFIG" ADD CONSTRAINT "TMS00_CONFIG_PK" PRIMARY KEY ("MS00_KEY_COD")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IA01"  ENABLE
--------------------------------------------------------
--  Constraints for Table TMS01_LOG
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TMS01_LOG" MODIFY ("MS01_LOG_DTA" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS01_LOG" MODIFY ("MS01_LOG_MSG" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS01_LOG" MODIFY ("MS01_USER_COD" NOT NULL ENABLE)
--------------------------------------------------------
--  Constraints for Table TMS10_ESITI_CA
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_SYS_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_RAW_XID_DES" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_PROG_TRNS_DES" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_STAT_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_CUST_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_SEDE_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_INS_DTA" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" MODIFY ("MS10_CDB_INS_DTA" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" ADD CONSTRAINT "TMS10_ESITI_CA_PK" PRIMARY KEY ("MS10_SYS_COD", "MS10_RAW_XID_DES", "MS10_PROG_TRNS_DES")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 262144 NEXT 262144 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "SCD8IE01"  ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" ADD CONSTRAINT "TMS10_STAT_COD" CHECK (MS10_STAT_COD IN ('A','D','I','P','Q','R','S','N','O','K')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" ADD CONSTRAINT "TMS10_SYS_COD" CHECK (MS10_SYS_COD IN ('CA')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS10_ESITI_CA" ADD CONSTRAINT "TMS10_UPD_USER_COD" CHECK (MS10_UPD_USER_COD IN ('ORACLE','CA','MULE-PREM','MULE-CLOUD')) ENABLE
--------------------------------------------------------
--  Constraints for Table TMS11_STORICO_ESITI_CA
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_SYS_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_RAW_XID_DES" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_PROG_TRNS_DES" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_OLD_STAT_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_NEW_STAT_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_INS_DTA" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_USER_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" MODIFY ("MS11_FORCE_FLG" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" ADD CONSTRAINT "TMS11_FORCE_FLG" CHECK (MS11_FORCE_FLG IN (0,1)) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" ADD CONSTRAINT "TMS11_NEW_STAT_COD" CHECK (MS11_NEW_STAT_COD IN ('A','D','I','P','Q','R','S','N','O','K')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" ADD CONSTRAINT "TMS11_OLD_STAT_COD" CHECK (MS11_OLD_STAT_COD IN ('A','D','I','P','Q','R','S','N','O','K')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" ADD CONSTRAINT "TMS11_SYS_COD" CHECK (MS11_SYS_COD IN ('CA')) ENABLE
 
  ALTER TABLE "DBACD8ESB"."TMS11_STORICO_ESITI_CA" ADD CONSTRAINT "TMS11_USER_COD" CHECK (MS11_USER_COD IN ('ORACLE','CA','MULE-PREM','MULE-CLOUD')) ENABLE
--------------------------------------------------------
--  Constraints for Table TMS12_POLLED_CA_TRX
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TMS12_POLLED_CA_TRX" MODIFY ("MS12_POLL_GUID" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS12_POLLED_CA_TRX" MODIFY ("MS12_TRX_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS12_POLLED_CA_TRX" MODIFY ("MS12_SEDE_COD" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS12_POLLED_CA_TRX" MODIFY ("MS12_POLL_DTA" NOT NULL ENABLE)
--------------------------------------------------------
--  Constraints for Table TMS13_POLLED_CA_TK
--------------------------------------------------------

  ALTER TABLE "DBACD8ESB"."TMS13_POLLED_CA_TK" MODIFY ("MS13_POLL_GUID" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS13_POLLED_CA_TK" MODIFY ("MS13_TK_NUM" NOT NULL ENABLE)
 
  ALTER TABLE "DBACD8ESB"."TMS13_POLLED_CA_TK" MODIFY ("MS13_POLL_DTA" NOT NULL ENABLE)