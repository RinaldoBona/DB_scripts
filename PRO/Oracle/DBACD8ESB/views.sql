--------------------------------------------------------
--  File creato - venerd�-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View V_CDB_CONFRONTO_DETT
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD8ESB"."V_CDB_CONFRONTO_DETT" ("CD02_RUN_GUID", "CD02_SOURCE", "CD02_TIPO_DIFF", "CD02_DESC_DIFF", "CD02_INS_DTA", "ANNO_FONDAZIONE__C", "ANNUALREVENUE", "BILLINGCITY", "BILLINGCOUNTRY", "BILLINGSTATE", "BILLINGSTREET", "CAP__C", "CELLULA__C", "CODICE_COMUNE_IOL__C", "CODICE_CUSTOMER__C", "CODICE_FRAZIONE__C", "CODICE_IPA__C", "CODICE_PROVINCIA_REA__C", "CODICE_SEDE__C", "CODICE_STATO_CUSTOMER__C", "CODICE_STATO_SEDE__C", "CODICEFISCALE__C", "COMUNE_IOL__C", "COORD_ANAG__LATITUDE__S", "COORD_ANAG__LONGITUDE__S", "COORDINATE_MANUALI__C", "DATO_FISC_EST_CERT__C", "DUG__C", "DUG_TOPONIMO__C", "EMAIL_ARRICCHIMENTO__C", "EMAIL_ARRICCHIMENTO_ID__C", "EMAIL_BOZZE__C", "EMAIL_BOZZE_ID__C", "EMAIL_COMMERCIALE_IOL__C", "EMAIL_COMMERCIALE_IOL_ID__C", "EMAIL_FATTURAZIONE__C", "EMAIL_FATTURAZIONE_ID__C", "EMAIL_POST_VENDITA__C", "EMAIL_POST_VENDITA_ID__C", "ENTE_PUBBLICO__C", "FATTURAZIONE_COD_FISC__C", "FATT_ELETTR_OBBLIGATORIA__C", "FRAZIONE__C", "ID", "INDUSTRY", "INFO_TOPONIMO__C", "INSEGNA__C", "IOL_ANNOFATTURATO__C", "IOL_ANNORIFDIPENDENTI__C", "IOL_BILLINGCOUNTRYCOD__C", "IOL_BUONOORDINE__C", "IOL_CATEGORIA__C", "IOL_CATEGORIAISTAT__C", "IOL_CATEGORIAMERCEOLOGICA__C", "IOL_CDBTRANSACTIONID__C", "IOL_CLASSIFICAZIONEPMI__C", "IOL_CODICEAREAELENCO__C", "IOL_CODICECATEGORIAISTAT__C", "IOL_CODCATEGMERCEOLOGICA__C", "IOL_CODCATEGMERCMAXSPESA__C", "IOL_CODICECUSTOMER_OLD__C", "IOL_CODINDICCLIENTISPEC__C", "IOL_CODICESEDE_OLD__C", "IOL_COGNOMEPERSONAFISICA__C", "IOL_CUSTCESSRIAT__C", "IOL_CUSTOMERCOUNTRYCOD__C", "IOL_EMAIL_ARR_PEC__C", "IOL_EMAIL_BOZZE_PEC__C", "IOL_EMAIL_COMM_IOL_PEC__C", "IOL_EMAIL_FATTURAZIONE_PEC__C", "IOL_EMAIL_POST_VENDITA_PEC__C", "IOL_INDIRIZZOID__C", "IOL_MERCATOAGGREGATO__C", "IOL_NOMEPERSONAFISICA__C", "IOL_NUMEROREA__C", "IOL_OPECCONSEGNABILE__C", "IOL_POTENZIALENIP__C", "IOL_SEDECESSRIATT_C", "IOL_SOTTOAREAMERCATO__C", "IOL_SOTTOCLASSEDIPENDENTI__C", "IOL_SOTTOCLASSEFATTURATO__C", "IOL_TIPOMERCATO__C", "LATITUDINE__C", "LONGITUDINE__C", "NAME", "NAZIONE__C", "NOTE_SUL_RECAPITO_FATTURA__C", "NUMBEROFEMPLOYEES", "NUMERO_CIVICO__C", "OPEC__C", "PARTITAIVA__C", "PHONE", "PRIMARIO1__C", "PRIMARIO2__C", "PRIMARIO3__C", "PRIMARIO4__C", "PRIMARIO5__C", "PRIMARIO6__C", "PRIMARIO7__C", "PRIMARIO8__C", "PROF_COORD_ANAGRAFICHE__C", "PROVINCIA_IOL__C", "SEDE_PRIMARIA_OPEC__C", "TELEFONO_1_ID__C", "TELEFONO_2_ID__C", "TELEFONO_3_ID__C", "TELEFONO_4_ID__C", "TELEFONO_5_ID__C", "TELEFONO_6_ID__C", "TELEFONO_7_ID__C", "TELEFONO_8_ID__C", "TELEFONO2__C", "TELEFONO3__C", "TELEFONO4__C", "TELEFONO5__C", "TELEFONO6__C", "TELEFONO7__C", "TELEFONO8__C", "TIPO_PERSONA_GIURIDICA__C", "TIPO_SOCIETA_GIURIDICA__C", "TIPOLOGIATELEFONO1__C", "TIPOLOGIATELEFONO2__C", "TIPOLOGIATELEFONO3__C", "TIPOLOGIATELEFONO4__C", "TIPOLOGIATELEFONO5__C", "TIPOLOGIATELEFONO6__C", "TIPOLOGIATELEFONO7__C", "TIPOLOGIATELEFONO8__C", "TOPONIMO__C", "TYPE", "TYPESOC", "ULT_POS_AMMINSTRATIVA__C", "URL_FANPAGE__C", "URL_ID_FANPAGE__C", "URL_ID_ISTITUZIONALE__C", "URL_ID_PAGINE_BIANCHE__C", "URL_ID_PAGINE_GIALLE__C", "URL_PAGINE_BIANCHE__C", "URL_PAGINE_GIALLE__C", "WEBSITE", "ROW_ID") AS 
  SELECT CD02_RUN_GUID, CD02_SOURCE, CD02_TIPO_DIFF, 
  CASE CD02_TIPO_DIFF 
	WHEN '0' THEN CASE WHEN COMPAREROWS(CD02_RUN_GUID, CODICE_CUSTOMER__C, CODICE_SEDE__C) = '0' THEN 'Diversi' ELSE 'Uguali' END
	WHEN '1' THEN 'Customer CDB mancante su SF'
	WHEN '2' THEN 'Sede CDB mancante su SF'
	WHEN '3' THEN 'Customer SF mancante su CDB'
	WHEN '4' THEN 'Sede SF mancante su CDB' 
	ELSE '?' END AS CD02_DESC_DIFF,
  CD02_INS_DTA, ANNO_FONDAZIONE__C, FORMATNUMBER(ANNUALREVENUE), BILLINGCITY, BILLINGCOUNTRY, BILLINGSTATE, BILLINGSTREET, CAP__C, CELLULA__C, CODICE_COMUNE_IOL__C, CODICE_CUSTOMER__C, CODICE_FRAZIONE__C, CODICE_IPA__C, CODICE_PROVINCIA_REA__C, CODICE_SEDE__C, CODICE_STATO_CUSTOMER__C, CODICE_STATO_SEDE__C, CODICEFISCALE__C, COMUNE_IOL__C, FORMATNUMBER(COORD_ANAG__LATITUDE__S), FORMATNUMBER(COORD_ANAG__LONGITUDE__S), COORDINATE_MANUALI__C, DATO_FISC_EST_CERT__C, DUG__C, DUG_TOPONIMO__C, EMAIL_ARRICCHIMENTO__C, FORMATNUMBER(EMAIL_ARRICCHIMENTO_ID__C), EMAIL_BOZZE__C, FORMATNUMBER(EMAIL_BOZZE_ID__C), EMAIL_COMMERCIALE_IOL__C, FORMATNUMBER(EMAIL_COMMERCIALE_IOL_ID__C), EMAIL_FATTURAZIONE__C, FORMATNUMBER(EMAIL_FATTURAZIONE_ID__C), EMAIL_POST_VENDITA__C, FORMATNUMBER(EMAIL_POST_VENDITA_ID__C), ENTE_PUBBLICO__C, FATTURAZIONE_COD_FISC__C, FATT_ELETTR_OBBLIGATORIA__C, FRAZIONE__C, ID, INDUSTRY, INFO_TOPONIMO__C, INSEGNA__C, IOL_ANNOFATTURATO__C, IOL_ANNORIFDIPENDENTI__C, IOL_BILLINGCOUNTRYCOD__C, IOL_BUONOORDINE__C, IOL_CATEGORIA__C, IOL_CATEGORIAISTAT__C, IOL_CATEGORIAMERCEOLOGICA__C, IOL_CDBTRANSACTIONID__C, IOL_CLASSIFICAZIONEPMI__C, IOL_CODICEAREAELENCO__C, IOL_CODICECATEGORIAISTAT__C, IOL_CODCATEGMERCEOLOGICA__C, IOL_CODCATEGMERCMAXSPESA__C, IOL_CODICECUSTOMER_OLD__C, IOL_CODINDICCLIENTISPEC__C, IOL_CODICESEDE_OLD__C, IOL_COGNOMEPERSONAFISICA__C, IOL_CUSTCESSRIAT__C, IOL_CUSTOMERCOUNTRYCOD__C, IOL_EMAIL_ARR_PEC__C, IOL_EMAIL_BOZZE_PEC__C, IOL_EMAIL_COMM_IOL_PEC__C, IOL_EMAIL_FATTURAZIONE_PEC__C, IOL_EMAIL_POST_VENDITA_PEC__C, FORMATNUMBER(IOL_INDIRIZZOID__C), IOL_MERCATOAGGREGATO__C, IOL_NOMEPERSONAFISICA__C, IOL_NUMEROREA__C, IOL_OPECCONSEGNABILE__C, IOL_POTENZIALENIP__C, IOL_SEDECESSRIATT_C, IOL_SOTTOAREAMERCATO__C, IOL_SOTTOCLASSEDIPENDENTI__C, IOL_SOTTOCLASSEFATTURATO__C, IOL_TIPOMERCATO__C, LATITUDINE__C, LONGITUDINE__C, NAME, NAZIONE__C, NOTE_SUL_RECAPITO_FATTURA__C, NUMBEROFEMPLOYEES, NUMERO_CIVICO__C, OPEC__C, PARTITAIVA__C, PHONE, PRIMARIO1__C, PRIMARIO2__C, PRIMARIO3__C, PRIMARIO4__C, PRIMARIO5__C, PRIMARIO6__C, PRIMARIO7__C, PRIMARIO8__C, PROF_COORD_ANAGRAFICHE__C, PROVINCIA_IOL__C, 
  CASE WHEN CD02_SOURCE = 'CDB' THEN
        CASE WHEN SEDE_PRIMARIA_OPEC__C = 'S' THEN 'true' ELSE 'false' END 
       ELSE SEDE_PRIMARIA_OPEC__C END AS SEDE_PRIMARIA_OPEC__C, 
  FORMATNUMBER(TELEFONO_1_ID__C), FORMATNUMBER(TELEFONO_2_ID__C), FORMATNUMBER(TELEFONO_3_ID__C), FORMATNUMBER(TELEFONO_4_ID__C), FORMATNUMBER(TELEFONO_5_ID__C), FORMATNUMBER(TELEFONO_6_ID__C), FORMATNUMBER(TELEFONO_7_ID__C), FORMATNUMBER(TELEFONO_8_ID__C), TELEFONO2__C, TELEFONO3__C, TELEFONO4__C, TELEFONO5__C, TELEFONO6__C, TELEFONO7__C, TELEFONO8__C, TIPO_PERSONA_GIURIDICA__C, TIPO_SOCIETA_GIURIDICA__C, TIPOLOGIATELEFONO1__C, TIPOLOGIATELEFONO2__C, TIPOLOGIATELEFONO3__C, TIPOLOGIATELEFONO4__C, TIPOLOGIATELEFONO5__C, TIPOLOGIATELEFONO6__C, TIPOLOGIATELEFONO7__C, TIPOLOGIATELEFONO8__C, TOPONIMO__C, TYPE, TYPESOC, ULT_POS_AMMINSTRATIVA__C, URL_FANPAGE__C, URL_ID_FANPAGE__C, URL_ID_ISTITUZIONALE__C, URL_ID_PAGINE_BIANCHE__C, URL_ID_PAGINE_GIALLE__C, URL_PAGINE_BIANCHE__C, URL_PAGINE_GIALLE__C, WEBSITE, rowid
  FROM DBACD8ESB.CDB_CONFRONTO_DETT
  ORDER BY CODICE_CUSTOMER__C, CODICE_SEDE__C, CD02_SOURCE
--------------------------------------------------------
--  DDL for View VMS12_POLLED_TEL_CA
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD8ESB"."VMS12_POLLED_TEL_CA" ("TRX", "RAWXID", "CODICECUSTOMER", "CODICESEDE", "TELEID", "PREFINT", "PREFNAZ", "TELENUM", "STATOTEL", "CELLFLAG", "FAXFLAG", "TELPRIMARIOSEDE", "TELPRIMARIOCUSTOMER", "INDPOTN", "WHTSFLAG", "RELSFLAG", "CONSENSOFLAG", "POLLDTA", "POLLGUID") AS 
  SELECT DISTINCT
        s.cd0v_raw_xid_des || s.cd0v_prog_trns_des       AS trx,
        '' || cd0u_raw_xid_des                           AS rawxid,
        s.cdl0_cust_cod                                  AS codicecustomer,
        s.cdl1_sede_cod                                  AS codicesede,
        cdl4_tele_num                                    AS teleid,
        cdl4_pref_intn_cod                               AS prefint,
        cdl4_pref_nazn_cod                               AS prefnaz,
        cdl4_telf_numr_cod                               AS telenum,
        CASE cdmc_stat_rels_flg
            WHEN '0' THEN
                'N'
            ELSE
                cdlt_con1_tfis_flg
        END                                              AS statotel,
        cdl4_cell_flg                                    AS cellflag,
        cdl4_fax_flg                                     AS faxflag,
        cdmc_prim_tels_flg                               AS telprimariosede,
        cdmc_prim_telc_flg                               AS telprimariocustomer,
        cdmc_ind_potn_num                                AS indpotn,
        cdl4_whatsapp_flg                                AS whtsflag,
        --cdmc_stat_rels_flg                               AS relsflag,
        case when cdlt_con1_tfis_flg = 'N' then '0' else cdmc_stat_rels_flg end AS relsflag,
        cdlt_con1_tfis_flg                               AS consensoflag,
        p.ms12_poll_dta                                  AS polldta,
        p.ms12_poll_guid                                 AS pollguid
    FROM
             tms12_polled_ca_trx p
        JOIN tcd0v_rp_gen_sede      s ON s.cd0v_raw_xid_des || s.cd0v_prog_trns_des = p.ms12_trx_cod
        JOIN tcd0u_rp_gen_telefono  t ON s.cd0v_raw_xid_des = t.cd0u_raw_xid_des
                                        AND s.cdl1_sede_cod = t.cdl1_sede_cod
--------------------------------------------------------
--  DDL for View VMS12_POLLED_TRX_CA
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD8ESB"."VMS12_POLLED_TRX_CA" ("TRX", "RAW_XID_DES", "PROG_TRNS_DES", "COD_CUSTOMER", "COD_SEDE", "COD_VERS_SEDE", "COD_STATO_CUSTOMER", "COD_STATO_SEDE", "COD_SEDE_PREC", "COD_VERS_SEDE_PREC", "RAGIONE_SOCIALE", "COGNOME", "NOME", "PARTITA_IVA", "CODICE_FISCALE", "COD_NAZIONE_CUSTOMER", "NAZIONE_CUSTOMER", "COD_CATEGORIA_CUSTOMER", "CATEGORIA_CUSTOMER", "URL_PRIMARIA_CUSTOMER", "SEDE_PRIMARIA_CUSTOMER", "INSEGNA_SEDE", "DUG_SEDE", "TOPONIMO_SEDE", "CIVICO_SEDE", "INFO_AGG_SEDE", "CAP_SEDE", "COD_LOCALITA_SEDE", "LOCALITA_SEDE", "COD_FRAZIONE_SEDE", "FRAZIONE_SEDE", "PROVINCIA_SEDE", "COD_NAZIONE_SEDE", "NAZIONE_SEDE", "COD_PROF_COORD_SEDE", "COORD_SEDE", "COORD_X_SEDE", "COORD_Y_SEDE", "OPEC", "COD_CATEGORIA_SEDE", "CATEGORIA_SEDE", "DATA_INS_CDB", "POLLDTA", "POLLGUID") AS 
  SELECT 
	s.CD0V_RAW_XID_DES||s.CD0V_PROG_TRNS_DES AS trx,
    s.CD0V_RAW_XID_DES as RAW_XID_DES,
    s.CD0V_PROG_TRNS_DES AS PROG_TRNS_DES,
	s.CDL0_CUST_COD AS COD_CUSTOMER,
	s.CDL1_SEDE_COD AS COD_SEDE,
	s.CDL1_NVER_SEDE_COD AS COD_VERS_SEDE,
	c.CDL0_STAT_COD AS COD_STATO_CUSTOMER,
	case 
        when not exists 
        ( 
            select 1 from tcd0u_rp_gen_telefono t 
            /*where t.CD0u_RAW_XID_DES = substr(p.MS12_TRX_COD, 0, 10) */
            where t.CD0u_RAW_XID_DES = substr(p.MS12_TRX_COD, 0, 16)
            and t.cdl1_sede_cod = s.CDL1_SEDE_COD 
            and (case when t.cdlt_con1_tfis_flg = 'N' then '0' else cdmc_stat_rels_flg end) = '1' 
        ) then 'C'
        when s.CDL1_STAT_COD = 'F' then 'C' else s.CDL1_STAT_COD 
    end AS COD_STATO_SEDE,
	s.CDL1_PREC_SEDE_COD AS COD_SEDE_PREC,
	s.CDL1_PREC_NVER_SEDE_COD AS COD_VERS_SEDE_PREC,
	replace(c.CDL0_RAGI_SOCL_DES, '"', '\"')AS RAGIONE_SOCIALE,
	c.CDL0_COGN_COG AS COGNOME,
	c.CDL0_NOME_NOM AS NOME,
	c.CDL0_PIVA_COD AS PARTITA_IVA,
	c.CDL0_CFIS_COD AS CODICE_FISCALE,
	c.CDL0_NAZN_SEAT_COD AS COD_NAZIONE_CUSTOMER,
	c.CD17_NAZN_SEAT_DES AS NAZIONE_CUSTOMER,
	c.CD81_CATG_PREV_COD AS COD_CATEGORIA_CUSTOMER,
	replace(c.CD81_CATG_PREV_DES, '"', '\"') AS CATEGORIA_CUSTOMER,
	c.CDL6_URL_DES AS URL_PRIMARIA_CUSTOMER,
	s.CDL1_PRIM_SEDC_FLG AS SEDE_PRIMARIA_CUSTOMER,
	replace(s.CDL1_INSG_DES, '"', '\"') AS INSEGNA_SEDE,
	s.CDL3_DUG_DES AS DUG_SEDE,
	replace(s.CDL3_TOPN_STRD_DES, '"', '\"') AS TOPONIMO_SEDE,
	s.CDL3_CIVC_DES AS CIVICO_SEDE,
	s.CDL3_INFG_TOPN_DES AS INFO_AGG_SEDE,
	s.CDL3_CAP_COD AS CAP_SEDE,
	s.CDL3_COMN_SEAT_COD AS COD_LOCALITA_SEDE,
	s.CDL3_COMN_SEAT_DES AS LOCALITA_SEDE,
	s.CDL3_FRAZ_COD AS COD_FRAZIONE_SEDE,
	s.CDL3_FRAZ_SEAT_DES AS FRAZIONE_SEDE,
	s.CDL3_PROV_SEAT_COD AS PROVINCIA_SEDE,
	s.CDL3_NAZN_SEAT_COD AS COD_NAZIONE_SEDE,
	s.CDL3_NAZN_SEAT_DES AS NAZIONE_SEDE,
	s.CD46_PROF_CORD_COD AS COD_PROF_COORD_SEDE,
	s.CD46_PROF_CORD_DES AS COORD_SEDE,
	s.CDL3_CRDX_VAL AS COORD_X_SEDE,
	s.CDL3_CRDY_VAL AS COORD_Y_SEDE,
	s.CDL2_PDR_COD AS OPEC,
	s.CD81_CATG_PREV_COD AS COD_CATEGORIA_SEDE,
	replace(s.CD81_CATG_PREV_DES, '"', '\"') AS CATEGORIA_SEDE,
    TO_CHAR(s.cd0v_inse_dta, 'YYYY-MM-DD HH24:MI:SS') as DATA_INS_CDB,
	P.MS12_POLL_DTA AS POLLDTA,
	P.MS12_POLL_GUID AS POLLGUID 
FROM 
	TMS12_POLLED_CA_TRX p
	JOIN TCD0V_RP_GEN_SEDE s ON s.CD0V_RAW_XID_DES||s.CD0V_PROG_TRNS_DES = p.MS12_TRX_COD
	JOIN TCD0Z_RP_GEN_CUSTOMER c ON c.CD0Z_RAW_XID_DES = s.CD0V_RAW_XID_DES AND c.CDL0_CUST_COD = s.CDL0_CUST_COD
