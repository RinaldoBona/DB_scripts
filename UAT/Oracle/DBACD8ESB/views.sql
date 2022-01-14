--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
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
        cdmc_stat_rels_flg                               AS relsflag,
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
        when not exists ( select 1 from tcd0u_rp_gen_telefono t where t.CD0u_RAW_XID_DES = substr(p.MS12_TRX_COD, 0, 16) and t.cdl1_sede_cod = s.CDL1_SEDE_COD and t.CDMC_STAT_RELS_FLG = '1' ) then 'C'
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
--------------------------------------------------------
--  DDL for View VMS22_POLLED_TKCDB
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD8ESB"."VMS22_POLLED_TKCDB" ("MS22_POLL_GUID", "CD0K_RECD_NUM", "CD0K_INSE_DTA", "CD0K_STAT_RECD_COD", "CD0K_PRTY_FLG", "CDW6_TCK_NUM", "CD97_FONT_COD", "CDW0_TIPO_TCK_COD", "CDW4_STAT_TCK_COD", "CDW2_PROB_NUM", "CDW2_PROB_DES", "CDW6_APRT_TCK_DTA", "CDW6_MODI_TIM", "CDW6_CHIU_TCK_DTA", "CD0K_TIPO_CHIU_DES", "CDW5_NOTA_INT_NUM", "CDW5_NOTA_TCK_DES", "CDW6_TIPO_CLNT_COD", "CDW6_CUST_COD", "CDW6_SEDE_COD", "CDW6_PDR_COD", "CD0K_DENOM_DES", "CDW6_AREA_ELEN_COD", "CD98_MATR_COD", "CDIP_MATR_INP_COD", "CD0K_CASE_SF_NUM", "CD0K_MLSF_MSG_DES", "CD0K_POLL_DTA", "CD0K_MULE_TRX_ID") AS 
  SELECT
        ms22_poll_guid,
        cd0k_recd_num,
        cd0k_inse_dta,
        cd0k_stat_recd_cod,
        cd0k_prty_flg,
        cdw6_tck_num,
        cd97_font_cod,
        /*decode(cdw0_tipo_tck_cod,
            'A', 'APPROVAZIONE',
            'B', 'BLOCCANTE',
            'E', 'ERRORE DI SISTEMA',
            'W', 'WARNING'
        ) as */CDW0_TIPO_TCK_COD,
        /*decode(cdw4_stat_tck_cod, 
            'A', 'IN ATTESA',
            'C', 'CHIUSO',
            'D', 'RILIEVO',
            'L', 'LAVORATO',
            'PC', 'PRESO IN CARICO',
            'S', 'SCARTATO',
            'SU', 'SCARTATO DA UTENZA'
            ) as */cdw4_stat_tck_cod,
        cdw2_prob_num,
        cdw2_prob_des,
        cdw6_aprt_tck_dta,
        --to_date (to_char(CAST(cdw6_aprt_tck_dta AS TIMESTAMP )at time zone 'UTC', 'DD-MM-YYYY HH24:MI:SS' ), 'DD-MM-YYYY HH24:MI:SS') as cdw6_aprt_tck_dta,
        cdw6_modi_tim,
        --to_date (to_char(CAST(cdw6_modi_tim AS TIMESTAMP )at time zone 'UTC', 'DD-MM-YYYY HH24:MI:SS' ), 'DD-MM-YYYY HH24:MI:SS') as cdw6_modi_tim,
        cdw6_chiu_tck_dta,
        --case when cdw6_chiu_tck_dta is not null then to_date (to_char(CAST(cdw6_chiu_tck_dta AS TIMESTAMP )at time zone 'UTC', 'DD-MM-YYYY HH24:MI:SS' ), 'DD-MM-YYYY HH24:MI:SS') else cdw6_chiu_tck_dta end as cdw6_chiu_tck_dta,
        decode(cd0k_tipo_chiu_des,
            'MANUALE', 'Manuale',
            'AUTOMATICA', 'Automatica',
            cd0k_tipo_chiu_des) as cd0k_tipo_chiu_des,
        cdw5_nota_int_num,
        replace(cdw5_nota_tck_des, '"', '\"') as cdw5_nota_tck_des,
        cdw6_tipo_clnt_cod,
        CASE WHEN CDW6_CUST_COD LIKE '%,' THEN substr(CDW6_CUST_COD, 0,length(CDW6_CUST_COD)-1) ELSE CDW6_CUST_COD end as CDW6_CUST_COD,
        cdw6_sede_cod,
        cdw6_pdr_cod,
        cd0k_denom_des,
        cdw6_area_elen_cod,
        cd98_matr_cod,
        cdip_matr_inp_cod,
        cd0k_case_sf_num,
        cd0k_mlsf_msg_des,
        cd0k_poll_dta,
        cd0k_mule_trx_id
    FROM
             tcd0k_rp_ticket_cdb
        JOIN tms22_polled_tkcdb ON cd0k_recd_num = ms22_recd_num
