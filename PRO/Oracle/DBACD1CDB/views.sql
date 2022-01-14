--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View VCD0A_RP_SF_TRANSACTIONS_REL
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0A_RP_SF_TRANSACTIONS_REL" ("PADRE", "DATA", "FIGLIO", "DATAPADRE") AS 
  WITH transactionsCustomerState
        AS (SELECT DISTINCT
                   TO_CHAR (
                         ''
                      || a.CD0A_RAW_XID_DES
                      || NVL (a.CD0A_PROG_TRNS_DES, '000'))
                      AS TransactionId,
                   a.CDL0_CUST_COD AS CustomerId,
                   a.cd0a_stat_recd_cod AS Stato,
                   a.CD0A_STRT_TRNS_TIM AS TransactionDate
              FROM    TCD0A_RP_SF_CUSTOMER a
                   JOIN
                      TCD0B_RP_SF_SEDE b
                   ON     a.CD0A_RAW_XID_DES = b.CD0b_RAW_XID_DES
                      AND a.CD0A_PROG_TRNS_DES = b.CD0B_PROG_TRNS_DES
                      AND a.CDL0_CUST_COD = b.CDL0_CUST_COD
                      AND b.CDL1_PRIM_SEDC_FLG = 'S'
            UNION
            SELECT DISTINCT
                   TO_CHAR (
                         ''
                      || CD0B_RAW_XID_DES
                      || NVL (CD0B_PROG_TRNS_DES, '000')),
                   CDL0_CUST_COD,
                   CD0B_STAT_RECD_COD,
                   CD0B_STRT_TRNS_TIM
              FROM TCD0B_RP_SF_SEDE
             WHERE CDL1_PRIM_SEDC_FLG = 'N'
            MINUS
            SELECT DISTINCT
                   TO_CHAR (
                         ''
                      || CD0B_RAW_XID_DES
                      || NVL (CD0B_PROG_TRNS_DES, '000')),
                   CDL0_CUST_COD,
                   CD0B_STAT_RECD_COD,
                   CD0B_STRT_TRNS_TIM
              FROM TCD0B_RP_SF_SEDE a
             WHERE     CDL1_PRIM_SEDC_FLG = 'S'
                   AND NOT EXISTS
                              (SELECT 1
                                 FROM TCD0A_RP_SF_CUSTOMER b
                                WHERE     CD0A_RAW_XID_DES = CD0b_RAW_XID_DES
                                      AND CD0B_PROG_TRNS_DES =
                                             CD0A_PROG_TRNS_DES
                                      AND a.CDL0_CUST_COD = b.CDL0_CUST_COD)
            MINUS
            SELECT DISTINCT
                   TO_CHAR ('' || a.CD0A_RAW_XID_DES) AS TransactionId,
                   a.CDL0_CUST_COD AS CustomerId,
                   a.cd0a_stat_recd_cod AS Stato,
                   a.CD0A_STRT_TRNS_TIM AS TransactionDate
              FROM TCD0A_RP_SF_CUSTOMER a
             WHERE NOT EXISTS
                          (SELECT 1
                             FROM TCD0B_RP_SF_SEDE b
                            WHERE     CD0A_RAW_XID_DES = CD0b_RAW_XID_DES
                                  AND CD0B_PROG_TRNS_DES = CD0A_PROG_TRNS_DES
                                  AND a.CDL0_CUST_COD = b.CDL0_CUST_COD
                                  AND b.CDL1_PRIM_SEDC_FLG = 'S'))
   SELECT DISTINCT t1.transactionid AS Padre,
                   t2.transactiondate AS data,
                   t2.transactionid AS Figlio,
                   t1.transactiondate AS DATAPADRE
     FROM transactionsCustomerState t1, transactionsCustomerState t2
    WHERE --t1.CustomerId in ( select distinct CustomerId from transactionsCustomerState t2 where t2.transactionid = '0004001400E0BB28' )
         t1   .customerid = t2.customerid
          --and t1.transactiondate = ( select min(t4.transactiondate) from transactionsCustomerState t4 where t4.customerid = t1.customerid)
          AND t1.Stato = 'A'
          AND t2.stato = 'A'
          AND NOT EXISTS
                     (SELECT 1
                        FROM transactionsCustomerState t3
                       WHERE     t3.customerid = t1.customerid
                             AND t3.stato IN ('K', 'Q', 'P'))
  GRANT SELECT ON "DBACD1CDB"."VCD0A_RP_SF_TRANSACTIONS_REL" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for View VCD0A_RP_SF_TRANSACTIONS_REL_P
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0A_RP_SF_TRANSACTIONS_REL_P" ("PADRE", "DATA", "FIGLIO", "DATAPADRE") AS 
  WITH transactionsCustomerState
        AS (SELECT DISTINCT
                   TO_CHAR ('' || CD0A_RAW_XID_DES || cd0a_prog_trns_des)
                      AS TransactionId,
                   CDL0_CUST_COD AS CustomerId,
                   cd0a_stat_recd_cod AS Stato,
                   CD0A_STRT_TRNS_TIM AS TransactionDate
              FROM TCD0A_RP_SF_CUSTOMER
            UNION
            SELECT DISTINCT
                   TO_CHAR ('' || CD0B_RAW_XID_DES || cd0b_prog_trns_des),
                   CDL0_CUST_COD,
                   CD0B_STAT_RECD_COD,
                   CD0B_STRT_TRNS_TIM
              FROM TCD0B_RP_SF_SEDE)
   SELECT DISTINCT t1.transactionid AS Padre,
                   t2.transactiondate AS data,
                   t2.transactionid AS Figlio,
                   t1.transactiondate AS DATAPADRE
     FROM transactionsCustomerState t1, transactionsCustomerState t2
    WHERE --t1.CustomerId in ( select distinct CustomerId from transactionsCustomerState t2 where t2.transactionid = '0004001400E0BB28' )
         t1.customerid = t2.customerid AND t1.Stato = 'P' AND t2.stato = 'P'
  GRANT SELECT ON "DBACD1CDB"."VCD0A_RP_SF_TRANSACTIONS_REL_P" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for View VCD0B_RP_SF_SCTYPE_VIEW
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_RP_SF_SCTYPE_VIEW" ("TIPO", "RAW_XID_DES", "PROG_TRNS_DES", "CUST_COD", "SEDE_COD", "HATELEFONI", "HAMAIL", "EMAILARRICCHIMENTO", "EMAILARRICCHIMENTOID", "EMAILBOZZE", "EMAILBOZZEID", "EMAILCOMMERCIALEIOL", "EMAILCOMMERCIALEIOLID", "EMAILFATTURAZIONE", "EMAILFATTURAZIONEID", "EMAILFATTELETTRONICA", "EMAILFATTELETTRONICAID", "EMAILPOSTVENDITA", "EMAILPOSTVENDITAID", "EMAILARRICCHIMENTOPECFLAG", "EMAILBOZZEPECFLAG", "EMAILCOMMERCIALEIOLPECFLAG", "EMAILFATTURAZIONEPECFLAG", "EMAILFATTELETTRONICAPECFLAG", "EMAILPOSTVENDITAPECFLAG", "TELEFONO1ID", "TELEFONO1", "TELEFONO1PRIMARIO", "TIPOTELEFONO1", "TELEFONO2ID", "TELEFONO2", "TELEFONO2PRIMARIO", "TIPOTELEFONO2", "TELEFONO3ID", "TELEFONO3", "TELEFONO3PRIMARIO", "TIPOTELEFONO3", "TELEFONO4ID", "TELEFONO4", "TELEFONO4PRIMARIO", "TIPOTELEFONO4", "TELEFONO5ID", "TELEFONO5", "TELEFONO5PRIMARIO", "TIPOTELEFONO5", "TELEFONO6ID", "TELEFONO6", "TELEFONO6PRIMARIO", "TIPOTELEFONO6", "TELEFONO7ID", "TELEFONO7", "TELEFONO7PRIMARIO", "TIPOTELEFONO7", "TELEFONO8ID", "TELEFONO8", "TELEFONO8PRIMARIO", "TIPOTELEFONO8") AS 
  WITH sedeCust
        AS (SELECT DISTINCT
                   tipo,
                   raw_xid_des,
                   PROG_TRNS_DES,
                   cust_cod,
                   sede_cod,
                   CASE
                      WHEN EXISTS
                              (SELECT 1
                                 FROM TCD0C_RP_SF_TELEFONO t
                                WHERE     a.raw_xid_des = t.cd0c_raw_xid_des
                                      AND a.PROG_TRNS_DES =
                                             t.CD0c_PROG_TRNS_DES
                                      AND a.CUST_COD = t.CDL0_CUST_COD
                                      AND a.SEDE_COD = t.CDL1_SEDE_COD)
                      THEN
                         '1'
                      ELSE
                         '0'
                   END
                      AS HaTelefoni,
                   CASE
                      WHEN EXISTS
                              (SELECT 1
                                 FROM tcd0d_rp_sf_mail m
                                WHERE     a.raw_xid_des = m.cd0d_raw_xid_des
                                      AND a.PROG_TRNS_DES =
                                             m.CD0d_PROG_TRNS_DES
                                      AND a.CUST_COD = m.CDL0_CUST_COD
                                      AND a.SEDE_COD = m.CDL1_SEDE_COD)
                      THEN
                         '1'
                      ELSE
                         '0'
                   END
                      AS HaMail
              FROM (SELECT DISTINCT 'C' AS tipo,
                                    s.cd0b_raw_xid_des AS raw_xid_des,
                                    s.CD0B_PROG_TRNS_DES AS PROG_TRNS_DES,
                                    s.CDL0_CUST_COD AS cust_cod,
                                    s.CDL1_SEDE_COD AS sede_cod
                      FROM    tcd0b_rp_sf_sede s
                           JOIN
                              tcd0a_rp_sf_customer c
                           ON     s.cd0b_raw_xid_des = c.cd0a_raw_xid_des
                              AND s.CD0B_PROG_TRNS_DES = c.CD0a_PROG_TRNS_DES
                              AND s.CDL0_CUST_COD = c.CDL0_CUST_COD
                     WHERE s.CDL1_PRIM_SEDC_FLG = 'S'
                    UNION
                    SELECT DISTINCT 'S' AS tipo,
                                    s.cd0b_raw_xid_des,
                                    s.CD0B_PROG_TRNS_DES,
                                    s.CDL0_CUST_COD AS cust_cod,
                                    s.CDL1_SEDE_COD AS sede_cod
                      FROM tcd0b_rp_sf_sede s
                     WHERE s.CDL1_PRIM_SEDC_FLG = 'N') a)
   SELECT s.*,
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          t.t1_tel_numr AS Telefono1,                                  --Phone
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          t.t8_tel_type AS TipoTelefono8               --TipologiaTelefono8__c
     FROM sedeCust s
          JOIN VCD0D_RP_SF_MAIL m
             ON     s.raw_xid_des = m.RAW_XID_DES
                AND s.PROG_TRNS_DES = m.PROG_TRNS_DES
                AND s.cust_cod = m.CUSTOMER
                AND s.sede_cod = m.CODICESEDE
          JOIN VCD0C_RP_SF_TELEFONO t
             ON     s.raw_xid_des = t.RAW_XID_DES
                AND s.PROG_TRNS_DES = t.PROG_TRNS_DES
                AND s.cust_cod = t.CUSTOMER
                AND s.sede_cod = t.CODICESEDE
    WHERE s.haTelefoni = '1' AND s.haMail = '1'
   UNION
   SELECT s.*,
          NULL AS EmailArricchimento,                 --Email_Arricchimento__c
          NULL AS EmailArricchimentoId,            --Email_Arricchimento_ID__c
          NULL AS EmailBozze,                                 --Email_Bozze__c
          NULL AS EmailBozzeId,                            --Email_Bozze_ID__c
          NULL AS EmailCommercialeIOL,              --Email_Commerciale_IOL__c
          NULL AS EmailCommercialeIOLId,         --Email_Commerciale_IOL_ID__c
          NULL AS EmailFatturazione,                   --Email_Fatturazione__c
          NULL AS EmailFatturazioneId,              --Email_Fatturazione_ID__c
          NULL AS EmailFattElettronica,    --Email_Fatturazione_Elettronica__c
          NULL AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          NULL AS EmailPostVendita,                    --Email_Post_Vendita__c
          NULL AS EmailPostVenditaId,               --Email_Post_Vendita_ID__c
          NULL AS EmailArricchimentoPECFlag,  --IOL_Email_Arricchimento_PEC__c
          NULL AS EmailBozzePECFlag,                  --IOL_Email_Bozze_PEC__c
          NULL AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          NULL AS EmailFatturazionePECFlag,    --IOL_Email_Fatturazione_PEC__c
          NULL AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          NULL AS EmailPostVenditaPECFlag,     --IOL_Email_Post_Vendita_PEC__c
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          t.t1_tel_numr AS Telefono1,                                  --Phone
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          t.t8_tel_type AS TipoTelefono8               --TipologiaTelefono8__c
     FROM    sedeCust s
          JOIN
             VCD0C_RP_SF_TELEFONO t
          ON     s.raw_xid_des = t.RAW_XID_DES
             AND s.PROG_TRNS_DES = t.PROG_TRNS_DES
             AND s.cust_cod = t.CUSTOMER
             AND s.sede_cod = t.CODICESEDE
    WHERE s.haTelefoni = '1' AND s.haMail = '0'
   UNION
   SELECT s.*,
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          NULL, --TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          NULL, --t.t1_tel_numr AS Telefono1,                                  --Phone
          NULL, --NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          NULL, --t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          NULL, --TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          NULL, --t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NULL, --NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          NULL, --t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          NULL, --TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          NULL, --t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NULL, --NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          NULL, --t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          NULL, --TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          NULL, --t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NULL, --NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          NULL, --t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          NULL, --TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          NULL, --t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NULL, --NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          NULL, --t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          NULL, --TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          NULL, --t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NULL, --NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          NULL, --t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          NULL, --TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          NULL, --t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NULL, --NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          NULL, --t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          NULL, --TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          NULL, --t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NULL, --NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          NULL --t.t8_tel_type AS TipoTelefono8              --TipologiaTelefono8__c
     FROM    sedeCust s
          JOIN
             VCD0D_RP_SF_MAIL m
          ON     s.raw_xid_des = m.RAW_XID_DES
             AND s.PROG_TRNS_DES = m.PROG_TRNS_DES
             AND s.cust_cod = m.CUSTOMER
             AND s.sede_cod = m.CODICESEDE
    WHERE s.haTelefoni = '0' AND s.haMail = '1'
   UNION
   SELECT s."TIPO",
          s."RAW_XID_DES",
          s."PROG_TRNS_DES",
          s."CUST_COD",
          s."SEDE_COD",
          s."HATELEFONI",
          s."HAMAIL",
          NULL, --m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          NULL, --m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          NULL, --m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          NULL, --m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          NULL, --m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          NULL, --m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          NULL, --m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          NULL, --m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          NULL, --m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          NULL, --m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          NULL, --m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          NULL, --m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          NULL, --m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          NULL, --m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          NULL, --m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          NULL, --m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          NULL, --m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          NULL, --m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          NULL, --TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          NULL, --t.t1_tel_numr AS Telefono1,                                  --Phone
          NULL, --NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          NULL, --t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          NULL, --TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          NULL, --t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NULL, --NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          NULL, --t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          NULL, --TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          NULL, --t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NULL, --NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          NULL, --t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          NULL, --TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          NULL, --t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NULL, --NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          NULL, --t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          NULL, --TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          NULL, --t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NULL, --NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          NULL, --t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          NULL, --TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          NULL, --t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NULL, --NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          NULL, --t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          NULL, --TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          NULL, --t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NULL, --NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          NULL, --t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          NULL, --TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          NULL, --t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NULL, --NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          NULL --t.t8_tel_type AS TipoTelefono8              --TipologiaTelefono8__c
     FROM sedeCust s
    WHERE s.haTelefoni = '0' AND s.haMail = '0'
--------------------------------------------------------
--  DDL for View VCD0B_RP_SF_SEDECUST
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_RP_SF_SEDECUST" ("TRANSACTIONID", "CUSTOMERID", "CODICESEDE", "SEDE_COD_TEST", "EVENTO", "ORDCUST", "ORDSEDE", "DATATRANSAZIONE", "STATO", "CUSTOMERID_OLD", "CODICESEDE_OLD", "RAGIONESOCIALE", "ANNOFATTURATO", "ANNOFONDAZIONE", "ANNORIFERIMENTODIPENDENTI", "FATTURATO", "CITTA", "NAZIONESEDE", "CODICEPROVINCIA", "INDIRIZZO", "BUONOORDINE", "CAP", "CODICECATEGORIAMASSIMASPESA", "CATEGORIAMASSIMASPESA", "CELLULA", "CLASSIFICAZIONEPMI", "AREAELENCO", "CODICECATEGORIAISTAT", "CODICECATEGORIAMERCEOLOGICA", "CODICECOMUNE", "STATOCUSTOMER", "CODICEFISCALE", "CODICEFRAZIONE", "IPA", "CODPROVINCIAREA", "NUMEROREA", "STATOSEDE", "COGNOME", "COMUNE", "COORDX", "COORDY", "COORDINATEMANUALI", "DATOFISCALEESTEROCERTIFICATO", "DENOMINAZIONEALTERNATIVA", "CATEGORIAISTAT", "CATEGORIAMERCEOLOGICA", "DUG", "EMAILARRICCHIMENTO", "EMAILARRICCHIMENTOID", "EMAILBOZZE", "EMAILBOZZEID", "EMAILCOMMERCIALEIOL", "EMAILCOMMERCIALEIOLID", "EMAILFATTURAZIONE", "EMAILFATTURAZIONEID", "EMAILFATTELETTRONICA", "EMAILFATTELETTRONICAID", "EMAILPOSTVENDITA", "EMAILPOSTVENDITAID", "EMAILARRICCHIMENTOPECFLAG", "EMAILBOZZEPECFLAG", "EMAILCOMMERCIALEIOLPECFLAG", "EMAILFATTURAZIONEPECFLAG", "EMAILFATTELETTRONICAPECFLAG", "EMAILPOSTVENDITAPECFLAG", "ENTEPUBBLICOFLAG", "CODICEFISCALEFATTURAZIONE", "FATTELETTRONICAOBBLIGATORIA", "FRAZIONE", "IDINDIRIZZO", "INFOTOPONIMO", "INSEGNA", "MERCATOAGGREGATO", "INDUSTRY", "CODICENAZIONE", "DESCNAZIONE", "CODICENAZIONECUSTOMER", "POTENZIALENIP", "NOME", "NOTERECAPITOFATTURA", "CIVICO", "NUMERODIPENDENTI", "OPEC", "OPECCONSEGNABILE", "PARTITAIVA", "PROFCOORDINATEGEOGRAFICHE", "PROVINCIA", "CUSTOMERCESSATORIATTIVABILE", "SEDECESSATARIATTIVABILE", "SEDEPRIMARIAFLAG", "SOTTOAREAMERCATO", "SOTTOCLASSEDIPENDENTI", "SOTTOCLASSEFATTURATO", "STATOOPECCOMMATTUALIZ", "TIPOMERCATO", "TELEFONO1ID", "TELEFONO1", "TELEFONO1PRIMARIO", "TIPOTELEFONO1", "TELEFONO2ID", "TELEFONO2", "TELEFONO2PRIMARIO", "TIPOTELEFONO2", "TELEFONO3ID", "TELEFONO3", "TELEFONO3PRIMARIO", "TIPOTELEFONO3", "TELEFONO4ID", "TELEFONO4", "TELEFONO4PRIMARIO", "TIPOTELEFONO4", "TELEFONO5ID", "TELEFONO5", "TELEFONO5PRIMARIO", "TIPOTELEFONO5", "TELEFONO6ID", "TELEFONO6", "TELEFONO6PRIMARIO", "TIPOTELEFONO6", "TELEFONO7ID", "TELEFONO7", "TELEFONO7PRIMARIO", "TIPOTELEFONO7", "TELEFONO8ID", "TELEFONO8", "TELEFONO8PRIMARIO", "TIPOTELEFONO8", "FORMAGIURIDICA", "TIPOSOCGIURIDICA", "TOPONIMO", "ULTIMAPOSIZIONEAMMINISTRATIVA", "TIPOSOCIETA", "URLFANPAGEID", "URLFANPAGE", "URLISTITUZIONALEID", "URLISTITUZIONALE", "URLPAGINEBIANCHEID", "URLPAGINEBIANCHE", "URLPAGINEGIALLEID", "URLPAGINEGIALLE", "CLIENTESPECIALEFLG", "PRIORITA") AS 
  SELECT /*+ NO_INDEX(s) */
         TO_CHAR (
             '' || c.CD0A_RAW_XID_DES || NVL (c.cd0a_prog_trns_des, '000'))
             AS TransactionId,
          /*c.cd0a_raw_xid_des as raw_xid_des,
          c.cd0a_prog_trns_des as prog_trns_des,*/
          c.CDL0_CUST_COD AS CustomerId,                  --Codice_Customer__c
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede, --Codice_Sede__c
          s.CDL1_SEDE_COD sede_cod_test,
          c.CD0A_TIPO_EVEN_COD AS Evento,
          TO_CHAR (c.CD0A_RECD_NUM) AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (c.cd0a_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          c.cd0a_stat_recd_cod AS Stato,
          c.CDL0_PREC_CUST_COD AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          REPLACE (
             DECODE (c.CDL0_RAGI_SOCL_DES,
                     NULL, c.CDL0_NOME_NOM || ' ' || c.CDL0_COGN_COG,
                     c.CDL0_RAGI_SOCL_DES),
             '"',
             '\"')
             AS RagioneSociale,                                        -- Name
          c.CDL0_ANNO_RIFF_ANN AS AnnoFatturato,        --IOL_AnnoFatturato__c
          c.CDL0_ANNO_FOND_ANN AS AnnoFondazione,         --Anno_Fondazione__c
          c.CDL0_ANNO_RIFD_ANN AS AnnoRiferimentoDipendenti, --IOL_AnnoRiferimentoDipendenti__c
          TO_CHAR (TRUNC (c.CDL0_FATT_PUNT_IMP)) AS Fatturato, --AnnualRevenue
          SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40) AS Citta,         --BillingCity
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,                --BillingCountry
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,              --BillingState
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,                                     --BillingStreet
          DECODE (c.CDL0_BUON_ORDN_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS BuonoOrdine,                              --IOL_BuonoOrdine__c
          s.CDL3_CAP_COD AS Cap,                                      --Cap__c
          c.CD81_CATG_MAXS_COD AS CodiceCategoriaMassimaSpesa, --IOL_CodiceCategoriaMercMassimaSpesa__c
          REPLACE (c.CD81_CATG_MAXS_DES, '"', '\"') AS CategoriaMassimaSpesa, --IOL_Categoria__c
          s.CDL3_NUCL_COMM_COD AS Cellula,                        --Cellula__c
          c.CDLG_CLASS_PMI_DES AS ClassificazionePMI, --IOL_classificazionePMI__c
          s.CD18_AREA_ELEN_COD AS AreaElenco,        --IOL_CodiceAreaElenco__c
          c.CD82_CATG_ISTA_COD AS CodiceCategoriaISTAT, --IOL_CodiceCategoriaISTAT__c
          c.CD81_CATG_PREV_COD AS CodiceCategoriaMerceologica, --IOL_CodiceCategoriaMerceologica__c
          s.CDL3_COMN_SEAT_COD AS CodiceComune,         --Codice_Comune_IOL__c
          c.CDL0_STAT_COD AS StatoCustomer,         --Codice_Stato_Customer__c
          c.CDL0_CFIS_COD AS CodiceFiscale,                 --CodiceFiscale__c
          s.CDL3_FRAZ_COD AS CodiceFrazione,              --Codice_Frazione__c
          --s.CDL1_UNI_ORG_COD AS IPA,                           --Codice_IPA__c
          NULL AS IPA,                                         --Codice_IPA__c
          c.CDFE_CCIAA AS CodProvinciaREA,           --Codice_Provincia_REA__c
          c.CDFE_REA AS NumeroRea,                          --IOL_NumeroREA__c
          s.CDL1_STAT_COD AS StatoSede,                 --Codice_Stato_Sede__c
          c.CDL0_COGN_COG AS Cognome,            --IOL_CognomePersonaFisica__c
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune, --Comune_IOL__c
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX, --Coordinate_Anagrafiche__c (1)
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY, --Coordinate_Anagrafiche__c (2)
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true') -- [RB20190517 - orrore]
             AS CoordinateManuali,                     --Coordinate_Manuali__c
          --c.CDL0_MRST_CUST_FLG as PreScoring, --Credit_Prescoring__c
          DECODE (c.CDL0_DFES_CERT_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS DatoFiscaleEsteroCertificato, --Dato_Fiscale_Estero_Certificato__c
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa, --Denominazione_Alternativa__c
          NULL AS DenominazioneAlternativa,     --Denominazione_Alternativa__c
          REPLACE (SUBSTR (c.CD82_CATG_ISTA_DES, 0, 255), '"', '\"')
             AS CategoriaISTAT,                        --IOL_CategoriaISTAT__c
          REPLACE (c.CD81_CATG_PREV_DES, '"', '\"') AS CategoriaMerceologica, --IOL_CategoriaMerceologica__c
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,                 --DUG__c
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          DECODE (c.CDL0_ENTE_PUBB_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS EntePubblicoFlag,                           --Ente_Pubblico__c
          DECODE (c.CDL0_FATT_CFIS_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS CodiceFiscaleFatturazione,    --Fatturazione_Codice_Fiscale__c
          DECODE (c.CDL0_FATT_OBBL_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS FattElettronicaObbligatoria, --Fatturazione_Elettronica_Obbligatoria__c
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione, --Frazione__c
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,       --IOL_IndirizzoID__c
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo, --Info_Toponimo__c
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,        --Insegna__c
          DECODE (c.CD44_MERC_AGGR_COD, NULL, '0', c.CD44_MERC_AGGR_COD)
             AS MercatoAggregato,                    --IOL_MercatoAggregato__c
          c.CD68_AREA_MERC_COD AS Industry,                         --Industry
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,           --BillingCountryCod
          c.CD17_NAZN_SEAT_DES AS DescNazione,                    --Nazione__c
          c.cdl0_nazn_seat_cod AS CodiceNazioneCustomer, --IOL_CustomerCountrCod__c
          DECODE (c.CDLH_ATRB_NIP_FLG,  'S', 'SI',  'N', 'NO')
             AS PotenzialeNIP,                          --IOL_PotenzialeNIP__c
          c.CDL0_NOME_NOM AS Nome,                  --IOL_NomePersonaFisica__c
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura, --Note_sul_Recapito_Fattura__c
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,   --Numero_Civico__c
          TO_CHAR (c.CDL0_NUMR_DIPE_NUM) AS NumeroDipendenti, --NumberOfEmployees
          s.CDL2_PDR_COD AS Opec,                                    --Opec__c
          DECODE (c.CDLH_CONS_FFVV_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS OpecConsegnabile,                    --IOL_OpecConsegnabile__c
          c.CDL0_PIVA_COD AS PartitaIva,                       --PartitaIva__c
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche, --Profondita_Coordinate_Anagrafiche__c
          s.CDL3_PROV_SEAT_DES AS Provincia,                --Provincia_IOL__c
          DECODE (c.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,    --Sede_Primaria_OPEC__c
          DECODE (c.CDAJ_SARE_MERC_COD, NULL, 'X', c.CDAJ_SARE_MERC_COD)
             AS SottoAreaMercato,                    --IOL_SottoareaMercato__c
          c.CD71_STCL_DIPN_DES AS SottoclasseDipendenti, --IOL_SottoclasseDipendenti__c
          c.CD66_STCL_FATT_DES AS SottoclasseFatturato, --IOL_SottoclasseFatturato__c
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz, --Stato_Opec_Commerciale__Attualizzato__c
          NULL AS STATOOPECCOMMATTUALIZ,
          DECODE (c.CD67_TIPO_MERC_COD, NULL, '0', c.CD67_TIPO_MERC_cod)
             AS TipoMercato,                              --IOL_TipoMercato__c
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          t.t1_tel_numr AS Telefono1,                                  --Phone
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          t.t8_tel_type AS TipoTelefono8,              --TipologiaTelefono8__c
          DECODE (c.CDL0_PERS_GIUR_COD,
                  'PF', 'PF',
                  'PG', 'PG',
                  NULL, 'DD',
                  'DD')
             AS FormaGiuridica,               --Tipologia_Persona_Giuridica__c
          c.CD74_TIPO_SOCT_COD AS TipoSocGiuridica, --Tipologia_societa_giuridica__c
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo, --Toponimo__c
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa, --Ultima_Posizione_Amminstrativa__c
          REPLACE (c.CD74_TIPO_SOCT_DES, '"', '\"') AS TipoSocieta,     --Type
          TO_CHAR (c.CDL6_URL_NUM_FNPG) AS UrlFanPageId,   --URL_ID_Fanpage__c
          c.CDL6_URL_DES_FNPG AS UrlFanPage,                  --URL_Fanpage__c
          TO_CHAR (c.CDL6_URL_NUM_ISTZ) AS UrlIstituzionaleId, --URL_ID_Istituzionale__c
          c.CDL6_URL_DES_ISTZ AS UrlIstituzionale,                   --Website
          TO_CHAR (c.CDL6_URL_NUM_PGBI) AS UrlPagineBiancheId, --URL_ID_Pagine_Bianche__c
          c.CDL6_URL_DES_PGBI AS UrlPagineBianche,     --URL_Pagine_Bianche__c
          TO_CHAR (c.CDL6_URL_NUM_PGGI) AS UrlPagineGialleId, --URL_ID_Pagine_Gialle__c
          c.CDL6_URL_DES_PGGI AS UrlPagineGialle,       --URL_Pagine_Gialle__c
          COALESCE (c.CDAG_SPEC_CLNT_COD, 'G') AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (c.CD0A_PRTY_COD, 'H') AS Priorita
     FROM tcd0b_rp_sf_sede s
          JOIN tcd0a_rp_sf_customer c
             ON     s.cdl0_cust_cod || '0' = c.cdl0_cust_cod || '0'
                AND s.cd0b_stat_recd_cod = c.cd0a_stat_recd_cod
                AND c.cd0a_raw_xid_des = s.cd0b_raw_xid_des
                AND NVL (c.cd0a_prog_trns_des, '000') =
                       NVL (s.cd0b_prog_trns_des, '000')
                --and c.cd0a_prog_trns_des = s.cd0b_prog_trns_des
                AND s.cdl1_prim_sedc_flg = 'S'
          LEFT JOIN "VCD0D_RP_SF_MAIL_idx" m
             ON     m.codicesede || '0' = s.cdl1_sede_cod || '0'
                AND s.cd0b_stat_recd_cod = m.stato
                AND m.customer || '0' = c.cdl0_cust_cod || '0'
                AND m.transactionid =
                       TO_CHAR (
                             ''
                          || c.CD0A_RAW_XID_DES
                          || NVL (c.cd0a_prog_trns_des, '000'))
          /*and m.raw_xid_des = c.CD0A_RAW_XID_DES
          and m.prog_trns_des = c.cd0a_prog_trns_des*/
          LEFT JOIN "VCD0C_RP_SF_TELEFONO_idx" t
             ON     t.codicesede || '0' = s.cdl1_sede_cod || '0'
                AND t.customer || '0' = c.cdl0_cust_cod || '0'
                AND s.cd0b_stat_recd_cod = t.stato
                AND t.transactionid =
                       TO_CHAR (
                             ''
                          || c.CD0A_RAW_XID_DES
                          || NVL (c.cd0a_prog_trns_des, '000'))
   /*and t.raw_xid_des = c.CD0A_RAW_XID_DES
   and t.prog_trns_des = c.cd0a_prog_trns_des*/
   UNION
   SELECT /*+ NO_INDEX(s) */
         TO_CHAR (
             '' || s.CD0b_RAW_XID_DES || NVL (s.cd0b_prog_trns_des, '000'))
             AS TransactionId,
          /*s.cd0b_raw_xid_des as raw_xid_des,
          s.cd0b_prog_trns_des as prog_trns_des,*/
          s.cdl0_cust_cod AS CustomerId,
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede,
          s.CDL1_SEDE_COD sede_cod_test,
          s.CD0B_TIPO_EVEN_COD AS Evento,
          NULL AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (s.cd0b_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          s.cd0b_stat_recd_cod AS Stato,
          NULL AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          NULL AS RagioneSociale,
          NULL AS AnnoFatturato,
          NULL AS AnnoFondazione,
          NULL AS AnnoRiferimentoDipendenti,
          NULL AS Fatturato,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Citta,
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,
          'false' AS BuonoOrdine,
          s.CDL3_CAP_COD AS Cap,
          NULL AS CodiceCategoriaMassimaSpesa,
          NULL AS CategoriaMassimaSpesa,
          s.CDL3_NUCL_COMM_COD AS Cellula,
          NULL AS ClassificazionePMI,
          s.CD18_AREA_ELEN_COD AS AreaElenco,
          NULL AS CodiceCategoriaISTAT,
          NULL AS CodiceCategoriaMerceologica,
          s.CDL3_COMN_SEAT_COD AS CodiceComune,
          NULL AS StatoCustomer,
          NULL AS CodiceFiscale,
          s.CDL3_FRAZ_COD AS CodiceFrazione,
          --s.CDL1_UNI_ORG_COD AS IPA,
          NULL AS IPA,
          NULL AS CodProvinciaREA,
          NULL AS NumeroRea,
          s.CDL1_STAT_COD AS StatoSede,
          NULL AS Cognome,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune,
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX,
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY,
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true')
             AS CoordinateManuali,
          --null as PreScoring,
          'false' AS DatoFiscaleEsteroCertificato,
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa,
          NULL AS DenominazioneAlternativa,
          NULL AS CategoriaISTAT,
          NULL AS CategoriaMerceologica,
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          'false' AS EntePubblicoFlag,
          'false' AS CodiceFiscaleFatturazione,
          'false' AS FattElettronicaObbligatoria,
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione,
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo,
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,
          '0' AS MercatoAggregato,
          NULL AS Industry,
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,
          NULL AS DescNazione,
          NULL AS CodiceNazioneCustomer,            --IOL_CustomerCountrCod__c
          'NO' AS PotenzialeNIP,
          NULL AS Nome,
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura,
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,
          NULL AS NumeroDipendenti,
          s.CDL2_PDR_COD AS Opec,
          'false' AS OpecConsegnabile,
          NULL AS PartitaIva,
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche,
          s.CDL3_PROV_SEAT_DES AS Provincia,
          'true' AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,
          'X' AS SottoAreaMercato,
          NULL AS SottoclasseDipendenti,
          NULL AS SottoclasseFatturato,
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz,
          NULL AS STATOOPECCOMMATTUALIZ,
          '0' AS TipoMercato,
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,
          t.t1_tel_numr AS Telefono1,
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,
          t.t1_tel_type AS TipoTelefono1,
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,
          t.t2_tel_numr AS Telefono2,
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,
          t.t2_tel_type AS TipoTelefono2,
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,
          t.t3_tel_numr AS Telefono3,
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,
          t.t3_tel_type AS TipoTelefono3,
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,
          t.t4_tel_numr AS Telefono4,
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,
          t.t4_tel_type AS TipoTelefono4,
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,
          t.t5_tel_numr AS Telefono5,
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,
          t.t5_tel_type AS TipoTelefono5,
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,
          t.t6_tel_numr AS Telefono6,
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,
          t.t6_tel_type AS TipoTelefono6,
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,
          t.t7_tel_numr AS Telefono7,
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,
          t.t7_tel_type AS TipoTelefono7,
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,
          t.t8_tel_numr AS Telefono8,
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,
          t.t8_tel_type AS TipoTelefono8,
          'DD' AS FormaGiuridica,
          NULL AS TipoSocGiuridica,
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo,
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa,
          NULL AS TipoSocieta,
          NULL AS UrlFanPageId,
          NULL AS UrlFanPage,
          NULL AS UrlIstituzionaleId,
          NULL AS UrlIstituzionale,
          NULL AS UrlPagineBiancheId,
          NULL AS UrlPagineBianche,
          NULL AS UrlPagineGialleId,
          NULL AS UrlPagineGialle,
          NULL AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (s.CD0b_PRTY_COD, 'H')
     FROM tcd0b_rp_sf_sede s
          LEFT JOIN "VCD0D_RP_SF_MAIL_idx" m
             ON     s.cdl1_sede_cod || '0' = m.codicesede || '0'
                AND s.cd0b_stat_recd_cod = m.stato
                AND s.cdl0_cust_cod || '0' = m.customer || '0'
                AND m.transactionid =
                       TO_CHAR (
                             ''
                          || s.CD0b_RAW_XID_DES
                          || NVL (s.cd0b_prog_trns_des, '000'))
          /*and m.raw_xid_des = s.CD0b_RAW_XID_DES
          and m.prog_trns_des = s.cd0b_prog_trns_des*/
          LEFT JOIN "VCD0C_RP_SF_TELEFONO_idx" t
             ON     t.codicesede || '0' = s.cdl1_sede_cod || '0'
                AND s.cd0b_stat_recd_cod = t.stato
                AND t.customer || '0' = s.cdl0_cust_cod || '0'
                AND t.transactionid =
                       TO_CHAR (
                             ''
                          || s.CD0b_RAW_XID_DES
                          || NVL (s.cd0b_prog_trns_des, '000'))
    /*and t.raw_xid_des = s.CD0b_RAW_XID_DES
    and t.prog_trns_des = s.cd0b_prog_trns_des            */
    WHERE s.cdl1_prim_sedc_flg = 'N'
  GRANT SELECT ON "DBACD1CDB"."VCD0B_RP_SF_SEDECUST" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for View VCD0B_RP_SF_SEDECUST_idx
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_RP_SF_SEDECUST_idx" ("TRANSACTIONID", "CUSTOMERID", "CODICESEDE", "SEDE_COD_TEST", "EVENTO", "ORDCUST", "ORDSEDE", "DATATRANSAZIONE", "STATO", "CUSTOMERID_OLD", "CODICESEDE_OLD", "RAGIONESOCIALE", "ANNOFATTURATO", "ANNOFONDAZIONE", "ANNORIFERIMENTODIPENDENTI", "FATTURATO", "CITTA", "NAZIONESEDE", "CODICEPROVINCIA", "INDIRIZZO", "BUONOORDINE", "CAP", "CODICECATEGORIAMASSIMASPESA", "CATEGORIAMASSIMASPESA", "CELLULA", "CLASSIFICAZIONEPMI", "AREAELENCO", "CODICECATEGORIAISTAT", "CODICECATEGORIAMERCEOLOGICA", "CODICECOMUNE", "STATOCUSTOMER", "CODICEFISCALE", "CODICEFRAZIONE", "IPA", "CODPROVINCIAREA", "NUMEROREA", "STATOSEDE", "COGNOME", "COMUNE", "COORDX", "COORDY", "COORDINATEMANUALI", "DATOFISCALEESTEROCERTIFICATO", "DENOMINAZIONEALTERNATIVA", "CATEGORIAISTAT", "CATEGORIAMERCEOLOGICA", "DUG", "EMAILARRICCHIMENTO", "EMAILARRICCHIMENTOID", "EMAILBOZZE", "EMAILBOZZEID", "EMAILCOMMERCIALEIOL", "EMAILCOMMERCIALEIOLID", "EMAILFATTURAZIONE", "EMAILFATTURAZIONEID", "EMAILFATTELETTRONICA", "EMAILFATTELETTRONICAID", "EMAILPOSTVENDITA", "EMAILPOSTVENDITAID", "EMAILARRICCHIMENTOPECFLAG", "EMAILBOZZEPECFLAG", "EMAILCOMMERCIALEIOLPECFLAG", "EMAILFATTURAZIONEPECFLAG", "EMAILFATTELETTRONICAPECFLAG", "EMAILPOSTVENDITAPECFLAG", "ENTEPUBBLICOFLAG", "CODICEFISCALEFATTURAZIONE", "FATTELETTRONICAOBBLIGATORIA", "FRAZIONE", "IDINDIRIZZO", "INFOTOPONIMO", "INSEGNA", "MERCATOAGGREGATO", "INDUSTRY", "CODICENAZIONE", "DESCNAZIONE", "CODICENAZIONECUSTOMER", "POTENZIALENIP", "NOME", "NOTERECAPITOFATTURA", "CIVICO", "NUMERODIPENDENTI", "OPEC", "OPECCONSEGNABILE", "PARTITAIVA", "PROFCOORDINATEGEOGRAFICHE", "PROVINCIA", "CUSTOMERCESSATORIATTIVABILE", "SEDECESSATARIATTIVABILE", "SEDEPRIMARIAFLAG", "SOTTOAREAMERCATO", "SOTTOCLASSEDIPENDENTI", "SOTTOCLASSEFATTURATO", "STATOOPECCOMMATTUALIZ", "TIPOMERCATO", "TELEFONO1ID", "TELEFONO1", "TELEFONO1PRIMARIO", "TIPOTELEFONO1", "TELEFONO2ID", "TELEFONO2", "TELEFONO2PRIMARIO", "TIPOTELEFONO2", "TELEFONO3ID", "TELEFONO3", "TELEFONO3PRIMARIO", "TIPOTELEFONO3", "TELEFONO4ID", "TELEFONO4", "TELEFONO4PRIMARIO", "TIPOTELEFONO4", "TELEFONO5ID", "TELEFONO5", "TELEFONO5PRIMARIO", "TIPOTELEFONO5", "TELEFONO6ID", "TELEFONO6", "TELEFONO6PRIMARIO", "TIPOTELEFONO6", "TELEFONO7ID", "TELEFONO7", "TELEFONO7PRIMARIO", "TIPOTELEFONO7", "TELEFONO8ID", "TELEFONO8", "TELEFONO8PRIMARIO", "TIPOTELEFONO8", "FORMAGIURIDICA", "TIPOSOCGIURIDICA", "TOPONIMO", "ULTIMAPOSIZIONEAMMINISTRATIVA", "TIPOSOCIETA", "URLFANPAGEID", "URLFANPAGE", "URLISTITUZIONALEID", "URLISTITUZIONALE", "URLPAGINEBIANCHEID", "URLPAGINEBIANCHE", "URLPAGINEGIALLEID", "URLPAGINEGIALLE", "CLIENTESPECIALEFLG", "PRIORITA") AS 
  SELECT TO_CHAR (
             '' || c.CD0A_RAW_XID_DES || NVL (c.cd0a_prog_trns_des, '000'))
             AS TransactionId,
          /*c.cd0a_raw_xid_des as raw_xid_des,
          c.cd0a_prog_trns_des as prog_trns_des,*/
          c.CDL0_CUST_COD AS CustomerId,                  --Codice_Customer__c
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede, --Codice_Sede__c
          s.CDL1_SEDE_COD sede_cod_test,
          c.CD0A_TIPO_EVEN_COD AS Evento,
          TO_CHAR (c.CD0A_RECD_NUM) AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (c.cd0a_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          c.cd0a_stat_recd_cod AS Stato,
          c.CDL0_PREC_CUST_COD AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          REPLACE (
             DECODE (c.CDL0_RAGI_SOCL_DES,
                     NULL, c.CDL0_NOME_NOM || ' ' || c.CDL0_COGN_COG,
                     c.CDL0_RAGI_SOCL_DES),
             '"',
             '\"')
             AS RagioneSociale,                                        -- Name
          c.CDL0_ANNO_RIFF_ANN AS AnnoFatturato,        --IOL_AnnoFatturato__c
          c.CDL0_ANNO_FOND_ANN AS AnnoFondazione,         --Anno_Fondazione__c
          c.CDL0_ANNO_RIFD_ANN AS AnnoRiferimentoDipendenti, --IOL_AnnoRiferimentoDipendenti__c
          TO_CHAR (TRUNC (c.CDL0_FATT_PUNT_IMP)) AS Fatturato, --AnnualRevenue
          SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40) AS Citta,         --BillingCity
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,                --BillingCountry
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,              --BillingState
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,                                     --BillingStreet
          DECODE (c.CDL0_BUON_ORDN_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS BuonoOrdine,                              --IOL_BuonoOrdine__c
          s.CDL3_CAP_COD AS Cap,                                      --Cap__c
          c.CD81_CATG_MAXS_COD AS CodiceCategoriaMassimaSpesa, --IOL_CodiceCategoriaMercMassimaSpesa__c
          REPLACE (c.CD81_CATG_MAXS_DES, '"', '\"') AS CategoriaMassimaSpesa, --IOL_Categoria__c
          s.CDL3_NUCL_COMM_COD AS Cellula,                        --Cellula__c
          c.CDLG_CLASS_PMI_DES AS ClassificazionePMI, --IOL_classificazionePMI__c
          s.CD18_AREA_ELEN_COD AS AreaElenco,        --IOL_CodiceAreaElenco__c
          c.CD82_CATG_ISTA_COD AS CodiceCategoriaISTAT, --IOL_CodiceCategoriaISTAT__c
          c.CD81_CATG_PREV_COD AS CodiceCategoriaMerceologica, --IOL_CodiceCategoriaMerceologica__c
          s.CDL3_COMN_SEAT_COD AS CodiceComune,         --Codice_Comune_IOL__c
          c.CDL0_STAT_COD AS StatoCustomer,         --Codice_Stato_Customer__c
          c.CDL0_CFIS_COD AS CodiceFiscale,                 --CodiceFiscale__c
          s.CDL3_FRAZ_COD AS CodiceFrazione,              --Codice_Frazione__c
          --s.CDL1_UNI_ORG_COD AS IPA,                           --Codice_IPA__c
          NULL AS IPA,                                         --Codice_IPA__c
          c.CDFE_CCIAA AS CodProvinciaREA,           --Codice_Provincia_REA__c
          c.CDFE_REA AS NumeroRea,                          --IOL_NumeroREA__c
          s.CDL1_STAT_COD AS StatoSede,                 --Codice_Stato_Sede__c
          c.CDL0_COGN_COG AS Cognome,            --IOL_CognomePersonaFisica__c
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune, --Comune_IOL__c
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX, --Coordinate_Anagrafiche__c (1)
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY, --Coordinate_Anagrafiche__c (2)
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true') -- [RB20190517 - orrore]
             AS CoordinateManuali,                     --Coordinate_Manuali__c
          --c.CDL0_MRST_CUST_FLG as PreScoring, --Credit_Prescoring__c
          DECODE (c.CDL0_DFES_CERT_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS DatoFiscaleEsteroCertificato, --Dato_Fiscale_Estero_Certificato__c
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa, --Denominazione_Alternativa__c
          NULL AS DenominazioneAlternativa,     --Denominazione_Alternativa__c
          REPLACE (SUBSTR (c.CD82_CATG_ISTA_DES, 0, 255), '"', '\"')
             AS CategoriaISTAT,                        --IOL_CategoriaISTAT__c
          REPLACE (c.CD81_CATG_PREV_DES, '"', '\"') AS CategoriaMerceologica, --IOL_CategoriaMerceologica__c
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,                 --DUG__c
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          DECODE (c.CDL0_ENTE_PUBB_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS EntePubblicoFlag,                           --Ente_Pubblico__c
          DECODE (c.CDL0_FATT_CFIS_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS CodiceFiscaleFatturazione,    --Fatturazione_Codice_Fiscale__c
          DECODE (c.CDL0_FATT_OBBL_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS FattElettronicaObbligatoria, --Fatturazione_Elettronica_Obbligatoria__c
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione, --Frazione__c
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,       --IOL_IndirizzoID__c
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo, --Info_Toponimo__c
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,        --Insegna__c
          DECODE (c.CD44_MERC_AGGR_COD, NULL, '0', c.CD44_MERC_AGGR_COD)
             AS MercatoAggregato,                    --IOL_MercatoAggregato__c
          c.CD68_AREA_MERC_COD AS Industry,                         --Industry
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,           --BillingCountryCod
          c.CD17_NAZN_SEAT_DES AS DescNazione,                    --Nazione__c
          c.cdl0_nazn_seat_cod AS CodiceNazioneCustomer, --IOL_CustomerCountrCod__c
          DECODE (c.CDLH_ATRB_NIP_FLG,  'S', 'SI',  'N', 'NO')
             AS PotenzialeNIP,                          --IOL_PotenzialeNIP__c
          c.CDL0_NOME_NOM AS Nome,                  --IOL_NomePersonaFisica__c
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura, --Note_sul_Recapito_Fattura__c
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,   --Numero_Civico__c
          TO_CHAR (c.CDL0_NUMR_DIPE_NUM) AS NumeroDipendenti, --NumberOfEmployees
          s.CDL2_PDR_COD AS Opec,                                    --Opec__c
          DECODE (c.CDLH_CONS_FFVV_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS OpecConsegnabile,                    --IOL_OpecConsegnabile__c
          c.CDL0_PIVA_COD AS PartitaIva,                       --PartitaIva__c
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche, --Profondita_Coordinate_Anagrafiche__c
          s.CDL3_PROV_SEAT_DES AS Provincia,                --Provincia_IOL__c
          DECODE (c.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,    --Sede_Primaria_OPEC__c
          DECODE (c.CDAJ_SARE_MERC_COD, NULL, 'X', c.CDAJ_SARE_MERC_COD)
             AS SottoAreaMercato,                    --IOL_SottoareaMercato__c
          c.CD71_STCL_DIPN_DES AS SottoclasseDipendenti, --IOL_SottoclasseDipendenti__c
          c.CD66_STCL_FATT_DES AS SottoclasseFatturato, --IOL_SottoclasseFatturato__c
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz, --Stato_Opec_Commerciale__Attualizzato__c
          NULL AS STATOOPECCOMMATTUALIZ,
          DECODE (c.CD67_TIPO_MERC_COD, NULL, '0', c.CD67_TIPO_MERC_cod)
             AS TipoMercato,                              --IOL_TipoMercato__c
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          t.t1_tel_numr AS Telefono1,                                  --Phone
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          t.t8_tel_type AS TipoTelefono8,              --TipologiaTelefono8__c
          DECODE (c.CDL0_PERS_GIUR_COD,
                  'PF', 'PF',
                  'PG', 'PG',
                  NULL, 'DD',
                  'DD')
             AS FormaGiuridica,               --Tipologia_Persona_Giuridica__c
          c.CD74_TIPO_SOCT_COD AS TipoSocGiuridica, --Tipologia_societa_giuridica__c
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo, --Toponimo__c
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa, --Ultima_Posizione_Amminstrativa__c
          REPLACE (c.CD74_TIPO_SOCT_DES, '"', '\"') AS TipoSocieta,     --Type
          TO_CHAR (c.CDL6_URL_NUM_FNPG) AS UrlFanPageId,   --URL_ID_Fanpage__c
          c.CDL6_URL_DES_FNPG AS UrlFanPage,                  --URL_Fanpage__c
          TO_CHAR (c.CDL6_URL_NUM_ISTZ) AS UrlIstituzionaleId, --URL_ID_Istituzionale__c
          c.CDL6_URL_DES_ISTZ AS UrlIstituzionale,                   --Website
          TO_CHAR (c.CDL6_URL_NUM_PGBI) AS UrlPagineBiancheId, --URL_ID_Pagine_Bianche__c
          c.CDL6_URL_DES_PGBI AS UrlPagineBianche,     --URL_Pagine_Bianche__c
          TO_CHAR (c.CDL6_URL_NUM_PGGI) AS UrlPagineGialleId, --URL_ID_Pagine_Gialle__c
          c.CDL6_URL_DES_PGGI AS UrlPagineGialle,       --URL_Pagine_Gialle__c
          COALESCE (c.CDAG_SPEC_CLNT_COD, 'G') AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (c.CD0A_PRTY_COD, 'H') AS Priorita
     FROM tcd0b_rp_sf_sede s
          JOIN tcd0a_rp_sf_customer c
             ON     s.cdl0_cust_cod = c.cdl0_cust_cod
                AND s.cd0b_stat_recd_cod = c.cd0a_stat_recd_cod
                AND c.cd0a_raw_xid_des = s.cd0b_raw_xid_des
                /*AND NVL (c.cd0a_prog_trns_des, '000') =
                       NVL (s.cd0b_prog_trns_des, '000')*/
                AND c.cd0a_prog_trns_des = s.cd0b_prog_trns_des
                AND s.cdl1_prim_sedc_flg = 'S'
          LEFT JOIN "VCD0D_RP_SF_MAIL_idx" m
             ON     m.codicesede = s.cdl1_sede_cod
                --and s.cd0b_stat_recd_cod = m.stato
                AND m.transactionid = --s.CD0b_RAW_XID_DES||s.cd0b_prog_trns_des
                       TO_CHAR (
                             ''
                          || c.CD0A_RAW_XID_DES
                          || NVL (c.cd0a_prog_trns_des, '000'))
          /*and m.raw_xid_des = c.CD0A_RAW_XID_DES
          and m.prog_trns_des = c.cd0a_prog_trns_des*/
          LEFT JOIN "VCD0C_RP_SF_TELEFONO_idx" t
             ON     t.codicesede = s.cdl1_sede_cod
                --and s.cd0b_stat_recd_cod = t.stato
                AND t.transactionid = --s.CD0b_RAW_XID_DES||s.cd0b_prog_trns_des
                       TO_CHAR (
                             ''
                          || c.CD0A_RAW_XID_DES
                          || NVL (c.cd0a_prog_trns_des, '000'))
   /* and t.raw_xid_des = c.CD0A_RAW_XID_DES
    and t.prog_trns_des = c.cd0a_prog_trns_des*/
   UNION
   SELECT TO_CHAR (
             '' || s.CD0b_RAW_XID_DES || NVL (s.cd0b_prog_trns_des, '000'))
             AS TransactionId,
          /*s.cd0b_raw_xid_des as raw_xid_des,
          s.cd0b_prog_trns_des as prog_trns_des,*/
          s.cdl0_cust_cod AS CustomerId,
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede,
          s.CDL1_SEDE_COD sede_cod_test,
          s.CD0B_TIPO_EVEN_COD AS Evento,
          NULL AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (s.cd0b_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          s.cd0b_stat_recd_cod AS Stato,
          NULL AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          NULL AS RagioneSociale,
          NULL AS AnnoFatturato,
          NULL AS AnnoFondazione,
          NULL AS AnnoRiferimentoDipendenti,
          NULL AS Fatturato,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Citta,
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,
          'false' AS BuonoOrdine,
          s.CDL3_CAP_COD AS Cap,
          NULL AS CodiceCategoriaMassimaSpesa,
          NULL AS CategoriaMassimaSpesa,
          s.CDL3_NUCL_COMM_COD AS Cellula,
          NULL AS ClassificazionePMI,
          s.CD18_AREA_ELEN_COD AS AreaElenco,
          NULL AS CodiceCategoriaISTAT,
          NULL AS CodiceCategoriaMerceologica,
          s.CDL3_COMN_SEAT_COD AS CodiceComune,
          NULL AS StatoCustomer,
          NULL AS CodiceFiscale,
          s.CDL3_FRAZ_COD AS CodiceFrazione,
          --s.CDL1_UNI_ORG_COD AS IPA,
          NULL AS IPA,
          NULL AS CodProvinciaREA,
          NULL AS NumeroRea,
          s.CDL1_STAT_COD AS StatoSede,
          NULL AS Cognome,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune,
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX,
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY,
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true')
             AS CoordinateManuali,
          --null as PreScoring,
          'false' AS DatoFiscaleEsteroCertificato,
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa,
          NULL AS DenominazioneAlternativa,
          NULL AS CategoriaISTAT,
          NULL AS CategoriaMerceologica,
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          'false' AS EntePubblicoFlag,
          'false' AS CodiceFiscaleFatturazione,
          'false' AS FattElettronicaObbligatoria,
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione,
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo,
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,
          '0' AS MercatoAggregato,
          NULL AS Industry,
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,
          NULL AS DescNazione,
          NULL AS CodiceNazioneCustomer,            --IOL_CustomerCountrCod__c
          'NO' AS PotenzialeNIP,
          NULL AS Nome,
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura,
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,
          NULL AS NumeroDipendenti,
          s.CDL2_PDR_COD AS Opec,
          'false' AS OpecConsegnabile,
          NULL AS PartitaIva,
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche,
          s.CDL3_PROV_SEAT_DES AS Provincia,
          'true' AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,
          'X' AS SottoAreaMercato,
          NULL AS SottoclasseDipendenti,
          NULL AS SottoclasseFatturato,
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz,
          NULL AS STATOOPECCOMMATTUALIZ,
          '0' AS TipoMercato,
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,
          t.t1_tel_numr AS Telefono1,
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,
          t.t1_tel_type AS TipoTelefono1,
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,
          t.t2_tel_numr AS Telefono2,
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,
          t.t2_tel_type AS TipoTelefono2,
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,
          t.t3_tel_numr AS Telefono3,
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,
          t.t3_tel_type AS TipoTelefono3,
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,
          t.t4_tel_numr AS Telefono4,
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,
          t.t4_tel_type AS TipoTelefono4,
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,
          t.t5_tel_numr AS Telefono5,
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,
          t.t5_tel_type AS TipoTelefono5,
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,
          t.t6_tel_numr AS Telefono6,
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,
          t.t6_tel_type AS TipoTelefono6,
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,
          t.t7_tel_numr AS Telefono7,
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,
          t.t7_tel_type AS TipoTelefono7,
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,
          t.t8_tel_numr AS Telefono8,
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,
          t.t8_tel_type AS TipoTelefono8,
          'DD' AS FormaGiuridica,
          NULL AS TipoSocGiuridica,
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo,
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa,
          NULL AS TipoSocieta,
          NULL AS UrlFanPageId,
          NULL AS UrlFanPage,
          NULL AS UrlIstituzionaleId,
          NULL AS UrlIstituzionale,
          NULL AS UrlPagineBiancheId,
          NULL AS UrlPagineBianche,
          NULL AS UrlPagineGialleId,
          NULL AS UrlPagineGialle,
          NULL AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (s.CD0b_PRTY_COD, 'H')
     FROM tcd0b_rp_sf_sede s
          LEFT JOIN "VCD0D_RP_SF_MAIL_idx" m
             ON     s.cdl1_sede_cod = m.codicesede
                --and s.cd0b_stat_recd_cod = m.stato
                AND m.transactionid = --s.CD0b_RAW_XID_DES||s.cd0b_prog_trns_des
                       TO_CHAR (
                             ''
                          || s.CD0b_RAW_XID_DES
                          || NVL (s.cd0b_prog_trns_des, '000'))
          /*and m.raw_xid_des = s.CD0b_RAW_XID_DES
          and m.prog_trns_des = s.cd0b_prog_trns_des*/
          LEFT JOIN "VCD0C_RP_SF_TELEFONO_idx" t
             ON     t.codicesede = s.cdl1_sede_cod
                --and s.cd0b_stat_recd_cod = t.stato
                AND t.transactionid = --s.CD0b_RAW_XID_DES||s.cd0b_prog_trns_des
                       TO_CHAR (
                             ''
                          || s.CD0b_RAW_XID_DES
                          || NVL (s.cd0b_prog_trns_des, '000'))
    /*and t.raw_xid_des = s.CD0b_RAW_XID_DES
    and t.prog_trns_des = s.cd0b_prog_trns_des            */
    WHERE s.cdl1_prim_sedc_flg = 'N'
--------------------------------------------------------
--  DDL for View VCD0B_RP_SF_SEDECUST_idx_v2
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_RP_SF_SEDECUST_idx_v2" ("TRANSACTIONID", "RAW_XID_DES", "PROG_TRNS_DES", "CUSTOMERID", "CODICESEDE", "SEDE_COD_TEST", "EVENTO", "ORDCUST", "ORDSEDE", "DATATRANSAZIONE", "STATO", "CUSTOMERID_OLD", "CODICESEDE_OLD", "RAGIONESOCIALE", "ANNOFATTURATO", "ANNOFONDAZIONE", "ANNORIFERIMENTODIPENDENTI", "FATTURATO", "CITTA", "NAZIONESEDE", "CODICEPROVINCIA", "INDIRIZZO", "BUONOORDINE", "CAP", "CODICECATEGORIAMASSIMASPESA", "CATEGORIAMASSIMASPESA", "CELLULA", "CLASSIFICAZIONEPMI", "AREAELENCO", "CODICECATEGORIAISTAT", "CODICECATEGORIAMERCEOLOGICA", "CODICECOMUNE", "STATOCUSTOMER", "CODICEFISCALE", "CODICEFRAZIONE", "IPA", "CODPROVINCIAREA", "NUMEROREA", "STATOSEDE", "COGNOME", "COMUNE", "COORDX", "COORDY", "COORDINATEMANUALI", "DATOFISCALEESTEROCERTIFICATO", "DENOMINAZIONEALTERNATIVA", "CATEGORIAISTAT", "CATEGORIAMERCEOLOGICA", "DUG", "EMAILARRICCHIMENTO", "EMAILARRICCHIMENTOID", "EMAILBOZZE", "EMAILBOZZEID", "EMAILCOMMERCIALEIOL", "EMAILCOMMERCIALEIOLID", "EMAILFATTURAZIONE", "EMAILFATTURAZIONEID", "EMAILFATTELETTRONICA", "EMAILFATTELETTRONICAID", "EMAILPOSTVENDITA", "EMAILPOSTVENDITAID", "EMAILARRICCHIMENTOPECFLAG", "EMAILBOZZEPECFLAG", "EMAILCOMMERCIALEIOLPECFLAG", "EMAILFATTURAZIONEPECFLAG", "EMAILFATTELETTRONICAPECFLAG", "EMAILPOSTVENDITAPECFLAG", "ENTEPUBBLICOFLAG", "CODICEFISCALEFATTURAZIONE", "FATTELETTRONICAOBBLIGATORIA", "FRAZIONE", "IDINDIRIZZO", "INFOTOPONIMO", "INSEGNA", "MERCATOAGGREGATO", "INDUSTRY", "CODICENAZIONE", "DESCNAZIONE", "CODICENAZIONECUSTOMER", "POTENZIALENIP", "NOME", "NOTERECAPITOFATTURA", "CIVICO", "NUMERODIPENDENTI", "OPEC", "OPECCONSEGNABILE", "PARTITAIVA", "PROFCOORDINATEGEOGRAFICHE", "PROVINCIA", "CUSTOMERCESSATORIATTIVABILE", "SEDECESSATARIATTIVABILE", "SEDEPRIMARIAFLAG", "SOTTOAREAMERCATO", "SOTTOCLASSEDIPENDENTI", "SOTTOCLASSEFATTURATO", "STATOOPECCOMMATTUALIZ", "TIPOMERCATO", "TELEFONO1ID", "TELEFONO1", "TELEFONO1PRIMARIO", "TIPOTELEFONO1", "TELEFONO2ID", "TELEFONO2", "TELEFONO2PRIMARIO", "TIPOTELEFONO2", "TELEFONO3ID", "TELEFONO3", "TELEFONO3PRIMARIO", "TIPOTELEFONO3", "TELEFONO4ID", "TELEFONO4", "TELEFONO4PRIMARIO", "TIPOTELEFONO4", "TELEFONO5ID", "TELEFONO5", "TELEFONO5PRIMARIO", "TIPOTELEFONO5", "TELEFONO6ID", "TELEFONO6", "TELEFONO6PRIMARIO", "TIPOTELEFONO6", "TELEFONO7ID", "TELEFONO7", "TELEFONO7PRIMARIO", "TIPOTELEFONO7", "TELEFONO8ID", "TELEFONO8", "TELEFONO8PRIMARIO", "TIPOTELEFONO8", "FORMAGIURIDICA", "TIPOSOCGIURIDICA", "TOPONIMO", "ULTIMAPOSIZIONEAMMINISTRATIVA", "TIPOSOCIETA", "URLFANPAGEID", "URLFANPAGE", "URLISTITUZIONALEID", "URLISTITUZIONALE", "URLPAGINEBIANCHEID", "URLPAGINEBIANCHE", "URLPAGINEGIALLEID", "URLPAGINEGIALLE", "CLIENTESPECIALEFLG", "PRIORITA") AS 
  SELECT  
          TO_CHAR (
             '' || c.CD0A_RAW_XID_DES || NVL (c.cd0a_prog_trns_des, '000'))
             AS TransactionId,
          c.cd0a_raw_xid_des as raw_xid_des,
          c.cd0a_prog_trns_des as prog_trns_des,
          c.CDL0_CUST_COD AS CustomerId,                  --Codice_Customer__c
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede, --Codice_Sede__c
          s.CDL1_SEDE_COD sede_cod_test,
          c.CD0A_TIPO_EVEN_COD AS Evento,
          TO_CHAR (c.CD0A_RECD_NUM) AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (c.cd0a_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          c.cd0a_stat_recd_cod AS Stato,
          c.CDL0_PREC_CUST_COD AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          REPLACE (
             DECODE (c.CDL0_RAGI_SOCL_DES,
                     NULL, c.CDL0_NOME_NOM || ' ' || c.CDL0_COGN_COG,
                     c.CDL0_RAGI_SOCL_DES),
             '"',
             '\"')
             AS RagioneSociale,                                        -- Name
          c.CDL0_ANNO_RIFF_ANN AS AnnoFatturato,        --IOL_AnnoFatturato__c
          c.CDL0_ANNO_FOND_ANN AS AnnoFondazione,         --Anno_Fondazione__c
          c.CDL0_ANNO_RIFD_ANN AS AnnoRiferimentoDipendenti, --IOL_AnnoRiferimentoDipendenti__c
          TO_CHAR (TRUNC (c.CDL0_FATT_PUNT_IMP)) AS Fatturato, --AnnualRevenue
          SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40) AS Citta,         --BillingCity
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,                --BillingCountry
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,              --BillingState
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,                                     --BillingStreet
          DECODE (c.CDL0_BUON_ORDN_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS BuonoOrdine,                              --IOL_BuonoOrdine__c
          s.CDL3_CAP_COD AS Cap,                                      --Cap__c
          c.CD81_CATG_MAXS_COD AS CodiceCategoriaMassimaSpesa, --IOL_CodiceCategoriaMercMassimaSpesa__c
          REPLACE (c.CD81_CATG_MAXS_DES, '"', '\"') AS CategoriaMassimaSpesa, --IOL_Categoria__c
          s.CDL3_NUCL_COMM_COD AS Cellula,                        --Cellula__c
          c.CDLG_CLASS_PMI_DES AS ClassificazionePMI, --IOL_classificazionePMI__c
          s.CD18_AREA_ELEN_COD AS AreaElenco,        --IOL_CodiceAreaElenco__c
          c.CD82_CATG_ISTA_COD AS CodiceCategoriaISTAT, --IOL_CodiceCategoriaISTAT__c
          c.CD81_CATG_PREV_COD AS CodiceCategoriaMerceologica, --IOL_CodiceCategoriaMerceologica__c
          s.CDL3_COMN_SEAT_COD AS CodiceComune,         --Codice_Comune_IOL__c
          c.CDL0_STAT_COD AS StatoCustomer,         --Codice_Stato_Customer__c
          c.CDL0_CFIS_COD AS CodiceFiscale,                 --CodiceFiscale__c
          s.CDL3_FRAZ_COD AS CodiceFrazione,              --Codice_Frazione__c
          --s.CDL1_UNI_ORG_COD AS IPA,                           --Codice_IPA__c
          NULL AS IPA,                                         --Codice_IPA__c
          c.CDFE_CCIAA AS CodProvinciaREA,           --Codice_Provincia_REA__c
          c.CDFE_REA AS NumeroRea,                          --IOL_NumeroREA__c
          s.CDL1_STAT_COD AS StatoSede,                 --Codice_Stato_Sede__c
          c.CDL0_COGN_COG AS Cognome,            --IOL_CognomePersonaFisica__c
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune, --Comune_IOL__c
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX, --Coordinate_Anagrafiche__c (1)
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY, --Coordinate_Anagrafiche__c (2)
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true') -- [RB20190517 - orrore]
             AS CoordinateManuali,                     --Coordinate_Manuali__c
          --c.CDL0_MRST_CUST_FLG as PreScoring, --Credit_Prescoring__c
          DECODE (c.CDL0_DFES_CERT_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS DatoFiscaleEsteroCertificato, --Dato_Fiscale_Estero_Certificato__c
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa, --Denominazione_Alternativa__c
          NULL AS DenominazioneAlternativa,     --Denominazione_Alternativa__c
          REPLACE (SUBSTR (c.CD82_CATG_ISTA_DES, 0, 255), '"', '\"')
             AS CategoriaISTAT,                        --IOL_CategoriaISTAT__c
          REPLACE (c.CD81_CATG_PREV_DES, '"', '\"') AS CategoriaMerceologica, --IOL_CategoriaMerceologica__c
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,                 --DUG__c
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          DECODE (c.CDL0_ENTE_PUBB_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS EntePubblicoFlag,                           --Ente_Pubblico__c
          DECODE (c.CDL0_FATT_CFIS_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS CodiceFiscaleFatturazione,    --Fatturazione_Codice_Fiscale__c
          DECODE (c.CDL0_FATT_OBBL_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS FattElettronicaObbligatoria, --Fatturazione_Elettronica_Obbligatoria__c
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione, --Frazione__c
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,       --IOL_IndirizzoID__c
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo, --Info_Toponimo__c
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,        --Insegna__c
          DECODE (c.CD44_MERC_AGGR_COD, NULL, '0', c.CD44_MERC_AGGR_COD)
             AS MercatoAggregato,                    --IOL_MercatoAggregato__c
          c.CD68_AREA_MERC_COD AS Industry,                         --Industry
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,           --BillingCountryCod
          c.CD17_NAZN_SEAT_DES AS DescNazione,                    --Nazione__c
          c.cdl0_nazn_seat_cod AS CodiceNazioneCustomer, --IOL_CustomerCountrCod__c
          DECODE (c.CDLH_ATRB_NIP_FLG,  'S', 'SI',  'N', 'NO')
             AS PotenzialeNIP,                          --IOL_PotenzialeNIP__c
          c.CDL0_NOME_NOM AS Nome,                  --IOL_NomePersonaFisica__c
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura, --Note_sul_Recapito_Fattura__c
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,   --Numero_Civico__c
          TO_CHAR (c.CDL0_NUMR_DIPE_NUM) AS NumeroDipendenti, --NumberOfEmployees
          s.CDL2_PDR_COD AS Opec,                                    --Opec__c
          DECODE (c.CDLH_CONS_FFVV_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS OpecConsegnabile,                    --IOL_OpecConsegnabile__c
          c.CDL0_PIVA_COD AS PartitaIva,                       --PartitaIva__c
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche, --Profondita_Coordinate_Anagrafiche__c
          s.CDL3_PROV_SEAT_DES AS Provincia,                --Provincia_IOL__c
          DECODE (c.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,    --Sede_Primaria_OPEC__c
          DECODE (c.CDAJ_SARE_MERC_COD, NULL, 'X', c.CDAJ_SARE_MERC_COD)
             AS SottoAreaMercato,                    --IOL_SottoareaMercato__c
          c.CD71_STCL_DIPN_DES AS SottoclasseDipendenti, --IOL_SottoclasseDipendenti__c
          c.CD66_STCL_FATT_DES AS SottoclasseFatturato, --IOL_SottoclasseFatturato__c
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz, --Stato_Opec_Commerciale__Attualizzato__c
          NULL AS STATOOPECCOMMATTUALIZ,
          DECODE (c.CD67_TIPO_MERC_COD, NULL, '0', c.CD67_TIPO_MERC_cod)
             AS TipoMercato,                              --IOL_TipoMercato__c
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,             --Telefono_1_ID__c
          t.t1_tel_numr AS Telefono1,                                  --Phone
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,    --Primario1__c
          t.t1_tel_type AS TipoTelefono1,              --TipologiaTelefono1__c
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,             --Telefono_2_ID__c
          t.t2_tel_numr AS Telefono2,                          --Telefono2__ c
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,    --Primario2__c
          t.t2_tel_type AS TipoTelefono2,              --TipologiaTelefono2__c
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,             --Telefono_3_ID__c
          t.t3_tel_numr AS Telefono3,                          --Telefono3__ c
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,    --Primario3__c
          t.t3_tel_type AS TipoTelefono3,              --TipologiaTelefono3__c
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,             --Telefono_4_ID__c
          t.t4_tel_numr AS Telefono4,                          --Telefono4__ c
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,    --Primario4__c
          t.t4_tel_type AS TipoTelefono4,              --TipologiaTelefono4__c
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,             --Telefono_5_ID__c
          t.t5_tel_numr AS Telefono5,                          --Telefono5__ c
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,    --Primario5__c
          t.t5_tel_type AS TipoTelefono5,              --TipologiaTelefono5__c
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,             --Telefono_6_ID__c
          t.t6_tel_numr AS Telefono6,                          --Telefono6__ c
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,    --Primario6__c
          t.t6_tel_type AS TipoTelefono6,              --TipologiaTelefono6__c
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,             --Telefono_7_ID__c
          t.t7_tel_numr AS Telefono7,                          --Telefono7__ c
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,    --Primario7__c
          t.t7_tel_type AS TipoTelefono7,              --TipologiaTelefono7__c
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,             --Telefono_8_ID__c
          t.t8_tel_numr AS Telefono8,                          --Telefono8__ c
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,    --Primario8__c
          t.t8_tel_type AS TipoTelefono8,              --TipologiaTelefono8__c
          DECODE (c.CDL0_PERS_GIUR_COD,
                  'PF', 'PF',
                  'PG', 'PG',
                  NULL, 'DD',
                  'DD')
             AS FormaGiuridica,               --Tipologia_Persona_Giuridica__c
          c.CD74_TIPO_SOCT_COD AS TipoSocGiuridica, --Tipologia_societa_giuridica__c
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo, --Toponimo__c
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa, --Ultima_Posizione_Amminstrativa__c
          REPLACE (c.CD74_TIPO_SOCT_DES, '"', '\"') AS TipoSocieta,     --Type
          TO_CHAR (c.CDL6_URL_NUM_FNPG) AS UrlFanPageId,   --URL_ID_Fanpage__c
          c.CDL6_URL_DES_FNPG AS UrlFanPage,                  --URL_Fanpage__c
          TO_CHAR (c.CDL6_URL_NUM_ISTZ) AS UrlIstituzionaleId, --URL_ID_Istituzionale__c
          c.CDL6_URL_DES_ISTZ AS UrlIstituzionale,                   --Website
          TO_CHAR (c.CDL6_URL_NUM_PGBI) AS UrlPagineBiancheId, --URL_ID_Pagine_Bianche__c
          c.CDL6_URL_DES_PGBI AS UrlPagineBianche,     --URL_Pagine_Bianche__c
          TO_CHAR (c.CDL6_URL_NUM_PGGI) AS UrlPagineGialleId, --URL_ID_Pagine_Gialle__c
          c.CDL6_URL_DES_PGGI AS UrlPagineGialle,       --URL_Pagine_Gialle__c
          COALESCE (c.CDAG_SPEC_CLNT_COD, 'G') AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (c.CD0A_PRTY_COD, 'H') AS Priorita
     FROM tcd0b_rp_sf_sede s
          join tcd0a_rp_sf_customer c 
            on s.cd0b_raw_xid_des = c.cd0a_raw_xid_des 
            and s.cd0b_prog_trns_des = c.cd0a_prog_trns_des 
            and s.cdl0_cust_cod = c.cdl0_cust_cod
          left join "VCD0C_RP_SF_TELEFONO_idx" t 
            on t.codicesede = s.cdl1_sede_cod 
            and t.customer = s.cdl0_cust_cod 
            and t.transactionid = s.cd0b_raw_xid_des||s.cd0b_prog_trns_des
          left join "VCD0D_RP_SF_MAIL_idx" m 
            on m.CODICESEDE = s.cdl1_sede_cod 
            and m.CUSTOMER = s.cdl0_cust_cod 
            and m.TRANSACTIONID = s.cd0b_raw_xid_des||s.cd0b_prog_trns_des
     where
          s.cdl1_prim_sedc_flg = 'S'
          --and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = '001A001700740B89000'
   UNION
   SELECT 
          TO_CHAR (
             '' || s.CD0b_RAW_XID_DES || NVL (s.cd0b_prog_trns_des, '000'))
             AS TransactionId,
          s.cd0b_raw_xid_des as raw_xid_des,
          s.cd0b_prog_trns_des as prog_trns_des,
          s.cdl0_cust_cod AS CustomerId,
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede,
          s.CDL1_SEDE_COD sede_cod_test,
          s.CD0B_TIPO_EVEN_COD AS Evento,
          NULL AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (s.cd0b_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          s.cd0b_stat_recd_cod AS Stato,
          NULL AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          NULL AS RagioneSociale,
          NULL AS AnnoFatturato,
          NULL AS AnnoFondazione,
          NULL AS AnnoRiferimentoDipendenti,
          NULL AS Fatturato,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Citta,
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,
          'false' AS BuonoOrdine,
          s.CDL3_CAP_COD AS Cap,
          NULL AS CodiceCategoriaMassimaSpesa,
          NULL AS CategoriaMassimaSpesa,
          s.CDL3_NUCL_COMM_COD AS Cellula,
          NULL AS ClassificazionePMI,
          s.CD18_AREA_ELEN_COD AS AreaElenco,
          NULL AS CodiceCategoriaISTAT,
          NULL AS CodiceCategoriaMerceologica,
          s.CDL3_COMN_SEAT_COD AS CodiceComune,
          NULL AS StatoCustomer,
          NULL AS CodiceFiscale,
          s.CDL3_FRAZ_COD AS CodiceFrazione,
          --s.CDL1_UNI_ORG_COD AS IPA,
          NULL AS IPA,
          NULL AS CodProvinciaREA,
          NULL AS NumeroRea,
          s.CDL1_STAT_COD AS StatoSede,
          NULL AS Cognome,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune,
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX,
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY,
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true')
             AS CoordinateManuali,
          --null as PreScoring,
          'false' AS DatoFiscaleEsteroCertificato,
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa,
          NULL AS DenominazioneAlternativa,
          NULL AS CategoriaISTAT,
          NULL AS CategoriaMerceologica,
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,
          m.ar_eml_txt AS EmailArricchimento,         --Email_Arricchimento__c
          m.ar_eml_id AS EmailArricchimentoId,     --Email_Arricchimento_ID__c
          m.bz_eml_txt AS EmailBozze,                         --Email_Bozze__c
          m.bz_eml_id AS EmailBozzeId,                     --Email_Bozze_ID__c
          m.cs_eml_txt AS EmailCommercialeIOL,      --Email_Commerciale_IOL__c
          m.cs_eml_id AS EmailCommercialeIOLId,  --Email_Commerciale_IOL_ID__c
          m.ft_eml_txt AS EmailFatturazione,           --Email_Fatturazione__c
          m.ft_eml_id AS EmailFatturazioneId,       --Email_Fatturazione_ID__c
          m.fe_eml_txt AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          m.fe_eml_id AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          m.pv_eml_txt AS EmailPostVendita,            --Email_Post_Vendita__c
          m.pv_eml_id AS EmailPostVenditaId,        --Email_Post_Vendita_ID__c
          m.ar_pec_flg AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          m.bz_pec_flg AS EmailBozzePECFlag,          --IOL_Email_Bozze_PEC__c
          m.cs_pec_flg AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          m.ft_pec_flg AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          m.fe_pec_flg AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          m.pv_pec_flg AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          'false' AS EntePubblicoFlag,
          'false' AS CodiceFiscaleFatturazione,
          'false' AS FattElettronicaObbligatoria,
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione,
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo,
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,
          '0' AS MercatoAggregato,
          NULL AS Industry,
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,
          NULL AS DescNazione,
          NULL AS CodiceNazioneCustomer,            --IOL_CustomerCountrCod__c
          'NO' AS PotenzialeNIP,
          NULL AS Nome,
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura,
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,
          NULL AS NumeroDipendenti,
          s.CDL2_PDR_COD AS Opec,
          'false' AS OpecConsegnabile,
          NULL AS PartitaIva,
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche,
          s.CDL3_PROV_SEAT_DES AS Provincia,
          'true' AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,
          'X' AS SottoAreaMercato,
          NULL AS SottoclasseDipendenti,
          NULL AS SottoclasseFatturato,
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz,
          NULL AS STATOOPECCOMMATTUALIZ,
          '0' AS TipoMercato,
          TO_CHAR (t.t1_tel_id) AS Telefono1Id,
          t.t1_tel_numr AS Telefono1,
          NVL (t.t1_prim_flg, 'false') AS Telefono1Primario,
          t.t1_tel_type AS TipoTelefono1,
          TO_CHAR (t.t2_tel_id) AS Telefono2Id,
          t.t2_tel_numr AS Telefono2,
          NVL (t.t2_prim_flg, 'false') AS Telefono2Primario,
          t.t2_tel_type AS TipoTelefono2,
          TO_CHAR (t.t3_tel_id) AS Telefono3Id,
          t.t3_tel_numr AS Telefono3,
          NVL (t.t3_prim_flg, 'false') AS Telefono3Primario,
          t.t3_tel_type AS TipoTelefono3,
          TO_CHAR (t.t4_tel_id) AS Telefono4Id,
          t.t4_tel_numr AS Telefono4,
          NVL (t.t4_prim_flg, 'false') AS Telefono4Primario,
          t.t4_tel_type AS TipoTelefono4,
          TO_CHAR (t.t5_tel_id) AS Telefono5Id,
          t.t5_tel_numr AS Telefono5,
          NVL (t.t5_prim_flg, 'false') AS Telefono5Primario,
          t.t5_tel_type AS TipoTelefono5,
          TO_CHAR (t.t6_tel_id) AS Telefono6Id,
          t.t6_tel_numr AS Telefono6,
          NVL (t.t6_prim_flg, 'false') AS Telefono6Primario,
          t.t6_tel_type AS TipoTelefono6,
          TO_CHAR (t.t7_tel_id) AS Telefono7Id,
          t.t7_tel_numr AS Telefono7,
          NVL (t.t7_prim_flg, 'false') AS Telefono7Primario,
          t.t7_tel_type AS TipoTelefono7,
          TO_CHAR (t.t8_tel_id) AS Telefono8Id,
          t.t8_tel_numr AS Telefono8,
          NVL (t.t8_prim_flg, 'false') AS Telefono8Primario,
          t.t8_tel_type AS TipoTelefono8,
          'DD' AS FormaGiuridica,
          NULL AS TipoSocGiuridica,
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo,
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa,
          NULL AS TipoSocieta,
          NULL AS UrlFanPageId,
          NULL AS UrlFanPage,
          NULL AS UrlIstituzionaleId,
          NULL AS UrlIstituzionale,
          NULL AS UrlPagineBiancheId,
          NULL AS UrlPagineBianche,
          NULL AS UrlPagineGialleId,
          NULL AS UrlPagineGialle,
          NULL AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (s.CD0b_PRTY_COD, 'H')
     FROM tcd0b_rp_sf_sede s
          left join "VCD0C_RP_SF_TELEFONO_idx" t 
            on t.codicesede = s.cdl1_sede_cod 
            and t.customer = s.cdl0_cust_cod 
            and t.transactionid = s.cd0b_raw_xid_des||s.cd0b_prog_trns_des
          left join "VCD0D_RP_SF_MAIL_idx" m 
            on m.CODICESEDE = s.cdl1_sede_cod 
            and m.CUSTOMER = s.cdl0_cust_cod 
            and m.TRANSACTIONID = s.cd0b_raw_xid_des||s.cd0b_prog_trns_des
     where
          s.cdl1_prim_sedc_flg = 'N'
          --and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = '001A001700740B89000'
--------------------------------------------------------
--  DDL for View VCD0B_RP_SF_SEDECUST_v2
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_RP_SF_SEDECUST_v2" ("TRANSACTIONID", "CUSTOMERID", "CODICESEDE", "SEDE_COD_TEST", "EVENTO", "ORDCUST", "ORDSEDE", "DATATRANSAZIONE", "STATO", "CUSTOMERID_OLD", "CODICESEDE_OLD", "RAGIONESOCIALE", "ANNOFATTURATO", "ANNOFONDAZIONE", "ANNORIFERIMENTODIPENDENTI", "FATTURATO", "CITTA", "NAZIONESEDE", "CODICEPROVINCIA", "INDIRIZZO", "BUONOORDINE", "CAP", "CODICECATEGORIAMASSIMASPESA", "CATEGORIAMASSIMASPESA", "CELLULA", "CLASSIFICAZIONEPMI", "AREAELENCO", "CODICECATEGORIAISTAT", "CODICECATEGORIAMERCEOLOGICA", "CODICECOMUNE", "STATOCUSTOMER", "CODICEFISCALE", "CODICEFRAZIONE", "IPA", "CODPROVINCIAREA", "NUMEROREA", "STATOSEDE", "COGNOME", "COMUNE", "COORDX", "COORDY", "COORDINATEMANUALI", "DATOFISCALEESTEROCERTIFICATO", "DENOMINAZIONEALTERNATIVA", "CATEGORIAISTAT", "CATEGORIAMERCEOLOGICA", "DUG", "EMAILARRICCHIMENTO", "EMAILARRICCHIMENTOID", "EMAILBOZZE", "EMAILBOZZEID", "EMAILCOMMERCIALEIOL", "EMAILCOMMERCIALEIOLID", "EMAILFATTURAZIONE", "EMAILFATTURAZIONEID", "EMAILFATTELETTRONICA", "EMAILFATTELETTRONICAID", "EMAILPOSTVENDITA", "EMAILPOSTVENDITAID", "EMAILARRICCHIMENTOPECFLAG", "EMAILBOZZEPECFLAG", "EMAILCOMMERCIALEIOLPECFLAG", "EMAILFATTURAZIONEPECFLAG", "EMAILFATTELETTRONICAPECFLAG", "EMAILPOSTVENDITAPECFLAG", "ENTEPUBBLICOFLAG", "CODICEFISCALEFATTURAZIONE", "FATTELETTRONICAOBBLIGATORIA", "FRAZIONE", "IDINDIRIZZO", "INFOTOPONIMO", "INSEGNA", "MERCATOAGGREGATO", "INDUSTRY", "CODICENAZIONE", "DESCNAZIONE", "CODICENAZIONECUSTOMER", "POTENZIALENIP", "NOME", "NOTERECAPITOFATTURA", "CIVICO", "NUMERODIPENDENTI", "OPEC", "OPECCONSEGNABILE", "PARTITAIVA", "PROFCOORDINATEGEOGRAFICHE", "PROVINCIA", "CUSTOMERCESSATORIATTIVABILE", "SEDECESSATARIATTIVABILE", "SEDEPRIMARIAFLAG", "SOTTOAREAMERCATO", "SOTTOCLASSEDIPENDENTI", "SOTTOCLASSEFATTURATO", "STATOOPECCOMMATTUALIZ", "TIPOMERCATO", "TELEFONO1ID", "TELEFONO1", "TELEFONO1PRIMARIO", "TIPOTELEFONO1", "TELEFONO2ID", "TELEFONO2", "TELEFONO2PRIMARIO", "TIPOTELEFONO2", "TELEFONO3ID", "TELEFONO3", "TELEFONO3PRIMARIO", "TIPOTELEFONO3", "TELEFONO4ID", "TELEFONO4", "TELEFONO4PRIMARIO", "TIPOTELEFONO4", "TELEFONO5ID", "TELEFONO5", "TELEFONO5PRIMARIO", "TIPOTELEFONO5", "TELEFONO6ID", "TELEFONO6", "TELEFONO6PRIMARIO", "TIPOTELEFONO6", "TELEFONO7ID", "TELEFONO7", "TELEFONO7PRIMARIO", "TIPOTELEFONO7", "TELEFONO8ID", "TELEFONO8", "TELEFONO8PRIMARIO", "TIPOTELEFONO8", "FORMAGIURIDICA", "TIPOSOCGIURIDICA", "TOPONIMO", "ULTIMAPOSIZIONEAMMINISTRATIVA", "TIPOSOCIETA", "URLFANPAGEID", "URLFANPAGE", "URLISTITUZIONALEID", "URLISTITUZIONALE", "URLPAGINEBIANCHEID", "URLPAGINEBIANCHE", "URLPAGINEGIALLEID", "URLPAGINEGIALLE", "CLIENTESPECIALEFLG", "PRIORITA") AS 
  SELECT TO_CHAR (
             '' || c.CD0A_RAW_XID_DES || NVL (c.cd0a_prog_trns_des, '000'))
             AS TransactionId,
          /*c.cd0a_raw_xid_des as raw_xid_des,
          c.cd0a_prog_trns_des as prog_trns_des,*/
          c.CDL0_CUST_COD AS CustomerId,                  --Codice_Customer__c
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede, --Codice_Sede__c
          s.CDL1_SEDE_COD sede_cod_test,
          c.CD0A_TIPO_EVEN_COD AS Evento,
          TO_CHAR (c.CD0A_RECD_NUM) AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (c.cd0a_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          c.cd0a_stat_recd_cod AS Stato,
          c.CDL0_PREC_CUST_COD AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          REPLACE (
             DECODE (c.CDL0_RAGI_SOCL_DES,
                     NULL, c.CDL0_NOME_NOM || ' ' || c.CDL0_COGN_COG,
                     c.CDL0_RAGI_SOCL_DES),
             '"',
             '\"')
             AS RagioneSociale,                                        -- Name
          c.CDL0_ANNO_RIFF_ANN AS AnnoFatturato,        --IOL_AnnoFatturato__c
          c.CDL0_ANNO_FOND_ANN AS AnnoFondazione,         --Anno_Fondazione__c
          c.CDL0_ANNO_RIFD_ANN AS AnnoRiferimentoDipendenti, --IOL_AnnoRiferimentoDipendenti__c
          TO_CHAR (TRUNC (c.CDL0_FATT_PUNT_IMP)) AS Fatturato, --AnnualRevenue
          SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40) AS Citta,         --BillingCity
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,                --BillingCountry
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,              --BillingState
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,                                     --BillingStreet
          DECODE (c.CDL0_BUON_ORDN_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS BuonoOrdine,                              --IOL_BuonoOrdine__c
          s.CDL3_CAP_COD AS Cap,                                      --Cap__c
          c.CD81_CATG_MAXS_COD AS CodiceCategoriaMassimaSpesa, --IOL_CodiceCategoriaMercMassimaSpesa__c
          REPLACE (c.CD81_CATG_MAXS_DES, '"', '\"') AS CategoriaMassimaSpesa, --IOL_Categoria__c
          s.CDL3_NUCL_COMM_COD AS Cellula,                        --Cellula__c
          c.CDLG_CLASS_PMI_DES AS ClassificazionePMI, --IOL_classificazionePMI__c
          s.CD18_AREA_ELEN_COD AS AreaElenco,        --IOL_CodiceAreaElenco__c
          c.CD82_CATG_ISTA_COD AS CodiceCategoriaISTAT, --IOL_CodiceCategoriaISTAT__c
          c.CD81_CATG_PREV_COD AS CodiceCategoriaMerceologica, --IOL_CodiceCategoriaMerceologica__c
          s.CDL3_COMN_SEAT_COD AS CodiceComune,         --Codice_Comune_IOL__c
          c.CDL0_STAT_COD AS StatoCustomer,         --Codice_Stato_Customer__c
          c.CDL0_CFIS_COD AS CodiceFiscale,                 --CodiceFiscale__c
          s.CDL3_FRAZ_COD AS CodiceFrazione,              --Codice_Frazione__c
          --s.CDL1_UNI_ORG_COD AS IPA,                           --Codice_IPA__c
          NULL AS IPA,                                         --Codice_IPA__c
          c.CDFE_CCIAA AS CodProvinciaREA,           --Codice_Provincia_REA__c
          c.CDFE_REA AS NumeroRea,                          --IOL_NumeroREA__c
          s.CDL1_STAT_COD AS StatoSede,                 --Codice_Stato_Sede__c
          c.CDL0_COGN_COG AS Cognome,            --IOL_CognomePersonaFisica__c
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune, --Comune_IOL__c
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX, --Coordinate_Anagrafiche__c (1)
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY, --Coordinate_Anagrafiche__c (2)
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true') -- [RB20190517 - orrore]
             AS CoordinateManuali,                     --Coordinate_Manuali__c
          --c.CDL0_MRST_CUST_FLG as PreScoring, --Credit_Prescoring__c
          DECODE (c.CDL0_DFES_CERT_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS DatoFiscaleEsteroCertificato, --Dato_Fiscale_Estero_Certificato__c
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa, --Denominazione_Alternativa__c
          NULL AS DenominazioneAlternativa,     --Denominazione_Alternativa__c
          REPLACE (SUBSTR (c.CD82_CATG_ISTA_DES, 0, 255), '"', '\"')
             AS CategoriaISTAT,                        --IOL_CategoriaISTAT__c
          REPLACE (c.CD81_CATG_PREV_DES, '"', '\"') AS CategoriaMerceologica, --IOL_CategoriaMerceologica__c
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,                 --DUG__c
          v.EmailArricchimento AS EmailArricchimento, --Email_Arricchimento__c
          v.EmailArricchimentoId AS EmailArricchimentoId, --Email_Arricchimento_ID__c
          v.EmailBozze AS EmailBozze,                         --Email_Bozze__c
          v.EmailBozzeId AS EmailBozzeId,                  --Email_Bozze_ID__c
          v.EmailCommercialeIOL AS EmailCommercialeIOL, --Email_Commerciale_IOL__c
          v.EmailCommercialeIOLId AS EmailCommercialeIOLId, --Email_Commerciale_IOL_ID__c
          v.EmailFatturazione AS EmailFatturazione,    --Email_Fatturazione__c
          v.EmailFatturazioneId AS EmailFatturazioneId, --Email_Fatturazione_ID__c
          v.EmailFattElettronica AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          v.EmailFattElettronicaId AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          v.EmailPostVendita AS EmailPostVendita,      --Email_Post_Vendita__c
          v.EmailPostVenditaId AS EmailPostVenditaId, --Email_Post_Vendita_ID__c
          v.EmailArricchimentoPECFlag AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          v.EmailBozzePECFlag AS EmailBozzePECFlag,   --IOL_Email_Bozze_PEC__c
          v.EmailCommercialeIOLPECFlag AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          v.EmailFatturazionePECFlag AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          v.EmailFattElettronicaPECFlag AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          v.EmailPostVenditaPECFlag AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          DECODE (c.CDL0_ENTE_PUBB_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS EntePubblicoFlag,                           --Ente_Pubblico__c
          DECODE (c.CDL0_FATT_CFIS_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS CodiceFiscaleFatturazione,    --Fatturazione_Codice_Fiscale__c
          DECODE (c.CDL0_FATT_OBBL_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS FattElettronicaObbligatoria, --Fatturazione_Elettronica_Obbligatoria__c
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione, --Frazione__c
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,       --IOL_IndirizzoID__c
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo, --Info_Toponimo__c
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,        --Insegna__c
          DECODE (c.CD44_MERC_AGGR_COD, NULL, '0', c.CD44_MERC_AGGR_COD)
             AS MercatoAggregato,                    --IOL_MercatoAggregato__c
          c.CD68_AREA_MERC_COD AS Industry,                         --Industry
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,           --BillingCountryCod
          c.CD17_NAZN_SEAT_DES AS DescNazione,                    --Nazione__c
          c.cdl0_nazn_seat_cod AS CodiceNazioneCustomer, --IOL_CustomerCountrCod__c
          DECODE (c.CDLH_ATRB_NIP_FLG,  'S', 'SI',  'N', 'NO')
             AS PotenzialeNIP,                          --IOL_PotenzialeNIP__c
          c.CDL0_NOME_NOM AS Nome,                  --IOL_NomePersonaFisica__c
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura, --Note_sul_Recapito_Fattura__c
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,   --Numero_Civico__c
          TO_CHAR (c.CDL0_NUMR_DIPE_NUM) AS NumeroDipendenti, --NumberOfEmployees
          s.CDL2_PDR_COD AS Opec,                                    --Opec__c
          DECODE (c.CDLH_CONS_FFVV_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS OpecConsegnabile,                    --IOL_OpecConsegnabile__c
          c.CDL0_PIVA_COD AS PartitaIva,                       --PartitaIva__c
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche, --Profondita_Coordinate_Anagrafiche__c
          s.CDL3_PROV_SEAT_DES AS Provincia,                --Provincia_IOL__c
          DECODE (c.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,    --Sede_Primaria_OPEC__c
          DECODE (c.CDAJ_SARE_MERC_COD, NULL, 'X', c.CDAJ_SARE_MERC_COD)
             AS SottoAreaMercato,                    --IOL_SottoareaMercato__c
          c.CD71_STCL_DIPN_DES AS SottoclasseDipendenti, --IOL_SottoclasseDipendenti__c
          c.CD66_STCL_FATT_DES AS SottoclasseFatturato, --IOL_SottoclasseFatturato__c
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz, --Stato_Opec_Commerciale__Attualizzato__c
          NULL AS STATOOPECCOMMATTUALIZ,
          DECODE (c.CD67_TIPO_MERC_COD, NULL, '0', c.CD67_TIPO_MERC_cod)
             AS TipoMercato,                              --IOL_TipoMercato__c
          v.telefono1id AS Telefono1Id,                     --Telefono_1_ID__c
          v.Telefono1 AS Telefono1,                                    --Phone
          v.Telefono1Primario AS Telefono1Primario,             --Primario1__c
          v.TipoTelefono1 AS TipoTelefono1,            --TipologiaTelefono1__c
          v.Telefono2Id AS Telefono2Id,                     --Telefono_2_ID__c
          v.Telefono2 AS Telefono2,                            --Telefono2__ c
          v.Telefono2Primario AS Telefono2Primario,             --Primario2__c
          v.TipoTelefono2 AS TipoTelefono2,            --TipologiaTelefono2__c
          v.Telefono3Id AS Telefono3Id,                     --Telefono_3_ID__c
          v.Telefono3 AS Telefono3,                            --Telefono3__ c
          v.Telefono3Primario AS Telefono3Primario,             --Primario3__c
          v.TipoTelefono3 AS TipoTelefono3,            --TipologiaTelefono3__c
          v.Telefono4Id AS Telefono4Id,                     --Telefono_4_ID__c
          v.Telefono4 AS Telefono4,                            --Telefono4__ c
          v.Telefono4Primario AS Telefono4Primario,             --Primario4__c
          v.TipoTelefono4 AS TipoTelefono4,            --TipologiaTelefono4__c
          v.Telefono5Id AS Telefono5Id,                     --Telefono_5_ID__c
          v.Telefono5 AS Telefono5,                            --Telefono5__ c
          v.Telefono5Primario AS Telefono5Primario,             --Primario5__c
          v.TipoTelefono5 AS TipoTelefono5,            --TipologiaTelefono5__c
          v.Telefono6Id AS Telefono6Id,                     --Telefono_6_ID__c
          v.Telefono6 AS Telefono6,                            --Telefono6__ c
          v.Telefono6Primario AS Telefono6Primario,             --Primario6__c
          v.TipoTelefono6 AS TipoTelefono6,            --TipologiaTelefono6__c
          v.Telefono7Id AS Telefono7Id,                     --Telefono_7_ID__c
          v.Telefono7 AS Telefono7,                            --Telefono7__ c
          v.Telefono7Primario AS Telefono7Primario,             --Primario7__c
          v.TipoTelefono7 AS TipoTelefono7,            --TipologiaTelefono7__c
          v.Telefono8Id AS Telefono8Id,                     --Telefono_8_ID__c
          v.Telefono8 AS Telefono8,                            --Telefono8__ c
          v.Telefono8Primario AS Telefono8Primario,             --Primario8__c
          v.TipoTelefono8 AS TipoTelefono8,            --TipologiaTelefono8__c
          DECODE (c.CDL0_PERS_GIUR_COD,
                  'PF', 'PF',
                  'PG', 'PG',
                  NULL, 'DD',
                  'DD')
             AS FormaGiuridica,               --Tipologia_Persona_Giuridica__c
          c.CD74_TIPO_SOCT_COD AS TipoSocGiuridica, --Tipologia_societa_giuridica__c
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo, --Toponimo__c
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa, --Ultima_Posizione_Amminstrativa__c
          REPLACE (c.CD74_TIPO_SOCT_DES, '"', '\"') AS TipoSocieta,     --Type
          TO_CHAR (c.CDL6_URL_NUM_FNPG) AS UrlFanPageId,   --URL_ID_Fanpage__c
          c.CDL6_URL_DES_FNPG AS UrlFanPage,                  --URL_Fanpage__c
          TO_CHAR (c.CDL6_URL_NUM_ISTZ) AS UrlIstituzionaleId, --URL_ID_Istituzionale__c
          c.CDL6_URL_DES_ISTZ AS UrlIstituzionale,                   --Website
          TO_CHAR (c.CDL6_URL_NUM_PGBI) AS UrlPagineBiancheId, --URL_ID_Pagine_Bianche__c
          c.CDL6_URL_DES_PGBI AS UrlPagineBianche,     --URL_Pagine_Bianche__c
          TO_CHAR (c.CDL6_URL_NUM_PGGI) AS UrlPagineGialleId, --URL_ID_Pagine_Gialle__c
          c.CDL6_URL_DES_PGGI AS UrlPagineGialle,       --URL_Pagine_Gialle__c
          COALESCE (c.CDAG_SPEC_CLNT_COD, 'G') AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (c.CD0A_PRTY_COD, 'H') AS Priorita
     FROM tcd0b_rp_sf_sede s
          JOIN tcd0a_rp_sf_customer c
             ON     s.cdl0_cust_cod = c.cdl0_cust_cod
                --and s.cd0b_stat_recd_cod = c.cd0a_stat_recd_cod
                /*AND c.cd0a_raw_xid_des = s.cd0b_raw_xid_des
                AND NVL (c.cd0a_prog_trns_des, '000') =
                       NVL (s.cd0b_prog_trns_des, '000')
                and c.cd0a_prog_trns_des = s.cd0b_prog_trns_des*/
                AND c.cd0a_raw_xid_des || c.cd0a_prog_trns_des =
                       s.cd0b_raw_xid_des || s.cd0b_prog_trns_des
                AND s.cdl1_prim_sedc_flg = 'S'
          LEFT JOIN mcd0b_rp_sf_sctype v
             ON     v.cust_cod = s.cdl0_cust_cod
                AND v.sede_cod = s.cdl1_sede_cod
                AND v.raw_xid_des = s.cd0b_raw_xid_des
                AND v.prog_trns_des = s.cd0b_prog_trns_des
                AND v.tipo = 'C'
   UNION
   SELECT TO_CHAR (
             '' || s.CD0b_RAW_XID_DES || NVL (s.cd0b_prog_trns_des, '000'))
             AS TransactionId,
          /*s.cd0b_raw_xid_des as raw_xid_des,
          s.cd0b_prog_trns_des as prog_trns_des,*/
          s.cdl0_cust_cod AS CustomerId,
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede,
          s.CDL1_SEDE_COD sede_cod_test,
          s.CD0B_TIPO_EVEN_COD AS Evento,
          NULL AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (s.cd0b_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          s.cd0b_stat_recd_cod AS Stato,
          NULL AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          NULL AS RagioneSociale,
          NULL AS AnnoFatturato,
          NULL AS AnnoFondazione,
          NULL AS AnnoRiferimentoDipendenti,
          NULL AS Fatturato,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Citta,
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,
          'false' AS BuonoOrdine,
          s.CDL3_CAP_COD AS Cap,
          NULL AS CodiceCategoriaMassimaSpesa,
          NULL AS CategoriaMassimaSpesa,
          s.CDL3_NUCL_COMM_COD AS Cellula,
          NULL AS ClassificazionePMI,
          s.CD18_AREA_ELEN_COD AS AreaElenco,
          NULL AS CodiceCategoriaISTAT,
          NULL AS CodiceCategoriaMerceologica,
          s.CDL3_COMN_SEAT_COD AS CodiceComune,
          NULL AS StatoCustomer,
          NULL AS CodiceFiscale,
          s.CDL3_FRAZ_COD AS CodiceFrazione,
          --s.CDL1_UNI_ORG_COD AS IPA,
          NULL AS IPA,
          NULL AS CodProvinciaREA,
          NULL AS NumeroRea,
          s.CDL1_STAT_COD AS StatoSede,
          NULL AS Cognome,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune,
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX,
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY,
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true')
             AS CoordinateManuali,
          --null as PreScoring,
          'false' AS DatoFiscaleEsteroCertificato,
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa,
          NULL AS DenominazioneAlternativa,
          NULL AS CategoriaISTAT,
          NULL AS CategoriaMerceologica,
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,
          v.EmailArricchimento AS EmailArricchimento, --Email_Arricchimento__c
          v.EmailArricchimentoId AS EmailArricchimentoId, --Email_Arricchimento_ID__c
          v.EmailBozze AS EmailBozze,                         --Email_Bozze__c
          v.EmailBozzeId AS EmailBozzeId,                  --Email_Bozze_ID__c
          v.EmailCommercialeIOL AS EmailCommercialeIOL, --Email_Commerciale_IOL__c
          v.EmailCommercialeIOLId AS EmailCommercialeIOLId, --Email_Commerciale_IOL_ID__c
          v.EmailFatturazione AS EmailFatturazione,    --Email_Fatturazione__c
          v.EmailFatturazioneId AS EmailFatturazioneId, --Email_Fatturazione_ID__c
          v.EmailFattElettronica AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          v.EmailFattElettronicaId AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          v.EmailPostVendita AS EmailPostVendita,      --Email_Post_Vendita__c
          v.EmailPostVenditaId AS EmailPostVenditaId, --Email_Post_Vendita_ID__c
          v.EmailArricchimentoPECFlag AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          v.EmailBozzePECFlag AS EmailBozzePECFlag,   --IOL_Email_Bozze_PEC__c
          v.EmailCommercialeIOLPECFlag AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          v.EmailFatturazionePECFlag AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          v.EmailFattElettronicaPECFlag AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          v.EmailPostVenditaPECFlag AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          'false' AS EntePubblicoFlag,
          'false' AS CodiceFiscaleFatturazione,
          'false' AS FattElettronicaObbligatoria,
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione,
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo,
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,
          '0' AS MercatoAggregato,
          NULL AS Industry,
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,
          NULL AS DescNazione,
          NULL AS CodiceNazioneCustomer,            --IOL_CustomerCountrCod__c
          'NO' AS PotenzialeNIP,
          NULL AS Nome,
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura,
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,
          NULL AS NumeroDipendenti,
          s.CDL2_PDR_COD AS Opec,
          'false' AS OpecConsegnabile,
          NULL AS PartitaIva,
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche,
          s.CDL3_PROV_SEAT_DES AS Provincia,
          'true' AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,
          'X' AS SottoAreaMercato,
          NULL AS SottoclasseDipendenti,
          NULL AS SottoclasseFatturato,
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz,
          NULL AS STATOOPECCOMMATTUALIZ,
          '0' AS TipoMercato,
          v.telefono1id AS Telefono1Id,                     --Telefono_1_ID__c
          v.Telefono1 AS Telefono1,                                    --Phone
          v.Telefono1Primario AS Telefono1Primario,             --Primario1__c
          v.TipoTelefono1 AS TipoTelefono1,            --TipologiaTelefono1__c
          v.Telefono2Id AS Telefono2Id,                     --Telefono_2_ID__c
          v.Telefono2 AS Telefono2,                            --Telefono2__ c
          v.Telefono2Primario AS Telefono2Primario,             --Primario2__c
          v.TipoTelefono2 AS TipoTelefono2,            --TipologiaTelefono2__c
          v.Telefono3Id AS Telefono3Id,                     --Telefono_3_ID__c
          v.Telefono3 AS Telefono3,                            --Telefono3__ c
          v.Telefono3Primario AS Telefono3Primario,             --Primario3__c
          v.TipoTelefono3 AS TipoTelefono3,            --TipologiaTelefono3__c
          v.Telefono4Id AS Telefono4Id,                     --Telefono_4_ID__c
          v.Telefono4 AS Telefono4,                            --Telefono4__ c
          v.Telefono4Primario AS Telefono4Primario,             --Primario4__c
          v.TipoTelefono4 AS TipoTelefono4,            --TipologiaTelefono4__c
          v.Telefono5Id AS Telefono5Id,                     --Telefono_5_ID__c
          v.Telefono5 AS Telefono5,                            --Telefono5__ c
          v.Telefono5Primario AS Telefono5Primario,             --Primario5__c
          v.TipoTelefono5 AS TipoTelefono5,            --TipologiaTelefono5__c
          v.Telefono6Id AS Telefono6Id,                     --Telefono_6_ID__c
          v.Telefono6 AS Telefono6,                            --Telefono6__ c
          v.Telefono6Primario AS Telefono6Primario,             --Primario6__c
          v.TipoTelefono6 AS TipoTelefono6,            --TipologiaTelefono6__c
          v.Telefono7Id AS Telefono7Id,                     --Telefono_7_ID__c
          v.Telefono7 AS Telefono7,                            --Telefono7__ c
          v.Telefono7Primario AS Telefono7Primario,             --Primario7__c
          v.TipoTelefono7 AS TipoTelefono7,            --TipologiaTelefono7__c
          v.Telefono8Id AS Telefono8Id,                     --Telefono_8_ID__c
          v.Telefono8 AS Telefono8,                            --Telefono8__ c
          v.Telefono8Primario AS Telefono8Primario,             --Primario8__c
          v.TipoTelefono8 AS TipoTelefono8,            --TipologiaTelefono8__c
          'DD' AS FormaGiuridica,
          NULL AS TipoSocGiuridica,
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo,
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa,
          NULL AS TipoSocieta,
          NULL AS UrlFanPageId,
          NULL AS UrlFanPage,
          NULL AS UrlIstituzionaleId,
          NULL AS UrlIstituzionale,
          NULL AS UrlPagineBiancheId,
          NULL AS UrlPagineBianche,
          NULL AS UrlPagineGialleId,
          NULL AS UrlPagineGialle,
          NULL AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (s.CD0b_PRTY_COD, 'H')
     FROM    tcd0b_rp_sf_sede s
          LEFT JOIN
             mcd0b_rp_sf_sctype v
          ON     v.cust_cod = s.cdl0_cust_cod
             AND v.sede_cod = s.cdl1_sede_cod
             AND v.raw_xid_des = s.cd0b_raw_xid_des
             AND v.prog_trns_des = s.cd0b_prog_trns_des
             AND v.tipo = 'S'
    WHERE s.cdl1_prim_sedc_flg = 'N'
--------------------------------------------------------
--  DDL for View VCD0B_RP_SF_SEDECUST_v3
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_RP_SF_SEDECUST_v3" ("TRANSACTIONID", "CUSTOMERID", "CODICESEDE", "SEDE_COD_TEST", "EVENTO", "ORDCUST", "ORDSEDE", "DATATRANSAZIONE", "STATO", "CUSTOMERID_OLD", "CODICESEDE_OLD", "RAGIONESOCIALE", "ANNOFATTURATO", "ANNOFONDAZIONE", "ANNORIFERIMENTODIPENDENTI", "FATTURATO", "CITTA", "NAZIONESEDE", "CODICEPROVINCIA", "INDIRIZZO", "BUONOORDINE", "CAP", "CODICECATEGORIAMASSIMASPESA", "CATEGORIAMASSIMASPESA", "CELLULA", "CLASSIFICAZIONEPMI", "AREAELENCO", "CODICECATEGORIAISTAT", "CODICECATEGORIAMERCEOLOGICA", "CODICECOMUNE", "STATOCUSTOMER", "CODICEFISCALE", "CODICEFRAZIONE", "IPA", "CODPROVINCIAREA", "NUMEROREA", "STATOSEDE", "COGNOME", "COMUNE", "COORDX", "COORDY", "COORDINATEMANUALI", "DATOFISCALEESTEROCERTIFICATO", "DENOMINAZIONEALTERNATIVA", "CATEGORIAISTAT", "CATEGORIAMERCEOLOGICA", "DUG", "EMAILARRICCHIMENTO", "EMAILARRICCHIMENTOID", "EMAILBOZZE", "EMAILBOZZEID", "EMAILCOMMERCIALEIOL", "EMAILCOMMERCIALEIOLID", "EMAILFATTURAZIONE", "EMAILFATTURAZIONEID", "EMAILFATTELETTRONICA", "EMAILFATTELETTRONICAID", "EMAILPOSTVENDITA", "EMAILPOSTVENDITAID", "EMAILARRICCHIMENTOPECFLAG", "EMAILBOZZEPECFLAG", "EMAILCOMMERCIALEIOLPECFLAG", "EMAILFATTURAZIONEPECFLAG", "EMAILFATTELETTRONICAPECFLAG", "EMAILPOSTVENDITAPECFLAG", "ENTEPUBBLICOFLAG", "CODICEFISCALEFATTURAZIONE", "FATTELETTRONICAOBBLIGATORIA", "FRAZIONE", "IDINDIRIZZO", "INFOTOPONIMO", "INSEGNA", "MERCATOAGGREGATO", "INDUSTRY", "CODICENAZIONE", "DESCNAZIONE", "CODICENAZIONECUSTOMER", "POTENZIALENIP", "NOME", "NOTERECAPITOFATTURA", "CIVICO", "NUMERODIPENDENTI", "OPEC", "OPECCONSEGNABILE", "PARTITAIVA", "PROFCOORDINATEGEOGRAFICHE", "PROVINCIA", "CUSTOMERCESSATORIATTIVABILE", "SEDECESSATARIATTIVABILE", "SEDEPRIMARIAFLAG", "SOTTOAREAMERCATO", "SOTTOCLASSEDIPENDENTI", "SOTTOCLASSEFATTURATO", "STATOOPECCOMMATTUALIZ", "TIPOMERCATO", "TELEFONO1ID", "TELEFONO1", "TELEFONO1PRIMARIO", "TIPOTELEFONO1", "TELEFONO2ID", "TELEFONO2", "TELEFONO2PRIMARIO", "TIPOTELEFONO2", "TELEFONO3ID", "TELEFONO3", "TELEFONO3PRIMARIO", "TIPOTELEFONO3", "TELEFONO4ID", "TELEFONO4", "TELEFONO4PRIMARIO", "TIPOTELEFONO4", "TELEFONO5ID", "TELEFONO5", "TELEFONO5PRIMARIO", "TIPOTELEFONO5", "TELEFONO6ID", "TELEFONO6", "TELEFONO6PRIMARIO", "TIPOTELEFONO6", "TELEFONO7ID", "TELEFONO7", "TELEFONO7PRIMARIO", "TIPOTELEFONO7", "TELEFONO8ID", "TELEFONO8", "TELEFONO8PRIMARIO", "TIPOTELEFONO8", "FORMAGIURIDICA", "TIPOSOCGIURIDICA", "TOPONIMO", "ULTIMAPOSIZIONEAMMINISTRATIVA", "TIPOSOCIETA", "URLFANPAGEID", "URLFANPAGE", "URLISTITUZIONALEID", "URLISTITUZIONALE", "URLPAGINEBIANCHEID", "URLPAGINEBIANCHE", "URLPAGINEGIALLEID", "URLPAGINEGIALLE", "CLIENTESPECIALEFLG", "PRIORITA") AS 
  SELECT 
          c.CD0A_RAW_XID_DES || c.cd0a_prog_trns_des AS TransactionId,
          /*c.cd0a_raw_xid_des as raw_xid_des,
          c.cd0a_prog_trns_des as prog_trns_des,*/
          c.CDL0_CUST_COD AS CustomerId,                  --Codice_Customer__c
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede, --Codice_Sede__c
          s.CDL1_SEDE_COD sede_cod_test,
          c.CD0A_TIPO_EVEN_COD AS Evento,
          TO_CHAR (c.CD0A_RECD_NUM) AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (c.cd0a_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          c.cd0a_stat_recd_cod AS Stato,
          c.CDL0_PREC_CUST_COD AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          REPLACE (
             DECODE (c.CDL0_RAGI_SOCL_DES,
                     NULL, c.CDL0_NOME_NOM || ' ' || c.CDL0_COGN_COG,
                     c.CDL0_RAGI_SOCL_DES),
             '"',
             '\"')
             AS RagioneSociale,                                        -- Name
          c.CDL0_ANNO_RIFF_ANN AS AnnoFatturato,        --IOL_AnnoFatturato__c
          c.CDL0_ANNO_FOND_ANN AS AnnoFondazione,         --Anno_Fondazione__c
          c.CDL0_ANNO_RIFD_ANN AS AnnoRiferimentoDipendenti, --IOL_AnnoRiferimentoDipendenti__c
          TO_CHAR (TRUNC (c.CDL0_FATT_PUNT_IMP)) AS Fatturato, --AnnualRevenue
          SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40) AS Citta,         --BillingCity
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,                --BillingCountry
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,              --BillingState
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,                                     --BillingStreet
          DECODE (c.CDL0_BUON_ORDN_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS BuonoOrdine,                              --IOL_BuonoOrdine__c
          s.CDL3_CAP_COD AS Cap,                                      --Cap__c
          c.CD81_CATG_MAXS_COD AS CodiceCategoriaMassimaSpesa, --IOL_CodiceCategoriaMercMassimaSpesa__c
          REPLACE (c.CD81_CATG_MAXS_DES, '"', '\"') AS CategoriaMassimaSpesa, --IOL_Categoria__c
          s.CDL3_NUCL_COMM_COD AS Cellula,                        --Cellula__c
          c.CDLG_CLASS_PMI_DES AS ClassificazionePMI, --IOL_classificazionePMI__c
          s.CD18_AREA_ELEN_COD AS AreaElenco,        --IOL_CodiceAreaElenco__c
          c.CD82_CATG_ISTA_COD AS CodiceCategoriaISTAT, --IOL_CodiceCategoriaISTAT__c
          c.CD81_CATG_PREV_COD AS CodiceCategoriaMerceologica, --IOL_CodiceCategoriaMerceologica__c
          s.CDL3_COMN_SEAT_COD AS CodiceComune,         --Codice_Comune_IOL__c
          c.CDL0_STAT_COD AS StatoCustomer,         --Codice_Stato_Customer__c
          c.CDL0_CFIS_COD AS CodiceFiscale,                 --CodiceFiscale__c
          s.CDL3_FRAZ_COD AS CodiceFrazione,              --Codice_Frazione__c
          --s.CDL1_UNI_ORG_COD AS IPA,                           --Codice_IPA__c
          NULL AS IPA,                                         --Codice_IPA__c
          c.CDFE_CCIAA AS CodProvinciaREA,           --Codice_Provincia_REA__c
          c.CDFE_REA AS NumeroRea,                          --IOL_NumeroREA__c
          s.CDL1_STAT_COD AS StatoSede,                 --Codice_Stato_Sede__c
          c.CDL0_COGN_COG AS Cognome,            --IOL_CognomePersonaFisica__c
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune, --Comune_IOL__c
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX, --Coordinate_Anagrafiche__c (1)
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY, --Coordinate_Anagrafiche__c (2)
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true') -- [RB20190517 - orrore]
             AS CoordinateManuali,                     --Coordinate_Manuali__c
          --c.CDL0_MRST_CUST_FLG as PreScoring, --Credit_Prescoring__c
          DECODE (c.CDL0_DFES_CERT_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS DatoFiscaleEsteroCertificato, --Dato_Fiscale_Estero_Certificato__c
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa, --Denominazione_Alternativa__c
          NULL AS DenominazioneAlternativa,     --Denominazione_Alternativa__c
          REPLACE (SUBSTR (c.CD82_CATG_ISTA_DES, 0, 255), '"', '\"')
             AS CategoriaISTAT,                        --IOL_CategoriaISTAT__c
          REPLACE (c.CD81_CATG_PREV_DES, '"', '\"') AS CategoriaMerceologica, --IOL_CategoriaMerceologica__c
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,                 --DUG__c
          v.EmailArricchimento AS EmailArricchimento, --Email_Arricchimento__c
          v.EmailArricchimentoId AS EmailArricchimentoId, --Email_Arricchimento_ID__c
          v.EmailBozze AS EmailBozze,                         --Email_Bozze__c
          v.EmailBozzeId AS EmailBozzeId,                  --Email_Bozze_ID__c
          v.EmailCommercialeIOL AS EmailCommercialeIOL, --Email_Commerciale_IOL__c
          v.EmailCommercialeIOLId AS EmailCommercialeIOLId, --Email_Commerciale_IOL_ID__c
          v.EmailFatturazione AS EmailFatturazione,    --Email_Fatturazione__c
          v.EmailFatturazioneId AS EmailFatturazioneId, --Email_Fatturazione_ID__c
          v.EmailFattElettronica AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          v.EmailFattElettronicaId AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          v.EmailPostVendita AS EmailPostVendita,      --Email_Post_Vendita__c
          v.EmailPostVenditaId AS EmailPostVenditaId, --Email_Post_Vendita_ID__c
          v.EmailArricchimentoPECFlag AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          v.EmailBozzePECFlag AS EmailBozzePECFlag,   --IOL_Email_Bozze_PEC__c
          v.EmailCommercialeIOLPECFlag AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          v.EmailFatturazionePECFlag AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          v.EmailFattElettronicaPECFlag AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          v.EmailPostVenditaPECFlag AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          DECODE (c.CDL0_ENTE_PUBB_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS EntePubblicoFlag,                           --Ente_Pubblico__c
          DECODE (c.CDL0_FATT_CFIS_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS CodiceFiscaleFatturazione,    --Fatturazione_Codice_Fiscale__c
          DECODE (c.CDL0_FATT_OBBL_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS FattElettronicaObbligatoria, --Fatturazione_Elettronica_Obbligatoria__c
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione, --Frazione__c
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,       --IOL_IndirizzoID__c
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo, --Info_Toponimo__c
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,        --Insegna__c
          DECODE (c.CD44_MERC_AGGR_COD, NULL, '0', c.CD44_MERC_AGGR_COD)
             AS MercatoAggregato,                    --IOL_MercatoAggregato__c
          c.CD68_AREA_MERC_COD AS Industry,                         --Industry
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,           --BillingCountryCod
          c.CD17_NAZN_SEAT_DES AS DescNazione,                    --Nazione__c
          c.cdl0_nazn_seat_cod AS CodiceNazioneCustomer, --IOL_CustomerCountrCod__c
          DECODE (c.CDLH_ATRB_NIP_FLG,  'S', 'SI',  'N', 'NO')
             AS PotenzialeNIP,                          --IOL_PotenzialeNIP__c
          c.CDL0_NOME_NOM AS Nome,                  --IOL_NomePersonaFisica__c
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura, --Note_sul_Recapito_Fattura__c
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,   --Numero_Civico__c
          TO_CHAR (c.CDL0_NUMR_DIPE_NUM) AS NumeroDipendenti, --NumberOfEmployees
          s.CDL2_PDR_COD AS Opec,                                    --Opec__c
          DECODE (c.CDLH_CONS_FFVV_FLG,
                  'S', 'true',
                  'N', 'false',
                  'false')
             AS OpecConsegnabile,                    --IOL_OpecConsegnabile__c
          c.CDL0_PIVA_COD AS PartitaIva,                       --PartitaIva__c
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche, --Profondita_Coordinate_Anagrafiche__c
          s.CDL3_PROV_SEAT_DES AS Provincia,                --Provincia_IOL__c
          DECODE (c.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,    --Sede_Primaria_OPEC__c
          DECODE (c.CDAJ_SARE_MERC_COD, NULL, 'X', c.CDAJ_SARE_MERC_COD)
             AS SottoAreaMercato,                    --IOL_SottoareaMercato__c
          c.CD71_STCL_DIPN_DES AS SottoclasseDipendenti, --IOL_SottoclasseDipendenti__c
          c.CD66_STCL_FATT_DES AS SottoclasseFatturato, --IOL_SottoclasseFatturato__c
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz, --Stato_Opec_Commerciale__Attualizzato__c
          NULL AS STATOOPECCOMMATTUALIZ,
          DECODE (c.CD67_TIPO_MERC_COD, NULL, '0', c.CD67_TIPO_MERC_cod)
             AS TipoMercato,                              --IOL_TipoMercato__c
          v.telefono1id AS Telefono1Id,                     --Telefono_1_ID__c
          v.Telefono1 AS Telefono1,                                    --Phone
          v.Telefono1Primario AS Telefono1Primario,             --Primario1__c
          v.TipoTelefono1 AS TipoTelefono1,            --TipologiaTelefono1__c
          v.Telefono2Id AS Telefono2Id,                     --Telefono_2_ID__c
          v.Telefono2 AS Telefono2,                            --Telefono2__ c
          v.Telefono2Primario AS Telefono2Primario,             --Primario2__c
          v.TipoTelefono2 AS TipoTelefono2,            --TipologiaTelefono2__c
          v.Telefono3Id AS Telefono3Id,                     --Telefono_3_ID__c
          v.Telefono3 AS Telefono3,                            --Telefono3__ c
          v.Telefono3Primario AS Telefono3Primario,             --Primario3__c
          v.TipoTelefono3 AS TipoTelefono3,            --TipologiaTelefono3__c
          v.Telefono4Id AS Telefono4Id,                     --Telefono_4_ID__c
          v.Telefono4 AS Telefono4,                            --Telefono4__ c
          v.Telefono4Primario AS Telefono4Primario,             --Primario4__c
          v.TipoTelefono4 AS TipoTelefono4,            --TipologiaTelefono4__c
          v.Telefono5Id AS Telefono5Id,                     --Telefono_5_ID__c
          v.Telefono5 AS Telefono5,                            --Telefono5__ c
          v.Telefono5Primario AS Telefono5Primario,             --Primario5__c
          v.TipoTelefono5 AS TipoTelefono5,            --TipologiaTelefono5__c
          v.Telefono6Id AS Telefono6Id,                     --Telefono_6_ID__c
          v.Telefono6 AS Telefono6,                            --Telefono6__ c
          v.Telefono6Primario AS Telefono6Primario,             --Primario6__c
          v.TipoTelefono6 AS TipoTelefono6,            --TipologiaTelefono6__c
          v.Telefono7Id AS Telefono7Id,                     --Telefono_7_ID__c
          v.Telefono7 AS Telefono7,                            --Telefono7__ c
          v.Telefono7Primario AS Telefono7Primario,             --Primario7__c
          v.TipoTelefono7 AS TipoTelefono7,            --TipologiaTelefono7__c
          v.Telefono8Id AS Telefono8Id,                     --Telefono_8_ID__c
          v.Telefono8 AS Telefono8,                            --Telefono8__ c
          v.Telefono8Primario AS Telefono8Primario,             --Primario8__c
          v.TipoTelefono8 AS TipoTelefono8,            --TipologiaTelefono8__c
          DECODE (c.CDL0_PERS_GIUR_COD,
                  'PF', 'PF',
                  'PG', 'PG',
                  NULL, 'DD',
                  'DD')
             AS FormaGiuridica,               --Tipologia_Persona_Giuridica__c
          c.CD74_TIPO_SOCT_COD AS TipoSocGiuridica, --Tipologia_societa_giuridica__c
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo, --Toponimo__c
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa, --Ultima_Posizione_Amminstrativa__c
          REPLACE (c.CD74_TIPO_SOCT_DES, '"', '\"') AS TipoSocieta,     --Type
          TO_CHAR (c.CDL6_URL_NUM_FNPG) AS UrlFanPageId,   --URL_ID_Fanpage__c
          c.CDL6_URL_DES_FNPG AS UrlFanPage,                  --URL_Fanpage__c
          TO_CHAR (c.CDL6_URL_NUM_ISTZ) AS UrlIstituzionaleId, --URL_ID_Istituzionale__c
          c.CDL6_URL_DES_ISTZ AS UrlIstituzionale,                   --Website
          TO_CHAR (c.CDL6_URL_NUM_PGBI) AS UrlPagineBiancheId, --URL_ID_Pagine_Bianche__c
          c.CDL6_URL_DES_PGBI AS UrlPagineBianche,     --URL_Pagine_Bianche__c
          TO_CHAR (c.CDL6_URL_NUM_PGGI) AS UrlPagineGialleId, --URL_ID_Pagine_Gialle__c
          c.CDL6_URL_DES_PGGI AS UrlPagineGialle,       --URL_Pagine_Gialle__c
          COALESCE (c.CDAG_SPEC_CLNT_COD, 'G') AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (c.CD0A_PRTY_COD, 'H') AS Priorita
     FROM tcd0b_rp_sf_sede s
          JOIN tcd0a_rp_sf_customer c
             ON     s.cdl0_cust_cod = c.cdl0_cust_cod
                --and s.cd0b_stat_recd_cod = c.cd0a_stat_recd_cod
                /*AND c.cd0a_raw_xid_des = s.cd0b_raw_xid_des
                AND NVL (c.cd0a_prog_trns_des, '000') =
                       NVL (s.cd0b_prog_trns_des, '000')
                and c.cd0a_prog_trns_des = s.cd0b_prog_trns_des*/
                AND c.cd0a_raw_xid_des || c.cd0a_prog_trns_des =
                       s.cd0b_raw_xid_des || s.cd0b_prog_trns_des
                AND s.cdl1_prim_sedc_flg = 'S'
          LEFT JOIN mcd0b_rp_sf_sctype v
             ON     v.cust_cod = s.cdl0_cust_cod
                AND v.sede_cod = s.cdl1_sede_cod
                AND v.raw_xid_des = s.cd0b_raw_xid_des
                AND v.prog_trns_des = s.cd0b_prog_trns_des
                AND v.tipo = 'C'
--    where s.cd0b_raw_xid_des || s.cd0b_prog_trns_des = '001100070150C6A7000'
   UNION
   SELECT 
          s.CD0b_RAW_XID_DES || s.cd0b_prog_trns_des AS TransactionId,
          /*s.cd0b_raw_xid_des as raw_xid_des,
          s.cd0b_prog_trns_des as prog_trns_des,*/
          s.cdl0_cust_cod AS CustomerId,
          s.CDL1_SEDE_COD || CDL1_NVER_SEDE_COD AS CodiceSede,
          s.CDL1_SEDE_COD sede_cod_test,
          s.CD0B_TIPO_EVEN_COD AS Evento,
          NULL AS OrdCust,
          TO_CHAR (s.CD0B_RECD_NUM) AS OrdSede,
          TO_CHAR (s.cd0b_strt_trns_tim, 'YYYY-mm-DD HH:MM:SS')
             AS DataTransazione,
          s.cd0b_stat_recd_cod AS Stato,
          NULL AS CustomerId_Old,
          s.CDL1_PREC_SEDE_COD || CDL1_PREC_NVER_SEDE_COD AS CodiceSede_Old,
          NULL AS RagioneSociale,
          NULL AS AnnoFatturato,
          NULL AS AnnoFondazione,
          NULL AS AnnoRiferimentoDipendenti,
          NULL AS Fatturato,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Citta,
          s.CDL3_NAZN_SEAT_DES AS NazioneSede,
          s.CDL3_PROV_SEAT_COD AS CodiceProvincia,
          ltrim(REPLACE (
                s.CDL3_DUG_DES
             || ' '
             || s.CDL3_TOPN_STRD_DES
             || ' '
             || s.CDL3_CIVC_DES,
             '"',
             '\"'))
             AS Indirizzo,
          'false' AS BuonoOrdine,
          s.CDL3_CAP_COD AS Cap,
          NULL AS CodiceCategoriaMassimaSpesa,
          NULL AS CategoriaMassimaSpesa,
          s.CDL3_NUCL_COMM_COD AS Cellula,
          NULL AS ClassificazionePMI,
          s.CD18_AREA_ELEN_COD AS AreaElenco,
          NULL AS CodiceCategoriaISTAT,
          NULL AS CodiceCategoriaMerceologica,
          s.CDL3_COMN_SEAT_COD AS CodiceComune,
          NULL AS StatoCustomer,
          NULL AS CodiceFiscale,
          s.CDL3_FRAZ_COD AS CodiceFrazione,
          --s.CDL1_UNI_ORG_COD AS IPA,
          NULL AS IPA,
          NULL AS CodProvinciaREA,
          NULL AS NumeroRea,
          s.CDL1_STAT_COD AS StatoSede,
          NULL AS Cognome,
          REPLACE (SUBSTR (s.CDL3_COMN_SEAT_DES, 0, 40), '"', '\"') AS Comune,
          TO_CHAR (s.CDL3_CRDX_VAL) AS CoordX,
          TO_CHAR (s.CDL3_CRDY_VAL) AS CoordY,
          DECODE (s.CDD6B_CRD_AUT_FLG,  'S', 'false',  'N', 'true',  'true')
             AS CoordinateManuali,
          --null as PreScoring,
          'false' AS DatoFiscaleEsteroCertificato,
          --replace(s.CDL1_DENM_ALTE_DES, '"', '\"') AS DenominazioneAlternativa,
          NULL AS DenominazioneAlternativa,
          NULL AS CategoriaISTAT,
          NULL AS CategoriaMerceologica,
          REPLACE (s.CDL3_DUG_DES, '"', '\"') AS DUG,
          v.EmailArricchimento AS EmailArricchimento, --Email_Arricchimento__c
          v.EmailArricchimentoId AS EmailArricchimentoId, --Email_Arricchimento_ID__c
          v.EmailBozze AS EmailBozze,                         --Email_Bozze__c
          v.EmailBozzeId AS EmailBozzeId,                  --Email_Bozze_ID__c
          v.EmailCommercialeIOL AS EmailCommercialeIOL, --Email_Commerciale_IOL__c
          v.EmailCommercialeIOLId AS EmailCommercialeIOLId, --Email_Commerciale_IOL_ID__c
          v.EmailFatturazione AS EmailFatturazione,    --Email_Fatturazione__c
          v.EmailFatturazioneId AS EmailFatturazioneId, --Email_Fatturazione_ID__c
          v.EmailFattElettronica AS EmailFattElettronica, --Email_Fatturazione_Elettronica__c
          v.EmailFattElettronicaId AS EmailFattElettronicaId, --Email_Fatturazione_Elettronica_ID__c
          v.EmailPostVendita AS EmailPostVendita,      --Email_Post_Vendita__c
          v.EmailPostVenditaId AS EmailPostVenditaId, --Email_Post_Vendita_ID__c
          v.EmailArricchimentoPECFlag AS EmailArricchimentoPECFlag, --IOL_Email_Arricchimento_PEC__c
          v.EmailBozzePECFlag AS EmailBozzePECFlag,   --IOL_Email_Bozze_PEC__c
          v.EmailCommercialeIOLPECFlag AS EmailCommercialeIOLPECFlag, --IOL_Email_Commerciale_IOL_PEC__c
          v.EmailFatturazionePECFlag AS EmailFatturazionePECFlag, --IOL_Email_Fatturazione_PEC__c
          v.EmailFattElettronicaPECFlag AS EmailFattElettronicaPECFlag, --IOL_Email_Fatturazione_Elettronica_PEC__c
          v.EmailPostVenditaPECFlag AS EmailPostVenditaPECFlag, --IOL_Email_Post_Vendita_PEC__c
          'false' AS EntePubblicoFlag,
          'false' AS CodiceFiscaleFatturazione,
          'false' AS FattElettronicaObbligatoria,
          REPLACE (s.CDL3_FRAZ_SEAT_DES, '"', '\"') AS Frazione,
          TO_CHAR (s.CDL3_INDI_NUM) AS IdIndirizzo,
          REPLACE (s.CDL3_INFG_TOPN_DES, '"', '\"') AS InfoToponimo,
          REPLACE (s.CDL1_INSG_DES, '"', '\"') AS Insegna,
          '0' AS MercatoAggregato,
          NULL AS Industry,
          s.CDL3_NAZN_SEAT_COD AS CodiceNazione,
          NULL AS DescNazione,
          NULL AS CodiceNazioneCustomer,            --IOL_CustomerCountrCod__c
          'NO' AS PotenzialeNIP,
          NULL AS Nome,
          REPLACE (s.CDL1_RECP_FATT_DES, '"', '\"') AS NoteRecapitoFattura,
          REPLACE (s.CDL3_CIVC_DES, '"', '\"') AS Civico,
          NULL AS NumeroDipendenti,
          s.CDL2_PDR_COD AS Opec,
          'false' AS OpecConsegnabile,
          NULL AS PartitaIva,
          DECODE (s.CD46_PROF_CORD_DES, NULL, 'ND', s.CD46_PROF_CORD_COD)
             AS ProfCoordinateGeografiche,
          s.CDL3_PROV_SEAT_DES AS Provincia,
          'true' AS CustomerCessatoRiattivabile, --IOL_CustomerCessatoRiattivabile__c
          DECODE (s.CD28_STAT_RIAT_FLG,  'S', 'true',  'N', 'false',  'true')
             AS SedeCessataRiattivabile,      --IOL_SedeCessataRiattivabile__c
          s.CDL1_PRIM_SEDC_FLG AS SedePrimariaFlag,
          'X' AS SottoAreaMercato,
          NULL AS SottoclasseDipendenti,
          NULL AS SottoclasseFatturato,
          --s.CD47_PROS_TYPE_DES as StatoOpecCommAttualiz,
          NULL AS STATOOPECCOMMATTUALIZ,
          '0' AS TipoMercato,
          v.telefono1id AS Telefono1Id,                     --Telefono_1_ID__c
          v.Telefono1 AS Telefono1,                                    --Phone
          v.Telefono1Primario AS Telefono1Primario,             --Primario1__c
          v.TipoTelefono1 AS TipoTelefono1,            --TipologiaTelefono1__c
          v.Telefono2Id AS Telefono2Id,                     --Telefono_2_ID__c
          v.Telefono2 AS Telefono2,                            --Telefono2__ c
          v.Telefono2Primario AS Telefono2Primario,             --Primario2__c
          v.TipoTelefono2 AS TipoTelefono2,            --TipologiaTelefono2__c
          v.Telefono3Id AS Telefono3Id,                     --Telefono_3_ID__c
          v.Telefono3 AS Telefono3,                            --Telefono3__ c
          v.Telefono3Primario AS Telefono3Primario,             --Primario3__c
          v.TipoTelefono3 AS TipoTelefono3,            --TipologiaTelefono3__c
          v.Telefono4Id AS Telefono4Id,                     --Telefono_4_ID__c
          v.Telefono4 AS Telefono4,                            --Telefono4__ c
          v.Telefono4Primario AS Telefono4Primario,             --Primario4__c
          v.TipoTelefono4 AS TipoTelefono4,            --TipologiaTelefono4__c
          v.Telefono5Id AS Telefono5Id,                     --Telefono_5_ID__c
          v.Telefono5 AS Telefono5,                            --Telefono5__ c
          v.Telefono5Primario AS Telefono5Primario,             --Primario5__c
          v.TipoTelefono5 AS TipoTelefono5,            --TipologiaTelefono5__c
          v.Telefono6Id AS Telefono6Id,                     --Telefono_6_ID__c
          v.Telefono6 AS Telefono6,                            --Telefono6__ c
          v.Telefono6Primario AS Telefono6Primario,             --Primario6__c
          v.TipoTelefono6 AS TipoTelefono6,            --TipologiaTelefono6__c
          v.Telefono7Id AS Telefono7Id,                     --Telefono_7_ID__c
          v.Telefono7 AS Telefono7,                            --Telefono7__ c
          v.Telefono7Primario AS Telefono7Primario,             --Primario7__c
          v.TipoTelefono7 AS TipoTelefono7,            --TipologiaTelefono7__c
          v.Telefono8Id AS Telefono8Id,                     --Telefono_8_ID__c
          v.Telefono8 AS Telefono8,                            --Telefono8__ c
          v.Telefono8Primario AS Telefono8Primario,             --Primario8__c
          v.TipoTelefono8 AS TipoTelefono8,            --TipologiaTelefono8__c
          'DD' AS FormaGiuridica,
          NULL AS TipoSocGiuridica,
          REPLACE (s.CDL3_TOPN_STRD_DES, '"', '\"') AS Toponimo,
          s.CDL1_UPA_COD AS UltimaPosizioneAmministrativa,
          NULL AS TipoSocieta,
          NULL AS UrlFanPageId,
          NULL AS UrlFanPage,
          NULL AS UrlIstituzionaleId,
          NULL AS UrlIstituzionale,
          NULL AS UrlPagineBiancheId,
          NULL AS UrlPagineBianche,
          NULL AS UrlPagineGialleId,
          NULL AS UrlPagineGialle,
          NULL AS ClienteSpecialeFlg, --IOL_CodiceIndicatoreClientiSpeciali__c
          COALESCE (s.CD0b_PRTY_COD, 'H')
     FROM    tcd0b_rp_sf_sede s
          LEFT JOIN
             mcd0b_rp_sf_sctype v
          ON     v.cust_cod = s.cdl0_cust_cod
             AND v.sede_cod = s.cdl1_sede_cod
             AND v.raw_xid_des = s.cd0b_raw_xid_des
             AND v.prog_trns_des = s.cd0b_prog_trns_des
             AND v.tipo = 'S'
    WHERE s.cdl1_prim_sedc_flg = 'N'
--------------------------------------------------------
--  DDL for View VCD0B_SF_POLLER_STATUS
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0B_SF_POLLER_STATUS" ("TYPE", "DES", "VAL", "DTA") AS 
  select 'LAST_RUN' as TYPE, 'Ultimo run POLLER' as DES, 0 AS VAL, to_char(max(s.cd0b_poll_dta), 'DD/MM/YYYY hh24:mi:ss') as DTA from tcd0b_rp_sf_sede s where s.cd0b_poll_dta is not null union 
select cd96_parm_cod, cd96_parm_des, to_number(cd96_parm_valr_des), to_char(cd96_modi_tim, 'DD/MM/YYYY hh24:mi:ss') from tcd96_lc_parametri where cd96_parm_cod = 'SFS' union 
select 'STATO', cd0b_stat_recd_cod, count(DISTINCT CD0B_RAW_XID_DES||CD0B_PROG_TRNS_DES) as n,  
to_char(case when cd0b_stat_recd_cod in ('O', 'K') then max(CD0B_MLSF_UPD_DTA) when cd0b_stat_recd_cod in ('Q', 'P') then max(cd0b_poll_dta) else null end, 'DD/MM/YYYY hh24:mi:ss') as DTA 
from tcd0b_rp_sf_sede group by cd0b_stat_recd_cod order by 1, 3 desc
--------------------------------------------------------
--  DDL for View VCD0C_RP_SF_TELEFONO
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0C_RP_SF_TELEFONO" ("TRANSACTIONID", "CUSTOMER", "CODICESEDE", "STATO", "ORDNUM", "RAW_XID_DES", "PROG_TRNS_DES", "T1_TEL_ID", "T1_TEL_NUMR", "T1_TEL_TYPE", "T1_PRIM_FLG", "T2_TEL_ID", "T2_TEL_NUMR", "T2_TEL_TYPE", "T2_PRIM_FLG", "T3_TEL_ID", "T3_TEL_NUMR", "T3_TEL_TYPE", "T3_PRIM_FLG", "T4_TEL_ID", "T4_TEL_NUMR", "T4_TEL_TYPE", "T4_PRIM_FLG", "T5_TEL_ID", "T5_TEL_NUMR", "T5_TEL_TYPE", "T5_PRIM_FLG", "T6_TEL_ID", "T6_TEL_NUMR", "T6_TEL_TYPE", "T6_PRIM_FLG", "T7_TEL_ID", "T7_TEL_NUMR", "T7_TEL_TYPE", "T7_PRIM_FLG", "T8_TEL_ID", "T8_TEL_NUMR", "T8_TEL_TYPE", "T8_PRIM_FLG") AS 
  WITH telefoni
        AS (SELECT /*+ NO_INDEX(a) */
                  DENSE_RANK ()
                   OVER (
                      PARTITION BY    CD0C_RAW_XID_DES
                                   || NVL (cd0c_prog_trns_des, '000'),
                                   CDL1_SEDE_COD
                      ORDER BY CD0C_RECD_NUM)
                      AS n,
                   CD0C_RAW_XID_DES || NVL (cd0c_prog_trns_des, '000')
                      AS CD0C_RAW_XID_DES,
                   a.cd0c_raw_xid_des AS RAW_XID_DES,
                   a.cd0c_prog_trns_des AS prog_trns_des,
                   CDL0_CUST_COD,
                   CDL1_SEDE_COD,
                   CD0C_RECD_NUM,
                   CDL4_TELE_NUM,
                   CDL4_TELF_PREF_COD,
                   CDL4_TELF_NUMR_COD,
                   CDL4_FISS_FLG,
                   CDL4_CELL_FLG,
                   CDL4_FAX_FLG,
                   CDL4_CENT_FLG,
                   CD62_TIPO_VERD_COD,
                   CD62_TIPO_VERD_DES,
                   DECODE (CDMC_PRIM_TELS_FLG,
                           'S', 'true',
                           'N', 'false',
                           NULL, 'false')
                      AS CDMC_PRIM_TELS_FLG,
                   CDL4_PUBBL_FLG,
                   CDMC_ORDINE,
                   CDMC_IND_POTN_NUM,
                   CD97_FONT_CREA_COD,
                   CDL4_CREA_TIM,
                   CD0C_STAT_RECD_COD
              FROM tcd0c_rp_sf_telefono a)
   SELECT t1.CD0C_RAW_XID_DES AS TransactionId,
          t1.CDL0_CUST_COD AS Customer,
          t1.CDL1_SEDE_COD AS CodiceSede,
          t1.CD0C_STAT_RECD_COD AS Stato,
          t1.CD0C_RECD_NUM AS OrdNum,
          t1.RAW_XID_DES,
          t1.prog_trns_des,
          t1.CDL4_TELE_NUM AS t1_TEL_ID,
          t1.CDL4_TELF_PREF_COD || t1.CDL4_TELF_NUMR_COD AS t1_TEL_NUMR,
          CASE
             WHEN t1.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t1.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t1.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t1.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t1.CDL4_FISS_FLG = 'S' OR t1.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t1_TEL_TYPE,
          CASE
             WHEN t1.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t1.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t1_PRIM_FLG,
          t2.CDL4_TELE_NUM AS t2_TEL_ID,
          t2.CDL4_TELF_PREF_COD || t2.CDL4_TELF_NUMR_COD AS t2_TEL_NUMR,
          CASE
             WHEN t2.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t2.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t2.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t2.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t2.CDL4_FISS_FLG = 'S' OR t2.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t2_TEL_TYPE,
          CASE
             WHEN t2.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t2.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t2_PRIM_FLG,
          t3.CDL4_TELE_NUM AS t3_TEL_ID,
          t3.CDL4_TELF_PREF_COD || t3.CDL4_TELF_NUMR_COD AS t3_TEL_NUMR,
          CASE
             WHEN t3.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t3.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t3.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t3.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t3.CDL4_FISS_FLG = 'S' OR t3.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t3_TEL_TYPE,
          CASE
             WHEN t3.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t3.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t3_PRIM_FLG,
          t4.CDL4_TELE_NUM AS t4_TEL_ID,
          t4.CDL4_TELF_PREF_COD || t4.CDL4_TELF_NUMR_COD AS t4_TEL_NUMR,
          CASE
             WHEN t4.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t4.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t4.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t4.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t4.CDL4_FISS_FLG = 'S' OR t4.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t4_TEL_TYPE,
          CASE
             WHEN t4.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t4.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t4_PRIM_FLG,
          t5.CDL4_TELE_NUM AS t5_TEL_ID,
          t5.CDL4_TELF_PREF_COD || t5.CDL4_TELF_NUMR_COD AS t5_TEL_NUMR,
          CASE
             WHEN t5.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t5.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t5.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t5.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t5.CDL4_FISS_FLG = 'S' OR t5.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t5_TEL_TYPE,
          CASE
             WHEN t5.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t5.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t5_PRIM_FLG,
          t6.CDL4_TELE_NUM AS t6_TEL_ID,
          t6.CDL4_TELF_PREF_COD || t6.CDL4_TELF_NUMR_COD AS t6_TEL_NUMR,
          CASE
             WHEN t6.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t6.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t6.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t6.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t6.CDL4_FISS_FLG = 'S' OR t6.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t6_TEL_TYPE,
          CASE
             WHEN t6.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t6.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t6_PRIM_FLG,
          t7.CDL4_TELE_NUM AS t7_TEL_ID,
          t7.CDL4_TELF_PREF_COD || t7.CDL4_TELF_NUMR_COD AS t7_TEL_NUMR,
          CASE
             WHEN t7.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t7.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t7.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t7.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t7.CDL4_FISS_FLG = 'S' OR t7.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t7_TEL_TYPE,
          CASE
             WHEN t7.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t7.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t7_PRIM_FLG,
          t8.CDL4_TELE_NUM AS t8_TEL_ID,
          t8.CDL4_TELF_PREF_COD || t8.CDL4_TELF_NUMR_COD AS t8_TEL_NUMR,
          CASE
             WHEN t8.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t8.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t8.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t8.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t8.CDL4_FISS_FLG = 'S' OR t8.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t8_TEL_TYPE,
          CASE
             WHEN t8.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t8.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t8_PRIM_FLG
     FROM telefoni t1
          LEFT JOIN telefoni t2
             ON     t1.cd0c_raw_xid_des = t2.cd0c_raw_xid_des
                --t1.raw_xid_des = t2.raw_xid_des
                --and t1.prog_trns_des = t2.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t2.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t2.cdl1_sede_cod
                AND t2.n = 2
          LEFT JOIN telefoni t3
             ON     t1.cd0c_raw_xid_des = t3.cd0c_raw_xid_des
                --t1.raw_xid_des = t3.raw_xid_des
                --and t1.prog_trns_des = t3.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t3.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t3.cdl1_sede_cod
                AND t3.n = 3
          LEFT JOIN telefoni t4
             ON     t1.cd0c_raw_xid_des = t4.cd0c_raw_xid_des
                --t1.raw_xid_des = t4.raw_xid_des
                --and t1.prog_trns_des = t4.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t4.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t4.cdl1_sede_cod
                AND t4.n = 4
          LEFT JOIN telefoni t5
             ON     t1.cd0c_raw_xid_des = t5.cd0c_raw_xid_des
                --t1.raw_xid_des = t5.raw_xid_des
                --and t1.prog_trns_des = t5.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t5.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t5.cdl1_sede_cod
                AND t5.n = 5
          LEFT JOIN telefoni t6
             ON     t1.cd0c_raw_xid_des = t6.cd0c_raw_xid_des
                --t1.raw_xid_des = t6.raw_xid_des
                --and t1.prog_trns_des = t6.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t6.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t6.cdl1_sede_cod
                AND t6.n = 6
          LEFT JOIN telefoni t7
             ON     t1.cd0c_raw_xid_des = t7.cd0c_raw_xid_des
                --t1.raw_xid_des = t7.raw_xid_des
                --and t1.prog_trns_des = t7.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t7.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t7.cdl1_sede_cod
                AND t7.n = 7
          LEFT JOIN telefoni t8
             ON     t1.cd0c_raw_xid_des = t8.cd0c_raw_xid_des
                --t1.raw_xid_des = t8.raw_xid_des
                --and t1.prog_trns_des = t8.prog_trns_des
                AND t1.CD0C_STAT_RECD_COD = t8.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t8.cdl1_sede_cod
                AND t8.n = 8
    WHERE t1.n = 1
  GRANT SELECT ON "DBACD1CDB"."VCD0C_RP_SF_TELEFONO" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for View VCD0C_RP_SF_TELEFONO_idx
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0C_RP_SF_TELEFONO_idx" ("TRANSACTIONID", "CUSTOMER", "CODICESEDE", "STATO", "ORDNUM", "RAW_XID_DES", "PROG_TRNS_DES", "T1_TEL_ID", "T1_TEL_NUMR", "T1_TEL_TYPE", "T1_PRIM_FLG", "T1_TEL_CONS", "T2_TEL_ID", "T2_TEL_NUMR", "T2_TEL_TYPE", "T2_PRIM_FLG", "T2_TEL_CONS", "T3_TEL_ID", "T3_TEL_NUMR", "T3_TEL_TYPE", "T3_PRIM_FLG", "T3_TEL_CONS", "T4_TEL_ID", "T4_TEL_NUMR", "T4_TEL_TYPE", "T4_PRIM_FLG", "T4_TEL_CONS", "T5_TEL_ID", "T5_TEL_NUMR", "T5_TEL_TYPE", "T5_PRIM_FLG", "T5_TEL_CONS", "T6_TEL_ID", "T6_TEL_NUMR", "T6_TEL_TYPE", "T6_PRIM_FLG", "T6_TEL_CONS", "T7_TEL_ID", "T7_TEL_NUMR", "T7_TEL_TYPE", "T7_PRIM_FLG", "T7_TEL_CONS", "T8_TEL_ID", "T8_TEL_NUMR", "T8_TEL_TYPE", "T8_PRIM_FLG", "T8_TEL_CONS") AS 
  WITH telefoni
        AS (SELECT DENSE_RANK ()
                   OVER (
                      PARTITION BY CD0C_RAW_XID_DES || cd0c_prog_trns_des,
                                   CDL1_SEDE_COD
                      ORDER BY a.cdmc_ordine)
                      AS n,
                   CD0C_RAW_XID_DES || cd0c_prog_trns_des AS CD0C_RAW_XID_DES,
                   a.cd0c_raw_xid_des AS RAW_XID_DES,
                   a.cd0c_prog_trns_des AS prog_trns_des,
                   CDL0_CUST_COD,
                   CDL1_SEDE_COD,
                   CD0C_RECD_NUM,
                   CDL4_TELE_NUM,
                   CDL4_TELF_PREF_COD,
                   CDL4_TELF_NUMR_COD,
                   CDL4_FISS_FLG,
                   CDL4_CELL_FLG,
                   CDL4_FAX_FLG,
                   CDL4_CENT_FLG,
                   CD62_TIPO_VERD_COD,
                   CD62_TIPO_VERD_DES,
                   DECODE (upper(CDMC_PRIM_TELS_FLG),
                           'S', 'true',
                           'N', 'false',
                           NULL, 'false')
                      AS CDMC_PRIM_TELS_FLG,
                   CDL4_PUBBL_FLG,
                   CDMC_ORDINE,
                   CDMC_IND_POTN_NUM,
                   CD97_FONT_CREA_COD,
                   CDL4_CREA_TIM,
                   CD0C_STAT_RECD_COD,
                   decode(upper(CDLT_CON5_COMM_FLG),'S','true','N','false',null,'false') as CDLT_CON5_COMM_FLG
              FROM tcd0c_rp_sf_telefono a)
   SELECT t1.CD0C_RAW_XID_DES AS TransactionId,
          t1.CDL0_CUST_COD AS Customer,
          t1.CDL1_SEDE_COD AS CodiceSede,
          t1.CD0C_STAT_RECD_COD AS Stato,
          t1.cdmc_ordine AS OrdNum,
          t1.RAW_XID_DES,
          t1.prog_trns_des,
          t1.CDL4_TELE_NUM AS t1_TEL_ID,
          t1.CDL4_TELF_PREF_COD || t1.CDL4_TELF_NUMR_COD AS t1_TEL_NUMR,
          CASE
             WHEN t1.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t1.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t1.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t1.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t1.CDL4_FISS_FLG = 'S' OR t1.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t1_TEL_TYPE,
          CASE
             WHEN t1.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t1.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t1_PRIM_FLG,
          nvl(t1.CDLT_CON5_COMM_FLG,'false') as t1_TEL_CONS,
          t2.CDL4_TELE_NUM AS t2_TEL_ID,
          t2.CDL4_TELF_PREF_COD || t2.CDL4_TELF_NUMR_COD AS t2_TEL_NUMR,
          CASE
             WHEN t2.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t2.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t2.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t2.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t2.CDL4_FISS_FLG = 'S' OR t2.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t2_TEL_TYPE,
          CASE
             WHEN t2.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t2.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t2_PRIM_FLG,
          nvl(t2.CDLT_CON5_COMM_FLG,'false') as t2_TEL_CONS,
          t3.CDL4_TELE_NUM AS t3_TEL_ID,
          t3.CDL4_TELF_PREF_COD || t3.CDL4_TELF_NUMR_COD AS t3_TEL_NUMR,
          CASE
             WHEN t3.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t3.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t3.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t3.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t3.CDL4_FISS_FLG = 'S' OR t3.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t3_TEL_TYPE,
          CASE
             WHEN t3.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t3.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t3_PRIM_FLG,
          nvl(t3.CDLT_CON5_COMM_FLG,'false') as t3_TEL_CONS,
          t4.CDL4_TELE_NUM AS t4_TEL_ID,
          t4.CDL4_TELF_PREF_COD || t4.CDL4_TELF_NUMR_COD AS t4_TEL_NUMR,
          CASE
             WHEN t4.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t4.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t4.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t4.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t4.CDL4_FISS_FLG = 'S' OR t4.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t4_TEL_TYPE,
          CASE
             WHEN t4.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t4.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t4_PRIM_FLG,
          nvl(t4.CDLT_CON5_COMM_FLG,'false') as t4_TEL_CONS,
          t5.CDL4_TELE_NUM AS t5_TEL_ID,
          t5.CDL4_TELF_PREF_COD || t5.CDL4_TELF_NUMR_COD AS t5_TEL_NUMR,
          CASE
             WHEN t5.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t5.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t5.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t5.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t5.CDL4_FISS_FLG = 'S' OR t5.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t5_TEL_TYPE,
          CASE
             WHEN t5.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t5.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t5_PRIM_FLG,
          nvl(t5.CDLT_CON5_COMM_FLG,'false') as t5_TEL_CONS,
          t6.CDL4_TELE_NUM AS t6_TEL_ID,
          t6.CDL4_TELF_PREF_COD || t6.CDL4_TELF_NUMR_COD AS t6_TEL_NUMR,
          CASE
             WHEN t6.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t6.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t6.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t6.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t6.CDL4_FISS_FLG = 'S' OR t6.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t6_TEL_TYPE,
          CASE
             WHEN t6.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t6.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t6_PRIM_FLG,
          nvl(t6.CDLT_CON5_COMM_FLG,'false') as t6_TEL_CONS,
          t7.CDL4_TELE_NUM AS t7_TEL_ID,
          t7.CDL4_TELF_PREF_COD || t7.CDL4_TELF_NUMR_COD AS t7_TEL_NUMR,
          CASE
             WHEN t7.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t7.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t7.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t7.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t7.CDL4_FISS_FLG = 'S' OR t7.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t7_TEL_TYPE,
          CASE
             WHEN t7.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t7.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t7_PRIM_FLG,
          nvl(t7.CDLT_CON5_COMM_FLG,'false') as t7_TEL_CONS,
          t8.CDL4_TELE_NUM AS t8_TEL_ID,
          t8.CDL4_TELF_PREF_COD || t8.CDL4_TELF_NUMR_COD AS t8_TEL_NUMR,
          CASE
             WHEN t8.CDL4_CELL_FLG = 'S'
             THEN
                'Mobile'
             WHEN t8.CDL4_FAX_FLG = 'S'
             THEN
                'Fax'
             WHEN     t8.CD62_TIPO_VERD_COD IS NOT NULL
                  AND t8.CD62_TIPO_VERD_COD >= 1
             THEN
                'Numero Verde'
             WHEN t8.CDL4_FISS_FLG = 'S' OR t8.CDL4_CENT_FLG = 'S'
             THEN
                'Fisso'
          END
             AS t8_TEL_TYPE,
          CASE
             WHEN t8.CDL1_SEDE_COD IS NOT NULL
             THEN
                NVL (t8.CDMC_PRIM_TELS_FLG, 'false')
             ELSE
                'false'
          END
             AS t8_PRIM_FLG,
          nvl(t8.CDLT_CON5_COMM_FLG,'false') as t8_TEL_CONS
     FROM telefoni t1
          LEFT JOIN telefoni t2
             ON     t1.cd0c_raw_xid_des = t2.cd0c_raw_xid_des
                --t1.raw_xid_des = t2.raw_xid_des
                --and t1.prog_trns_des = t2.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t2.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t2.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t2.CDL0_CUST_COD
                AND t2.n = 2
          LEFT JOIN telefoni t3
             ON     t1.cd0c_raw_xid_des = t3.cd0c_raw_xid_des
                --t1.raw_xid_des = t3.raw_xid_des
                --and t1.prog_trns_des = t3.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t3.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t3.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t3.CDL0_CUST_COD
                AND t3.n = 3
          LEFT JOIN telefoni t4
             ON     t1.cd0c_raw_xid_des = t4.cd0c_raw_xid_des
                --t1.raw_xid_des = t4.raw_xid_des
                --and t1.prog_trns_des = t4.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t4.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t4.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t4.CDL0_CUST_COD
                AND t4.n = 4
          LEFT JOIN telefoni t5
             ON     t1.cd0c_raw_xid_des = t5.cd0c_raw_xid_des
                --t1.raw_xid_des = t5.raw_xid_des
                --and t1.prog_trns_des = t5.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t5.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t5.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t5.CDL0_CUST_COD
                AND t5.n = 5
          LEFT JOIN telefoni t6
             ON     t1.cd0c_raw_xid_des = t6.cd0c_raw_xid_des
                --t1.raw_xid_des = t6.raw_xid_des
                --and t1.prog_trns_des = t6.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t6.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t6.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t6.CDL0_CUST_COD
                AND t6.n = 6
          LEFT JOIN telefoni t7
             ON     t1.cd0c_raw_xid_des = t7.cd0c_raw_xid_des
                --t1.raw_xid_des = t7.raw_xid_des
                --and t1.prog_trns_des = t7.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t7.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t7.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t7.CDL0_CUST_COD
                AND t7.n = 7
          LEFT JOIN telefoni t8
             ON     t1.cd0c_raw_xid_des = t8.cd0c_raw_xid_des
                --t1.raw_xid_des = t8.raw_xid_des
                --and t1.prog_trns_des = t8.prog_trns_des
                --and t1.CD0C_STAT_RECD_COD = t8.CD0C_STAT_RECD_COD
                AND t1.cdl1_sede_cod = t8.cdl1_sede_cod
                AND t1.CDL0_CUST_COD = t8.CDL0_CUST_COD
                AND t8.n = 8
    WHERE t1.n = 1
--------------------------------------------------------
--  DDL for View VCD0D_RP_SF_MAIL
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0D_RP_SF_MAIL" ("TRANSACTIONID", "RAW_XID_DES", "PROG_TRNS_DES", "CUSTOMER", "CODICESEDE", "STATO", "AR_EML_ID", "AR_EML_TXT", "AR_PEC_FLG", "BZ_EML_ID", "BZ_EML_TXT", "BZ_PEC_FLG", "CS_EML_ID", "CS_EML_TXT", "CS_PEC_FLG", "FT_EML_ID", "FT_EML_TXT", "FT_PEC_FLG", "FE_EML_ID", "FE_EML_TXT", "FE_PEC_FLG", "PV_EML_ID", "PV_EML_TXT", "PV_PEC_FLG") AS 
  WITH mails
        AS (SELECT /*+ NO_INDEX(m) */
                  CDL0_CUST_COD,
                   CD0D_RAW_XID_DES || NVL (CD0d_PROG_TRNS_DES, '000')
                      AS CD0D_RAW_XID_DES,
                   m.cd0d_raw_xid_des AS raw_xid_des,
                   m.CD0d_PROG_TRNS_DES AS PROG_TRNS_DES,
                   CDL1_SEDE_COD,
                   CD0D_RECD_NUM,
                   CDL5_MAIL_NUM,
                   CDL5_MAIL_DES,
                   CDL5_EML_PEC_FLG,
                   CD35_UTEM_COD,
                   CD0D_STAT_RECD_COD
              FROM TCD0D_RP_SF_MAIL m)
   SELECT /*+ NO_INDEX(s) */
         s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000')
             AS TransactionId,
          s.CD0B_RAW_XID_DES AS RAW_XID_DES,
          s.CD0B_PROG_TRNS_DES AS PROG_TRNS_DES,
          s.CDL0_CUST_COD AS Customer,
          s.cdl1_sede_cod AS CodiceSede,
          s.CD0B_STAT_RECD_COD AS Stato,
          m1.CDL5_MAIL_NUM AS AR_EML_ID,
          SUBSTR (m1.CDL5_MAIL_DES, 0, 80) AS AR_EML_TXT,
          DECODE (m1.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS AR_PEC_FLG,
          m2.CDL5_MAIL_NUM AS BZ_EML_ID,
          SUBSTR (m2.CDL5_MAIL_DES, 0, 80) AS BZ_EML_TXT,
          DECODE (m2.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS BZ_PEC_FLG,
          m3.CDL5_MAIL_NUM AS CS_EML_ID,
          SUBSTR (m3.CDL5_MAIL_DES, 0, 80) AS CS_EML_TXT,
          DECODE (m3.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS CS_PEC_FLG,
          m4.CDL5_MAIL_NUM AS FT_EML_ID,
          SUBSTR (m4.CDL5_MAIL_DES, 0, 80) AS FT_EML_TXT,
          DECODE (m4.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS FT_PEC_FLG,
          m5.CDL5_MAIL_NUM AS FE_EML_ID,
          SUBSTR (m5.CDL5_MAIL_DES, 0, 80) AS FE_EML_TXT,
          DECODE (m5.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS FE_PEC_FLG,
          m6.CDL5_MAIL_NUM AS PV_EML_ID,
          SUBSTR (m6.CDL5_MAIL_DES, 0, 80) AS PV_EML_TXT,
          DECODE (m6.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS PV_PEC_FLG
     FROM TCD0B_RP_SF_SEDE s
          LEFT JOIN mails m1
             ON     s.cdl1_sede_cod = m1.cdl1_sede_cod
                AND s.CD0B_STAT_RECD_COD = m1.CD0D_STAT_RECD_COD
                /*AND s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000') =
                       m1.cd0d_raw_xid_des*/
                AND s.CD0B_RAW_XID_DES = m1.raw_xid_des
                AND s.CD0B_PROG_TRNS_DES = m1.prog_trns_des
                AND m1.cd35_utem_cod = 'AR'
          LEFT JOIN mails m2
             ON     s.cdl1_sede_cod = m2.cdl1_sede_cod
                AND s.CD0B_STAT_RECD_COD = m2.CD0D_STAT_RECD_COD
                /*AND s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000') =
                       m2.cd0d_raw_xid_des*/
                AND s.CD0B_RAW_XID_DES = m2.raw_xid_des
                AND s.CD0B_PROG_TRNS_DES = m2.prog_trns_des
                AND m2.cd35_utem_cod = 'BZ'
          LEFT JOIN mails m3
             ON     s.cdl1_sede_cod = m3.cdl1_sede_cod
                AND s.CD0B_STAT_RECD_COD = m3.CD0D_STAT_RECD_COD
                /*AND s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000') =
                       m3.cd0d_raw_xid_des*/
                AND s.CD0B_RAW_XID_DES = m3.raw_xid_des
                AND s.CD0B_PROG_TRNS_DES = m3.prog_trns_des
                AND m3.cd35_utem_cod = 'CS'
          LEFT JOIN mails m4
             ON     s.cdl1_sede_cod = m4.cdl1_sede_cod
                AND s.CD0B_STAT_RECD_COD = m4.CD0D_STAT_RECD_COD
                /*AND s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000') =
                       m4.cd0d_raw_xid_des*/
                AND s.CD0B_RAW_XID_DES = m4.raw_xid_des
                AND s.CD0B_PROG_TRNS_DES = m4.prog_trns_des
                AND m4.cd35_utem_cod = 'FT'
          LEFT JOIN mails m5
             ON     s.cdl1_sede_cod = m5.cdl1_sede_cod
                AND s.CD0B_STAT_RECD_COD = m5.CD0D_STAT_RECD_COD
                /*AND s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000') =
                       m5.cd0d_raw_xid_des*/
                AND s.CD0B_RAW_XID_DES = m5.raw_xid_des
                AND s.CD0B_PROG_TRNS_DES = m5.prog_trns_des
                AND m5.cd35_utem_cod = 'FE'
          LEFT JOIN mails m6
             ON     s.cdl1_sede_cod = m6.cdl1_sede_cod
                AND s.CD0B_STAT_RECD_COD = m6.CD0D_STAT_RECD_COD
                /*AND s.CD0B_RAW_XID_DES || NVL (s.CD0B_PROG_TRNS_DES, '000') =
                       m6.cd0d_raw_xid_des*/
                AND s.CD0B_RAW_XID_DES = m6.raw_xid_des
                AND s.CD0B_PROG_TRNS_DES = m6.prog_trns_des
                AND m6.cd35_utem_cod = 'PV'
  GRANT SELECT ON "DBACD1CDB"."VCD0D_RP_SF_MAIL" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for View VCD0D_RP_SF_MAIL_idx
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0D_RP_SF_MAIL_idx" ("TRANSACTIONID", "RAW_XID_DES", "PROG_TRNS_DES", "CUSTOMER", "CODICESEDE", "STATO", "AR_EML_ID", "AR_EML_TXT", "AR_PEC_FLG", "BZ_EML_ID", "BZ_EML_TXT", "BZ_PEC_FLG", "CS_EML_ID", "CS_EML_TXT", "CS_PEC_FLG", "FT_EML_ID", "FT_EML_TXT", "FT_PEC_FLG", "FE_EML_ID", "FE_EML_TXT", "FE_PEC_FLG", "PV_EML_ID", "PV_EML_TXT", "PV_PEC_FLG") AS 
  WITH mails
        AS (SELECT CDL0_CUST_COD,
                   CD0D_RAW_XID_DES || CD0d_PROG_TRNS_DES AS CD0D_RAW_XID_DES,
                   m.cd0d_raw_xid_des AS raw_xid_des,
                   m.CD0d_PROG_TRNS_DES AS PROG_TRNS_DES,
                   CDL1_SEDE_COD,
                   CD0D_RECD_NUM,
                   CDL5_MAIL_NUM,
                   CDL5_MAIL_DES,
                   CDL5_EML_PEC_FLG,
                   CD35_UTEM_COD,
                   CD0D_STAT_RECD_COD
              FROM TCD0D_RP_SF_MAIL m)
   SELECT s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES AS TransactionId,
          s.CD0B_RAW_XID_DES AS RAW_XID_DES,
          s.CD0B_PROG_TRNS_DES AS PROG_TRNS_DES,
          s.CDL0_CUST_COD AS Customer,
          s.cdl1_sede_cod AS CodiceSede,
          s.CD0B_STAT_RECD_COD AS Stato,
          m1.CDL5_MAIL_NUM AS AR_EML_ID,
          SUBSTR (m1.CDL5_MAIL_DES, 0, 80) AS AR_EML_TXT,
          DECODE (m1.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS AR_PEC_FLG,
          m2.CDL5_MAIL_NUM AS BZ_EML_ID,
          SUBSTR (m2.CDL5_MAIL_DES, 0, 80) AS BZ_EML_TXT,
          DECODE (m2.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS BZ_PEC_FLG,
          m3.CDL5_MAIL_NUM AS CS_EML_ID,
          SUBSTR (m3.CDL5_MAIL_DES, 0, 80) AS CS_EML_TXT,
          DECODE (m3.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS CS_PEC_FLG,
          m4.CDL5_MAIL_NUM AS FT_EML_ID,
          SUBSTR (m4.CDL5_MAIL_DES, 0, 80) AS FT_EML_TXT,
          DECODE (m4.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS FT_PEC_FLG,
          m5.CDL5_MAIL_NUM AS FE_EML_ID,
          SUBSTR (m5.CDL5_MAIL_DES, 0, 80) AS FE_EML_TXT,
          DECODE (m5.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS FE_PEC_FLG,
          m6.CDL5_MAIL_NUM AS PV_EML_ID,
          SUBSTR (m6.CDL5_MAIL_DES, 0, 80) AS PV_EML_TXT,
          DECODE (m6.CDL5_EML_PEC_FLG,  'S', 'true',  'N', 'false',  'false')
             AS PV_PEC_FLG
     FROM TCD0B_RP_SF_SEDE s
          LEFT JOIN mails m1
             ON     s.cdl1_sede_cod = m1.cdl1_sede_cod
                AND s.CDL0_CUST_COD = m1.CDL0_CUST_COD
                --and s.CD0B_STAT_RECD_COD = m1.CD0D_STAT_RECD_COD
                AND s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES =
                       m1.cd0d_raw_xid_des
                /*and s.CD0B_RAW_XID_DES = m1.raw_xid_des
                and s.CD0B_PROG_TRNS_DES = m1.prog_trns_des*/
                AND m1.cd35_utem_cod = 'AR'
          LEFT JOIN mails m2
             ON     s.cdl1_sede_cod = m2.cdl1_sede_cod
                AND s.CDL0_CUST_COD = m2.CDL0_CUST_COD
                --and s.CD0B_STAT_RECD_COD = m2.CD0D_STAT_RECD_COD
                AND s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES =
                       m2.cd0d_raw_xid_des
                /*and s.CD0B_RAW_XID_DES = m2.raw_xid_des
                and s.CD0B_PROG_TRNS_DES = m2.prog_trns_des*/
                AND m2.cd35_utem_cod = 'BZ'
          LEFT JOIN mails m3
             ON     s.cdl1_sede_cod = m3.cdl1_sede_cod
                AND s.CDL0_CUST_COD = m3.CDL0_CUST_COD
                --and s.CD0B_STAT_RECD_COD = m3.CD0D_STAT_RECD_COD
                AND s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES =
                       m3.cd0d_raw_xid_des
                /*and s.CD0B_RAW_XID_DES = m3.raw_xid_des
                and s.CD0B_PROG_TRNS_DES = m3.prog_trns_des*/
                AND m3.cd35_utem_cod = 'CS'
          LEFT JOIN mails m4
             ON     s.cdl1_sede_cod = m4.cdl1_sede_cod
                AND s.CDL0_CUST_COD = m4.CDL0_CUST_COD
                --and s.CD0B_STAT_RECD_COD = m4.CD0D_STAT_RECD_COD
                AND s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES =
                       m4.cd0d_raw_xid_des
                /*and s.CD0B_RAW_XID_DES = m4.raw_xid_des
                and s.CD0B_PROG_TRNS_DES = m4.prog_trns_des*/
                AND m4.cd35_utem_cod = 'FT'
          LEFT JOIN mails m5
             ON     s.cdl1_sede_cod = m5.cdl1_sede_cod
                AND s.CDL0_CUST_COD = m5.CDL0_CUST_COD
                --and s.CD0B_STAT_RECD_COD = m5.CD0D_STAT_RECD_COD
                AND s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES =
                       m5.cd0d_raw_xid_des
                /*and s.CD0B_RAW_XID_DES = m5.raw_xid_des
                and s.CD0B_PROG_TRNS_DES = m5.prog_trns_des*/
                AND m5.cd35_utem_cod = 'FE'
          LEFT JOIN mails m6
             ON     s.cdl1_sede_cod = m6.cdl1_sede_cod
                AND s.CDL0_CUST_COD = m6.CDL0_CUST_COD
                --and s.CD0B_STAT_RECD_COD = m6.CD0D_STAT_RECD_COD
                AND s.CD0B_RAW_XID_DES || s.CD0B_PROG_TRNS_DES =
                       m6.cd0d_raw_xid_des
                /*and s.CD0B_RAW_XID_DES = m6.raw_xid_des
                and s.CD0B_PROG_TRNS_DES = m6.prog_trns_des*/
                AND m6.cd35_utem_cod = 'PV'
--------------------------------------------------------
--  DDL for View VCD0E_RP_SF_CESSAZIONI
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0E_RP_SF_CESSAZIONI" ("TRANSACTIONID", "RAW_XID_DES", "PROG_TRNS_DES", "TIPO", "CUSTOMERCOD", "SEDECOD", "ID", "VALUE", "STATO", "DATA") AS 
  SELECT DISTINCT cd0h_raw_xid_des || CD0H_PROG_TRNS_DES AS TransactionId,
                   cd0h_raw_xid_des AS raw_xid_des,
                   CD0H_PROG_TRNS_DES AS PROG_TRNS_DES,
                   'telefono' AS tipo,
                   CDL0_CUST_COD AS CustomerCod,
                   CDL1_SEDE_COD || cdl1_nver_sede_cod AS SedeCod,
                   CDL4_TELE_NUM AS Id,
                   CDL4_TELF_PREF_COD || CDL4_TELF_NUMR_COD AS VALUE,
                   cd0h_stat_recd_cod AS stato,
                   cd0h_strt_trns_tim AS data
     FROM tcd0h_rp_sf_tel_cessati
   UNION
   SELECT DISTINCT CD0I_RAW_XID_DES || CD0I_PROG_TRNS_DES,
                   CD0I_RAW_XID_DES AS RAW_XID_DES,
                   CD0I_PROG_TRNS_DES AS PROG_TRNS_DES,
                   'email',
                   CDL0_CUST_COD,
                   CDL1_SEDE_COD || cdl1_nver_sede_cod,
                   CDL5_MAIL_NUM,
                   CDL5_MAIL_DES,
                   cd0i_stat_recd_cod,
                   cd0i_strt_trns_tim AS data
     FROM tcd0i_rp_sf_mail_cessate
  GRANT SELECT ON "DBACD1CDB"."VCD0E_RP_SF_CESSAZIONI" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for View VCD0M_CHECK_TRX_KO
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "DBACD1CDB"."VCD0M_CHECK_TRX_KO" ("CD0B_RAW_XID_DES", "CD0B_PROG_TRNS_DES", "CD0B_STAT_RECD_COD", "CD0B_INSE_DTA", "CDL0_CUST_COD", "CD0B_TIPO_EVEN_COD", "CUSTHATRXKO", "SEDEPRECHATRXKO", "EVENTOGESTITO", "ESISTETRXCUSTCONTIMMINORE", "ESISTETRXPRECSEDECONTIMMINORE", "TRXVALIDA") AS 
  WITH trx AS
( SELECT DISTINCT s.CD0B_RAW_XID_DES, s.CD0B_PROG_TRNS_DES, s.CD0B_STAT_RECD_COD, s.CD0B_INSE_DTA, s.CDL0_CUST_COD, s.CDL1_PREC_SEDE_COD, cd0b_tipo_even_cod, CD0B_STRT_TRNS_TIM FROM TCD0B_RP_SF_SEDE s WHERE s.CD0B_STAT_RECD_COD IN ('A', 'D') )
SELECT distinct
	CD0B_RAW_XID_DES, CD0B_PROG_TRNS_DES,CD0B_STAT_RECD_COD,CD0B_INSE_DTA, CDL0_CUST_COD, cd0b_tipo_even_cod,
	CASE WHEN EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE trx.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D')) THEN 'KO' ELSE 'OK' END AS CustHaTrxKO, 
	CASE WHEN EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE trx.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND trx.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D') ) THEN 'KO' ELSE 'OK' END AS SedePrecHaTrxKO,
	CASE when (trx.cd0b_tipo_even_cod NOT in ( 'CRCU', 'VGCU', 'VGSP', 'CRSS', 'VSCU', 'VASP', 'VISP', 'VISS', 'VGSS', 'VSSS', 'VDF', 'VRG', 'VGEN', 'SCO', 'SUB' )) THEN 'KO' ELSE 'OK' END AS EventoGestito,
	CASE when EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE trx.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < trx.CD0B_STRT_TRNS_TIM ) THEN 'KO' ELSE 'OK' END AS EsisteTrxCustConTimMinore,
	CASE when EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE trx.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND trx.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < trx.CD0B_STRT_TRNS_TIM ) THEN 'KO' ELSE 'OK' END AS EsisteTrxPrecSedeConTimMinore,
	CASE WHEN is_valid_transaction(trx.CD0B_RAW_XID_DES,trx.CD0B_PROG_TRNS_DES) = '0' THEN 'KO' ELSE 'OK' END AS TrxValida 
FROM trx
