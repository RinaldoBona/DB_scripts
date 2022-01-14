--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package MS_REP_SYS_CA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBACD8ESB"."MS_REP_SYS_CA" AS 
    
    procedure pollTransactions_CA
    (
        p_transactions  OUT SYS_REFCURSOR
    );
    
    procedure pollTransactions_CA_v2
    (
        p_transactions     OUT SYS_REFCURSOR,
        p_telefoni         OUT SYS_REFCURSOR
    );
    
    procedure readTelefoni_CA
    (
        p_raw_xid_des   in  varchar2,
        p_sede_cod      in  tcd0u_rp_gen_telefono.cdl1_sede_cod%type,
        p_result        out SYS_REFCURSOR
    );

END MS_REP_SYS_CA;
--------------------------------------------------------
--  DDL for Package MS_REP_UTILS
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBACD8ESB"."MS_REP_UTILS" AS 

  PROCEDURE logga
    (
      p_message    IN       VARCHAR2,
      p_user       IN       VARCHAR2
    );
    
  procedure aggiornaStatoMulti_v2
  (
    p_transactions      IN  aggStatoTrx_table,
    p_results           OUT SYS_REFCURSOR
  );
    
  procedure aggiornaStatoMulti
  (
    p_transactions  IN aggStatoTrx_table,
    p_result        OUT VARCHAR2,
    p_error_cod     OUT VARCHAR2,
    p_error_des     OUT VARCHAR2
  );
    
  PROCEDURE aggiornaStato
  (
    p_sys           IN  VARCHAR2, -- il sistema a cui fa riferimento l'aggiornamento richiesto
    p_trx           IN  VARCHAR2,
    p_status        IN  CHAR,
    p_user          IN  VARCHAR2, 
    p_reply_trx_id  IN  VARCHAR2,
    p_mule_trx_id   IN  VARCHAR2,
    p_msg           IN  VARCHAR2,
    p_force_flg     IN  NUMBER,
    p_result        OUT VARCHAR2,
    p_error_cod     OUT VARCHAR2,
    p_error_des     OUT VARCHAR2
  );
  
  procedure aggiornaConfig
  (
    p_key varchar2,
    p_val varchar2
  );
  
  procedure replicaTrxCDB_CA
  (
    p_result        OUT number
  );
  
  procedure aggiornaStatoSuTabelleCDB
  (
    p_raw_xid_des   in  varchar2,
    p_prog_trns_des in  varchar2,
    p_sedeCod       in  varchar2,
    p_status        in  varchar2,
    p_err_des       in  varchar2,
    p_esito out number
  );
  
  procedure cleanLogs;

END MS_REP_UTILS;
--------------------------------------------------------
--  DDL for Package MS_TK_CA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBACD8ESB"."MS_TK_CA" AS 

  procedure aggiornaStatoTk
  (
    p_tck_num       IN  TCD0S_RP_GEN_TICKET.CDW6_TCK_NUM%TYPE,
    p_status        IN  TCD0S_RP_GEN_TICKET.CD0S_STAT_RECD_COD%TYPE,
    p_msg           IN  TCD0S_RP_GEN_TICKET.CD0S_MLSF_MSG_DES%TYPE,
    p_mule_trx_id   IN  TCD0S_RP_GEN_TICKET.CD0S_MULE_TRX_ID%TYPE,
    p_result        OUT VARCHAR2,
    p_error_cod     OUT VARCHAR2,
    p_error_des     OUT VARCHAR2
  );
  
  procedure pollTk
  (
    p_tickets       OUT SYS_REFCURSOR
  );

END MS_TK_CA;
--------------------------------------------------------
--  DDL for Package MS_TKCDB_SF
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBACD8ESB"."MS_TKCDB_SF" AS 

 procedure writeLog
    (
      p_message    IN       VARCHAR2,
      p_user       IN       VARCHAR2
    );

 procedure updateConfig
  (
    p_key IN varchar2,
    p_val IN varchar2
  );

 procedure updateTkStatus
    (
        p_transactionId IN  tcd0k_rp_ticket_cdb.cd0k_recd_num%TYPE,
        p_status        IN  tcd0k_rp_ticket_cdb.cd0k_stat_recd_cod%TYPE,
        p_msg           IN  tcd0k_rp_ticket_cdb.cd0k_mlsf_msg_des%TYPE,
        p_mule_trx_id   IN  tcd0k_rp_ticket_cdb.cd0k_mule_trx_id%TYPE,
        p_caseNumber    IN  tcd0k_rp_ticket_cdb.cd0k_case_sf_num%TYPE,
        p_force_flg     IN  NUMBER,
        p_result        OUT VARCHAR2,
        p_error_cod     OUT VARCHAR2,
        p_error_des     OUT VARCHAR2
    );

      procedure poll_TkCDB
    (
        p_tickets OUT SYS_REFCURSOR
    );

 procedure updateTkStatusMulti
    (
        p_transactions      IN  aggstatotkcdb_table,
        p_results           OUT SYS_REFCURSOR
    );
 
 procedure cleanlogs;

END MS_TKCDB_SF;
--------------------------------------------------------
--  DDL for Package SF_DATACHECK
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBACD8ESB"."SF_DATACHECK" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */
  
  procedure ReadAndReserveRUN
    (
        runData     OUT SYS_REFCURSOR
    );
    
    PROCEDURE loggamelo 
    (
      p_message    IN       VARCHAR2
    );
    
    procedure UpdateRUNStatus
    (
        runGuid     tcd00_sfdc_input.cd00_run_guid%TYPE,
        runStatus   tcd00_sfdc_input.cd00_run_stat%TYPE
    );

END SF_DATACHECK;
