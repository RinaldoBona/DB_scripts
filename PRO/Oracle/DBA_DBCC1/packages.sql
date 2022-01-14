--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package SF_DATACHECK
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBA_DBCC1"."SF_DATACHECK" AS 

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
        runGuid     tds00_sfdc_input.ds00_run_guid%TYPE,
        runStatus   tds00_sfdc_input.ds00_run_stat%TYPE
    );


    function Confronta_Lavorabili
    (
        OPEC_SF    IN check_sf_dwh_lavorabile.opec%TYPE,
		    LAVR_DWH   IN dwh_rinnovi_contratti.lavr_opec_iolpresence_nelq%TYPE,
		    NUM_OPP_SF IN check_sf_dwh_lavorabile.num_opp_sf_qc%TYPE,
		    LAVR_SF    IN check_sf_dwh_lavorabile.lavr_opec_sf_qc%TYPE
    )
   return NUMBER;

    function Calcola_Quarter
    (
       DATA_CALC IN DATE,
	     SCOSTAMENTO IN INTEGER
    )
   return VARCHAR2;
   
    procedure EseguiCheckSfDwhSigleVend
    (
        esito     in OUT INTEGER
    );    

    procedure EseguiCheckSfDwhAssegnazioni
    (
        runGuid     tds00_sfdc_input.ds00_run_guid%TYPE,
        esito     in OUT INTEGER
    );

    procedure EseguiCheckSfDwhAssegn_V2_old
    (
        runGuid     tds00_sfdc_input.ds00_run_guid%TYPE,
        esito     in OUT INTEGER
    );

    procedure EseguiCheckSfDwhAssegn_V2
    (
        runGuid     tds00_sfdc_input.ds00_run_guid%TYPE,
        esito     in OUT INTEGER
    );

    procedure EseguiCheckDwhLavorabile
    (
        runGuid   tds00_sfdc_input.ds00_run_guid%TYPE,
        esito     in OUT INTEGER
    );
                
    procedure AggiornaPtfDwh
    (
        esito     in OUT INTEGER
    );    
    
    procedure AggPtfDwh_2
    (
        esito out number
    );

    procedure AggiornaRinnoviDWH
    (
        esito     in OUT INTEGER
    );   
    
    procedure ciao
    (
        nome in VARCHAR2,
        message OUT VARCHAR2
    );
    
        procedure testCursor
    (
        pippo in varchar2,
        cursore out SYS_REFCURSOR
    );
    
END SF_DATACHECK;
