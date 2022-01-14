--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package SF_REPLICA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "DBACD1CDB"."SF_REPLICA" 
IS
   -- Author  : RBona

    PROCEDURE AggiornaStatoTransazione 
    (
      p_TransactionId   IN       VARCHAR2,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       );
    
    PROCEDURE AggiornaStatoTransazioni 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       );
    
    PROCEDURE loggamelo 
    (
      p_message    IN       VARCHAR2
    );
    
    procedure   RARTransactions_v2_old
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    );
    
     procedure ReadAndReserveTransactions
    (
        p_howMany IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    procedure ReadAndReserveTransactions_old
    (
        p_howMany IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    procedure ReadAndReserveTransactions_new
    (
        p_howMany IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    procedure ReadAndReserveTickets
    (
        p_howMany   IN number,
        tickets     OUT SYS_REFCURSOR
    );
    
    PROCEDURE AggiornaStatoTickets 
    (
      p_Progressivi     IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
    );
    
     procedure ReadAndReserveContacts
    (
        p_howMany   IN number,
        contacts    OUT SYS_REFCURSOR
    );
    
    PROCEDURE AggiornaStatoContacts 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       );
    
     procedure   ReadAndReserveTransactions_v2
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    procedure   ReadAndReserveTransactions_v4
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    procedure   ReadAndReserveTransactions_v5
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    procedure   ReadAndReserveTransactions_v6
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    PROCEDURE AggiornaStatoTransazioni_new 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       );
       
       PROCEDURE UpdateSemaforo 
    (
      p_value    IN       VARCHAR2
    );
    
    PROCEDURE UpdateTransactionFromPE
    (
        p_TransactionId   IN       VARCHAR2,
        p_Status          IN       VARCHAR2,
        p_user            IN       VARCHAR2,
        p_error           IN       VARCHAR2,
        p_reply_num       IN       VARCHAR2,
        p_result          OUT      NUMBER   
    );
    
    procedure   ReadAndReserveTransactions_v3
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    );
    
    PROCEDURE AggiornaStatiDaPE 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_Err_des         IN       VARCHAR2,
      p_User            IN       VARCHAR2,
      p_reply_id        IN       NUMBER,       
      p_result          OUT      NUMBER
       );
    
    procedure   RefreshMaterializedViews
    (
        p_result out number
    );
    
    procedure RefreshValidTransactions
    (
        p_result out number
    );
    


END SF_REPLICA;
  GRANT EXECUTE ON "DBACD1CDB"."SF_REPLICA" TO "DBACD8ESB"
