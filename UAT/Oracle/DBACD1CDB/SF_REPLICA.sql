CREATE OR REPLACE PACKAGE           SF_REPLICA
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
/


CREATE OR REPLACE PACKAGE body           SF_REPLICA
IS
   -- Author  : RBona

    PROCEDURE AggiornaStatoTransazione
    (
      p_TransactionId   IN       VARCHAR2,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       )
    IS
    
        e_data_exception EXCEPTION;
        progr char(3);
        new_tr_id char(16);
    
    BEGIN
    
        p_result := 0;

        if p_TransactionId is null or  p_Status is null then
            raise e_data_exception;
        end if;
        
        if ( length(p_TransactionId) = 16 )
        then 
            begin
                update 
                    TCD0A_RP_SF_CUSTOMER
                set 
                    cd0a_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0a_raw_xid_des) = p_TransactionId;
                    
                update 
                    tcd0b_rp_sf_sede
                set 
                    cd0b_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0b_raw_xid_des) = p_TransactionId;
                    
                update 
                    tcd0c_rp_sf_telefono
                set 
                    cd0c_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0c_raw_xid_des) = p_TransactionId;
                
                update 
                    tcd0d_rp_sf_mail
                set 
                    cd0d_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0d_raw_xid_des) = p_TransactionId;
            end;
        end if;
        if ( length(p_TransactionId) = 19 ) then
        begin
            select substr(p_TransactionId, 17, 3) into progr from dual;
            select substr(p_TransactionId, 0, 16) into new_tr_id from dual;
            
            update 
                    TCD0A_RP_SF_CUSTOMER
                set 
                    cd0a_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0a_raw_xid_des) = new_tr_id
                    and cd0a_prog_trns_des = progr;
                    
                update 
                    tcd0b_rp_sf_sede
                set 
                    cd0b_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0b_raw_xid_des) = new_tr_id
                    and cd0b_prog_trns_des = progr;
                    
                update 
                    tcd0c_rp_sf_telefono
                set 
                    cd0c_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0c_raw_xid_des) = new_tr_id
                    and cd0c_prog_trns_des = progr;
                
                update 
                    tcd0d_rp_sf_mail
                set 
                    cd0d_stat_recd_cod = p_Status
                where 
                    to_char(''||cd0d_raw_xid_des) = new_tr_id
                    and cd0d_prog_trns_des = progr;
        end;
        end if;
        --commit;
        
        EXCEPTION
        
        when e_data_exception then p_result := 1;
            --rollback;
        
        WHEN OTHERS THEN p_result := 2;
            --ROLLBACK;

    END AggiornaStatoTransazione;

PROCEDURE AggiornaStatoTransazioni 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       )
    is
    
    trans_id varchar2(20);
    conta number;
   -- p_array ARRAY_TYPE_VARCHAR100;
    
    begin
    
        p_result := 0;
        conta :=0;
        
        loggamelo('sono dentro!'); 
        
        loggamelo('ho ' || p_Transactions.COUNT || ' record da elaborare!');
        
        --p_array := p_Transactions.FIRST;
        
        --loggamelo('(2) ho ' || p_array.COUNT || ' record da elaborare!');
        
        --loggamelo('p_Transactions ' || p_Transactions);
        
        for i in p_Transactions.FIRST .. p_Transactions.LAST
        loop
        loggamelo(''||conta || ': '|| p_Transactions(i));
        trans_id := p_Transactions(i);
        
        loggamelo('tr -> '|| trans_id);
        
        --INSERT INTO DBACD1CDB.SF_AAA (PIPPO) VALUES (trans_id);
        
        DBMS_OUTPUT.put_line(trans_id);
        if ( p_Status = 'Q' ) then
            update 
                TCD0A_RP_SF_CUSTOMER
            set 
                cd0a_stat_recd_cod = p_Status, cd0a_send_dta=sysdate
            where 
                to_char(cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000')) = trans_id;
                --or to_char('0x'||cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000') = trans_id;
                
            update 
                tcd0b_rp_sf_sede
            set 
                cd0b_stat_recd_cod = p_Status, cd0b_send_dta=sysdate
            where 
                to_char(cd0b_raw_xid_des || nvl(cd0b_prog_trns_des,'000')) = trans_id;
                
            update 
                tcd0c_rp_sf_telefono
            set 
                cd0c_stat_recd_cod = p_Status, cd0c_send_dta=sysdate
            where 
                to_char(cd0c_raw_xid_des || nvl(cd0c_prog_trns_des,'000')) = trans_id;
            
            update 
                tcd0d_rp_sf_mail
            set 
                cd0d_stat_recd_cod = p_Status, cd0d_send_dta=sysdate
            where 
                to_char(cd0d_raw_xid_des || nvl(cd0d_prog_trns_des,'000')) = trans_id;
        else
            update 
                TCD0A_RP_SF_CUSTOMER
            set 
                cd0a_stat_recd_cod = p_Status
            where 
                to_char(cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000')) = trans_id;
                --or to_char('0x'||cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000') = trans_id;
                
            update 
                tcd0b_rp_sf_sede
            set 
                cd0b_stat_recd_cod = p_Status
            where 
                to_char(cd0b_raw_xid_des || nvl(cd0b_prog_trns_des,'000')) = trans_id;
                
            update 
                tcd0c_rp_sf_telefono
            set 
                cd0c_stat_recd_cod = p_Status
            where 
                to_char(cd0c_raw_xid_des || nvl(cd0c_prog_trns_des,'000')) = trans_id;
            
            update 
                tcd0d_rp_sf_mail
            set 
                cd0d_stat_recd_cod = p_Status
            where 
                to_char(cd0d_raw_xid_des || nvl(cd0d_prog_trns_des,'000')) = trans_id;
        end if;
        conta := conta+1;
        
        end loop;
        
        loggamelo('finito!');
        
        EXCEPTION
        
        WHEN OTHERS THEN
        loggamelo('errore '|| SQLCODE || ' ' || SQLERRM);
        p_result := 2;
        
        
        loggamelo(''||p_result);
        
    end AggiornaStatoTransazioni;
    
    PROCEDURE AggiornaStatoTransazioni_new 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       )
    is
    
    trans_id varchar2(20);
    conta number;
   -- p_array ARRAY_TYPE_VARCHAR100;
    
    begin
    
        p_result := 0;
        conta :=0; 
        
        loggamelo('[AggiornaStatoTransazioni_new] Ho ' || p_Transactions.COUNT || ' record da elaborare!');
        
        --p_array := p_Transactions.FIRST;
        
        --loggamelo('(2) ho ' || p_array.COUNT || ' record da elaborare!');
        
        --loggamelo('p_Transactions ' || p_Transactions);
        
        if ( p_Status = 'Q' ) then
            update 
                TCD0A_RP_SF_CUSTOMER
            set 
                cd0a_stat_recd_cod = p_Status, cd0a_send_dta=sysdate
            where 
                cd0a_stat_recd_cod = 'P' and
                cd0a_raw_xid_des||CD0A_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                --to_char(cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000')) = trans_id;
                --or to_char('0x'||cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000') = trans_id;
                
            update 
                tcd0b_rp_sf_sede
            set 
                cd0b_stat_recd_cod = p_Status, cd0b_send_dta=sysdate
            where 
                cd0b_stat_recd_cod = 'P' and
                --to_char(cd0b_raw_xid_des || nvl(cd0b_prog_trns_des,'000')) = trans_id;
                cd0b_raw_xid_des||CD0b_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                
            update 
                tcd0c_rp_sf_telefono
            set 
                cd0c_stat_recd_cod = p_Status, cd0c_send_dta=sysdate
            where 
                cd0c_stat_recd_cod = 'P' and
                --to_char(cd0c_raw_xid_des || nvl(cd0c_prog_trns_des,'000')) = trans_id;
                cd0c_raw_xid_des||CD0c_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
            
            update 
                tcd0d_rp_sf_mail
            set 
                cd0d_stat_recd_cod = p_Status, cd0d_send_dta=sysdate
            where 
                cd0d_stat_recd_cod = 'P' and
                cd0d_raw_xid_des||CD0d_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                --to_char(cd0d_raw_xid_des || nvl(cd0d_prog_trns_des,'000')) = trans_id;
        else
            update 
                TCD0A_RP_SF_CUSTOMER
            set 
                cd0a_stat_recd_cod = p_Status
            where 
                cd0a_raw_xid_des||CD0A_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                --to_char(cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000')) = trans_id;
                --or to_char('0x'||cd0a_raw_xid_des || nvl(cd0a_prog_trns_des,'000') = trans_id;
                
            update 
                tcd0b_rp_sf_sede
            set 
                cd0b_stat_recd_cod = p_Status
            where 
                cd0b_raw_xid_des||CD0b_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                --to_char(cd0b_raw_xid_des || nvl(cd0b_prog_trns_des,'000')) = trans_id;
                
            update 
                tcd0c_rp_sf_telefono
            set 
                cd0c_stat_recd_cod = p_Status
            where 
                cd0c_raw_xid_des||CD0c_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                --to_char(cd0c_raw_xid_des || nvl(cd0c_prog_trns_des,'000')) = trans_id;
            
            update 
                tcd0d_rp_sf_mail
            set 
                cd0d_stat_recd_cod = p_Status
            where 
                cd0d_raw_xid_des||CD0d_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) );
                --to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) );
                --to_char(cd0d_raw_xid_des || nvl(cd0d_prog_trns_des,'000')) = trans_id;
        end if;
        
        loggamelo('[AggiornaStatoTransazioni_new] Finito!');
        
        EXCEPTION
        
        WHEN OTHERS THEN
        loggamelo('errore '|| SQLCODE || ' ' || SQLERRM);
        p_result := 2;
        
        
        loggamelo(''||p_result);
        
    end AggiornaStatoTransazioni_new;
    
    PROCEDURE AggiornaStatiDaPE 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_Err_des         IN       VARCHAR2,
      p_User            IN       VARCHAR2,
      p_reply_id        IN       NUMBER,       
      p_result          OUT      NUMBER
       )
    is
    
    trans_id varchar2(20);
    conta number;
    errore tcd0b_rp_sf_sede.cd0b_err_des%type;
   -- p_array ARRAY_TYPE_VARCHAR100;
    
    begin
    
        p_result := 0;
        conta :=0; 
        
        loggamelo('[AggiornaStatiDaPE] Ho ' || p_Transactions.COUNT || ' record da elaborare!');
        
        select case when p_Err_des is not null and p_Err_des = 'nuLL' then null else p_Err_des end into errore from dual;
        
        for i in p_Transactions.FIRST .. p_Transactions.LAST
        loop
        --loggamelo(''||conta || ': '|| p_Transactions(i));
        loggamelo('[AggiornaStatiDaPE] Aggiorno transazione ' || p_Transactions(i) || '(reply_num='|| p_reply_id ||') a stato ' || p_Status || ', errore >' || errore || '< ');
        trans_id := p_Transactions(i);
        end loop;
        
        --p_array := p_Transactions.FIRST;
        
        --loggamelo('(2) ho ' || p_array.COUNT || ' record da elaborare!');
        
        --loggamelo('p_Transactions ' || p_Transactions);
        
            update 
                TCD0A_RP_SF_CUSTOMER
            set 
                cd0a_stat_recd_cod = p_Status, CD0A_MLSF_UPD_DTA=sysdate, CD0A_ERR_DES=errore, CD0A_MLSF_USER_DES=p_User, CD0A_REPLY_NUM=p_reply_id
            where 
                cd0a_raw_xid_des||CD0A_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) ) and cd0a_stat_recd_cod not in ('A', 'I');
                --to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) ) and cd0a_stat_recd_cod not in ('A', 'I');
                --and cd0a_reply_num is null;

            update 
                tcd0b_rp_sf_sede
            set 
                cd0b_stat_recd_cod = p_Status, CD0b_MLSF_UPD_DTA=sysdate, CD0b_ERR_DES=errore, CD0b_MLSF_USER_DES=p_User, CD0b_REPLY_NUM=p_reply_id
            where 
                cd0b_raw_xid_des||CD0b_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) ) and cd0b_stat_recd_cod not in ('A', 'I');
                --to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) ) and cd0b_stat_recd_cod not in ('A', 'I');
                --and cd0b_reply_num is null;
                
            update 
                tcd0c_rp_sf_telefono
            set 
                cd0c_stat_recd_cod = p_Status, CD0c_MLSF_UPD_DTA=sysdate, CD0c_ERR_DES=errore, CD0c_MLSF_USER_DES=p_User, CD0c_REPLY_NUM=p_reply_id
            where 
                cd0c_raw_xid_des||CD0c_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) ) and cd0c_stat_recd_cod not in ('A', 'I');
                --to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) ) and cd0c_stat_recd_cod not in ('A', 'I');
                --and cd0c_reply_num is null;
            
            update 
                tcd0d_rp_sf_mail
            set 
                cd0d_stat_recd_cod = p_Status, CD0d_MLSF_UPD_DTA=sysdate, CD0d_ERR_DES=errore, CD0d_MLSF_USER_DES=p_User, CD0d_REPLY_NUM=p_reply_id
            where 
                cd0d_raw_xid_des||CD0d_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) ) and cd0d_stat_recd_cod not in ('A', 'I');
                --to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) ) and cd0d_stat_recd_cod not in ('A', 'I');
                --and cd0d_reply_num is null;
                
            update 
                tcd0h_rp_sf_tel_cessati
            set 
                cd0h_stat_recd_cod = p_Status, CD0h_MLSF_UPD_DTA=sysdate, CD0h_ERR_DES=errore, CD0h_MLSF_USER_DES=p_User, CD0h_REPLY_NUM=p_reply_id
            where 
                cd0h_raw_xid_des||CD0h_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) ) and cd0h_stat_recd_cod not in ('A', 'I');
                --to_char(''||cd0h_raw_xid_des || nvl(CD0h_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) ) and cd0h_stat_recd_cod not in ('A', 'I');
                --and cd0h_reply_num is null;
                
            update 
                tcd0i_rp_sf_mail_cessate
            set 
                cd0i_stat_recd_cod = p_Status, CD0i_MLSF_UPD_DTA=sysdate, CD0i_ERR_DES=errore, CD0i_MLSF_USER_DES=p_User, CD0i_REPLY_NUM=p_reply_id
            where 
                cd0i_raw_xid_des||CD0i_PROG_TRNS_DES in ( select column_value from table( p_Transactions ) ) and cd0i_stat_recd_cod not in ('A', 'I');
                --to_char(''||cd0i_raw_xid_des || nvl(CD0i_PROG_TRNS_DES,'000')) in ( select column_value from table( p_Transactions ) ) and cd0i_stat_recd_cod not in ('A', 'I');
                --and cd0i_reply_num is null;
        
        loggamelo('[AggiornaStatiDaPE] Finito!');
        
        EXCEPTION
        
        WHEN OTHERS THEN
        loggamelo('errore '|| SQLCODE || ' ' || SQLERRM);
        p_result := 2;
        
        loggamelo(''||p_result);
        
    end AggiornaStatiDaPE;

PROCEDURE AggiornaStatoTickets 
    (
      p_Progressivi     IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       )
    is
    
   -- p_array ARRAY_TYPE_VARCHAR100;
    
    begin
    
        p_result := 0;
        
        if ( p_progressivi is not null and p_progressivi.COUNT > 0 ) then
            if ( p_status = 'Q' ) then
                UPDATE tcd0g_rp_sf_ticket
                SET
                    cd0g_stat_recd_cod = p_status, cd0g_send_dta=sysdate
                WHERE
                    TO_CHAR('' || cd0g_strt_scn_num)IN(
                        SELECT
                            column_value
                        FROM
                            TABLE(p_progressivi)
                    );
            else
                UPDATE tcd0g_rp_sf_ticket
                SET
                    cd0g_stat_recd_cod = p_status
                WHERE
                    TO_CHAR('' || cd0g_strt_scn_num)IN(
                        SELECT
                            column_value
                        FROM
                            TABLE(p_progressivi)
                    );
            end if;
        end if;
        EXCEPTION
        
        WHEN OTHERS THEN
        loggamelo('errore '|| SQLCODE || ' ' || SQLERRM);
        p_result := 2;
        
        loggamelo(''||p_result);
        
    end AggiornaStatoTickets;
    
    PROCEDURE loggamelo 
    (
      p_message    IN       VARCHAR2
    )
    is
        PRAGMA AUTONOMOUS_TRANSACTION;
    begin
    
    INSERT INTO sf_poller_log(msg_date,msg_text)
    VALUES (sysdate, p_message);
    
    commit;
    
    end loggamelo;
    
    PROCEDURE UpdateSemaforo 
    (
      p_value    IN       VARCHAR2
    )
    is
        PRAGMA AUTONOMOUS_TRANSACTION;
    begin
    
    update tcd96_lc_parametri set cd96_parm_valr_des = p_value, cd96_modi_tim=sysdate where cd96_parm_cod = 'SFP';
    
    commit;
    
    end UpdateSemaforo;
    
    procedure   ReadAndReserveTransactions
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        arr         ARRAY_TYPE_VARCHAR100;
        arr_padri   ARRAY_TYPE_VARCHAR100;
        arr_figli   ARRAY_TYPE_VARCHAR100;
        cnt         number;
        cnt_padri   number;
    begin
    
    loggamelo('Start prenotazione di ' || p_howmany || ' packets');
        
    arr := ARRAY_TYPE_VARCHAR100();
    cnt := 1;
    arr_padri := ARRAY_TYPE_VARCHAR100();
    cnt_padri := 1;
    arr_figli := ARRAY_TYPE_VARCHAR100();
    
    --p_howMany := 0;
    
    /*
        1. seleziono i pacchetti ed i relativi padri
        2. i padri me li tengo da parte per riselezionarli alla fine (voglio uscire con un risultato di select)
        3. le transazioni (i packet spacchettati) li uso per aggiornare le tabelle di replica
        
        [20190410] cambio approccio. Calcolo separatamente gli insiemi di padri che hanno solo se stessi come figli e quelli che hanno effettivamente delle transazioni figlio.
    */
    
    for r in (
                /*with famiglie as
                (
                    select distinct 
                    find_child_transactions(padre) as parenti, 
                    padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel group by padre, datapadre --A!
                )
                select ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                select parenti AS packet, padre,
                data,
                dense_rank() over (order by a.data) as rank from famiglie a
                where a.data = (select min(b.data) from vcd0a_rp_sf_transactions_rel b where a.padre = b.padre )
                ) a
                where a.rank <= 1000
                order by a.rank*/
                WITH semplici AS
                ( SELECT padre, count(DISTINCT figlio) FROM VCD0A_RP_SF_TRANSACTIONS_REL GROUP BY padre HAVING count(DISTINCT figlio) = 1 ),
                complessi AS
                ( SELECT padre, count(DISTINCT figlio) FROM VCD0A_RP_SF_TRANSACTIONS_REL GROUP BY padre HAVING count(DISTINCT figlio) > 1 )
                select ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                SELECT padre, packet, data, dense_rank() over (order by data, padre) AS rank
                FROM (
                SELECT s.padre, s.padre AS packet, v.data
                FROM semplici s JOIN VCD0A_RP_SF_TRANSACTIONS_REL v ON v.PADRE = s.padre
                UNION
                SELECT padre, FIGLI AS packet, DATA FROM (
                SELECT DISTINCT v.padre, find_child_transactions(v.padre) AS figli, v.data, dense_rank() over (order by v.data, v.PADRE) as rank 
                FROM VCD0A_RP_SF_TRANSACTIONS_REL v JOIN complessi c ON c.padre = v.PADRE)
                WHERE rank = 1)
                )
                WHERE rank <= 1000
                ORDER BY rank
            )   
    loop
        --loggamelo('Padre: '||r.padre||', packet: '||r.packet);
        
        arr_padri.extend;
        arr_padri(cnt_padri) := r.padre;
        
        arr_figli.extend;
        arr_figli(cnt_padri) := r.packet;
        
        cnt_padri := cnt_padri+1;
        
        for t in 
        (
            select regexp_substr(r.packet,'[^,]+', 1, level) as trns from dual 
            connect by regexp_substr(r.packet, '[^,]+', 1, level) is not null
        )
        loop
            arr.extend;
            arr(cnt) := t.trns;
            cnt := cnt+1;
        end loop;
        
    end loop;
      
      loggamelo('Inizio update transazioni (' || p_howmany || ' packets)');
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
    
    loggamelo('Fine update transazioni (' || p_howmany || ' packets)');
    
        open packets for
        with padri as
        (
            select column_value as padre, rownum as rank from table( arr_padri )
        ),
        figli as
        (
            select column_value as figli, rownum as rank from table( arr_figli )
        )
        select distinct --guid, padre, packet, data, rank 
               ''||sys_guid() AS guid, 
               p.padre as padre, 
               f.figli as packet, 
               sysdate as data, 
               p.rank as rank
        from padri p join figli f on p.rank = f.rank
        order by p.rank;

        commit;
        
        loggamelo('End prenotazione di ' || p_howmany || ' packets');
    
    end ReadAndReserveTransactions;
    
    procedure   ReadAndReserveTransactions_old
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        arr         ARRAY_TYPE_VARCHAR100;
        arr_padri   ARRAY_TYPE_VARCHAR100;
        cnt         number;
        cnt_padri   number;
    begin
        
    arr := ARRAY_TYPE_VARCHAR100();
    cnt := 1;
    arr_padri := ARRAY_TYPE_VARCHAR100();
    cnt_padri := 1;
    
    loggamelo('Start OLD');
    
    --p_howMany := 0;
    
    /*
        1. seleziono i pacchetti ed i relativi padri
        2. i padri me li tengo da parte per riselezionarli alla fine (voglio uscire con un risultato di select)
        3. le transazioni (i packet spacchettati) li uso per aggiornare le tabelle di replica
    */
    
    for r in (
                /*with famiglie as
                (
                    select distinct 
                    find_child_transactions(padre) as parenti, padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel --A!
                ),
                famiglie2 as
                (
                    select distinct 
                    find_child_transactions(padre) as parenti, padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel --A!
                )
                select ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                select parenti AS packet, padre,
                data,
                dense_rank() over (order by data) as rank from famiglie a
                where a.data = (select min(b.data) from famiglie2 b where a.parenti = b.parenti )
                ) a
                where a.rank <= p_howMany
                order by a.rank*/
                with famiglie as
                (
                    select distinct 
                    find_child_transactions(padre) as parenti, 
                   --wm_concat(figlio) as parenti,
                    padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel group by padre, datapadre --A!
                )
                select ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                select parenti AS packet, padre,
                data,
                dense_rank() over (order by a.data) as rank from famiglie a
                where a.data = (select min(b.data) from vcd0a_rp_sf_transactions_rel b where a.padre = b.padre )
                ) a
                where a.rank <= p_howMany
                order by a.rank
            )
    loop
        loggamelo('Padre: '||r.padre||', packet: '||r.packet);
        
        arr_padri.extend;
        arr_padri(cnt_padri) := r.padre;
        cnt_padri := cnt_padri+1;
        
        for t in 
        (
            select regexp_substr(r.packet,'[^,]+', 1, level) as trns from dual 
            connect by regexp_substr(r.packet, '[^,]+', 1, level) is not null
        )
        loop
            arr.extend;
            arr(cnt) := t.trns;
            cnt := cnt+1;
        end loop;
        
    end loop;
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
    
        open packets for
        with famiglie as
                (
                    select distinct 
                    find_child_transactions_p(padre) as parenti, 
                   --wm_concat(figlio) as parenti,
                    padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel_p group by padre, datapadre --A!
                )
                select distinct ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                select parenti AS packet, padre,
                data,
                dense_rank() over (order by a.data) as rank from famiglie a
                where a.data = (select min(b.data) from vcd0a_rp_sf_transactions_rel_p b where a.padre = b.padre )
                ) a
                where a.rank <= p_howMany
                order by a.rank;
        /*with famiglie as
                (
                    select distinct 
                    find_child_transactions_p(padre) as parenti, padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel_p -- P! 
                ),
                famiglie2 as
                (
                    select distinct 
                    find_child_transactions_p(padre) as parenti, padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel_p -- P! 
                )
                select ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                select parenti AS packet, padre,
                data,
                dense_rank() over (order by data) as rank from famiglie a
                where a.data = (select min(b.data) from famiglie2 b where a.parenti = b.parenti )
                and padre in ( select column_value from table( arr_padri ) )
                ) a
                where a.rank <= p_howMany
                order by a.rank;*/
         
        commit;
        
        loggamelo('End OLD');
    
    end ReadAndReserveTransactions_old;
    
    procedure ReadAndReserveTickets
    (
        p_howMany   IN number,
        tickets     OUT SYS_REFCURSOR
    )
    is
        arr         ARRAY_TYPE_VARCHAR100;
        cnt         number;
        sfStop      char(1);
    begin
    
    arr := ARRAY_TYPE_VARCHAR100();
    cnt := 1;
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    if ( sfStop = '0' ) then
    begin
    
    loggamelo('[Tickets] Inizio estrazione ticket, richiesti: ' || p_howMany);
    
    for r in 
    (
        WITH q AS
        (
            /*SELECT
                t.cd0g_strt_scn_num   AS progressivo,
                t.cdl0_cust_cod       AS customerid,
                t.cdl1_sede_cod || t.cdl1_nver_sede_cod AS codicesede,
                t.cdw6_tck_num        AS numticket,
                t.cdw4_stat_tck_cod   AS statoticket,
                t.cdw2_prob_des       AS motivazione,
                ROW_NUMBER()OVER(
                    ORDER BY
                        t.cd0g_strt_scn_num
                ) AS seqnum
            FROM
                tcd0g_rp_sf_ticket t
            WHERE
                t.cd0g_stat_recd_cod = 'A'
                AND t.cd0g_strt_scn_num IS NOT NULL
                AND NOT EXISTS(
                    SELECT
                        1
                    FROM
                        tcd0b_rp_sf_sede s
                    WHERE
                        s.cdl0_cust_cod = t.cdl0_cust_cod
                        AND s.cd0b_stat_recd_cod <> 'O'
                        AND s.cd0b_strt_scn_num < t.cd0g_strt_scn_num -- mah
                )*/
                
            SELECT distinct
                t.cd0g_strt_scn_num   AS progressivo,
                t.cdl0_cust_cod       AS customerid,
                t.cdl1_sede_cod || t.cdl1_nver_sede_cod AS codicesede,
                t.cdw6_tck_num        AS numticket,
                t.cdw4_stat_tck_cod   AS statoticket,
                t.cdw2_prob_des       AS motivazione,
                ROW_NUMBER()OVER(
                    ORDER BY
                        t.cd0g_strt_scn_num
                ) AS seqnum
            FROM
                tcd0g_rp_sf_ticket t
            WHERE
                t.cd0g_stat_recd_cod = 'A'
--                and 1 = 0
                and t.CDW4_STAT_TCK_COD <> 'D'
                AND t.cd0g_strt_scn_num IS NOT NULL
                AND t.cdl0_cust_cod NOT IN
                (
                    SELECT DISTINCT
                        b.cdl0_cust_cod
                    FROM
                        tcd0b_rp_sf_sede b
                    WHERE
                        b.cd0b_strt_scn_num < t.cd0g_strt_scn_num
                        AND b.cd0b_stat_recd_cod not in ('O', 'W', 'I', 'F')
                )/*
                AND t.cd0g_strt_scn_num IN (SELECT DISTINCT NUM FROM ( 
                     SELECT DISTINCT cdl0_cust_cod AS CUSTOMER, cd0g_strt_scn_num AS num 
                     FROM TCD0G_RP_SF_TICKET
                     WHERE cd0g_stat_recd_cod = 'A' AND cd0g_strt_scn_num IS NOT NULL --AND CDL1_SEDE_COD IS NOT null
                 ))
                AND t.cd0g_strt_scn_num >
                (CASE 
                    WHEN EXISTS ( SELECT 1 FROM (
                         SELECT DISTINCT cdl0_cust_cod AS CUSTOMER, min(cd0b_strt_scn_num) AS num
                         FROM tcd0b_rp_sf_sede s
                         WHERE s.cd0b_stat_recd_cod <> 'O'
                         GROUP BY cdl0_cust_cod
                     ) WHERE CUSTOMER = t.cdl0_cust_cod )
                    THEN (SELECT NUM FROM (
                         SELECT DISTINCT cdl0_cust_cod AS CUSTOMER, min(cd0b_strt_scn_num) AS num
                         FROM tcd0b_rp_sf_sede s
                         WHERE s.cd0b_stat_recd_cod <> 'O'
                         GROUP BY cdl0_cust_cod
                     ) WHERE CUSTOMER = t.cdl0_cust_cod )
                    ELSE t.cd0g_strt_scn_num - 1
                END )*/
        )
        SELECT
            q.progressivo,
            q.customerid,
            q.codicesede,
            q.numticket,
            q.statoticket,
            q.motivazione,
            q.seqnum
        FROM
            q
        WHERE
            seqnum <= 10
    )
    loop
        arr.extend;
        arr(cnt) := r.progressivo;
        cnt := cnt+1;
    end loop;
    
    update
        tcd0g_rp_sf_ticket
    set 
        cd0g_stat_recd_cod = 'P', cd0g_poll_dta=sysdate
    where 
        to_char('' || cd0g_strt_scn_num) in ( select column_value from table( arr ) );
        
    loggamelo('[Tickets] Update di ' || arr.count || ' ticket');
    
     open tickets for
        SELECT
            t.cd0g_strt_scn_num    AS progressivo,
            t.cdl0_cust_cod       AS customerid,
            t.cdl1_sede_cod || t.cdl1_nver_sede_cod AS codicesede,
            t.cdw4_stat_tck_cod   AS statoticket,
            t.cdw6_tck_num        AS numticket,
            t.cdw2_prob_des       AS motivazione
        FROM
            tcd0g_rp_sf_ticket t
        WHERE
            t.cd0g_stat_recd_cod = 'P'
            and t.cd0g_strt_scn_num is not null
            AND TO_CHAR('' || t.cd0g_strt_scn_num)IN(
                SELECT
                    column_value
                FROM
                    TABLE(arr)
            )
        ORDER BY
            t.cd0g_strt_scn_num;
            
    loggamelo('[Tickets] Fine estrazione ticket, richiesti: ' || p_howMany || ', estratti: ' || arr.count);
    end;
    end if;
    
    end ReadAndReserveTickets;
    
    procedure ReadAndReserveContacts
    (
        p_howMany   IN number,
        contacts    OUT SYS_REFCURSOR
    )
    is
        arr         ARRAY_TYPE_VARCHAR100;
        cnt         number;
        sfStop      char(1);
    begin
    
    arr := ARRAY_TYPE_VARCHAR100();
    cnt := 1;
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    if ( sfStop = '0' ) then
    begin
    
    loggamelo('[Contacts] Inizio estrazione cessazioni, richieste: ' || p_howMany);
    
    for r in 
    (
        with q as 
        (
            select transactionId, 
            row_number() over(order by data) as num
            from (
            select transactionId, min(data) as data
            from vcd0e_rp_sf_cessazioni
            LEFT JOIN TCD0M_VALID_TRNS m ON m.CD0M_RAW_XID_DES = RAW_XID_DES AND m.CD0M_PROG_TRNS_DES = PROG_TRNS_DES 
            where stato = 'A' --and 0 = 1
            AND 
                CASE
                    WHEN m.CD0M_VALID_FLG IS NOT NULL THEN m.CD0M_VALID_FLG
                    ELSE IS_VALID_CSCT(raw_xid_des, prog_trns_des)
                end = '1'
            group by transactionId)
        )
        select distinct  transactionId from q
        where num <= p_howMany
    )
    loop
        --loggamelo(r.transactionId);
        arr.extend;
        arr(cnt) := r.transactionId;
        cnt := cnt+1;
    end loop;
    
    update
        tcd0h_rp_sf_tel_cessati
    set 
        cd0h_stat_recd_cod = 'P', cd0h_poll_dta=sysdate
    where 
        to_char('' || cd0h_raw_xid_des || cd0h_prog_trns_des) in ( select column_value from table( arr ) )
        and cd0h_stat_recd_cod = 'A';
        
    update
        tcd0i_rp_sf_mail_cessate
    set 
        cd0i_stat_recd_cod = 'P', cd0i_poll_dta=sysdate
    where 
        to_char('' || cd0i_raw_xid_des || cd0i_prog_trns_des) in ( select column_value from table( arr ) )
        and cd0i_stat_recd_cod = 'A';
    
    loggamelo('[Contacts] dopo update di ' || arr.count || ' cessazioni.');
    
    open contacts for
        select distinct transactionId
        from 
        vcd0e_rp_sf_cessazioni
        where stato = 'P'
        and transactionId in ( select column_value from table( arr ) );
        
    loggamelo('[Contacts] Fine estrazione cessazioni. Richieste ' || p_howMany || ', estratte ' || arr.count);
    
    end;
    end if;
    
    end ReadAndReserveContacts;
    
    PROCEDURE AggiornaStatoContacts 
    (
      p_Transactions    IN       ARRAY_TYPE_VARCHAR100,
      p_Status          IN       VARCHAR2,
      p_result          OUT      NUMBER
       )
    is
    
    trans_id varchar2(20);
    conta number;
   -- p_array ARRAY_TYPE_VARCHAR100;
    
    begin 
    
        p_result := 0;
        
        loggamelo('Transactions '  || p_Transactions.COUNT);
        
         for i in p_Transactions.FIRST .. p_Transactions.LAST
        loop
        loggamelo(''||conta || ': '|| p_Transactions(i));
        trans_id := p_Transactions(i);
        end loop;
        
        loggamelo('tr -> '|| trans_id);
        
        if ( p_Transactions is not null and p_Transactions.COUNT > 0 ) then
            if ( p_status = 'Q' ) then
                UPDATE tcd0h_rp_sf_tel_cessati
                SET
                    cd0h_stat_recd_cod = p_status, cd0h_send_dta=sysdate
                WHERE
                    TO_CHAR('' || cd0h_raw_xid_des|| CD0H_PROG_TRNS_DES)IN(
                        SELECT
                            column_value
                        FROM
                            TABLE(p_Transactions)
                    );
                    
                UPDATE tcd0i_rp_sf_mail_cessate
                SET
                    cd0i_stat_recd_cod = p_status, cd0i_send_dta=sysdate
                WHERE
                    TO_CHAR('' || cd0i_raw_xid_des || CD0i_PROG_TRNS_DES)IN(
                        SELECT
                            column_value
                        FROM
                            TABLE(p_Transactions)
                    );
            else
                    UPDATE tcd0h_rp_sf_tel_cessati
                    SET
                        cd0h_stat_recd_cod = p_status
                    WHERE
                        TO_CHAR('' || cd0h_raw_xid_des|| CD0H_PROG_TRNS_DES)IN(
                            SELECT
                                column_value
                            FROM
                                TABLE(p_Transactions)
                        );
                        
                    UPDATE tcd0i_rp_sf_mail_cessate
                    SET
                        cd0i_stat_recd_cod = p_status
                    WHERE
                        TO_CHAR('' || cd0i_raw_xid_des || CD0i_PROG_TRNS_DES)IN(
                            SELECT
                                column_value
                            FROM
                                TABLE(p_Transactions)
                        );
                end if;
        end if;
        EXCEPTION
        
        WHEN OTHERS THEN
        loggamelo('errore '|| SQLCODE || ' ' || SQLERRM);
        p_result := 2;
        
        loggamelo(''||p_result);
        
    end AggiornaStatoContacts;
    
    procedure   ReadAndReserveTransactions_new
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        arr         ARRAY_TYPE_VARCHAR100;
        arr_padri   ARRAY_TYPE_VARCHAR100;
        arr_figli   ARRAY_TYPE_VARCHAR100;
        cnt         number;
        cnt_padri   number;
    begin
    
    loggamelo('Start NEW');
        
    arr := ARRAY_TYPE_VARCHAR100();
    cnt := 1;
    arr_padri := ARRAY_TYPE_VARCHAR100();
    cnt_padri := 1;
    arr_figli := ARRAY_TYPE_VARCHAR100();
    
    --p_howMany := 0;
    
    /*
        1. seleziono i pacchetti ed i relativi padri
        2. i padri me li tengo da parte per riselezionarli alla fine (voglio uscire con un risultato di select)
        3. le transazioni (i packet spacchettati) li uso per aggiornare le tabelle di replica
    */
    
    for r in (
                with famiglie as
                (
                    select distinct 
                    find_child_transactions(padre) as parenti, 
                    padre, datapadre as data
                from vcd0a_rp_sf_transactions_rel group by padre, datapadre --A!
                )
                select ''||sys_guid() AS guid, padre, packet, data, rank 
                from (
                select parenti AS packet, padre,
                data,
                dense_rank() over (order by a.data) as rank from famiglie a
                where a.data = (select min(b.data) from vcd0a_rp_sf_transactions_rel b where a.padre = b.padre )
                ) a
                where a.rank <= p_howMany
                order by a.rank
            )
    loop
        loggamelo('Padre: '||r.padre||', packet: '||r.packet);
        
        arr_padri.extend;
        arr_padri(cnt_padri) := r.padre;
        
        arr_figli.extend;
        arr_figli(cnt_padri) := r.packet;
        
        cnt_padri := cnt_padri+1;
        
        for t in 
        (
            select regexp_substr(r.packet,'[^,]+', 1, level) as trns from dual 
            connect by regexp_substr(r.packet, '[^,]+', 1, level) is not null
        )
        loop
            arr.extend;
            arr(cnt) := t.trns;
            cnt := cnt+1;
        end loop;
        
    end loop;
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr ) );
    
        open packets for
        with padri as
        (
            select column_value as padre, rownum as rank from table( arr_padri )
        ),
        figli as
        (
            select column_value as figli, rownum as rank from table( arr_figli )
        )
        select distinct 
               ''||sys_guid() AS guid, 
               p.padre, 
               f.figli as packet, 
               sysdate, 
               p.rank 
        from padri p join figli f on p.rank = f.rank
        order by p.rank;

        commit;
        
        loggamelo('End NEW');
    
    end ReadAndReserveTransactions_new;
    
    procedure   ReadAndReserveTransactions_v2
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        --arr         ARRAY_TYPE_VARCHAR100;
        arr_padri   ARRAY_TYPE_VARCHAR100;
        --arr_figli   ARRAY_TYPE_VARCHAR1000;
        cnt         number;
        cnt_padri   number;
        semaforo    char(1);
        sfStop      char(1);
        refresh_dta date;
        --VCD0C_dta   date;
    begin
        
    SF_REPLICA.READANDRESERVETRANSACTIONS_V6(
    P_HOWMANY => P_HOWMANY,
    PACKETS => PACKETS
  );
  
  /*SF_REPLICA.RARTransactions_v2_old(
    P_HOWMANY => P_HOWMANY,
    PACKETS => PACKETS
  );*/
    
    end ReadAndReserveTransactions_v2;
    
    procedure   RARTransactions_v2_old
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        --arr         ARRAY_TYPE_VARCHAR100;
        arr_padri   ARRAY_TYPE_VARCHAR100;
        --arr_figli   ARRAY_TYPE_VARCHAR1000;
        cnt         number;
        cnt_padri   number;
        semaforo    char(1);
        sfStop      char(1);
        refresh_dta date;
        --VCD0C_dta   date;
    begin
        
    select cd96_parm_valr_des into semaforo from tcd96_lc_parametri where cd96_parm_cod = 'SFP';
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    select last_refresh_date -0.01 into refresh_dta from USER_MVIEWS where mview_name = 'MCD0B_RP_SF_SCTYPE';
    
    --select last_refresh_date -0.01 into VCD0C_dta from USER_MVIEWS where mview_name = 'VCD0C_VALID_TRNS';
        
    --arr := ARRAY_TYPE_VARCHAR100();
    cnt := 1;
    arr_padri := ARRAY_TYPE_VARCHAR100();
    cnt_padri := 1;
    --arr_figli := ARRAY_TYPE_VARCHAR1000();
    
    if ( semaforo = '1' and sfStop = '0' ) then
    begin
    --loggamelo('Imposto semaforo a 0');
    loggamelo('Start prenotazione v2 di ' || p_howmany || ' packets');
    UpdateSemaforo('0');
    
    for r in (
                WITH transCust AS 
                (
                    SELECT CASE WHEN conta = 1 THEN 'S' ELSE 'C' END AS tipo, customerId, data FROM 
                    ( 
                        SELECT distinct
                            s.CDL0_CUST_COD AS CustomerId, count(DISTINCT s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES) AS conta, min(s.CD0B_RECD_NUM) AS data
                        FROM 
                            TCD0B_RP_SF_SEDE s
                            LEFT JOIN TCD0M_VALID_TRNS t ON t.CD0M_RAW_XID_DES = s.CD0B_RAW_XID_DES AND t.CD0M_PROG_TRNS_DES = s.CD0B_PROG_TRNS_DES 
                        WHERE
                            s.CD0B_STAT_RECD_COD = 'A'
                            AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D')) 
                            AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND s.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D') )                            
                            and 
                            (
                                (
                                  1=1
                                   and CD0b_RAW_XID_DES||CD0b_PROG_TRNS_DES IN 
                                   ('000D001A01518C61000', 
                                    '001100130150591B000', 
                                    '000A002A0145D4D2000', 
                                    '000B00280170F9EC000', 
                                    '000600020108450C000', 
                                    '000900210131E6D8000', 
                                    '0011000B0150D1CF000', 
                                    '0006000401083805000', 
                                    '0011002D0150D075000', 
                                    '0010002D01481B3A000')
                                   
                                   --and s.cd0b_tipo_even_cod in ( 'CRCU', 'VGCU', 'VGSP', 'CRSS', 'VSCU', 'VASP', 'VISP', 'VISS', 'VGSS' )
                                   
                                   --and s.cd0b_tipo_even_cod in ( 'VGEN')
                                   
                                   --and s.cd0b_tipo_even_cod in ( 'VSCU')
                                   --and s.cd0b_tipo_even_cod in ( 'CRSS')
                                   --and s.cd0b_tipo_even_cod in ( 'VASP')
                                   --and s.cd0b_tipo_even_cod in ( 'VISP')
                                   --and s.cd0b_tipo_even_cod in ( 'VISS')
                                   --and s.cd0b_tipo_even_cod in ( 'VGSS')
                                
                                   AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < s.CD0B_STRT_TRNS_TIM ) 
                                   AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND s.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < s.CD0B_STRT_TRNS_TIM ) 
                                   --and  s.cd0b_inse_dta >= TO_DATE('2019/06/28 09', 'YYYY/MM/DD HH24')  
                                )
                                /*or 
                                (
                                    s.cd0b_tipo_even_cod in ( 'VGEN' )
                                    and s.cd0b_inse_dta  >= TO_DATE('2019/06/26 09', 'YYYY/MM/DD HH24')
                                )*/
                            )
                           AND 
                           CASE 
                                WHEN t.CD0M_VALID_FLG IS NULL THEN is_valid_transaction(s.CD0B_RAW_XID_DES,s.CD0B_PROG_TRNS_DES)
                                ELSE t.CD0M_VALID_FLG 
                            END = '1'
                        GROUP BY CDL0_CUST_COD
                        ORDER BY data
                    )
                    WHERE ROWNUM <= 5
                )
                SELECT DISTINCT ''||sys_guid() AS guid, padre, figli as packet, data FROM 
                (
                    SELECT distinct
                        s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS padre, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS figli, s.CD0B_STRT_TRNS_TIM AS DATA--, t.tipo, t.CustomerId
                    FROM
                        TCD0B_RP_SF_SEDE s 
                        JOIN transCust t ON s.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'S' AND s.CD0B_RECD_NUM = t.data
                        WHERE s.CD0B_STAT_RECD_COD = 'A'
                        -- escludo i pacchetti 'semplici' finti... cioe quelli per cui esiste una sola transazione per quel customer, peccato che faccia parte di un'altra con un altro customer...
                        --AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE s1 WHERE s.CD0B_RAW_XID_DES = s1.CD0B_RAW_XID_DES AND s.CD0B_PROG_TRNS_DES = s1.CD0B_PROG_TRNS_DES AND s.CDL0_CUST_COD <> s1.CDL0_CUST_COD AND s1.CD0B_STAT_RECD_COD = 'A' )
                    UNION
                    SELECT DISTINCT padre, padre as figli, data 
                    FROM 
                    (
                        SELECT distinct
                            c.CD0B_RAW_XID_DES||c.CD0B_PROG_TRNS_DES AS padre, FIND_CHILD_TRANSACTIONS_V3(t.CustomerId) as figli, c.CD0B_STRT_TRNS_TIM AS DATA--, t.tipo, t.CustomerId
                        FROM
                            TCD0B_RP_SF_SEDE c
                            JOIN transCust t ON c.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'C' AND c.CD0B_RECD_NUM = t.data
                            WHERE c.CD0B_STAT_RECD_COD = 'A'
                    )
                    /*UNION
                    --semplici ma per cui la transazione relativa al customer ha anche ALTRI customer
                    SELECT DISTINCT 
                        x.CD0B_RAW_XID_DES||x.CD0B_PROG_TRNS_DES AS padre, x.CD0B_RAW_XID_DES||x.CD0B_PROG_TRNS_DES AS figli, x.CD0B_STRT_TRNS_TIM AS DATA--, x.CD0B_TIPO_EVEN_COD AS evento, t.tipo, t.CustomerId
                    FROM TCD0B_RP_SF_SEDE x
                    JOIN transCust t ON x.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'S' AND x.CD0B_RECD_NUM = t.data
                    WHERE x.CD0B_STAT_RECD_COD = 'A'
                    AND (
                       SELECT count(DISTINCT CDL0_CUST_COD) FROM TCD0B_RP_SF_SEDE x
                       WHERE x.CD0B_STAT_RECD_COD = 'A' AND x.CD0B_RAW_XID_DES||x.CD0B_PROG_TRNS_DES IN 
                       ( 
                          SELECT DISTINCT w.CD0B_RAW_XID_DES||w.CD0B_PROG_TRNS_DES FROM TCD0B_RP_SF_SEDE w
                          WHERE w.CD0B_STAT_RECD_COD = 'A' AND w.CDL0_CUST_COD = CustomerId 
                       )
                    ) > 1*/
                )
                order by data
            )   
    loop
        --loggamelo('Padre: '||r.padre||', packet: '||r.packet);
        
        arr_padri.extend;
        arr_padri(cnt_padri) := r.padre;
        
        --arr_figli.extend;
        --arr_figli(cnt_padri) := r.packet;
        
        cnt_padri := cnt_padri+1;
        
        /*for t in 
        (
            select regexp_substr(r.packet,'[^,]+', 1, level) as trns from dual 
            connect by regexp_substr(r.packet, '[^,]+', 1, level) is not null
        )
        loop
            arr.extend;
            arr(cnt) := t.trns;
            cnt := cnt+1;
        end loop;
        */
    end loop;
      
      loggamelo('Inizio update transazioni v2 (' || arr_padri.count || ' packets)');
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) ); --arr
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
    
    loggamelo('Fine update transazioni v2 (' || arr_padri.count || ' packets), inizio estrazione.');
    UpdateSemaforo('1');
    DBMS_OUTPUT.PUT_LINE('passa0');
    
    open packets for
    with padri as
    (
        select distinct padre, rank, ( select min(s.CD0B_STRT_TRNS_TIM) from tcd0b_rp_sf_sede s where s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = padre) as data
        from 
        (
            select column_value as padre, rownum as rank from table( arr_padri )
        )
    )
    select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        p.padre as padre, 
        p.padre as packet, 
        sysdate as data, 
        p.rank as rank,
        v.*
    from
        padri p
        join "VCD0B_RP_SF_SEDECUST_v3" v on transactionid = p.padre
    where
        p.data < refresh_dta
    union
       select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        p.padre as padre, 
        p.padre as packet, 
        sysdate as data, 
        p.rank as rank,
        v.*
    from
        padri p
        join "VCD0B_RP_SF_SEDECUST_idx" v on transactionid = p.padre
    where
        p.data >= refresh_dta;
    
              /*
        open packets for
        with padri as
        (
            select column_value as padre, rownum as rank from table( arr_padri )
        ),
        figli as
        (
            select column_value as figli, rownum as rank from table( arr_figli )
        )
        select distinct 
               'S' as tipo,
               ''||sys_guid() AS guid, 
               p.padre as padre, 
               f.figli as packet, 
               sysdate as data, 
               p.rank as rank,
               transactionid,
               customerid,
               customerid_old,
               codicesede,
               codiceSede_old,
               evento,
               ordcust,
               ordsede,
               datatransazione,
               stato,
               ragionesociale,
               annofatturato,
               annofondazione,
               annoriferimentodipendenti,
               fatturato,
               citta,
               nazionesede,
               codiceprovincia,
               indirizzo,
               buonoordine,
               cap,
               codicecategoriamassimaspesa,
               categoriamassimaspesa,
               cellula,
               classificazionepmi,
               areaelenco,
               codicecategoriaistat,
               codicecategoriamerceologica,
               codicecomune,
               statocustomer,
               codicefiscale,
               codicefrazione,
               ipa,
               codprovinciarea,
               numerorea,
               statosede,
               cognome,
               comune,
               coordx,
               coordy,
               coordinatemanuali,
               datofiscaleesterocertificato,
               denominazionealternativa,
               categoriaistat,
               categoriamerceologica,
               dug,
               emailarricchimento,
               emailarricchimentoid,
               emailbozze,
               emailbozzeid,
               emailcommercialeiol,
               emailcommercialeiolid,
               emailfatturazione,
               emailfatturazioneid,
               emailpostvendita,
               emailpostvenditaid,
               emailarricchimentopecflag,
               emailbozzepecflag,
               emailcommercialeiolpecflag,
               emailfatturazionepecflag,
               emailpostvenditapecflag,
               entepubblicoflag,
               codicefiscalefatturazione,
               fattelettronicaobbligatoria,
               frazione,
               idindirizzo,
               infotoponimo,
               insegna,
               mercatoaggregato,
               industry,
               codicenazione,
               descnazione,
               codicenazionecustomer, 
               potenzialenip,
               nome,
               noterecapitofattura,
               civico,
               numerodipendenti,
               opec,
               opecconsegnabile,
               partitaiva,
               profcoordinategeografiche,
               provincia,
               customercessatoriattivabile,
               sedecessatariattivabile,
               sedeprimariaflag,
               sottoareamercato,
               sottoclassedipendenti,
               sottoclassefatturato,
               statoopeccommattualiz,
               tipomercato,
               telefono1id,
               telefono1,
               telefono1primario,
               tipotelefono1,
               telefono2id,
               telefono2,
               telefono2primario,
               tipotelefono2,
               telefono3id,
               telefono3,
               telefono3primario,
               tipotelefono3,
               telefono4id,
               telefono4,
               telefono4primario,
               tipotelefono4,
               telefono5id,
               telefono5,
               telefono5primario,
               tipotelefono5,
               telefono6id,
               telefono6,
               telefono6primario,
               tipotelefono6,
               telefono7id,
               telefono7,
               telefono7primario,
               tipotelefono7,
               telefono8id,
               telefono8,
               telefono8primario,
               tipotelefono8,
               formagiuridica,
               tiposocgiuridica,
               toponimo,
               ultimaposizioneamministrativa,
               tiposocieta,
               urlfanpageid,
               urlfanpage,
               urlistituzionaleid,
               urlistituzionale,
               urlpaginebiancheid,
               urlpaginebianche,
               urlpaginegialleid,
               urlpaginegialle,
               ClienteSpecialeFlg,
               priorita
        from padri p 
            join figli f on p.rank = f.rank and p.padre = f.figli --semplici
            join VCD0B_RP_SF_SEDECUST v on transactionid = p.padre and v.stato = 'P'
        union
        select distinct
               'C' as tipo,
               ''||sys_guid() AS guid, 
               p.padre as padre, 
               f.figli as packet, 
               sysdate as data, 
               p.rank as rank,
               transactionid,
               customerid,
               customerid_old,
               codicesede,
               codiceSede_old,
               evento,
               ordcust,
               ordsede,
               datatransazione,
               stato,
               ragionesociale,
               annofatturato,
               annofondazione,
               annoriferimentodipendenti,
               fatturato,
               citta,
               nazionesede,
               codiceprovincia,
               indirizzo,
               buonoordine,
               cap,
               codicecategoriamassimaspesa,
               categoriamassimaspesa,
               cellula,
               classificazionepmi,
               areaelenco,
               codicecategoriaistat,
               codicecategoriamerceologica,
               codicecomune,
               statocustomer,
               codicefiscale,
               codicefrazione,
               ipa,
               codprovinciarea,
               numerorea,
               statosede,
               cognome,
               comune,
               coordx,
               coordy,
               coordinatemanuali,
               datofiscaleesterocertificato,
               denominazionealternativa,
               categoriaistat,
               categoriamerceologica,
               dug,
               emailarricchimento,
               emailarricchimentoid,
               emailbozze,
               emailbozzeid,
               emailcommercialeiol,
               emailcommercialeiolid,
               emailfatturazione,
               emailfatturazioneid,
               emailpostvendita,
               emailpostvenditaid,
               emailarricchimentopecflag,
               emailbozzepecflag,
               emailcommercialeiolpecflag,
               emailfatturazionepecflag,
               emailpostvenditapecflag,
               entepubblicoflag,
               codicefiscalefatturazione,
               fattelettronicaobbligatoria,
               frazione,
               idindirizzo,
               infotoponimo,
               insegna,
               mercatoaggregato,
               industry,
               codicenazione,
               descnazione,
               codicenazionecustomer,
               potenzialenip,
               nome,
               noterecapitofattura,
               civico,
               numerodipendenti,
               opec,
               opecconsegnabile,
               partitaiva,
               profcoordinategeografiche,
               provincia,
               customercessatoriattivabile,
               sedecessatariattivabile,
               sedeprimariaflag,
               sottoareamercato,
               sottoclassedipendenti,
               sottoclassefatturato,
               statoopeccommattualiz,
               tipomercato,
               telefono1id,
               telefono1,
               telefono1primario,
               tipotelefono1,
               telefono2id,
               telefono2,
               telefono2primario,
               tipotelefono2,
               telefono3id,
               telefono3,
               telefono3primario,
               tipotelefono3,
               telefono4id,
               telefono4,
               telefono4primario,
               tipotelefono4,
               telefono5id,
               telefono5,
               telefono5primario,
               tipotelefono5,
               telefono6id,
               telefono6,
               telefono6primario,
               tipotelefono6,
               telefono7id,
               telefono7,
               telefono7primario,
               tipotelefono7,
               telefono8id,
               telefono8,
               telefono8primario,
               tipotelefono8,
               formagiuridica,
               tiposocgiuridica,
               toponimo,
               ultimaposizioneamministrativa,
               tiposocieta,
               urlfanpageid,
               urlfanpage,
               urlistituzionaleid,
               urlistituzionale,
               urlpaginebiancheid,
               urlpaginebianche,
               urlpaginegialleid,
               urlpaginegialle,
               ClienteSpecialeFlg,
               priorita
        from padri p 
            join figli f on p.rank = f.rank and p.padre <> f.figli --complessi
            join VCD0B_RP_SF_SEDECUST v on transactionId in (select regexp_substr(f.figli,'[^,]+', 1, level) from dual
                                connect by regexp_substr(f.figli, '[^,]+', 1, level) is not null)  and v.stato = 'P'
        order by 6, 13, 14;--p.rank, ordcust, ordsede;*/
        
        DBMS_OUTPUT.PUT_LINE('passa1');
        
        commit;
        
        DBMS_OUTPUT.PUT_LINE('passa2');
        
        loggamelo('End prenotazione v2 di ' || p_howmany || ' packets, estratti ' || arr_padri.count);
        --loggamelo('Imposto semaforo a 1');
    end;
    else
    begin
        loggamelo('Semaforo a 0');
        open packets for
        select distinct 
               'S' as tipo,
               ''||sys_guid() AS guid, 
               '' as padre, 
               '' as packet, 
               sysdate as data, 
               0 as rank,
               transactionid,
               customerid,
               customerid_old,
               codicesede,
               codiceSede_old,
               evento,
               ordcust,
               ordsede,
               datatransazione,
               stato,
               ragionesociale,
               annofatturato,
               annofondazione,
               annoriferimentodipendenti,
               fatturato,
               citta,
               nazionesede,
               codiceprovincia,
               indirizzo,
               buonoordine,
               cap,
               codicecategoriamassimaspesa,
               categoriamassimaspesa,
               cellula,
               classificazionepmi,
               areaelenco,
               codicecategoriaistat,
               codicecategoriamerceologica,
               codicecomune,
               statocustomer,
               codicefiscale,
               codicefrazione,
               ipa,
               codprovinciarea,
               numerorea,
               statosede,
               cognome,
               comune,
               coordx,
               coordy,
               coordinatemanuali,
               datofiscaleesterocertificato,
               denominazionealternativa,
               categoriaistat,
               categoriamerceologica,
               dug,
               emailarricchimento,
               emailarricchimentoid,
               emailbozze,
               emailbozzeid,
               emailcommercialeiol,
               emailcommercialeiolid,
               emailfatturazione,
               emailfatturazioneid,
               emailpostvendita,
               emailpostvenditaid,
               emailarricchimentopecflag,
               emailbozzepecflag,
               emailcommercialeiolpecflag,
               emailfatturazionepecflag,
               emailpostvenditapecflag,
               entepubblicoflag,
               codicefiscalefatturazione,
               fattelettronicaobbligatoria,
               frazione,
               idindirizzo,
               infotoponimo,
               insegna,
               mercatoaggregato,
               industry,
               codicenazione,
               descnazione,
               codicenazionecustomer, 
               potenzialenip,
               nome,
               noterecapitofattura,
               civico,
               numerodipendenti,
               opec,
               opecconsegnabile,
               partitaiva,
               profcoordinategeografiche,
               provincia,
               customercessatoriattivabile,
               sedecessatariattivabile,
               sedeprimariaflag,
               sottoareamercato,
               sottoclassedipendenti,
               sottoclassefatturato,
               statoopeccommattualiz,
               tipomercato,
               telefono1id,
               telefono1,
               telefono1primario,
               tipotelefono1,
               telefono2id,
               telefono2,
               telefono2primario,
               tipotelefono2,
               telefono3id,
               telefono3,
               telefono3primario,
               tipotelefono3,
               telefono4id,
               telefono4,
               telefono4primario,
               tipotelefono4,
               telefono5id,
               telefono5,
               telefono5primario,
               tipotelefono5,
               telefono6id,
               telefono6,
               telefono6primario,
               tipotelefono6,
               telefono7id,
               telefono7,
               telefono7primario,
               tipotelefono7,
               telefono8id,
               telefono8,
               telefono8primario,
               tipotelefono8,
               formagiuridica,
               tiposocgiuridica,
               toponimo,
               ultimaposizioneamministrativa,
               tiposocieta,
               urlfanpageid,
               urlfanpage,
               urlistituzionaleid,
               urlistituzionale,
               urlpaginebiancheid,
               urlpaginebianche,
               urlpaginegialleid,
               urlpaginegialle,
               ClienteSpecialeFlg,
               priorita
        from vcd0b_rp_sf_sedecust where transactionid = 'xyz';
    end;
    end if;
    
    EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          loggamelo('ERROR prenotazione v2 di ' || p_howmany || ' packets Errore: '|| SQLCODE || ', ' || SQLERRM);
          
          open packets for
        select distinct 
               'S' as tipo,
               ''||sys_guid() AS guid, 
               '' as padre, 
               '' as packet, 
               sysdate as data, 
               0 as rank,
               transactionid,
               customerid,
               customerid_old,
               codicesede,
               codiceSede_old,
               evento,
               ordcust,
               ordsede,
               datatransazione,
               stato,
               ragionesociale,
               annofatturato,
               annofondazione,
               annoriferimentodipendenti,
               fatturato,
               citta,
               nazionesede,
               codiceprovincia,
               indirizzo,
               buonoordine,
               cap,
               codicecategoriamassimaspesa,
               categoriamassimaspesa,
               cellula,
               classificazionepmi,
               areaelenco,
               codicecategoriaistat,
               codicecategoriamerceologica,
               codicecomune,
               statocustomer,
               codicefiscale,
               codicefrazione,
               ipa,
               codprovinciarea,
               numerorea,
               statosede,
               cognome,
               comune,
               coordx,
               coordy,
               coordinatemanuali,
               datofiscaleesterocertificato,
               denominazionealternativa,
               categoriaistat,
               categoriamerceologica,
               dug,
               emailarricchimento,
               emailarricchimentoid,
               emailbozze,
               emailbozzeid,
               emailcommercialeiol,
               emailcommercialeiolid,
               emailfatturazione,
               emailfatturazioneid,
               emailpostvendita,
               emailpostvenditaid,
               emailarricchimentopecflag,
               emailbozzepecflag,
               emailcommercialeiolpecflag,
               emailfatturazionepecflag,
               emailpostvenditapecflag,
               entepubblicoflag,
               codicefiscalefatturazione,
               fattelettronicaobbligatoria,
               frazione,
               idindirizzo,
               infotoponimo,
               insegna,
               mercatoaggregato,
               industry,
               codicenazione,
               descnazione,
               codicenazionecustomer, 
               potenzialenip,
               nome,
               noterecapitofattura,
               civico,
               numerodipendenti,
               opec,
               opecconsegnabile,
               partitaiva,
               profcoordinategeografiche,
               provincia,
               customercessatoriattivabile,
               sedecessatariattivabile,
               sedeprimariaflag,
               sottoareamercato,
               sottoclassedipendenti,
               sottoclassefatturato,
               statoopeccommattualiz,
               tipomercato,
               telefono1id,
               telefono1,
               telefono1primario,
               tipotelefono1,
               telefono2id,
               telefono2,
               telefono2primario,
               tipotelefono2,
               telefono3id,
               telefono3,
               telefono3primario,
               tipotelefono3,
               telefono4id,
               telefono4,
               telefono4primario,
               tipotelefono4,
               telefono5id,
               telefono5,
               telefono5primario,
               tipotelefono5,
               telefono6id,
               telefono6,
               telefono6primario,
               tipotelefono6,
               telefono7id,
               telefono7,
               telefono7primario,
               tipotelefono7,
               telefono8id,
               telefono8,
               telefono8primario,
               tipotelefono8,
               formagiuridica,
               tiposocgiuridica,
               toponimo,
               ultimaposizioneamministrativa,
               tiposocieta,
               urlfanpageid,
               urlfanpage,
               urlistituzionaleid,
               urlistituzionale,
               urlpaginebiancheid,
               urlpaginebianche,
               urlpaginegialleid,
               urlpaginegialle,
               ClienteSpecialeFlg,
               priorita
        from vcd0b_rp_sf_sedecust where transactionid = 'xyz';
    
    end RARTransactions_v2_old;

    procedure   UpdateTransactionFromPE
    (
        p_TransactionId   IN       VARCHAR2,
        p_Status          IN       VARCHAR2,
        p_user            IN       VARCHAR2,
        p_error           IN       VARCHAR2,
        p_reply_num       IN       VARCHAR2,
        p_result          OUT      NUMBER
    )
    is
        
    begin
    
        p_result := 0;
    
        loggamelo('Aggiorno transazione ' || p_TransactionId || '(reply_num='|| p_reply_num ||') a stato ' || p_Status || ', errore >' || p_error || '< ');
        
        update TCD0A_RP_SF_CUSTOMER 
        set CD0A_STAT_RECD_COD = p_Status,
            CD0A_MLSF_UPD_DTA = sysdate,
            CD0A_MLSF_USER_DES = p_user,
            CD0A_ERR_DES = p_error,
            CD0A_REPLY_NUM = p_reply_num
        --where CD0A_RAW_XID_DES || nvl(CD0A_PROG_TRNS_DES, '000') = p_TransactionId and cd0a_stat_recd_cod not in ('A', 'I');
        where CD0A_RAW_XID_DES || CD0A_PROG_TRNS_DES = p_TransactionId and cd0a_stat_recd_cod not in ('A', 'I');
        
        update tcd0b_rp_sf_sede 
        set CD0b_STAT_RECD_COD = p_Status,
            CD0b_MLSF_UPD_DTA = sysdate,
            CD0b_MLSF_USER_DES = p_user,
            CD0b_ERR_DES = p_error,
            CD0b_REPLY_NUM = p_reply_num
        --where CD0b_RAW_XID_DES || nvl(CD0b_PROG_TRNS_DES, '000') = p_TransactionId and cd0b_stat_recd_cod not in ('A', 'I');
        where CD0b_RAW_XID_DES || CD0b_PROG_TRNS_DES = p_TransactionId and cd0b_stat_recd_cod not in ('A', 'I');
        
        update tcd0c_rp_sf_telefono 
        set CD0c_STAT_RECD_COD = p_Status,
            CD0c_MLSF_UPD_DTA = sysdate,
            CD0c_MLSF_USER_DES = p_user,
            CD0c_ERR_DES = p_error,
            CD0c_REPLY_NUM = p_reply_num
        --where CD0c_RAW_XID_DES || nvl(CD0c_PROG_TRNS_DES, '000') = p_TransactionId and cd0c_stat_recd_cod not in ('A', 'I');
        where CD0c_RAW_XID_DES || CD0c_PROG_TRNS_DES = p_TransactionId and cd0c_stat_recd_cod not in ('A', 'I');
        
        update tcd0d_rp_sf_mail
        set CD0d_STAT_RECD_COD = p_Status,
            CD0d_MLSF_UPD_DTA = sysdate,
            CD0d_MLSF_USER_DES = p_user,
            CD0d_ERR_DES = p_error,
            CD0d_REPLY_NUM = p_reply_num
        --where CD0d_RAW_XID_DES || nvl(CD0d_PROG_TRNS_DES, '000') = p_TransactionId and cd0d_stat_recd_cod not in ('A', 'I');
        where CD0d_RAW_XID_DES || CD0d_PROG_TRNS_DES = p_TransactionId and cd0d_stat_recd_cod not in ('A', 'I');
        
        update tcd0h_rp_sf_tel_cessati
        set CD0h_STAT_RECD_COD = p_Status,
            CD0h_MLSF_UPD_DTA = sysdate,
            CD0h_MLSF_USER_DES = p_user,
            CD0h_ERR_DES = p_error,
            CD0h_REPLY_NUM = p_reply_num
        --where CD0h_RAW_XID_DES || nvl(CD0h_PROG_TRNS_DES, '000') = p_TransactionId and cd0h_stat_recd_cod not in ('A', 'I');
        where CD0h_RAW_XID_DES || CD0h_PROG_TRNS_DES = p_TransactionId and cd0h_stat_recd_cod not in ('A', 'I');
        
        update tcd0i_rp_sf_mail_cessate
        set CD0i_STAT_RECD_COD = p_Status,
            CD0i_MLSF_UPD_DTA = sysdate,
            CD0i_MLSF_USER_DES = p_user,
            CD0i_ERR_DES = p_error,
            CD0i_REPLY_NUM = p_reply_num
        --where CD0i_RAW_XID_DES || nvl(CD0i_PROG_TRNS_DES, '000') = p_TransactionId and cd0i_stat_recd_cod not in ('A', 'I');
        where CD0i_RAW_XID_DES || CD0i_PROG_TRNS_DES = p_TransactionId and cd0i_stat_recd_cod not in ('A', 'I');
        
        commit;
        
        EXCEPTION
          WHEN OTHERS THEN 
          p_result := 1;
          loggamelo('Transazione ' || p_TransactionId ||' Errore: '|| SQLCODE || ', ' || SQLERRM);
          ROLLBACK;
    
    end UpdateTransactionFromPE;
    
    procedure   ReadAndReserveTransactions_v3
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        semaforo    char(1);
        sfStop      char(1);
    begin
    
    loggamelo('Start prenotazione v3 di ' || p_howmany || ' packets');
        
    select cd96_parm_valr_des into semaforo from tcd96_lc_parametri where cd96_parm_cod = 'SFP';
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    if ( semaforo = '1' and sfStop = '0' ) then
    begin
    loggamelo('Imposto semaforo a 0');
    
    UpdateSemaforo('0');
    
    -- 1 Svuoto tabbella temporanea
    -- 2 Popolo tabbella temporanea
    -- 3 Aggiorno transazioni leggendo da tabbella temporanea
    -- 4 Popolo cursore in uscita con JOIN tra VCD0B e tabbella temporanea
    
    delete from SF_POLLER_TEMP;
    delete from sf_poller_cust;
    
    loggamelo('Svuotate tabelle temporanee.');
    
    insert into sf_poller_cust (
    SELECT distinct
                        tipo, CustomerId, data 
                    FROM
                    (
                        SELECT distinct
                            CASE WHEN conta = 1 THEN 'S' ELSE 'C' END AS tipo,
                            CustomerId, data 
                            FROM 
                        (
                        SELECT CustomerId, count(DISTINCT TransactionId) AS conta, min(data) AS data FROM
                            (
                            SELECT distinct
                            s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS TransactionId, s.CD0B_RECD_NUM AS DATA, s.CDL0_CUST_COD AS CustomerId 
                            FROM TCD0B_RP_SF_SEDE s
                            WHERE s.CD0B_STAT_RECD_COD = 'A' AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K'))
                           -- and CD0b_RAW_XID_DES||CD0b_PROG_TRNS_DES IN ('0001001400F191D5000', '0003002300F9CD34000', '0004002400F8798A000')
                            MINUS
                            SELECT distinct
                            s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS TransactionId, s.CD0B_RECD_NUM AS DATA, s.CDL0_CUST_COD AS CustomerId 
                            FROM TCD0B_RP_SF_SEDE s JOIN TCD0A_RP_SF_CUSTOMER c ON s.CD0B_RAW_XID_DES = c.CD0A_RAW_XID_DES AND s.CD0B_PROG_TRNS_DES = c.CD0A_PROG_TRNS_DES
                            WHERE s.CDL1_PRIM_SEDC_FLG = 'S'
                            AND s.CD0B_STAT_RECD_COD <> c.CD0A_STAT_RECD_COD
                            MINUS
                            SELECT distinct
                            s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS TransactionId, s.CD0B_RECD_NUM AS DATA, s.CDL0_CUST_COD AS CustomerId 
                            FROM TCD0B_RP_SF_SEDE s 
                            WHERE s.CDL1_PRIM_SEDC_FLG = 'S'
                            AND NOT EXISTS ( SELECT 1 FROM TCD0A_RP_SF_CUSTOMER c WHERE c.CD0A_RAW_XID_DES = s.CD0B_RAW_XID_DES AND c.CD0A_PROG_TRNS_DES = s.CD0B_PROG_TRNS_DES AND s.CDL0_CUST_COD = c.CDL0_CUST_COD )
                            )
                        GROUP BY CustomerId 
                        )
                        where rownum <= p_howMany));
            
            loggamelo('Inserite ' || TO_CHAR(SQL%ROWCOUNT) || ' righe su SF_POLLER_CUST, inizio popolamento SF_POLLER_TEMP.');
        
                INSERT INTO SF_POLLER_TEMP (
                SELECT DISTINCT /*''||sys_guid() AS guid,*/ sysdate, padre, figli as packet, data FROM 
                (
                    SELECT distinct
                        s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS padre, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS figli, s.CD0B_STRT_TRNS_TIM AS DATA--, t.tipo, t.CustomerId
                    FROM
                        TCD0B_RP_SF_SEDE s 
                        JOIN sf_poller_cust t ON s.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'S' AND s.CD0B_RECD_NUM = t.num
                        WHERE s.CD0B_STAT_RECD_COD = 'A'
                    UNION
                    SELECT DISTINCT padre, padre as figli, data 
                    FROM 
                    (
                        SELECT distinct
                            c.CD0B_RAW_XID_DES||c.CD0B_PROG_TRNS_DES AS padre, FIND_CHILD_TRANSACTIONS_V3(t.CustomerId) as figli, c.CD0B_STRT_TRNS_TIM AS DATA--, t.tipo, t.CustomerId
                        FROM
                            TCD0B_RP_SF_SEDE c
                            JOIN sf_poller_cust t ON c.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'C' AND c.CD0B_RECD_NUM = t.num
                            WHERE c.CD0B_STAT_RECD_COD = 'A'
                    )
                ));
      
        loggamelo('Inserite ' || TO_CHAR(SQL%ROWCOUNT) || ' righe su SF_POLLER_TEMP, inizio update transazioni');
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select distinct padre from SF_POLLER_TEMP union select distinct figlio from SF_POLLER_TEMP );
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select distinct padre from SF_POLLER_TEMP union select distinct figlio from SF_POLLER_TEMP );
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select distinct padre from SF_POLLER_TEMP union select distinct figlio from SF_POLLER_TEMP );
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select distinct padre from SF_POLLER_TEMP union select distinct figlio from SF_POLLER_TEMP );
    
    loggamelo('Fine update transazioni v3 (' || p_howmany || ' packets)');
    
        DBMS_OUTPUT.PUT_LINE('passa0');
    
        open packets for
        select distinct 
               'S' as tipo,
               ''||sys_guid() AS guid, 
               p.padre as padre, 
               p.padre as packet, 
               sysdate as data, 
               --p.rank as rank,
               ROW_NUMBER() over(partition by p.padre order by trns_date ) as rank,
               v.*
        from SF_POLLER_TEMP p 
            --join figli f on p.rank = f.rank and p.padre = f.figli --semplici
            join vcd0b_rp_sf_sedecust v on transactionid = p.padre
        /*union
        select distinct
               'C' as tipo,
               ''||sys_guid() AS guid, 
               p.padre as padre, 
               f.figli as packet, 
               sysdate as data, 
               p.rank as rank,
               v.*
        from padri p 
            join figli f on p.rank = f.rank and p.padre <> f.figli --complessi
            join vcd0b_rp_sf_sedecust v on transactionId in (select regexp_substr(f.figli,'[^,]+', 1, level) from dual
                                connect by regexp_substr(f.figli, '[^,]+', 1, level) is not null) */
        order by 6, 13, 14;--p.rank, ordcust, ordsede;
    
        DBMS_OUTPUT.PUT_LINE('passa1');
        
        commit;
        
        DBMS_OUTPUT.PUT_LINE('passa2');
        
        loggamelo('End prenotazione v3 di ' || p_howmany || ' packets');
        loggamelo('Imposto semaforo a 1');
        UpdateSemaforo('1');
    end;
    else
    begin
        loggamelo('Semaforo a 0');
        open packets for
        select distinct 
               'S' as tipo,
               ''||sys_guid() AS guid, 
               '' as padre, 
               '' as packet, 
               sysdate as data, 
               0 as rank,
               v.*
        from vcd0b_rp_sf_sedecust v where transactionid = 'xyz';
    end;
    end if;
    
    EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          loggamelo('ERROR prenotazione v2 di ' || p_howmany || ' packets Errore: '|| SQLCODE || ', ' || SQLERRM);

          open packets for
        select distinct 
               'S' as tipo,
               ''||sys_guid() AS guid, 
               '' as padre, 
               '' as packet, 
               sysdate as data, 
               0 as rank,
               v.*
        from vcd0b_rp_sf_sedecust v where transactionid = 'xyz';
    
    end ReadAndReserveTransactions_v3;
    
    procedure   RefreshMaterializedViews
    (
        p_result out number
    )
    is
        sfs_value char(1);
        sfs_date date;
    begin
        
      p_result := 0;
      
      select cd96_parm_valr_des, cd96_modi_tim into sfs_value, sfs_date from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
      
      update tcd96_lc_parametri set cd96_parm_valr_des = '1' where cd96_parm_cod = 'SFS';

      loggamelo('Init refresh MCD0B_RP_SF_SCTYPE');
      
      BEGIN
        DBMS_SNAPSHOT.REFRESH('MCD0B_RP_SF_SCTYPE');
      END;
      
      loggamelo('End refresh MCD0B_RP_SF_SCTYPE');
      
      exception
      when others then
      begin
        p_result := 1;
      end;
      
      update tcd96_lc_parametri set cd96_parm_valr_des = sfs_value where cd96_parm_cod = 'SFS';-- and cd96_modi_tim = sfs_date; -- se la modi_tim non e la stessa e passato qualcosa dopo... vince lui
      
      commit;
        
    end RefreshMaterializedViews;
    
    procedure RefreshValidTransactions
    (
        p_result out number
    )
    is
        sfs_value char(1);
        is_valid char(1);
        conta number;
        sfs_date date;
        numRec number;
        ind number;
        chk number;
        P_TRANSACTIONS DBACD1CDB.ARRAY_TYPE_VARCHAR100;
        P_STATUS VARCHAR2(200);
        P_ERR_DES VARCHAR2(200);
        P_USER VARCHAR2(200);
        P_REPLY_ID NUMBER;

    begin
        
        ind := 0;
        
        loggamelo('[RefreshValidTransactions] Inizio');
        
        for rk in 
        (
            select distinct cd0b_raw_xid_des||cd0b_prog_trns_des as trx from tcd0b_rp_sf_sede
            where cd0b_stat_recd_cod = 'K'
            and CD0B_ERR_DES is not null and upper(CD0B_ERR_DES) like '%UNABLE_TO_LOCK_ROW%'
            and trunc(CD0B_POLL_DTA) < trunc(sysdate)
        )
        loop
        
        -- Modify the code to initialize the variable
          P_TRANSACTIONS := DBACD1CDB.ARRAY_TYPE_VARCHAR100(rk.trx);
          P_STATUS := 'A';
          P_ERR_DES := NULL;
          P_USER := NULL;
          P_REPLY_ID := NULL;
        
          SF_REPLICA.AGGIORNASTATIDAPE(
            P_TRANSACTIONS => P_TRANSACTIONS,
            P_STATUS => P_STATUS,
            P_ERR_DES => P_ERR_DES,
            P_USER => P_USER,
            P_REPLY_ID => P_REPLY_ID,
            P_RESULT => P_RESULT
          );
          
        end loop;
        
        for rr in 
        (
            select distinct cd0b_raw_xid_des||cd0b_prog_trns_des as trx from tcd0b_rp_sf_sede
            where cd0b_stat_recd_cod = 'Q'
            and cd0b_poll_dta < sysdate -2
            and cd0b_tipo_even_cod in ('CRCU','CRSS','VGCU','VGSS','VGSP','VSCU','VSSS','VSSP')
        )
        loop
        
        -- Modify the code to initialize the variable
          P_TRANSACTIONS := DBACD1CDB.ARRAY_TYPE_VARCHAR100(rr.trx);
          P_STATUS := 'A';
          P_ERR_DES := NULL;
          P_USER := NULL;
          P_REPLY_ID := NULL;
        
          SF_REPLICA.AGGIORNASTATIDAPE(
            P_TRANSACTIONS => P_TRANSACTIONS,
            P_STATUS => P_STATUS,
            P_ERR_DES => P_ERR_DES,
            P_USER => P_USER,
            P_REPLY_ID => P_REPLY_ID,
            P_RESULT => P_RESULT
          );
        
        end loop;
        
        for rp in 
        (
            select distinct cd0b_raw_xid_des||cd0b_prog_trns_des as trx from tcd0b_rp_sf_sede
            where cd0b_stat_recd_cod = 'P'
            and cd0b_poll_dta < sysdate -1            
        )
        loop
        
        -- Modify the code to initialize the variable
          P_TRANSACTIONS := DBACD1CDB.ARRAY_TYPE_VARCHAR100(rp.trx);
          P_STATUS := 'A';
          P_ERR_DES := NULL;
          P_USER := NULL;
          P_REPLY_ID := NULL;
        
          SF_REPLICA.AGGIORNASTATIDAPE(
            P_TRANSACTIONS => P_TRANSACTIONS,
            P_STATUS => P_STATUS,
            P_ERR_DES => P_ERR_DES,
            P_USER => P_USER,
            P_REPLY_ID => P_REPLY_ID,
            P_RESULT => P_RESULT
          );
        
        end loop;
        
        /*select cd96_parm_valr_des, cd96_modi_tim into sfs_value, sfs_date from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
        
        loggamelo('[RefreshValidTransactions] Il valore di SFS e'' ' || sfs_value);
      
        update tcd96_lc_parametri set cd96_parm_valr_des = '1' where cd96_parm_cod = 'SFS';*/
        
        delete from tcd0m_valid_trns where CD0M_RAW_XID_DES||CD0M_PROG_TRNS_DES in 
        (
            select distinct cd0b_raw_xid_des||cd0b_prog_trns_des from tcd0b_rp_sf_sede
            where cd0b_stat_recd_cod in ('O', 'K', 'I', 'Q', 'P')
        ) or CD0M_VALID_FLG = '0';
        
        select count(distinct tcd0b_rp_sf_sede.cd0b_raw_xid_des || tcd0b_rp_sf_sede.cd0b_prog_trns_des) into numRec
            from 
                tcd0b_rp_sf_sede
            where 
                cd0b_stat_recd_cod = 'A'
                and not exists ( select 1 from TCD0M_VALID_TRNS where cd0b_raw_xid_des = CD0M_RAW_XID_DES and cd0b_prog_trns_des = CD0M_PROG_TRNS_DES );

        loggamelo('[RefreshValidTransactions] Ci sono ' || numRec || ' record da elaborare');
        
        for r in 
        (
            select distinct 
                tcd0b_rp_sf_sede.cd0b_raw_xid_des as raw_xid_des, 
                tcd0b_rp_sf_sede.cd0b_prog_trns_des as prog_trns_des 
            from 
                tcd0b_rp_sf_sede
            where 
                cd0b_stat_recd_cod = 'A'
                and not exists ( select 1 from TCD0M_VALID_TRNS where cd0b_raw_xid_des = CD0M_RAW_XID_DES and cd0b_prog_trns_des = CD0M_PROG_TRNS_DES )
        )
        loop
            
            ind := ind + 1;
            
           select mod(ind, 1000) into chk from dual;
            
           -- loggamelo('[RefreshValidTransactions] ' || ind || '/' || numRec);
           if ( chk = 0 or ind = numRec ) then
           begin
            loggamelo('[RefreshValidTransactions] ' || ind || '/' || numRec);
           end;
           end if;

            select is_valid_transaction(r.raw_xid_des, r.prog_trns_des) into is_valid from dual;
            
            if ( is_valid <> '1' ) then
            begin
            
                loggamelo('[RefreshValidTransactions] Transazione ' || r.raw_xid_des || r.prog_trns_des || ' NON valida, la aggiorno a D.');
                
                update 
                    TCD0A_RP_SF_CUSTOMER
                set 
                    cd0a_stat_recd_cod = case when is_valid = '0' then 'D' when is_valid = '2' then 'M' else null end
                where 
                    cd0a_raw_xid_des = r.raw_xid_des
                    and cd0a_prog_trns_des = r.prog_trns_des;
                    
                update 
                    tcd0b_rp_sf_sede
                set 
                    cd0b_stat_recd_cod = case when is_valid = '0' then 'D' when is_valid = '2' then 'M' else null end
                where 
                    cd0b_raw_xid_des = r.raw_xid_des
                    and cd0b_prog_trns_des = r.prog_trns_des;
                    
                update 
                    tcd0c_rp_sf_telefono
                set 
                    cd0c_stat_recd_cod = case when is_valid = '0' then 'D' when is_valid = '2' then 'M' else null end
                where 
                    cd0c_raw_xid_des = r.raw_xid_des
                    and cd0c_prog_trns_des = r.prog_trns_des;
                
                update 
                    tcd0d_rp_sf_mail
                set 
                    cd0d_stat_recd_cod = case when is_valid = '0' then 'D' when is_valid = '2' then 'M' else null end
                where 
                    cd0d_raw_xid_des = r.raw_xid_des
                    and cd0d_prog_trns_des = r.prog_trns_des;
            end;
            else
            begin
                INSERT INTO DBACD1CDB.TCD0M_VALID_TRNS (CD0M_RAW_XID_DES, CD0M_PROG_TRNS_DES, CD0M_VALID_FLG, CD0M_CALC_DTA)
                VALUES (r.raw_xid_des, r.prog_trns_des, is_valid, sysdate);
            end;
            end if;
            
        end loop;
        /*
        loggamelo('[RefreshValidTransactions] Fine calcolo transazioni standard, inizio calcolo CSCT ');
        
        for r in 
        (
            select 
                distinct v.raw_xid_des,  v.prog_trns_des
            from 
                vcd0e_rp_sf_cessazioni v
            where 
                v.stato = 'A'
        )
        loop
            
            select is_valid_CSCT(r.raw_xid_des, r.prog_trns_des) into is_valid from dual;
            
            select count(*) into conta from tcd0m_valid_trns m where m.CD0M_RAW_XID_DES = r.raw_xid_des and m.CD0M_PROG_TRNS_DES = r.prog_trns_des;
        
            if ( conta = 0 ) then
            begin
            
                INSERT INTO DBACD1CDB.TCD0M_VALID_TRNS (CD0M_RAW_XID_DES, CD0M_PROG_TRNS_DES, CD0M_VALID_FLG, CD0M_CALC_DTA)
                VALUES (r.raw_xid_des, r.prog_trns_des, is_valid, sysdate);
                
                if ( is_valid = '0' ) then
                begin
                    
                    loggamelo('[RefreshValidTransactions] Transazione CSCT ' || r.raw_xid_des || r.prog_trns_des || ' NON valida, la aggiorno a D.');
                    
                    update 
                    tcd0h_rp_sf_tel_cessati
                    set 
                        cd0h_stat_recd_cod = 'D'
                    where 
                        to_char(''||cd0h_raw_xid_des) = r.raw_xid_des
                        and cd0h_prog_trns_des = r.prog_trns_des;
                    
                    update 
                    tcd0i_rp_sf_mail_cessate
                    set 
                        cd0i_stat_recd_cod = 'D'
                    where 
                        to_char(''||cd0i_raw_xid_des) = r.raw_xid_des
                        and cd0i_prog_trns_des = r.prog_trns_des;
                    
                end;
                end if;
                
            end;
            else
            begin
                loggamelo('[RefreshValidTransactions] Transazione CSCT ' || r.raw_xid_des || r.prog_trns_des || ' gia presente in tabella... Elimino il dato su TCD0M per forzare il calcolo a runtime delle due funzioni.');
                delete from DBACD1CDB.TCD0M_VALID_TRNS where CD0M_RAW_XID_DES = r.raw_xid_des and CD0M_PROG_TRNS_DES = r.prog_trns_des;
            end;
            end if;
        end loop;
        */
        --update tcd96_lc_parametri set cd96_parm_valr_des = sfs_value where cd96_parm_cod = 'SFS';-- and cd96_modi_tim = sfs_date; -- se la modi_tim non e la stessa e passato qualcosa dopo... vince lui
        delete from SF_POLLER_LOG where MSG_DATE <= sysdate -15;
        commit;
        
        EXCEPTION
        when others then
        begin
            ROLLBACK;
            loggamelo('[RefreshValidTransactions] Errore: '|| SQLCODE || ', ' || SQLERRM);
        end;
        
        loggamelo('[RefreshValidTransactions] Fine');
    
    end RefreshValidTransactions;
    
     procedure   ReadAndReserveTransactions_v4
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        semaforo    char(1);
        sfStop      char(1);
        refresh_dta date;
        poll_dta    date;
        poll_guid   varchar2(32);
        cntExec     number;
        P_RESULT    NUMBER;
        primoS      NUMBER;
    begin
    
    primoS := 0;
        
    select sysdate, sys_guid() into poll_dta, poll_guid from dual;
    
    select cd96_parm_valr_des into semaforo from tcd96_lc_parametri where cd96_parm_cod = 'SFP';
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    select last_refresh_date -0.01 into refresh_dta from USER_MVIEWS where mview_name = 'MCD0B_RP_SF_SCTYPE';
    
    if ( semaforo = '1' and sfStop = '0' ) then
    begin
    UpdateSemaforo('0');
    
    loggamelo('['||poll_guid||'] Inizio estrazione ' || poll_dta);
    
    for r in (
                WITH transCust AS 
                (
                    SELECT /*CASE WHEN conta = 1 THEN 'S' ELSE 'C' END*/ 'S' AS tipo, customerId, data FROM -- [RB 20200323 - tutti semplici!]
                    --SELECT CASE WHEN conta = 1 THEN 'S' ELSE 'C' END AS tipo, customerId, data FROM       
                    ( 
                        SELECT distinct
                            s.CDL0_CUST_COD AS CustomerId, count(DISTINCT s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES) AS conta, min(s.CD0B_RECD_NUM) AS data
                        FROM 
                            TCD0B_RP_SF_SEDE s
                            LEFT JOIN TCD0M_VALID_TRNS t ON t.CD0M_RAW_XID_DES = s.CD0B_RAW_XID_DES AND t.CD0M_PROG_TRNS_DES = s.CD0B_PROG_TRNS_DES 
                        WHERE
                             s.CD0B_STAT_RECD_COD = 'A'
                            --and s.CD0B_INSE_DTA <= TO_DATE('2019/12/05 16:30', 'YYYY/MM/DD HH24:MI')
                            AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D')) 
                            AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND s.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D') )                            
                            and 
                            (
                                (
                                  1=1 
                                  --and CD0b_RAW_XID_DES||CD0B_PROG_TRNS_DES in ('0007001A0138AF89000')
                                  and (s.cd0b_tipo_even_cod in ( 'CRCU', 'VGCU', 'VGSP', 'CRSS', 'VSCU', 'VASP', 'VISP', 'VISS', 'VGSS', 'VSSS', 'VDF', 'VRG', 'VGEN', 'SUB', 'SCO' )
                                  --or CD0b_RAW_XID_DES||CD0B_PROG_TRNS_DES in ('0014001000FFD472000')
                                  --or (s.cd0b_tipo_even_cod in ( 'SCO' ) 
                                  --    and (cdl0_cust_cod in (select cust from togs_scorpori_salf)
                                  --      or cdl0_cust_cod in (select custprec from togs_scorpori_salf)))
                                  or CD0b_RAW_XID_DES||CD0B_PROG_TRNS_DES in (select CD00_RAW_XID_DES||CD00_PROG_TRNS_DES from TOGS_RP_FORZATURA_TRANSACTIONS)
                                  --and s.cd0b_tipo_even_cod in ( 'SUB' ) and trunc(s.cd0b_inse_dta) <= to_date('11/06/2019','DD/MM/RRRR')
                                  --and s.cd0b_tipo_even_cod in ( 'VGEN' )
                                  --and s.cd0b_tipo_even_cod in ( 'VDF', 'VRG')
                                      )
                                )
                            )
                           /*[RB 20200416 - logica su recd_num]*/ 
                           AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD = 'A' AND b.cd0b_recd_num < s.cd0b_recd_num and b.CD0b_RAW_XID_DES||b.CD0b_PROG_TRNS_DES <> s.CD0b_RAW_XID_DES||s.CD0b_PROG_TRNS_DES )
                           --AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < s.CD0B_STRT_TRNS_TIM ) 
                           /*-- D.Campa : 27/03/2020  -> verifico che tutti i customer coinvolti nella trx NON abbiamo movimenti precedenti (controllo il cd0b_recd_num )
                                                     -- in stato: 'A','Q', 'P', 'K', 'D'
                            and not exists (
                                select null 
                                from tcd0b_rp_sf_sede a, tcd0b_rp_sf_sede x
                                where a.cdl0_cust_cod      = x.cdl0_cust_cod
                                  and x.cd0b_stat_recd_cod = 'A'
                                  and a.cd0b_recd_num      < x.cd0b_recd_num and a.CD0b_RAW_XID_DES||a.CD0b_PROG_TRNS_DES <> x.CD0b_RAW_XID_DES||x.CD0b_PROG_TRNS_DES
                                  and a.cd0b_stat_recd_cod in ('A','Q', 'P', 'K', 'D')
                                  and x.cd0b_raw_xid_des || x.cd0b_prog_trns_des = s.cd0b_raw_xid_des || s.cd0b_prog_trns_des
                              )
                            -- Fine  D.Campa : 27/03/2020*/
                           /*AND NOT EXISTS 
                           (
                                   SELECT 1 FROM TCD0B_RP_SF_SEDE b 
                                   WHERE b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < s.CD0B_STRT_TRNS_TIM
                                   AND b.CDL0_CUST_COD IN 
                                   (
                                       SELECT DISTINCT c.CDL0_CUST_COD FROM TCD0B_RP_SF_SEDE c
                                       WHERE c.CD0B_RAW_XID_DES||c.CD0B_PROG_TRNS_DES IN 
                                       (
                                           SELECT DISTINCT d.CD0B_RAW_XID_DES||d.CD0B_PROG_TRNS_DES
                                           FROM TCD0B_RP_SF_SEDE d WHERE d.CD0B_STAT_RECD_COD = 'A'
                                           AND d.CDL0_CUST_COD = s.CDL0_CUST_COD
                                       )
                                   )
                           )*/
                           /*[RB 20200416 - logica su recd_num]*/ 
                           AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND s.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD = 'A' AND b.cd0b_recd_num < s.cd0b_recd_num and b.CD0b_RAW_XID_DES||b.CD0b_PROG_TRNS_DES <> s.CD0b_RAW_XID_DES||s.CD0b_PROG_TRNS_DES )         
                           --AND NOT EXISTS ( SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND s.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD = 'A' AND b.CD0B_STRT_TRNS_TIM < s.CD0B_STRT_TRNS_TIM )         
                           AND 
                           CASE 
                                WHEN t.CD0M_VALID_FLG IS NULL THEN is_valid_transaction(s.CD0B_RAW_XID_DES,s.CD0B_PROG_TRNS_DES)
                                ELSE t.CD0M_VALID_FLG 
                            END = '1'
                            AND 
                                CASE WHEN s.cd0b_tipo_even_cod IN ('SUB', 'SCO')
                                THEN 
                                    CASE WHEN exists 
                                    ( 
                                        select 1 from TCD0A_RP_SF_CUSTOMER x where x.CD0a_RAW_XID_DES||x.CD0a_PROG_TRNS_DES in 
                                        (
                                            select distinct y.CD0a_RAW_XID_DES||y.CD0a_PROG_TRNS_DES from TCD0A_RP_SF_CUSTOMER y where y.CDL0_CUST_COD in 
                                            (
                                                select distinct z.CDL0_CUST_COD from TCD0A_RP_SF_CUSTOMER z where z.CD0a_RAW_XID_DES||z.CD0a_PROG_TRNS_DES in
                                                (
                                                    select distinct w.CD0a_RAW_XID_DES||w.CD0a_PROG_TRNS_DES from TCD0A_RP_SF_CUSTOMER w 
                                                    where w.CDL0_CUST_COD = s.CDL0_CUST_COD
                                                )
                                            )
                                        )
                                        and x.CD0a_STAT_RECD_COD IN ('Q', 'P', 'K', 'D')
                                    ) THEN 0
                                    ELSE 1
                                    end
                            ELSE 1 END = 1
                        GROUP BY CDL0_CUST_COD
                        ORDER BY data
                    )
                    WHERE ROWNUM <= 150
                )
                SELECT DISTINCT ''||sys_guid() AS guid, padre, figli as packet, data, evento FROM 
                (
                    SELECT distinct
                        s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS padre, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS figli, s.CD0B_STRT_TRNS_TIM AS DATA, s.cd0b_tipo_even_cod as evento--, t.tipo, t.CustomerId
                    FROM
                        TCD0B_RP_SF_SEDE s 
                        JOIN transCust t ON s.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'S' AND s.CD0B_RECD_NUM = t.data
                        WHERE s.CD0B_STAT_RECD_COD = 'A'
                    UNION
                    SELECT DISTINCT padre, padre as figli, data, evento 
                    FROM 
                    (
                        SELECT distinct
                            c.CD0B_RAW_XID_DES||c.CD0B_PROG_TRNS_DES AS padre, FIND_CHILD_TRANSACTIONS_V3(t.CustomerId) as figli, c.CD0B_STRT_TRNS_TIM AS DATA, c.cd0b_tipo_even_cod as evento--, t.tipo, t.CustomerId
                        FROM
                            TCD0B_RP_SF_SEDE c
                            JOIN transCust t ON c.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'C' AND c.CD0B_RECD_NUM = t.data
                            WHERE c.CD0B_STAT_RECD_COD = 'A'
                    )
                )
                order by data
            )   
    loop
    
        loggamelo('[' || poll_guid || '] Analisi transazione ' || r.padre || ' (' || r.evento || ')' );
        
        /*if ( r.evento in ('SUB', 'SCO') ) then
        begin
            primoS := primoS+1;
        end;
        end if;
        
        if ( r.evento not in ('SUB', 'SCO') or (r.evento in ('SUB', 'SCO') and primoS = 1) ) then
        begin*/
        
        insert into tcd0n_polled_trns (POLL_GUID, DATEINS, TRANSACTIONID, CUSTOMERID, CODICESEDE, SEDE_COD_TEST, EVENTO, ORDCUST, ORDSEDE, DATATRANSAZIONE, STATO, CUSTOMERID_OLD, CODICESEDE_OLD, RAGIONESOCIALE, ANNOFATTURATO, ANNOFONDAZIONE, ANNORIFERIMENTODIPENDENTI, FATTURATO, CITTA, NAZIONESEDE, CODICEPROVINCIA, INDIRIZZO, BUONOORDINE, CAP, CODICECATEGORIAMASSIMASPESA, CATEGORIAMASSIMASPESA, CELLULA, CLASSIFICAZIONEPMI, AREAELENCO, CODICECATEGORIAISTAT, CODICECATEGORIAMERCEOLOGICA, CODICECOMUNE, STATOCUSTOMER, CODICEFISCALE, CODICEFRAZIONE, IPA, CODPROVINCIAREA, NUMEROREA, STATOSEDE, COGNOME, COMUNE, COORDX, COORDY, COORDINATEMANUALI, DATOFISCALEESTEROCERTIFICATO, DENOMINAZIONEALTERNATIVA, CATEGORIAISTAT, CATEGORIAMERCEOLOGICA, DUG, EMAILARRICCHIMENTO, EMAILARRICCHIMENTOID, EMAILBOZZE, EMAILBOZZEID, EMAILCOMMERCIALEIOL, EMAILCOMMERCIALEIOLID, EMAILFATTURAZIONE, EMAILFATTURAZIONEID, EMAILFATTELETTRONICA, EMAILFATTELETTRONICAID, EMAILPOSTVENDITA, EMAILPOSTVENDITAID, EMAILARRICCHIMENTOPECFLAG, EMAILBOZZEPECFLAG, EMAILCOMMERCIALEIOLPECFLAG, EMAILFATTURAZIONEPECFLAG, EMAILFATTELETTRONICAPECFLAG, EMAILPOSTVENDITAPECFLAG, ENTEPUBBLICOFLAG, CODICEFISCALEFATTURAZIONE, FATTELETTRONICAOBBLIGATORIA, FRAZIONE, IDINDIRIZZO, INFOTOPONIMO, INSEGNA, MERCATOAGGREGATO, INDUSTRY, CODICENAZIONE, DESCNAZIONE, CODICENAZIONECUSTOMER, POTENZIALENIP, NOME, NOTERECAPITOFATTURA, CIVICO, NUMERODIPENDENTI, OPEC, OPECCONSEGNABILE, PARTITAIVA, PROFCOORDINATEGEOGRAFICHE, PROVINCIA, CUSTOMERCESSATORIATTIVABILE, SEDECESSATARIATTIVABILE, SEDEPRIMARIAFLAG, SOTTOAREAMERCATO, SOTTOCLASSEDIPENDENTI, SOTTOCLASSEFATTURATO, STATOOPECCOMMATTUALIZ, TIPOMERCATO, TELEFONO1ID, TELEFONO1, TELEFONO1PRIMARIO, TIPOTELEFONO1, TELEFONO2ID, TELEFONO2, TELEFONO2PRIMARIO, TIPOTELEFONO2, TELEFONO3ID, TELEFONO3, TELEFONO3PRIMARIO, TIPOTELEFONO3, TELEFONO4ID, TELEFONO4, TELEFONO4PRIMARIO, TIPOTELEFONO4, TELEFONO5ID, TELEFONO5, TELEFONO5PRIMARIO, TIPOTELEFONO5, TELEFONO6ID, TELEFONO6, TELEFONO6PRIMARIO, TIPOTELEFONO6, TELEFONO7ID, TELEFONO7, TELEFONO7PRIMARIO, TIPOTELEFONO7, TELEFONO8ID, TELEFONO8, TELEFONO8PRIMARIO, TIPOTELEFONO8, FORMAGIURIDICA, TIPOSOCGIURIDICA, TOPONIMO, ULTIMAPOSIZIONEAMMINISTRATIVA, TIPOSOCIETA, URLFANPAGEID, URLFANPAGE, URLISTITUZIONALEID, URLISTITUZIONALE, URLPAGINEBIANCHEID, URLPAGINEBIANCHE, URLPAGINEGIALLEID, URLPAGINEGIALLE, CLIENTESPECIALEFLG, PRIORITA)
        SELECT distinct
          poll_guid,
          poll_dta,
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
          and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = r.padre
          and s.CD0B_STAT_RECD_COD = 'A'
    union
    SELECT 
          poll_guid,
          poll_dta,
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
          and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = r.padre
          and s.CD0B_STAT_RECD_COD = 'A';
    
        loggamelo('[' || poll_guid || '] Estratta transazione ' || r.padre || ' (' || r.evento || ', ' || TO_CHAR(SQL%ROWCOUNT) ||' record) con data ' || r.data );
        /*end;
        else
        begin
        loggamelo('[' || poll_guid || '] Ignorato SUB/SCO ' || r.padre || ' (' || r.evento || ') con data ' || r.data );
        end;
        end if;*/
    
    end loop;
      
      loggamelo('[' || poll_guid || '] Inizio update transazioni (' || p_howmany || ' packets)');
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            --to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) ); --arr
            cd0a_raw_xid_des||CD0A_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0a_stat_recd_cod = 'A';
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            --to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0b_raw_xid_des||CD0b_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0b_stat_recd_cod = 'A';
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            --to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0c_raw_xid_des||CD0c_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0c_stat_recd_cod = 'A';
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            --to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0d_raw_xid_des||CD0d_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0d_stat_recd_cod = 'A';
    
    loggamelo('[' || poll_guid || '] Fine update transazioni (' || p_howmany || ' packets)');
    
    DBMS_OUTPUT.PUT_LINE('passa0');
    
    open packets for
    select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = poll_guid
        order by n.DATATRANSAZIONE, n.ORDCUST, n.ORDSEDE;
    
    delete from tcd0n_polled_trns where POLL_GUID = poll_guid;
    
    
    delete from togs_rp_forzatura_transactions x
       where x.cd00_raw_xid_des||x.cd00_prog_trns_des in (select cd0b_raw_xid_des||cd0b_prog_trns_des 
                                                            from tcd0b_rp_sf_sede
                                                            where  cd0b_stat_recd_cod <> 'A')   ; 

    
    SELECT to_number(CD96_PARM_VALR_DES) INTO cntExec FROM TCD96_LC_PARAMETRI WHERE CD96_PARM_COD IN ('SFC');
    
    if ( cntExec >= 25 ) then
    begin
        
        loggamelo('[' || poll_guid || '] Lancio validatore di transazioni!');
        
        SF_REPLICA.REFRESHVALIDTRANSACTIONS(
            P_RESULT => P_RESULT
        );
        
        loggamelo('[' || poll_guid || '] Fine validazione transazioni (esito '|| P_RESULT ||')');
        
        UPDATE DBACD1CDB.TCD96_LC_PARAMETRI 
        SET CD96_PARM_VALR_DES = '0', CD96_MODI_TIM = sysdate
        WHERE CD96_PARM_COD = 'SFC';
    end;
    else
    begin
        UPDATE DBACD1CDB.TCD96_LC_PARAMETRI 
        SET CD96_PARM_VALR_DES = to_char(cntExec+1), CD96_MODI_TIM = sysdate
        WHERE CD96_PARM_COD = 'SFC';
    end;
    end if;
        
    commit;
        
    loggamelo('[' || poll_guid || '] End prenotazione v2 di ' || p_howmany || ' packets');
        
    
        
        UpdateSemaforo('1');
    end;
    else
    begin
        open packets for
        select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = 'xyz';
    end;
    end if;

    
    EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          loggamelo('ERROR prenotazione v2 di ' || p_howmany || ' packets Errore: '|| SQLCODE || ', ' || SQLERRM);
          
          open packets for
        select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = 'xyz';
    
    end ReadAndReserveTransactions_v4;
    
    procedure   ReadAndReserveTransactions_v5
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        semaforo    char(1);
        sfStop      char(1);
        refresh_dta date;
        poll_dta    date;
        poll_guid   varchar2(32);
        cntExec     number;
        P_RESULT    NUMBER;
        primoS      NUMBER;
    begin
    
    primoS := 0;
        
    select sysdate, sys_guid() into poll_dta, poll_guid from dual;
    
    select cd96_parm_valr_des into semaforo from tcd96_lc_parametri where cd96_parm_cod = 'SFP';
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    select last_refresh_date -0.01 into refresh_dta from USER_MVIEWS where mview_name = 'MCD0B_RP_SF_SCTYPE';
    
    if ( semaforo = '1' and sfStop = '0' ) then
    begin
    UpdateSemaforo('0');
    
    loggamelo('['||poll_guid||'] Inizio estrazione ' || poll_dta);
    
    for r in (
                WITH transCust AS 
                (
                    SELECT /*CASE WHEN conta = 1 THEN 'S' ELSE 'C' END*/ 'S' AS tipo, customerId, data FROM -- [RB 20200323 - tutti semplici!]
                    --SELECT CASE WHEN conta = 1 THEN 'S' ELSE 'C' END AS tipo, customerId, data FROM       
                    ( 
                        SELECT
                            s.CDL0_CUST_COD AS CustomerId, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES, min(s.cd0b_recd_num) AS data
                        FROM
                            TCD0B_RP_SF_SEDE s
                            LEFT JOIN TCD0M_VALID_TRNS t ON t.CD0M_RAW_XID_DES = s.CD0B_RAW_XID_DES AND t.CD0M_PROG_TRNS_DES = s.CD0B_PROG_TRNS_DES 
                        WHERE
                            s.CD0B_STAT_RECD_COD = 'A'
                            and 
                            (
                                s.cd0b_tipo_even_cod in ( 'CRCU', 'VGCU', 'VGSP', 'CRSS', 'VSCU', 'VASP', 'VISP', 'VISS', 'VGSS', 'VSSS', 'VDF', 'VRG', 'VGEN', 'SUB', 'SCO' )
                                or s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES in (select CD00_RAW_XID_DES||CD00_PROG_TRNS_DES from TOGS_RP_FORZATURA_TRANSACTIONS) 
                            )
                            AND NOT EXISTS 
                            (
                                SELECT 1 FROM TCD0B_RP_SF_SEDE a WHERE a.CDL0_CUST_COD IN ( SELECT DISTINCT b.CDL0_CUST_COD FROM TCD0B_RP_SF_SEDE b WHERE s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = b.CD0b_RAW_XID_DES||b.CD0B_PROG_TRNS_DES )
                                AND a.CD0B_STAT_RECD_COD IN ('Q','P','K','D')
                            ) 
                            AND NOT EXISTS
                            (
                                SELECT 1 FROM TCD0B_RP_SF_SEDE a WHERE a.CDL0_CUST_COD IN ( SELECT DISTINCT b.CDL0_CUST_COD FROM TCD0B_RP_SF_SEDE b WHERE s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = b.CD0b_RAW_XID_DES||b.CD0B_PROG_TRNS_DES )
                                AND a.CD0B_RECD_NUM < s.CD0B_RECD_NUM AND a.CD0B_STAT_RECD_COD = 'A'
                            )
                            AND NOT EXISTS
                            (
                                SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND s.CDL1_PREC_SEDE_COD IS NOT NULL AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D', 'A') AND b.CD0B_RECD_NUM < s.CD0B_RECD_NUM
                                and s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES <> b.CD0b_RAW_XID_DES||b.CD0B_PROG_TRNS_DES
                            )                   
                            AND 
                            CASE 
                                 WHEN t.CD0M_VALID_FLG IS NULL THEN is_valid_transaction(s.CD0B_RAW_XID_DES,s.CD0B_PROG_TRNS_DES)
                                 ELSE t.CD0M_VALID_FLG 
                            END = '1'
                        GROUP BY s.CDL0_CUST_COD, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES
                        ORDER BY data
                    )
                    WHERE ROWNUM <= 150
                )
                SELECT DISTINCT ''||sys_guid() AS guid, padre, figli as packet, data, evento FROM 
                (
                    SELECT distinct
                        s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS padre, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS figli, s.CD0B_STRT_TRNS_TIM AS DATA, s.cd0b_tipo_even_cod as evento--, t.tipo, t.CustomerId
                    FROM
                        TCD0B_RP_SF_SEDE s 
                        JOIN transCust t ON s.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'S' AND s.CD0B_RECD_NUM = t.data
                        WHERE s.CD0B_STAT_RECD_COD = 'A'
                    UNION
                    SELECT DISTINCT padre, padre as figli, data, evento 
                    FROM 
                    (
                        SELECT distinct
                            c.CD0B_RAW_XID_DES||c.CD0B_PROG_TRNS_DES AS padre, FIND_CHILD_TRANSACTIONS_V3(t.CustomerId) as figli, c.CD0B_STRT_TRNS_TIM AS DATA, c.cd0b_tipo_even_cod as evento--, t.tipo, t.CustomerId
                        FROM
                            TCD0B_RP_SF_SEDE c
                            JOIN transCust t ON c.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'C' AND c.CD0B_RECD_NUM = t.data
                            WHERE c.CD0B_STAT_RECD_COD = 'A'
                    )
                )
                order by data
            )   
    loop
    
        loggamelo('[' || poll_guid || '] Analisi transazione ' || r.padre || ' (' || r.evento || ')' );
        
        /*if ( r.evento in ('SUB', 'SCO') ) then
        begin
            primoS := primoS+1;
        end;
        end if;
        
        if ( r.evento not in ('SUB', 'SCO') or (r.evento in ('SUB', 'SCO') and primoS = 1) ) then
        begin*/
        
        insert into tcd0n_polled_trns (POLL_GUID, DATEINS, TRANSACTIONID, CUSTOMERID, CODICESEDE, SEDE_COD_TEST, EVENTO, ORDCUST, ORDSEDE, DATATRANSAZIONE, STATO, CUSTOMERID_OLD, CODICESEDE_OLD, RAGIONESOCIALE, ANNOFATTURATO, ANNOFONDAZIONE, ANNORIFERIMENTODIPENDENTI, FATTURATO, CITTA, NAZIONESEDE, CODICEPROVINCIA, INDIRIZZO, BUONOORDINE, CAP, CODICECATEGORIAMASSIMASPESA, CATEGORIAMASSIMASPESA, CELLULA, CLASSIFICAZIONEPMI, AREAELENCO, CODICECATEGORIAISTAT, CODICECATEGORIAMERCEOLOGICA, CODICECOMUNE, STATOCUSTOMER, CODICEFISCALE, CODICEFRAZIONE, IPA, CODPROVINCIAREA, NUMEROREA, STATOSEDE, COGNOME, COMUNE, COORDX, COORDY, COORDINATEMANUALI, DATOFISCALEESTEROCERTIFICATO, DENOMINAZIONEALTERNATIVA, CATEGORIAISTAT, CATEGORIAMERCEOLOGICA, DUG, EMAILARRICCHIMENTO, EMAILARRICCHIMENTOID, EMAILBOZZE, EMAILBOZZEID, EMAILCOMMERCIALEIOL, EMAILCOMMERCIALEIOLID, EMAILFATTURAZIONE, EMAILFATTURAZIONEID, EMAILFATTELETTRONICA, EMAILFATTELETTRONICAID, EMAILPOSTVENDITA, EMAILPOSTVENDITAID, EMAILARRICCHIMENTOPECFLAG, EMAILBOZZEPECFLAG, EMAILCOMMERCIALEIOLPECFLAG, EMAILFATTURAZIONEPECFLAG, EMAILFATTELETTRONICAPECFLAG, EMAILPOSTVENDITAPECFLAG, ENTEPUBBLICOFLAG, CODICEFISCALEFATTURAZIONE, FATTELETTRONICAOBBLIGATORIA, FRAZIONE, IDINDIRIZZO, INFOTOPONIMO, INSEGNA, MERCATOAGGREGATO, INDUSTRY, CODICENAZIONE, DESCNAZIONE, CODICENAZIONECUSTOMER, POTENZIALENIP, NOME, NOTERECAPITOFATTURA, CIVICO, NUMERODIPENDENTI, OPEC, OPECCONSEGNABILE, PARTITAIVA, PROFCOORDINATEGEOGRAFICHE, PROVINCIA, CUSTOMERCESSATORIATTIVABILE, SEDECESSATARIATTIVABILE, SEDEPRIMARIAFLAG, SOTTOAREAMERCATO, SOTTOCLASSEDIPENDENTI, SOTTOCLASSEFATTURATO, STATOOPECCOMMATTUALIZ, TIPOMERCATO, TELEFONO1ID, TELEFONO1, TELEFONO1PRIMARIO, TIPOTELEFONO1, TELEFONO2ID, TELEFONO2, TELEFONO2PRIMARIO, TIPOTELEFONO2, TELEFONO3ID, TELEFONO3, TELEFONO3PRIMARIO, TIPOTELEFONO3, TELEFONO4ID, TELEFONO4, TELEFONO4PRIMARIO, TIPOTELEFONO4, TELEFONO5ID, TELEFONO5, TELEFONO5PRIMARIO, TIPOTELEFONO5, TELEFONO6ID, TELEFONO6, TELEFONO6PRIMARIO, TIPOTELEFONO6, TELEFONO7ID, TELEFONO7, TELEFONO7PRIMARIO, TIPOTELEFONO7, TELEFONO8ID, TELEFONO8, TELEFONO8PRIMARIO, TIPOTELEFONO8, FORMAGIURIDICA, TIPOSOCGIURIDICA, TOPONIMO, ULTIMAPOSIZIONEAMMINISTRATIVA, TIPOSOCIETA, URLFANPAGEID, URLFANPAGE, URLISTITUZIONALEID, URLISTITUZIONALE, URLPAGINEBIANCHEID, URLPAGINEBIANCHE, URLPAGINEGIALLEID, URLPAGINEGIALLE, CLIENTESPECIALEFLG, PRIORITA)
        SELECT distinct
          poll_guid,
          poll_dta,
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
          and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = r.padre
          and s.CD0B_STAT_RECD_COD = 'A'
    union
    SELECT 
          poll_guid,
          poll_dta,
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
          and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = r.padre
          and s.CD0B_STAT_RECD_COD = 'A';
    
        loggamelo('[' || poll_guid || '] Estratta transazione ' || r.padre || ' (' || r.evento || ', ' || TO_CHAR(SQL%ROWCOUNT) ||' record) con data ' || r.data );
        /*end;
        else
        begin
        loggamelo('[' || poll_guid || '] Ignorato SUB/SCO ' || r.padre || ' (' || r.evento || ') con data ' || r.data );
        end;
        end if;*/
    
    end loop;
      
      loggamelo('[' || poll_guid || '] Inizio update transazioni (' || p_howmany || ' packets)');
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            --to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) ); --arr
            cd0a_raw_xid_des||CD0A_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0a_stat_recd_cod = 'A';
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            --to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0b_raw_xid_des||CD0b_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0b_stat_recd_cod = 'A';
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            --to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0c_raw_xid_des||CD0c_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0c_stat_recd_cod = 'A';
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            --to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0d_raw_xid_des||CD0d_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0d_stat_recd_cod = 'A';
    
    loggamelo('[' || poll_guid || '] Fine update transazioni (' || p_howmany || ' packets)');
    
    DBMS_OUTPUT.PUT_LINE('passa0');
    
    open packets for
    select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = poll_guid
        order by n.DATATRANSAZIONE, n.ORDCUST, n.ORDSEDE;
    
    delete from tcd0n_polled_trns where POLL_GUID = poll_guid;
    
    
    delete from togs_rp_forzatura_transactions x
       where x.cd00_raw_xid_des||x.cd00_prog_trns_des in (select cd0b_raw_xid_des||cd0b_prog_trns_des 
                                                            from tcd0b_rp_sf_sede
                                                            where  cd0b_stat_recd_cod <> 'A')   ; 

    
    SELECT to_number(CD96_PARM_VALR_DES) INTO cntExec FROM TCD96_LC_PARAMETRI WHERE CD96_PARM_COD IN ('SFC');
    
    if ( cntExec >= 25 ) then
    begin
        
        loggamelo('[' || poll_guid || '] Lancio validatore di transazioni!');
        
        SF_REPLICA.REFRESHVALIDTRANSACTIONS(
            P_RESULT => P_RESULT
        );
        
        loggamelo('[' || poll_guid || '] Fine validazione transazioni (esito '|| P_RESULT ||')');
        
        UPDATE DBACD1CDB.TCD96_LC_PARAMETRI 
        SET CD96_PARM_VALR_DES = '0', CD96_MODI_TIM = sysdate
        WHERE CD96_PARM_COD = 'SFC';
    end;
    else
    begin
        UPDATE DBACD1CDB.TCD96_LC_PARAMETRI 
        SET CD96_PARM_VALR_DES = to_char(cntExec+1), CD96_MODI_TIM = sysdate
        WHERE CD96_PARM_COD = 'SFC';
    end;
    end if;
        
    commit;
        
    loggamelo('[' || poll_guid || '] End prenotazione v2 di ' || p_howmany || ' packets');
        
    
        
        UpdateSemaforo('1');
    end;
    else
    begin
        open packets for
        select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = 'xyz';
    end;
    end if;

    
    EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          loggamelo('ERROR prenotazione v2 di ' || p_howmany || ' packets Errore: '|| SQLCODE || ', ' || SQLERRM);
          
          open packets for
        select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = 'xyz';
    
    end ReadAndReserveTransactions_v5;
    
        procedure   ReadAndReserveTransactions_v6
    (
        p_howMany   IN number,
        packets     OUT SYS_REFCURSOR
    )
    is
        semaforo    char(1);
        sfStop      char(1);
        refresh_dta date;
        poll_dta    date;
        poll_guid   varchar2(32);
        cntExec     number;
        P_RESULT    NUMBER;
        primoS      NUMBER;
        pollQ       NUMBER;
        pollS       varchar2(32);
    begin
    
    primoS := 0;
        
    select sysdate, sys_guid() into poll_dta, poll_guid from dual;
    
    select cd96_parm_valr_des into semaforo from tcd96_lc_parametri where cd96_parm_cod = 'SFP';
    
    select cd96_parm_valr_des into sfStop from tcd96_lc_parametri where cd96_parm_cod = 'SFS';
    
    select last_refresh_date -0.01 into refresh_dta from USER_MVIEWS where mview_name = 'MCD0B_RP_SF_SCTYPE';
    
    select cd96_parm_valr_des into pollS from tcd96_lc_parametri where cd96_parm_cod = 'SFQ';
    
    if ( pollS is not null ) then
    begin 
        select to_number(pollS) into pollQ from dual;
    end;
    else
    begin 
        pollQ := 150;
    end;
    end if;
    
    if ( semaforo = '1' and sfStop = '0' ) then
    begin
    UpdateSemaforo('0');
    
    loggamelo('['||poll_guid||'] Inizio estrazione ('||pollQ||') ' || poll_dta);
    
    for r in (
                WITH transCust AS 
                (
                    SELECT /*CASE WHEN conta = 1 THEN 'S' ELSE 'C' END*/ 'S' AS tipo, customerId, data FROM -- [RB 20200323 - tutti semplici!]
                    --SELECT CASE WHEN conta = 1 THEN 'S' ELSE 'C' END AS tipo, customerId, data FROM       
                    ( 
                        SELECT
                            s.CDL0_CUST_COD AS CustomerId, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES, min(s.cd0b_recd_num) AS data
                        FROM
                            TCD0B_RP_SF_SEDE s
                            JOIN (  SELECT t.cdl0_cust_cod AS cust, min(t.cd0b_recd_num) AS recd_num FROM TCD0B_RP_SF_SEDE t
									WHERE t.CD0B_STAT_RECD_COD = 'A' AND NOT EXISTS( SELECT 1 FROM togs_rp_forzatura_transactions f JOIN TCD0B_RP_SF_SEDE x ON f.CD00_RAW_XID_DES = x.CD0B_RAW_XID_DES AND f.CD00_PROG_TRNS_DES = x.CD0B_PROG_TRNS_DES where x.CDL0_CUST_COD = t.CDL0_CUST_COD )
									GROUP BY t.cdl0_cust_cod
									UNION
									SELECT t.cdl0_cust_cod AS cust, min(t.cd0b_recd_num) AS recd_num FROM TCD0B_RP_SF_SEDE t JOIN togs_rp_forzatura_transactions f ON f.CD00_RAW_XID_DES = t.CD0B_RAW_XID_DES AND f.CD00_PROG_TRNS_DES = t.CD0B_PROG_TRNS_DES
									WHERE t.CD0B_STAT_RECD_COD = 'A'
									GROUP BY t.cdl0_cust_cod ) a ON s.CDL0_CUST_COD = a.cust AND s.CD0B_RECD_NUM = a.recd_num
                            LEFT JOIN TCD0M_VALID_TRNS t ON t.CD0M_RAW_XID_DES = s.CD0B_RAW_XID_DES AND t.CD0M_PROG_TRNS_DES = s.CD0B_PROG_TRNS_DES 
                        WHERE
                            s.CD0B_STAT_RECD_COD = 'A'
                            --and s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = '000C001A015B710B000'
                            and CASE 
                                 WHEN t.CD0M_VALID_FLG IS NULL THEN is_valid_transaction(s.CD0B_RAW_XID_DES,s.CD0B_PROG_TRNS_DES)
                                 ELSE t.CD0M_VALID_FLG 
                            END = '1'
                            and s.cd0b_tipo_even_cod in ( 'CRCU', 'VGCU', 'VGSP', 'CRSS', 'VSCU', 'VASP', 'VISP', 'VISS', 'VGSS', 'VSSS', 'VDF', 'VRG', 'VGEN', 'SUB', 'SCO' )
                            --and s.cd0b_tipo_even_cod in ( 'VGSP', 'VGCU' )
                            and 
                            (
                                s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES IN ( select distinct CD00_RAW_XID_DES||CD00_PROG_TRNS_DES from TOGS_RP_FORZATURA_TRANSACTIONS )
                                OR
                                (
                                    NOT EXISTS 
                                    (
                                        SELECT 1 FROM TCD0B_RP_SF_SEDE a WHERE a.CDL0_CUST_COD IN ( SELECT DISTINCT b.CDL0_CUST_COD FROM TCD0B_RP_SF_SEDE b WHERE s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = b.CD0b_RAW_XID_DES||b.CD0B_PROG_TRNS_DES )
                                        AND a.CD0B_STAT_RECD_COD IN ('Q','P','K','D','M')
                                    ) 
                                    AND NOT EXISTS
                                    (
                                        SELECT 1 FROM TCD0B_RP_SF_SEDE a WHERE a.CDL0_CUST_COD IN ( SELECT DISTINCT b.CDL0_CUST_COD FROM TCD0B_RP_SF_SEDE b WHERE s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = b.CD0b_RAW_XID_DES||b.CD0B_PROG_TRNS_DES )
                                        AND a.CD0B_RECD_NUM < s.CD0B_RECD_NUM AND a.CD0B_STAT_RECD_COD = 'A'
                                    )
                                    AND NOT EXISTS
                                    (
                                        SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL1_PREC_SEDE_COD IS NOT NULL AND s.CDL1_PREC_SEDE_COD = b.CDL1_SEDE_COD AND b.CDL1_NVER_SEDE_COD = s.CDL1_PREC_NVER_SEDE_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K', 'D', 'A', 'M') AND b.CD0B_RECD_NUM < s.CD0B_RECD_NUM
                                        and s.CD0b_RAW_XID_DES||s.CD0B_PROG_TRNS_DES <> b.CD0b_RAW_XID_DES||b.CD0B_PROG_TRNS_DES
                                    )
                                )
                            )
                        GROUP BY s.CDL0_CUST_COD, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES
                        ORDER BY data
                    )
                    WHERE ROWNUM <= pollQ
                )
                SELECT DISTINCT ''||sys_guid() AS guid, padre, figli as packet, data, evento FROM 
                (
                    SELECT distinct
                        s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS padre, s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS figli, s.CD0B_STRT_TRNS_TIM AS DATA, s.cd0b_tipo_even_cod as evento--, t.tipo, t.CustomerId
                    FROM
                        TCD0B_RP_SF_SEDE s 
                        JOIN transCust t ON s.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'S' AND s.CD0B_RECD_NUM = t.data
                        WHERE s.CD0B_STAT_RECD_COD = 'A'
                    UNION
                    SELECT DISTINCT padre, padre as figli, data, evento 
                    FROM 
                    (
                        SELECT distinct
                            c.CD0B_RAW_XID_DES||c.CD0B_PROG_TRNS_DES AS padre, FIND_CHILD_TRANSACTIONS_V3(t.CustomerId) as figli, c.CD0B_STRT_TRNS_TIM AS DATA, c.cd0b_tipo_even_cod as evento--, t.tipo, t.CustomerId
                        FROM
                            TCD0B_RP_SF_SEDE c
                            JOIN transCust t ON c.CDL0_CUST_COD = t.CustomerId AND t.tipo = 'C' AND c.CD0B_RECD_NUM = t.data
                            WHERE c.CD0B_STAT_RECD_COD = 'A'
                    )
                )
                order by data
            )   
    loop
    
        loggamelo('[' || poll_guid || '] Analisi transazione ' || r.padre || ' (' || r.evento || ')' );
        
        /*if ( r.evento in ('SUB', 'SCO') ) then
        begin
            primoS := primoS+1;
        end;
        end if;
        
        if ( r.evento not in ('SUB', 'SCO') or (r.evento in ('SUB', 'SCO') and primoS = 1) ) then
        begin*/
        
        insert into tcd0n_polled_trns (POLL_GUID, DATEINS, TRANSACTIONID, CUSTOMERID, CODICESEDE, SEDE_COD_TEST, EVENTO, ORDCUST, ORDSEDE, DATATRANSAZIONE, STATO, CUSTOMERID_OLD, CODICESEDE_OLD, RAGIONESOCIALE, ANNOFATTURATO, ANNOFONDAZIONE, ANNORIFERIMENTODIPENDENTI, FATTURATO, CITTA, NAZIONESEDE, CODICEPROVINCIA, INDIRIZZO, BUONOORDINE, CAP, CODICECATEGORIAMASSIMASPESA, CATEGORIAMASSIMASPESA, CELLULA, CLASSIFICAZIONEPMI, AREAELENCO, CODICECATEGORIAISTAT, CODICECATEGORIAMERCEOLOGICA, CODICECOMUNE, STATOCUSTOMER, CODICEFISCALE, CODICEFRAZIONE, IPA, CODPROVINCIAREA, NUMEROREA, STATOSEDE, COGNOME, COMUNE, COORDX, COORDY, COORDINATEMANUALI, DATOFISCALEESTEROCERTIFICATO, DENOMINAZIONEALTERNATIVA, CATEGORIAISTAT, CATEGORIAMERCEOLOGICA, DUG, EMAILARRICCHIMENTO, EMAILARRICCHIMENTOID, EMAILBOZZE, EMAILBOZZEID, EMAILCOMMERCIALEIOL, EMAILCOMMERCIALEIOLID, EMAILFATTURAZIONE, EMAILFATTURAZIONEID, EMAILFATTELETTRONICA, EMAILFATTELETTRONICAID, EMAILPOSTVENDITA, EMAILPOSTVENDITAID, EMAILARRICCHIMENTOPECFLAG, EMAILBOZZEPECFLAG, EMAILCOMMERCIALEIOLPECFLAG, EMAILFATTURAZIONEPECFLAG, EMAILFATTELETTRONICAPECFLAG, EMAILPOSTVENDITAPECFLAG, ENTEPUBBLICOFLAG, CODICEFISCALEFATTURAZIONE, FATTELETTRONICAOBBLIGATORIA, FRAZIONE, IDINDIRIZZO, INFOTOPONIMO, INSEGNA, MERCATOAGGREGATO, INDUSTRY, CODICENAZIONE, DESCNAZIONE, CODICENAZIONECUSTOMER, POTENZIALENIP, NOME, NOTERECAPITOFATTURA, CIVICO, NUMERODIPENDENTI, OPEC, OPECCONSEGNABILE, PARTITAIVA, PROFCOORDINATEGEOGRAFICHE, PROVINCIA, CUSTOMERCESSATORIATTIVABILE, SEDECESSATARIATTIVABILE, SEDEPRIMARIAFLAG, SOTTOAREAMERCATO, SOTTOCLASSEDIPENDENTI, SOTTOCLASSEFATTURATO, STATOOPECCOMMATTUALIZ, TIPOMERCATO, TELEFONO1ID, TELEFONO1, TELEFONO1PRIMARIO, TIPOTELEFONO1, TELEFONO2ID, TELEFONO2, TELEFONO2PRIMARIO, TIPOTELEFONO2, TELEFONO3ID, TELEFONO3, TELEFONO3PRIMARIO, TIPOTELEFONO3, TELEFONO4ID, TELEFONO4, TELEFONO4PRIMARIO, TIPOTELEFONO4, TELEFONO5ID, TELEFONO5, TELEFONO5PRIMARIO, TIPOTELEFONO5, TELEFONO6ID, TELEFONO6, TELEFONO6PRIMARIO, TIPOTELEFONO6, TELEFONO7ID, TELEFONO7, TELEFONO7PRIMARIO, TIPOTELEFONO7, TELEFONO8ID, TELEFONO8, TELEFONO8PRIMARIO, TIPOTELEFONO8, FORMAGIURIDICA, TIPOSOCGIURIDICA, TOPONIMO, ULTIMAPOSIZIONEAMMINISTRATIVA, TIPOSOCIETA, URLFANPAGEID, URLFANPAGE, URLISTITUZIONALEID, URLISTITUZIONALE, URLPAGINEBIANCHEID, URLPAGINEBIANCHE, URLPAGINEGIALLEID, URLPAGINEGIALLE, CLIENTESPECIALEFLG, PRIORITA, CONTABILITACLIENTE, CATEGORIAAPPARETENENZA, INDICATOREPA, CONS_VT_FLG, CONS_PFLZ_FLG, CONS_TEL1_FLG, CONS_TEL2_FLG, CONS_TEL3_FLG, CONS_TEL4_FLG, CONS_TEL5_FLG, CONS_TEL6_FLG, CONS_TEL7_FLG, CONS_TEL8_FLG)
        SELECT distinct
          poll_guid,
          poll_dta,
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
          COALESCE (c.CD0A_PRTY_COD, 'H') AS Priorita,
          c.CDAA_CONT_CLNT_COD AS contabilitaCliente, --IOL_GruppoContabilitaCliente__c
          c.CDAB_CATP_CLNT_COD AS categoriaApparetenenza, --IOL_CategoriaDiAppartenenza__c
          COALESCE(c.CDLG_TIPO_PA_DES, 'NULL') AS indicatorePA, --IOL_TipoDiPubblicaAmministrazione__c
          decode(c.CDLS_CON4_VT_FLG, 'S', 'true', 'N', 'false', null, 'false') as consVTerziFlag, --IOL_ConsensoVenditaTerzi__c
          decode(c.CDLS_CON7_AMKT_FLG, 'S', 'true', 'N', 'false', null, 'false') as consProfilazFlag, --IOL_ConsensoProfilazione__c
          nvl(t.T1_TEL_CONS,'false') as tel1ConsFlag, --IOL_ConsensoTelefono1__c
          nvl(t.T2_TEL_CONS,'false') as tel2ConsFlag, --IOL_ConsensoTelefono2__c
          nvl(t.T3_TEL_CONS,'false') as tel3ConsFlag, --IOL_ConsensoTelefono3__c
          nvl(t.T4_TEL_CONS,'false') as tel4ConsFlag, --IOL_ConsensoTelefono4__c
          nvl(t.T5_TEL_CONS,'false') as tel5ConsFlag, --IOL_ConsensoTelefono5__c
          nvl(t.T6_TEL_CONS,'false') as tel6ConsFlag, --IOL_ConsensoTelefono6__c
          nvl(t.T7_TEL_CONS,'false') as tel7ConsFlag, --IOL_ConsensoTelefono7__c
          nvl(t.T8_TEL_CONS,'false') as tel8ConsFlag  --IOL_ConsensoTelefono8__c
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
          and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = r.padre
          and s.CD0B_STAT_RECD_COD = 'A'
    union
    SELECT 
          poll_guid,
          poll_dta,
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
          COALESCE (s.CD0b_PRTY_COD, 'H'),
          null AS contabilitaCliente, --IOL_GruppoContabilitaCliente__c
          null AS categoriaApparetenenza, --IOL_CategoriaDiAppartenenza__c
          null AS indicatorePA, --IOL_TipoDiPubblicaAmministrazione__c
          null as consVTerziFlag, --IOL_ConsensoVenditaTerzi__c
          null as consProfilazFlag, --IOL_ConsensoProfilazione__c
          nvl(t.T1_TEL_CONS,'false') as tel1ConsFlag, --IOL_ConsensoTelefono1__c
          nvl(t.T2_TEL_CONS,'false') as tel2ConsFlag, --IOL_ConsensoTelefono2__c
          nvl(t.T3_TEL_CONS,'false') as tel3ConsFlag, --IOL_ConsensoTelefono3__c
          nvl(t.T4_TEL_CONS,'false') as tel4ConsFlag, --IOL_ConsensoTelefono4__c
          nvl(t.T5_TEL_CONS,'false') as tel5ConsFlag, --IOL_ConsensoTelefono5__c
          nvl(t.T6_TEL_CONS,'false') as tel6ConsFlag, --IOL_ConsensoTelefono6__c
          nvl(t.T7_TEL_CONS,'false') as tel7ConsFlag, --IOL_ConsensoTelefono7__c
          nvl(t.T8_TEL_CONS,'false') as tel8ConsFlag  --IOL_ConsensoTelefono8__c
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
          and s.cd0b_raw_xid_des||s.cd0b_prog_trns_des = r.padre
          and s.CD0B_STAT_RECD_COD = 'A';
    
        loggamelo('[' || poll_guid || '] Estratta transazione ' || r.padre || ' (' || r.evento || ', ' || TO_CHAR(SQL%ROWCOUNT) ||' record) con data ' || r.data );
        /*end;
        else
        begin
        loggamelo('[' || poll_guid || '] Ignorato SUB/SCO ' || r.padre || ' (' || r.evento || ') con data ' || r.data );
        end;
        end if;*/
    
    end loop;
      
      loggamelo('[' || poll_guid || '] Inizio update transazioni (' || pollQ || ' packets)');
      
        update 
            TCD0A_RP_SF_CUSTOMER
        set 
            cd0a_stat_recd_cod = 'P', cd0a_poll_dta=sysdate
        where 
            --to_char(''||cd0a_raw_xid_des || nvl(CD0A_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) ); --arr
            cd0a_raw_xid_des||CD0A_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0a_stat_recd_cod = 'A';
            
        update 
            tcd0b_rp_sf_sede
        set 
            cd0b_stat_recd_cod = 'P', cd0b_poll_dta=sysdate
        where 
            --to_char(''||cd0b_raw_xid_des || nvl(CD0b_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0b_raw_xid_des||CD0b_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0b_stat_recd_cod = 'A';
            
        update 
            tcd0c_rp_sf_telefono
        set 
            cd0c_stat_recd_cod = 'P', cd0c_poll_dta=sysdate
        where 
            --to_char(''||cd0c_raw_xid_des || nvl(CD0c_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0c_raw_xid_des||CD0c_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0c_stat_recd_cod = 'A';
        
        update 
            tcd0d_rp_sf_mail
        set 
            cd0d_stat_recd_cod = 'P', cd0d_poll_dta=sysdate
        where 
            --to_char(''||cd0d_raw_xid_des || nvl(CD0d_PROG_TRNS_DES,'000')) in ( select column_value from table( arr_padri ) );
            cd0d_raw_xid_des||CD0d_PROG_TRNS_DES in ( select distinct n.TRANSACTIONID from tcd0n_polled_trns n where n.POLL_GUID = poll_guid )
            and cd0d_stat_recd_cod = 'A';
    
    loggamelo('[' || poll_guid || '] Fine update transazioni (' || pollQ || ' packets)');
    
    DBMS_OUTPUT.PUT_LINE('passa0');
    
    open packets for
    select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = poll_guid
        order by n.DATATRANSAZIONE, n.ORDCUST, n.ORDSEDE;
    
    delete from tcd0n_polled_trns where POLL_GUID = poll_guid;
    
    
    delete from togs_rp_forzatura_transactions x
       where x.cd00_raw_xid_des||x.cd00_prog_trns_des in (select cd0b_raw_xid_des||cd0b_prog_trns_des 
                                                            from tcd0b_rp_sf_sede
                                                            where  cd0b_stat_recd_cod <> 'A')   ; 

    
    SELECT to_number(CD96_PARM_VALR_DES) INTO cntExec FROM TCD96_LC_PARAMETRI WHERE CD96_PARM_COD IN ('SFC');
    
    if ( cntExec >= 25 ) then
    begin
        
        loggamelo('[' || poll_guid || '] Lancio validatore di transazioni!');
        
        SF_REPLICA.REFRESHVALIDTRANSACTIONS(
            P_RESULT => P_RESULT
        );
        
        loggamelo('[' || poll_guid || '] Fine validazione transazioni (esito '|| P_RESULT ||')');
        
        UPDATE DBACD1CDB.TCD96_LC_PARAMETRI 
        SET CD96_PARM_VALR_DES = '0', CD96_MODI_TIM = sysdate
        WHERE CD96_PARM_COD = 'SFC';
    end;
    else
    begin
        UPDATE DBACD1CDB.TCD96_LC_PARAMETRI 
        SET CD96_PARM_VALR_DES = to_char(cntExec+1), CD96_MODI_TIM = sysdate
        WHERE CD96_PARM_COD = 'SFC';
    end;
    end if;
        
    commit;
        
    loggamelo('[' || poll_guid || '] End prenotazione v6 di ' || pollQ || ' packets');
        
    
        
        UpdateSemaforo('1');
    end;
    else
    begin
        open packets for
        select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = 'xyz';
    end;
    end if;

    
    EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          loggamelo('ERROR prenotazione v6 di ' || p_howmany || ' packets Errore: '|| SQLCODE || ', ' || SQLERRM);
          
          open packets for
        select 
        'S' as tipo,
        ''||sys_guid() AS guid, 
        n.*
    from
        tcd0n_polled_trns n
    where
        n.POLL_GUID = 'xyz';
    
    end ReadAndReserveTransactions_v6;
    
     /*SELECT distinct
                        tipo, CustomerId, data 
                    FROM
                    (
                        SELECT distinct
                            CASE WHEN conta = 1 THEN 'S' ELSE 'C' END AS tipo,
                            CustomerId, data 
                            FROM 
                        (
                        SELECT CustomerId, count(DISTINCT TransactionId) AS conta, min(data) AS data FROM
                            (
                            select distinct
                                v.TransactionId, v.data, v.customerId
                            from 
                                vcd0c_valid_trns v
                                join TCD0B_RP_SF_SEDE s
                                    on  s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES = v.TransactionId
                                    and s.CD0B_STAT_RECD_COD = 'A'
                                    AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K'))                        
                                    and s.cd0b_tipo_even_cod not in ( 'SUB', 'SCO')
                                    and s.cd0b_inse_dta < VCD0C_dta
                            union                        
                            SELECT distinct
                            s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS TransactionId, s.CD0B_RECD_NUM AS DATA, s.CDL0_CUST_COD AS CustomerId 
                            FROM TCD0B_RP_SF_SEDE s
                            WHERE s.CD0B_STAT_RECD_COD = 'A' 
                            AND NOT EXISTS (SELECT 1 FROM TCD0B_RP_SF_SEDE b WHERE s.CDL0_CUST_COD = b.CDL0_CUST_COD AND b.CD0B_STAT_RECD_COD IN ('Q', 'P', 'K'))                        
                            and s.cd0b_tipo_even_cod not in ( 'SUB', 'SCO')
                            --and s.cd0b_tipo_even_cod in( 'VGEN')
                            --and s.cd0b_inse_dta >= TO_DATE('2019/06/19 10', 'YYYY/MM/DD HH24')
                            --and CD0b_RAW_XID_DES||CD0b_PROG_TRNS_DES IN ('0008001901152806000','0001002D00F26CAB000','0006000A0102A344000','0001001F00F2657D000','00090022012B5065000','000A0028013FE037000','000A001C013F9CF3000','0007001A012B9044002','000A0028013FF0EA000','0001001400F27AFA000')
                            and is_valid_transaction(s.CD0B_RAW_XID_DES,s.CD0B_PROG_TRNS_DES) = '1'
                            and s.cd0b_inse_dta >= VCD0C_dta
                            MINUS
                            SELECT distinct
                            s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS TransactionId, s.CD0B_RECD_NUM AS DATA, s.CDL0_CUST_COD AS CustomerId 
                            FROM TCD0B_RP_SF_SEDE s JOIN TCD0A_RP_SF_CUSTOMER c ON s.CD0B_RAW_XID_DES = c.CD0A_RAW_XID_DES AND s.CD0B_PROG_TRNS_DES = c.CD0A_PROG_TRNS_DES
                            WHERE s.CDL1_PRIM_SEDC_FLG = 'S'
                            AND s.CD0B_STAT_RECD_COD <> c.CD0A_STAT_RECD_COD
                           -- MINUS
                          --  SELECT distinct
                          --  s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS TransactionId, s.CD0B_RECD_NUM AS DATA, s.CDL0_CUST_COD AS CustomerId 
                         --   FROM TCD0B_RP_SF_SEDE s 
                         --   WHERE s.CDL1_PRIM_SEDC_FLG = 'S'
                        --    AND NOT EXISTS ( SELECT 1 FROM TCD0A_RP_SF_CUSTOMER c WHERE c.CD0A_RAW_XID_DES = s.CD0B_RAW_XID_DES AND c.CD0A_PROG_TRNS_DES = s.CD0B_PROG_TRNS_DES AND s.CDL0_CUST_COD = c.CDL0_CUST_COD )
                            )
                        GROUP BY CustomerId 
                        )
                        ORDER BY data
                    )
                    WHERE ROWNUM <= 300*/
    
END SF_REPLICA;
/
