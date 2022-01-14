--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body MS_REP_SYS_CA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "DBACD8ESB"."MS_REP_SYS_CA" AS 

    procedure logga
    (
      p_message    IN       VARCHAR2,
      p_user       IN       VARCHAR2
    ) 
    is
    begin
        ms_rep_utils.logga(p_message,p_user);
    end logga;

    procedure aggiornaConfig
    (
        p_key varchar2,
        p_val varchar2
    )
    is
    begin 
        ms_rep_utils.aggiornaconfig(p_key, p_val);
    end aggiornaConfig;

    procedure pollTransactions_CA
    (
        p_transactions     OUT SYS_REFCURSOR
    )
    is 

        pollSize    number;
        mainSemFlg tms00_config.ms00_key_val%type;
        applSemFlg tms00_config.ms00_key_val%type;

        poll_dta    date;
        poll_guid   varchar2(32);
        contaN      number;
        missingParamException   EXCEPTION;
        missingParam tms00_config.ms00_key_val%type;
        updateRecordException   EXCEPTION;

    begin

        select sysdate, sys_guid() into poll_dta, poll_guid from dual;

        --logga('['|| poll_guid || '] Start pollTransactions_CA '|| poll_dta, 'pollTransactions_CA');

        select ms00_key_val into mainSemFlg from tms00_config where ms00_key_cod = 'CA_POLL_MAIN_SEM_FLG';

        if (mainSemFlg is null) then
        begin
            missingParam := 'CA_POLL_MAIN_SEM_FLG';
            raise missingParamException;
        end;
        end if;

        if ( mainSemFlg = '1' ) then
        begin

            select ms00_key_val into applSemFlg from tms00_config where ms00_key_cod = 'CA_POLL_APPL_SEM_FLG';

            if (applSemFlg is null) then
            begin
            missingParam := 'CA_POLL_APPL_SEM_FLG';
            raise missingParamException;
            end;
            end if;

            if ( applSemFlg = '0' ) then
            begin

                aggiornaConfig('CA_POLL_APPL_SEM_FLG','1');

                select ms00_key_val into pollSize from tms00_config where ms00_key_cod = 'CA_POLL_SIZE_NUM';
                if (pollSize is null) then
                begin
                  missingParam := 'CA_POLL_SIZE_NUM';
                  raise missingParamException;
                end;
                end if;

                select count(*) into contaN from tms10_esiti_ca where ms10_stat_cod = 'N';

                if ( contaN > 0 ) then
                begin
                    logga('['|| poll_guid || '] Ci sono '|| contaN || ' record su tms010_esiti_ca in stato N => modalita'' recupero.','pollTransactions_CA');

                    insert into tms12_polled_ca_trx (ms12_poll_guid, ms12_trx_cod, ms12_sede_cod, ms12_sede_prec_cod, ms12_poll_dta)
                    select 
                        poll_guid,
                        trx,
                        sede,
                        sedePrec,
                        dta
                    from
                    (
                        select distinct 
                            poll_guid,
                            a.ms10_raw_xid_des||a.ms10_prog_trns_des as trx,
                            a.ms10_sede_cod as sede,
                            a.ms10_sede_prec_cod as sedePrec,
                            a.ms10_cdb_ins_dta as dta,
                            dense_rank() over (order by a.ms10_cdb_ins_dta) as rank
                        from
                            tms10_esiti_ca a
                        where
                            a.ms10_stat_cod = 'N'
                    )
                    where rank <= pollSize;
                end;
                else
                begin
                    logga('['|| poll_guid || '] Inizio estrazione (size='||pollSize||') da tms010_esiti_ca in stato A => modalita'' standard.','pollTransactions_CA');

                    for r in 
                    (
                        with sk as
                        (
                            select distinct ms10_sede_cod as sede from tms10_esiti_ca where ms10_stat_cod in ('Q', 'P', 'K', 'D')
                        ),
                        spk as
                        (
                            select distinct ms10_sede_prec_cod as sedeprec from tms10_esiti_ca where ms10_stat_cod in ('Q', 'P', 'K', 'D') and ms10_sede_prec_cod is not null
                        ),
                        s as 
                        (
                            select 
                                s.ms10_raw_xid_des||s.ms10_prog_trns_des as trx, 
                                s.ms10_sede_cod as sede,
                                null as sedeprec,
                                min(s.ms10_cdb_ins_dta) as dta,
                                /*dense_rank() 
                               over (partition by s.ms10_sede_cod order by min(s.ms10_cdb_ins_dta)) as rank*/
                               s.ms10_raw_xid_des as rawXid,
                                s.ms10_prog_trns_des as progr,
                                dense_rank()
                               over (partition by s.ms10_raw_xid_des order by min(s.ms10_cdb_ins_dta),s.ms10_prog_trns_des ) as rank
                            from 
                                tms10_esiti_ca s
                            where
                                s.ms10_stat_cod = 'A' 
                                and s.ms10_sede_prec_cod is null
                            --group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, null
                            group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, null, s.ms10_raw_xid_des, s.ms10_prog_trns_des 
                            order by dta, rank, sede
                        ),
                        ssp as
                        (
                            select 
                                s.ms10_raw_xid_des||s.ms10_prog_trns_des as trx, 
                                s.ms10_sede_cod as sede,
                                s.ms10_sede_prec_cod as sedeprec,
                                min(s.ms10_cdb_ins_dta) as dta,
                                /*dense_rank() 
                               over (partition by s.ms10_sede_cod order by min(s.ms10_cdb_ins_dta)) as rank*/
                               s.ms10_raw_xid_des as rawXid,
                                s.ms10_prog_trns_des as progr,
                                dense_rank() 
                               over (partition by s.ms10_raw_xid_des order by min(s.ms10_cdb_ins_dta),s.ms10_prog_trns_des) as rank
                            from 
                                tms10_esiti_ca s
                            where
                                s.ms10_stat_cod = 'A' 
                                and s.ms10_sede_prec_cod is not null
                            --group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, s.ms10_sede_prec_cod
                            group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, s.ms10_sede_prec_cod, s.ms10_raw_xid_des, s.ms10_prog_trns_des
                            order by dta, rank, sede
                        )
                        select distinct
                        	trx, sede, sedePrec, dta, rank from (
                        select distinct
                        	trx, sede, sedePrec, dta,
                        	dense_rank() over (order by dta) as rank
                        from 
                        (
	                        select 
	                            trx, sede, sedePrec, dta, rank
	                        from s
	                        where 
	                            rank = 1 
	                            and not exists ( select 1 from sk a where a.sede = s.sede )
	                            and not exists ( select 1 from spk a where a.sedeprec = s.sede )	
	                            and not exists ( select 1 from ssp a where a.rank = 1 and a.sede = s.sede and a.dta <= s.dta )
	                            and not exists ( select 1 from ssp a where a.rank = 1 and a.sedeprec = s.sede and a.dta <= s.dta )
	                        union
	                        select 
	                            trx, sede, sedePrec, dta, rank 
	                        from ssp
	                        where
	                            rank = 1
	                            and not exists ( select 1 from sk a where a.sede = ssp.sede )
	                            and not exists ( select 1 from spk a where a.sedeprec = ssp.sede )
                                and not exists ( select 1 from s a where a.sede = ssp.sede and a.dta < ssp.dta )
                                and not exists ( select 1 from s a where a.sede = ssp.sedePrec and a.dta < ssp.dta )
						) ) x
						where rank <= pollSize
                    )
                    loop

                        --logga('['|| poll_guid || '] Analisi trx ' || r.trx || ' (sede ' || r.sede || ', sedePrec ' || r.sedePrec || ', dta ' || r.dta ||')', 'pollTransactions_CA');
                        insert into
                            tms12_polled_ca_trx (ms12_poll_guid, ms12_trx_cod, ms12_sede_cod, ms12_sede_prec_cod, ms12_poll_dta)
                        values
                        (
                            poll_guid,
                            r.trx,
                            r.sede,
                            r.sedePrec,
                            r.dta
                        );

                        DECLARE
                          P_RESULT VARCHAR2(200);
                          P_ERROR_COD VARCHAR2(200);
                          P_ERROR_DES VARCHAR2(200);

                        BEGIN
                          P_RESULT := NULL;
                          P_ERROR_COD := NULL;
                          P_ERROR_DES := NULL;

                          MS_REP_UTILS.aggiornaStato (  
                          'CA',
                          r.trx,
                          'P',
                          'ORACLE',
                          null,
                          null,
                          null,
                          0,
                            P_RESULT => P_RESULT,
                            P_ERROR_COD => P_ERROR_COD,
                            P_ERROR_DES => P_ERROR_DES) ;  

                        if ( P_RESULT != 1 ) then
                        begin
                            logga('['|| poll_guid || '] Errore in aggiornamento trx ' || r.trx || ' (sede ' || r.sede || ', sedePrec ' || r.sedePrec || ', dta ' || r.dta || ')', 'pollTransactions_CA');
                            raise updateRecordException;
                        end;
                        end if;

                        END;

                    end loop;
                end;
                end if; --if ( contaN > 0 )

                open p_transactions for
                    SELECT distinct 
                        trx,
                        ''||raw_xid_des as raw_xid_des,
                        prog_trns_des,
                        cod_customer,
                        cod_sede,
                        cod_vers_sede,
                        cod_stato_customer,
                        cod_stato_sede,
                        cod_sede_prec,
                        cod_vers_sede_prec,
                        ragione_sociale,
                        cognome,
                        nome,
                        partita_iva,
                        codice_fiscale,
                        cod_nazione_customer,
                        nazione_customer,
                        cod_categoria_customer,
                        categoria_customer,
                        url_primaria_customer,
                        sede_primaria_customer,
                        insegna_sede,
                        dug_sede,
                        toponimo_sede,
                        civico_sede,
                        info_agg_sede,
                        cap_sede,
                        cod_localita_sede,
                        localita_sede,
                        cod_frazione_sede,
                        frazione_sede,
                        provincia_sede,
                        cod_nazione_sede,
                        nazione_sede,
                        cod_prof_coord_sede,
                        coord_sede,
                        coord_x_sede,
                        coord_y_sede,
                        opec,
                        cod_categoria_sede,
                        categoria_sede,
                        pollDta,
                        pollGuid,
                        data_ins_cdb
                    FROM
                        vms12_polled_trx_ca
                    where
                        pollGuid = poll_guid
                    order by poll_dta;

                    delete 
                        from tms12_polled_ca_trx
                    where
                        ms12_poll_guid = poll_guid;

                    logga('['|| poll_guid || '] Fine estrazione (size='||pollSize||', estratti:'|| TO_CHAR(SQL%ROWCOUNT) ||')','pollTransactions_CA');

                    commit;

                    aggiornaConfig('CA_POLL_APPL_SEM_FLG','0');

            end;
            else
            begin
                logga('['|| poll_guid || '] applSemFlg: '||applSemFlg,'pollTransactions_CA');
                open p_transactions for
                    select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                    from vms12_polled_trx_ca
                    where 1 = 0;
            end;
            end if; --if ( applSemFlg = '0' )

        end;
        else
        begin
            logga('['|| poll_guid || '] mainSemFlg: '||mainSemFlg,'pollTransactions_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;
        end;
        end if; --if ( mainSemFlg = '1' )

        EXCEPTION
        when missingParamException then
            rollback;
            logga('['|| poll_guid || '] Errore in pollTransactions_CA. Parametro mancante in TMS00_CONFIG: '||missingParam,'replicaTrxCDB_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;

        when updateRecordException then
            rollback;
            logga('['|| poll_guid || '] Errore in pollTransactions_CA. (Aggiornamento record)','replicaTrxCDB_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;

        when others then
            rollback;
            logga('['|| poll_guid || '] Errore generico in pollTransactions_CA: '|| SQLCODE || ', ' || SQLERRM,'replicaTrxCDB_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;

    end pollTransactions_CA;

    procedure pollTransactions_CA_v2
    (
        p_transactions     OUT SYS_REFCURSOR,
        p_telefoni         OUT SYS_REFCURSOR
    )
    is 

        pollSize    number;
        mainSemFlg tms00_config.ms00_key_val%type;
        applSemFlg tms00_config.ms00_key_val%type;

        poll_dta    date;
        poll_guid   varchar2(32);
        contaN      number;
        missingParamException   EXCEPTION;
        missingParam tms00_config.ms00_key_val%type;
        updateRecordException   EXCEPTION;
        numEsecuzioniV   varchar2(10);
        numEsecuzioniN   NUMBER;

    begin

        select sysdate, sys_guid() into poll_dta, poll_guid from dual;

        --logga('['|| poll_guid || '] Start pollTransactions_CA '|| poll_dta, 'pollTransactions_CA');

        select ms00_key_val into numEsecuzioniV from tms00_config where ms00_key_cod = 'CA_POLL_COUNTER';

        if ( numEsecuzioniV is not null ) then
        begin
            select to_number(numEsecuzioniV) into numEsecuzioniN from dual;
        end;
        else
        begin
            numEsecuzioniN := 0;
        end;
        end if;

        select ms00_key_val into mainSemFlg from tms00_config where ms00_key_cod = 'CA_POLL_MAIN_SEM_FLG';

        if (mainSemFlg is null) then
        begin
            missingParam := 'CA_POLL_MAIN_SEM_FLG';
            raise missingParamException;
        end;
        end if;

        if ( mainSemFlg = '1' ) then
        begin

            select ms00_key_val into applSemFlg from tms00_config where ms00_key_cod = 'CA_POLL_APPL_SEM_FLG';

            if (applSemFlg is null) then
            begin
            missingParam := 'CA_POLL_APPL_SEM_FLG';
            raise missingParamException;
            end;
            end if;

            if ( applSemFlg = '0' ) then
            begin

                aggiornaConfig('CA_POLL_APPL_SEM_FLG','1');

                select ms00_key_val into pollSize from tms00_config where ms00_key_cod = 'CA_POLL_SIZE_NUM';
                if (pollSize is null) then
                begin
                  missingParam := 'CA_POLL_SIZE_NUM';
                  raise missingParamException;
                end;
                end if;

                select count(*) into contaN from tms10_esiti_ca where ms10_stat_cod = 'N';

                if ( contaN > 0 ) then
                begin
                    logga('['|| poll_guid || '] Ci sono '|| contaN || ' record su tms010_esiti_ca in stato N => modalita'' recupero.','pollTransactions_CA');

                    insert into tms12_polled_ca_trx (ms12_poll_guid, ms12_trx_cod, ms12_sede_cod, ms12_sede_prec_cod, ms12_poll_dta)
                    select 
                        poll_guid,
                        trx,
                        sede,
                        sedePrec,
                        dta
                    from
                    (
                        select distinct 
                            poll_guid,
                            a.ms10_raw_xid_des||a.ms10_prog_trns_des as trx,
                            a.ms10_sede_cod as sede,
                            a.ms10_sede_prec_cod as sedePrec,
                            a.ms10_cdb_ins_dta as dta,
                            dense_rank() over (order by a.ms10_cdb_ins_dta) as rank
                        from
                            tms10_esiti_ca a
                        where
                            a.ms10_stat_cod = 'N'
                    )
                    where rank <= pollSize;
                end;
                else
                begin
                    logga('['|| poll_guid || '] Inizio estrazione (size='||pollSize||') da tms010_esiti_ca in stato A => modalita'' standard.','pollTransactions_CA');

                    for r in 
                    (
                        with sk as
                        (
                            select distinct ms10_sede_cod as sede from tms10_esiti_ca where ms10_stat_cod in ('Q', 'P', 'K', 'D', 'R')
                        ),
                        spk as
                        (
                            select distinct ms10_sede_prec_cod as sedeprec from tms10_esiti_ca where ms10_stat_cod in ('Q', 'P', 'K', 'D', 'R') and ms10_sede_prec_cod is not null
                        ),
                        s as 
                        (
                            select 
                                s.ms10_raw_xid_des||s.ms10_prog_trns_des as trx, 
                                s.ms10_sede_cod as sede,
                                null as sedeprec,
                                min(s.ms10_cdb_ins_dta) as dta,
                                /*dense_rank() 
                               over (partition by s.ms10_sede_cod order by min(s.ms10_cdb_ins_dta)) as rank*/
                               s.ms10_raw_xid_des as rawXid,
                                s.ms10_prog_trns_des as progr,
                                dense_rank()
                               --over (partition by s.ms10_raw_xid_des order by min(s.ms10_cdb_ins_dta),s.ms10_prog_trns_des ) as rank
                               over (partition by s.ms10_sede_cod order by min(c.CD0V_RECD_NUM), min(s.ms10_cdb_ins_dta),s.ms10_prog_trns_des ) as rank
                            from 
                                tms10_esiti_ca s JOIN TCD0V_RP_GEN_SEDE c ON s.ms10_raw_xid_des||s.ms10_prog_trns_des = c.CD0V_RAW_XID_DES ||c.CD0V_PROG_TRNS_DES
                            where
                                s.ms10_stat_cod = 'A' 
                                and s.ms10_sede_prec_cod is null
                            --group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, null
                            group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, null, s.ms10_raw_xid_des, s.ms10_prog_trns_des, c.CD0V_RECD_NUM
                            order by dta, rank, sede
                        ),
                        ssp as
                        (
                            select 
                                s.ms10_raw_xid_des||s.ms10_prog_trns_des as trx, 
                                s.ms10_sede_cod as sede,
                                s.ms10_sede_prec_cod as sedeprec,
                                min(s.ms10_cdb_ins_dta) as dta,
                                /*dense_rank() 
                               over (partition by s.ms10_sede_cod order by min(s.ms10_cdb_ins_dta)) as rank*/
                               s.ms10_raw_xid_des as rawXid,
                                s.ms10_prog_trns_des as progr,
                                dense_rank() 
                               --over (partition by s.ms10_raw_xid_des order by min(s.ms10_cdb_ins_dta),s.ms10_prog_trns_des) as rank
                               over (partition by s.ms10_sede_cod order by min(c.CD0V_RECD_NUM), min(s.ms10_cdb_ins_dta),s.ms10_prog_trns_des) as rank
                            from 
                                tms10_esiti_ca s JOIN TCD0V_RP_GEN_SEDE c ON s.ms10_raw_xid_des||s.ms10_prog_trns_des = c.CD0V_RAW_XID_DES ||c.CD0V_PROG_TRNS_DES
                            where
                                s.ms10_stat_cod = 'A' 
                                and s.ms10_sede_prec_cod is not null
                                --and s.ms10_raw_xid_des||s.ms10_prog_trns_des = '00040022012DD02000001'
                            --group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, s.ms10_sede_prec_cod
                            group by s.ms10_raw_xid_des||s.ms10_prog_trns_des, s.ms10_sede_cod, s.ms10_sede_prec_cod, s.ms10_raw_xid_des, s.ms10_prog_trns_des, c.CD0V_RECD_NUM
                            order by dta, rank, sede
                        )
                        select distinct
                        	trx, sede, sedePrec, dta, rank from (
                        select distinct
                        	trx, sede, sedePrec, dta,
                        	dense_rank() over (order by dta) as rank
                        from 
                        (
	                        select 
	                            trx, sede, sedePrec, dta, rank
	                        from s
	                        where 
	                            rank = 1 
	                            and not exists ( select 1 from sk a where a.sede = s.sede )
	                            and not exists ( select 1 from spk a where a.sedeprec is not null and a.sedeprec = s.sede )	
	                            and not exists ( select 1 from ssp a where /*a.rank = 1 and*/ a.sede = s.sede and a.dta <= s.dta )
	                            and not exists ( select 1 from ssp a where a.sedeprec is not null /*a.rank = 1 and*/ and a.sedeprec = s.sede and a.dta <= s.dta )
	                        union
	                        select 
	                            trx, sede, sedePrec, dta, rank 
	                        from ssp
	                        where
	                            rank = 1
	                            and not exists ( select 1 from sk a where a.sede = ssp.sede )
	                            and not exists ( select 1 from spk a where a.sedeprec is not null and a.sedeprec = ssp.sede )
                                and not exists ( select 1 from s a where a.sede = ssp.sede and a.dta < ssp.dta )
                                and not exists ( select 1 from s a where a.sede = ssp.sedePrec and a.dta < ssp.dta )
						) ) x
						where rank <= pollSize
                        order by rank
                    )
                    loop

                        --logga('['|| poll_guid || '] Analisi trx ' || r.trx || ' (sede ' || r.sede || ', sedePrec ' || r.sedePrec || ', dta ' || r.dta ||')', 'pollTransactions_CA');
                        insert into
                            tms12_polled_ca_trx (ms12_poll_guid, ms12_trx_cod, ms12_sede_cod, ms12_sede_prec_cod, ms12_poll_dta)
                        values
                        (
                            poll_guid,
                            r.trx,
                            r.sede,
                            r.sedePrec,
                            r.dta
                        );

                        DECLARE
                          P_RESULT VARCHAR2(200);
                          P_ERROR_COD VARCHAR2(200);
                          P_ERROR_DES VARCHAR2(200);

                        BEGIN
                          P_RESULT := NULL;
                          P_ERROR_COD := NULL;
                          P_ERROR_DES := NULL;

                          MS_REP_UTILS.aggiornaStato (  
                          'CA',
                          r.trx,
                          'P',
                          'ORACLE',
                          null,
                          null,
                          null,
                          0,
                            P_RESULT => P_RESULT,
                            P_ERROR_COD => P_ERROR_COD,
                            P_ERROR_DES => P_ERROR_DES) ;  

                        if ( P_RESULT != 1 ) then
                        begin
                            logga('['|| poll_guid || '] Errore in aggiornamento trx ' || r.trx || ' (sede ' || r.sede || ', sedePrec ' || r.sedePrec || ', dta ' || r.dta || ')', 'pollTransactions_CA');
                            raise updateRecordException;
                        end;
                        end if;

                        END;

                    end loop;
                end;
                end if; --if ( contaN > 0 )

                open p_transactions for
                    SELECT distinct 
                        trx,
                        ''||raw_xid_des as raw_xid_des,
                        prog_trns_des,
                        cod_customer,
                        cod_sede,
                        cod_vers_sede,
                        cod_stato_customer,
                        cod_stato_sede,
                        cod_sede_prec,
                        cod_vers_sede_prec,
                        ragione_sociale,
                        cognome,
                        nome,
                        partita_iva,
                        codice_fiscale,
                        cod_nazione_customer,
                        nazione_customer,
                        cod_categoria_customer,
                        categoria_customer,
                        url_primaria_customer,
                        sede_primaria_customer,
                        insegna_sede,
                        dug_sede,
                        toponimo_sede,
                        civico_sede,
                        info_agg_sede,
                        cap_sede,
                        cod_localita_sede,
                        localita_sede,
                        cod_frazione_sede,
                        frazione_sede,
                        provincia_sede,
                        cod_nazione_sede,
                        nazione_sede,
                        cod_prof_coord_sede,
                        coord_sede,
                        coord_x_sede,
                        coord_y_sede,
                        opec,
                        cod_categoria_sede,
                        categoria_sede,
                        pollDta,
                        pollGuid,
                        data_ins_cdb
                    FROM
                        vms12_polled_trx_ca
                    where
                        pollGuid = poll_guid
                    order by poll_dta;

                    open p_telefoni for
                        SELECT distinct
                            trx,
                            rawxid,
                            codicecustomer,
                            codicesede,
                            teleid,
                            prefint,
                            prefnaz,
                            telenum,
                            statotel,
                            cellflag,
                            faxflag,
                            telprimariosede,
                            telprimariocustomer,
                            indpotn,
                            whtsflag,
                            relsflag,
                            consensoflag,
                            polldta,
                            pollguid
                        FROM
                            vms12_polled_tel_ca
                        where
                            pollGuid = poll_guid
                        order by poll_dta;

                    delete 
                        from tms12_polled_ca_trx
                    where
                        ms12_poll_guid = poll_guid;

                    logga('['|| poll_guid || '] Fine estrazione (size='||pollSize||', estratti:'|| TO_CHAR(SQL%ROWCOUNT) ||')','pollTransactions_CA');

                    aggiornaConfig('CA_POLL_APPL_SEM_FLG','0');

                    commit;                                

            end;
            else
            begin
                logga('['|| poll_guid || '] applSemFlg: '||applSemFlg,'pollTransactions_CA');
                open p_transactions for
                    select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                    from vms12_polled_trx_ca
                    where 1 = 0;
                open p_telefoni for
                    select trx, rawxid, codicecustomer, codicesede, teleid, prefint, prefnaz, telenum, statotel, cellflag, faxflag, telprimariosede, telprimariocustomer, indpotn, whtsflag, relsflag, consensoflag, polldta, pollguid
                    from vms12_polled_tel_ca
                    where 1 = 0;
            end;
            end if; --if ( applSemFlg = '0' )

        end;
        else
        begin
            logga('['|| poll_guid || '] mainSemFlg: '||mainSemFlg,'pollTransactions_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;
            open p_telefoni for
                    select trx, rawxid, codicecustomer, codicesede, teleid, prefint, prefnaz, telenum, statotel, cellflag, faxflag, telprimariosede, telprimariocustomer, indpotn, whtsflag, relsflag, consensoflag, polldta, pollguid
                    from vms12_polled_tel_ca
                    where 1 = 0;
        end;
        end if; --if ( mainSemFlg = '1' )

        numEsecuzioniN := numEsecuzioniN +1;

        aggiornaConfig('CA_POLL_COUNTER', ''||numEsecuzioniN);

        if ( numEsecuzioniN >= 5 ) then
        begin
            ms_rep_utils.cleanlogs();
            ms_rep_utils.cleanData();
            aggiornaConfig('CA_POLL_COUNTER', '0');
        end;
        end if;

        EXCEPTION
        when missingParamException then
            rollback;
            logga('['|| poll_guid || '] Errore in pollTransactions_CA. Parametro mancante in TMS00_CONFIG: '||missingParam,'pollTransactions_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;
            open p_telefoni for
                    select trx, rawxid, codicecustomer, codicesede, teleid, prefint, prefnaz, telenum, statotel, cellflag, faxflag, telprimariosede, telprimariocustomer, indpotn, whtsflag, relsflag, consensoflag, polldta, pollguid
                    from vms12_polled_tel_ca
                    where 1 = 0;

        when updateRecordException then
            rollback;
            logga('['|| poll_guid || '] Errore in pollTransactions_CA. (Aggiornamento record)','pollTransactions_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;
            open p_telefoni for
                    select trx, rawxid, codicecustomer, codicesede, teleid, prefint, prefnaz, telenum, statotel, cellflag, faxflag, telprimariosede, telprimariocustomer, indpotn, whtsflag, relsflag, consensoflag, polldta, pollguid
                    from vms12_polled_tel_ca
                    where 1 = 0;

        when others then
            rollback;
            logga('['|| poll_guid || '] Errore generico in pollTransactions_CA: '|| SQLCODE || ', ' || SQLERRM,'pollTransactions_CA');
            open p_transactions for
                select trx, ''||raw_xid_des as raw_xid_des, prog_trns_des, cod_customer, cod_sede, cod_vers_sede, cod_stato_customer, cod_stato_sede, cod_sede_prec, cod_vers_sede_prec, ragione_sociale, cognome, nome, partita_iva, codice_fiscale, cod_nazione_customer, nazione_customer, cod_categoria_customer, categoria_customer, url_primaria_customer, sede_primaria_customer, insegna_sede, dug_sede, toponimo_sede, civico_sede, info_agg_sede, cap_sede, cod_localita_sede, localita_sede, cod_frazione_sede, frazione_sede, provincia_sede, cod_nazione_sede, nazione_sede, cod_prof_coord_sede, coord_sede, coord_x_sede, coord_y_sede, opec, cod_categoria_sede, categoria_sede, polldta, pollguid
                from vms12_polled_trx_ca
                where 1 = 0;
            open p_telefoni for
                    select trx, rawxid, codicecustomer, codicesede, teleid, prefint, prefnaz, telenum, statotel, cellflag, faxflag, telprimariosede, telprimariocustomer, indpotn, whtsflag, relsflag, consensoflag, polldta, pollguid
                    from vms12_polled_tel_ca
                    where 1 = 0;

    end pollTransactions_CA_v2;

    procedure readTelefoni_CA
    (
        p_raw_xid_des   in  varchar2,
        p_sede_cod      in  tcd0u_rp_gen_telefono.cdl1_sede_cod%type,
        p_result        out SYS_REFCURSOR
    )
    is
    begin

    if ( p_raw_xid_des is not null and p_sede_cod is not null ) then
    begin
        open p_result for
        select 
            ''||cd0u_raw_xid_des as rawXid, 
            cdl0_cust_cod as codiceCustomer, 
            cdl1_sede_cod as codiceSede, 
            cdl4_tele_num as teleId, 
            cdl4_pref_intn_cod as prefInt, 
            cdl4_pref_nazn_cod as prefNaz, 
            cdl4_telf_numr_cod as teleNum, 
            case cdmc_stat_rels_flg
                when '0' then 'N'
                else cdlt_con1_tfis_flg
            end as statoTel, 
            cdl4_cell_flg as cellFlag, 
            cdl4_fax_flg as faxFlag, 
            cdmc_prim_tels_flg as telPrimarioSede, 
            cdmc_prim_telc_flg as telPrimarioCustomer, 
            cdmc_ind_potn_num as indPotn, 
            cdl4_whatsapp_flg as whtsFlag,
            cdmc_stat_rels_flg as relsFlag,
            cdlt_con1_tfis_flg as consensoFlag
        from 
            tcd0u_rp_gen_telefono
        where
            cd0u_raw_xid_des = p_raw_xid_des
            and cdl1_sede_cod = p_sede_cod;
    end;
    else
    begin 
        open p_result for
        select 
            ''||cd0u_raw_xid_des as rawXid, 
            cdl0_cust_cod as codiceCustomer, 
            cdl1_sede_cod as codiceSede, 
            cdl4_tele_num as teleId, 
            cdl4_pref_intn_cod as prefInt, 
            cdl4_pref_nazn_cod as prefNaz, 
            cdl4_telf_numr_cod as teleNum, 
            case cdmc_stat_rels_flg
                when '0' then 'N'
                else cdlt_con1_tfis_flg
            end as statoTel, 
            cdl4_cell_flg as cellFlag, 
            cdl4_fax_flg as faxFlag, 
            cdmc_prim_tels_flg as telPrimarioSede, 
            cdmc_prim_telc_flg as telPrimarioCustomer, 
            cdmc_ind_potn_num as indPotn, 
            cdl4_whatsapp_flg as whtsFlag,
            cdmc_stat_rels_flg as relsFlag,
            cdlt_con1_tfis_flg as consensoFlag
        from 
            tcd0u_rp_gen_telefono
        where 1 = 0;
    end;
    end if;

    end readTelefoni_CA;

END MS_REP_SYS_CA;
--------------------------------------------------------
--  DDL for Package Body MS_REP_UTILS
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "DBACD8ESB"."MS_REP_UTILS" AS

  PROCEDURE logga
    (
      p_message    IN       VARCHAR2,
      p_user       IN       VARCHAR2
    ) 
    is
        PRAGMA AUTONOMOUS_TRANSACTION;

  BEGIN

    INSERT INTO tms01_log(MS01_LOG_MSG, MS01_USER_COD)
    VALUES(p_message, p_user);

    COMMIT;

  END logga;

  procedure aggiornaStatoMulti_v2
  (
    p_transactions      IN  aggStatoTrx_table,
    p_results           OUT SYS_REFCURSOR
  ) 
  is
    p_result            char(1);
    p_error_cod         VARCHAR2(50);
    p_error_des         VARCHAR2(500);
    p_result_agg        result_agg;
    p_result_agg_table  result_agg_table;
    conta               number;

  begin

    p_result_agg_table := new result_agg_table();

    if ( p_transactions is not null and p_transactions.count > 0 ) then
    begin

        conta := 1;

        --logga('START aggiornamento di ' || p_transactions.count ||' trx ','aggStatoMulti_v2');

        p_result_agg_table.extend(p_transactions.count);

        for r in 
        (
            select 
                p_sys,          
                p_trx,          
                p_status,       
                p_user,         
                p_reply_trx_id, 
                p_mule_trx_id,  
                p_msg,          
                p_force_flg,
                status_to_number(p_status)
            from
                TABLE(p_transactions)
            order by status_to_number(p_status)
        )
        loop

            aggiornaStato (  
                          r.p_sys,
                          r.p_trx,
                          r.p_status,
                          r.p_user,
                          r.p_reply_trx_id,
                          r.p_mule_trx_id,
                          r.p_msg,
                          r.p_force_flg,
                            P_RESULT => P_RESULT,
                            P_ERROR_COD => P_ERROR_COD,
                            P_ERROR_DES => P_ERROR_DES) ;

            --logga('Aggiornamento trx ' || r.p_trx ||' in stato '|| r.p_status,'aggStatoMulti_v2');

            select result_agg(r.p_trx, P_RESULT, P_ERROR_COD, P_ERROR_DES) into p_result_agg from dual;

            --p_result_agg_table.extend(1);
            p_result_agg_table(conta) := p_result_agg;

            conta := conta +1;

            logga('Aggiornamento trx ' || r.p_trx ||' in stato '|| r.p_status ||': ' || p_result,'aggStatoMulti_v2');

        end loop;

        open p_results for select * from table(p_result_agg_table);

        commit;

    end;
    else
    begin
            select 
                result_agg(null, '0', 'EMPTY_TRX_PARAM', 'Il parametro p_transactions in INPUT e'' nullo.')
            bulk collect into p_result_agg_table from dual;

            open p_results for select * from table(p_result_agg_table);
    end;
    end if;

    EXCEPTION

    WHEN OTHERS THEN 
        P_ERROR_DES := 'Errore generico in aggiornaStatoMulti_v2: '|| SQLCODE || ', ' || SQLERRM;
        logga(P_ERROR_DES,'aggStatoMulti_v2');
        select 
                result_agg(null, '0', 'GENERIC_ERROR', P_ERROR_DES)
            bulk collect into p_result_agg_table from dual;

        open p_results for select * from table(p_result_agg_table);
        rollback;

  end aggiornaStatoMulti_v2;

  procedure aggiornaStatoMulti
  (
    p_transactions  IN aggStatoTrx_table,
    p_result        OUT VARCHAR2,
    p_error_cod     OUT VARCHAR2,
    p_error_des     OUT VARCHAR2
  )
  is
    aggiornaStatoException exception;
  begin
    if ( p_transactions is not null ) then
    begin
        for r in 
        (
            select 
                p_sys,          
                p_trx,          
                p_status,       
                p_user,         
                p_reply_trx_id, 
                p_mule_trx_id,  
                p_msg,          
                p_force_flg    
            from
                TABLE(p_transactions)
        )
        loop
            --logga('Aggiornamento trx ' || r.p_trx ,'aggStatoMulti');
            aggiornaStato (  
                          r.p_sys,
                          r.p_trx,
                          r.p_status,
                          r.p_user,
                          r.p_reply_trx_id,
                          r.p_mule_trx_id,
                          r.p_msg,
                          r.p_force_flg,
                            P_RESULT => P_RESULT,
                            P_ERROR_COD => P_ERROR_COD,
                            P_ERROR_DES => P_ERROR_DES) ; 

            if ( p_result = 0 or p_result = '0' ) then
                raise aggiornaStatoException;
            end if;
        end loop;

        p_result := 1;
        p_error_cod := 'OK';
        p_error_des := 'Completato con successo';

        --commit;

    end;
    else
    begin
        p_result := 0;
        p_error_cod := 'EMPTY_TRX_PARAM';
        p_error_des := 'Il parametro p_transactions in INPUT e'' nullo.';
    end;
    end if;

    EXCEPTION

    WHEN aggiornaStatoException THEN
        logga('Errore in aggiornaStatoMulti: '||p_error_des,'aggStatoMulti');
        p_result := 0;
        --rollback;

    WHEN OTHERS THEN 
        logga('Errore generico in aggiornaStatoMulti: '|| SQLCODE || ', ' || SQLERRM,'aggStatoMulti');
        p_error_cod := 'GENERIC_ERROR';
        p_error_des := 'Errore generico in aggiornaStato: '|| SQLCODE || ', ' || SQLERRM;
        p_result := 0;
        --rollback;

  end aggiornaStatoMulti;

  procedure aggiornaStato
  (
    p_sys           IN  VARCHAR2,
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
  )
  IS
    invalidStatusException  EXCEPTION;
    trxNotFoundException    EXCEPTION;
    sysNotFoundException    EXCEPTION;
    ignoreUpdateException   EXCEPTION;
    currentStatus tms10_esiti_ca.ms10_stat_cod%TYPE;
    conta                   NUMBER;
  begin

    p_result := 0;

    --logga('aggiornaStato -> [p_sys: '|| p_sys ||', p_trx: '|| p_trx ||', p_status: '|| p_status ||', p_user: '|| p_user ||', p_force_flg: '|| p_force_flg ||']','aggiornaStato');

    if ( p_sys is not null and p_trx is not null and p_status is not null and p_user is not null ) then
    begin
        if ( p_sys = 'CA' ) then
        begin

            select count(*) into conta from tms10_esiti_ca where MS10_RAW_XID_DES||MS10_PROG_TRNS_DES = p_trx and MS10_SYS_COD = 'CA';

            IF ( conta = 0 ) then
                raise trxNotFoundException;
            end if;

            select MS10_STAT_COD into currentStatus from tms10_esiti_ca where MS10_RAW_XID_DES||MS10_PROG_TRNS_DES = p_trx and MS10_SYS_COD = 'CA';

            if ( currentStatus <> p_status ) then
                begin

                if ( p_force_flg = 0 ) then
                begin

                    if ( currentStatus = 'A' and p_status <> 'P' ) then
                        raise invalidStatusException;
                    end if;

                    if ( currentStatus = 'D' and p_status <> 'A' ) then
                        raise invalidStatusException;
                    end if;

                    if ( currentStatus = 'P' and p_status <> 'Q' ) then
                        raise invalidStatusException;
                    end if;

                    if ( currentStatus = 'Q' and p_status not in ('R','S','N') ) then
                        raise invalidStatusException;
                    end if;

                    if ( currentStatus = 'N' and p_status not in ('Q') ) then
                        raise invalidStatusException;
                    end if;

                    if ( currentStatus = 'R' and p_status not in ('S','N','O','K') ) then
                        raise invalidStatusException;
                    end if;

                    if ( currentStatus = 'S' and p_status not in ('O','K') ) then
                        raise invalidStatusException;
                    end if;
                    
                    if ( currentStatus in ( 'O', 'K') ) then
                        raise invalidStatusException;
                    end if;

                end;
                end if;

            end;
            else 
            begin 
                raise ignoreUpdateException;
            end;
            end if;

            insert into 
                TMS11_STORICO_ESITI_CA (
                    MS11_SYS_COD,
                    MS11_RAW_XID_DES,
                    MS11_PROG_TRNS_DES,
                    MS11_OLD_STAT_COD,
                    MS11_NEW_STAT_COD,
                    MS11_USER_COD,
                    MS11_REPLY_TRX_ID,
                    MS11_MULE_TRX_ID,
                    MS11_FORCE_FLG
                    )
            select
                'CA',
                substr(p_trx, 0, 16), 
                substr(p_trx, 17),
                /*substr(p_trx, 0, 10), 
                substr(p_trx, 11),*/
                currentStatus,
                p_status,
                p_user,
                p_reply_trx_id,
                p_mule_trx_id,
                p_force_flg
            from 
                dual;

            if ( p_status = 'P' ) then
            begin
                update 
                    tms10_esiti_ca 
                set
                    MS10_STAT_COD     = p_status,
                    MS10_REPLY_TRX_ID = p_reply_trx_id,
                    MS10_MSG_DES      = p_msg,
                    MS10_POLL_DTA     = sysdate,
                    MS10_UPD_DTA      = sysdate,
                    MS10_UPD_USER_COD = p_user,
                    MS10_MULE_TRX_ID  = p_mule_trx_id
                where
                   MS10_RAW_XID_DES||MS10_PROG_TRNS_DES = p_trx;
            end;
            else
            begin
                update 
                    tms10_esiti_ca 
                set
                    MS10_STAT_COD     = p_status,
                    MS10_REPLY_TRX_ID = p_reply_trx_id,
                    MS10_MSG_DES      = p_msg,
                    MS10_UPD_DTA      = sysdate,
                    MS10_UPD_USER_COD = p_user,
                    MS10_MULE_TRX_ID  = p_mule_trx_id
                where
                   MS10_RAW_XID_DES||MS10_PROG_TRNS_DES = p_trx;
            end;
            end if;        

            p_result := 1;
            --commit;

        end;
        else
        begin
            raise sysNotFoundException;
        end;
        end if; 
    end;
    else
    begin 
        p_result := 0;
        logga('Ricevuti dati insufficienti per aggiornamento stato. [p_sys: '|| p_sys ||', p_trx: '|| p_trx ||', p_status: '|| p_status ||', p_user: '|| p_user ||']','aggiornaStato');
    end;
    end if;

    EXCEPTION

        WHEN ignoreUpdateException THEN
        logga('[Trx ' || p_trx ||'] Sistema '|| p_sys || ' aggiornamento in '|| p_status ||' non effettuato; lo stato corrente e'' '|| currentStatus ||'.','aggiornaStato');
        p_error_cod := 'WARN_UPDATE_IGNORED';
        p_error_des := '[Trx ' || p_trx ||'] Sistema '|| p_sys || ' aggiornamento in '|| p_status ||' non effettuato; lo stato corrente e'' '|| currentStatus ||'.';
        p_result := 1;

        WHEN sysNotFoundException THEN
        logga('[Trx ' || p_trx ||'] Sistema '|| p_sys || ' non riconosciuto.','aggiornaStato');
        p_error_cod := 'INVALID_SYS';
        p_error_des := '[Trx ' || p_trx ||'] Sistema '|| p_sys || ' non riconosciuto.';
        p_result := 0;
        --rollback;

        WHEN trxNotFoundException THEN
        logga('Trx '|| p_trx || ' non trovata.','aggiornaStato');
        p_error_cod := 'TRX_NOT_FOUND';
        p_error_des := 'Trx '|| p_trx || ' non trovata.';
        p_result := 0;
        --rollback;

        WHEN invalidStatusException THEN
        logga('[Trx ' || p_trx ||'] Stato '|| p_status || ' non valido (stato corrente: '||currentStatus||')','aggiornaStato');
        p_error_cod := 'INVALID_STATUS';
        p_error_des := '[Trx ' || p_trx ||'] Stato '|| p_status || ' non valido (stato corrente: '||currentStatus||')';
        p_result := 0;
        --rollback;

        WHEN OTHERS THEN 
        logga('Errore generico in aggiornaStato: '|| SQLCODE || ', ' || SQLERRM,'aggiornaStato');
        p_error_cod := 'GENERIC_ERROR';
        p_error_des := 'Errore generico in aggiornaStato: '|| SQLCODE || ', ' || SQLERRM;
        p_result := 0;
        --rollback;

  end aggiornaStato;

  procedure aggiornaConfig
  (
    p_key varchar2,
    p_val varchar2
  )
  is
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin

    update
        tms00_config
    set
        ms00_key_val = p_val,
        ms00_modi_dta = sysdate
    where
        ms00_key_cod = p_key;

    commit;

    --logga('Aggiornato '|| p_key || ' a: ' || p_val, 'aggiornaConfig');

  end aggiornaConfig;

  procedure replicaTrxCDB_CA
  (
    p_result        OUT number
  )
  is 

    minRecdNum number;
    maxRecdNum number;
    replSize   number;
    mainSemFlg tms00_config.ms00_key_val%type;
    applSemFlg tms00_config.ms00_key_val%type;

    /*P_RAW_XID_DES VARCHAR2(200);
    P_PROG_TRNS_DES VARCHAR2(200);
    P_SEDECOD VARCHAR2(200);
    P_STATUS VARCHAR2(200);
    P_ERR_DES VARCHAR2(200);
    P_ESITO NUMBER;

    updateCDBException EXCEPTION;*/
    missingParamException EXCEPTION;
    missingParam tms00_config.ms00_key_val%type;

  begin

  p_result := 1;

  --logga('START','replicaTrxCDB_CA');

  select ms00_key_val into mainSemFlg from tms00_config where ms00_key_cod = 'CA_REPL_MAIN_SEM_FLG';

  if (mainSemFlg is null) then
  begin
    missingParam := 'CA_REPL_MAIN_SEM_FLG';
    raise missingParamException;
  end;
  end if;

  if ( mainSemFlg = '1' ) then
  begin

    select ms00_key_val into applSemFlg from tms00_config where ms00_key_cod = 'CA_REPL_APPL_SEM_FLG';

    if (applSemFlg is null) then
    begin
      missingParam := 'CA_REPL_APPL_SEM_FLG';
      raise missingParamException;
    end;
    end if;

    if ( applSemFlg = '0' ) then
    begin

        aggiornaConfig('CA_REPL_APPL_SEM_FLG','1');

        select ms00_key_val into minRecdNum from tms00_config where ms00_key_cod = 'CA_REPL_MAX_RECD_NUM';
        if (minRecdNum is null) then
        begin
          missingParam := 'CA_REPL_MAX_RECD_NUM';
          raise missingParamException;
        end;
        end if;

        select ms00_key_val into replSize from tms00_config where ms00_key_cod = 'CA_REPL_SIZE_NUM';
        if (replSize is null) then
        begin
          missingParam := 'CA_REPL_SIZE_NUM';
          raise missingParamException;
        end;
        end if;

        with q as
        (
            select 
                cd0v_recd_num as recd_num, row_number() over( order by cd0v_recd_num ) as num 
            from 
                tcd0v_rp_gen_sede 
            where 
                cd0v_stat_recd_cod = 'A'
                --cd0v_stat_recd_cod = 'M'
                and NVL(cd0v_rec_obs_flg, 'N') = 'N'
                and cd0v_recd_num > to_number(minRecdNum)
        )
        select max(recd_num) into maxRecdNum from q where num < to_number(replSize);

        if ( maxRecdNum is not null ) then
        begin

            logga('Inizio clonazione. ' || minRecdNum || ' -> ' || maxRecdNum ,'replicaTrxCDB_CA');

            insert into tms10_esiti_ca (ms10_sys_cod, ms10_raw_xid_des, ms10_prog_trns_des, ms10_stat_cod, ms10_cust_cod, ms10_pdr_cod, ms10_sede_cod, ms10_sede_prec_cod, ms10_ins_dta, ms10_poll_dta, ms10_upd_dta, ms10_upd_user_cod, ms10_reply_trx_id, ms10_msg_des, ms10_cdb_ins_dta)
            select 
                'CA',
                s.cd0v_raw_xid_des,
                s.cd0v_prog_trns_des,
                case 
                    when not exists ( select 1 from tcd0u_rp_gen_telefono t where s.cd0v_raw_xid_des = t.cd0u_raw_xid_des and s.cdl1_sede_cod = t.cdl1_sede_cod /*and t.CDMC_STAT_RELS_FLG ='1'*/ ) then 'I'
                    when s.cd0v_stat_recd_cod = 'M' then 'A'
                    else s.cd0v_stat_recd_cod
                end,
                s.cdl0_cust_cod,
                s.cdl2_pdr_cod,
                s.cdl1_sede_cod,
                s.cdl1_prec_sede_cod,
                sysdate,
                null,
                null,
                null,
                null,
                null,
                s.cd0v_strt_trns_tim
            from 
                tcd0v_rp_gen_sede s
            where
                s.cd0v_recd_num > minRecdNum
                and s.cd0v_recd_num <= maxRecdNum
                and s.cd0v_stat_recd_cod in ('A', 'D')
                --and s.cd0v_stat_recd_cod in ('M')
                and NVL(s.cd0v_rec_obs_flg, 'N') = 'N';
                --and s.cd0v_raw_xid_des||s.cd0v_prog_trns_des not in (select distinct MS10_RAW_XID_DES||MS10_PROG_TRNS_DES from tms10_esiti_ca)
                --and is_valid_transaction(s.cd0v_raw_xid_des, s.cd0v_prog_trns_des) = '1';

            logga(TO_CHAR(SQL%ROWCOUNT)||' righe inserite su tms10_esiti_ca','replicaTrxCDB_CA');

            /*for r in 
            (
                select 
                    distinct cd0v_raw_xid_des, cd0v_prog_trns_des, cdl1_sede_cod
                from
                    tcd0v_rp_gen_sede s
                where
                    s.cd0v_recd_num > minRecdNum
                    and s.cd0v_recd_num <= maxRecdNum
                    and s.cd0v_stat_recd_cod = 'A'
                    and NVL(s.cd0v_rec_obs_flg, 'N') = 'N'
                    and is_valid_transaction(s.cd0v_raw_xid_des, s.cd0v_prog_trns_des) = '1'
            )
            loop

              P_RAW_XID_DES := r.cd0v_raw_xid_des;
              P_PROG_TRNS_DES := r.cd0v_prog_trns_des;
              P_SEDECOD := r.cdl1_sede_cod;
              P_STATUS := 'L';
              P_ERR_DES := NULL;
              P_ESITO := NULL;

              aggiornaStatoSuTabelleCDB (  P_RAW_XID_DES => P_RAW_XID_DES,
                P_PROG_TRNS_DES => P_PROG_TRNS_DES,
                P_SEDECOD => P_SEDECOD,
                P_STATUS => P_STATUS,
                P_ERR_DES => P_ERR_DES,
                P_ESITO => P_ESITO) ;  

              if (P_ESITO = '0') then
                raise updateCDBException;
              end if;

            end loop;*/

            aggiornaConfig('CA_REPL_MAX_RECD_NUM',''||maxRecdNum);
        end;
        end if; --if ( maxRecdNum is not null )

        aggiornaConfig('CA_REPL_APPL_SEM_FLG','0');

        commit;

    end;
    else
    begin
        logga('applSemFlg: '||applSemFlg,'replicaTrxCDB_CA');
    end;
    end if; --if ( applSemFlg = '1' )

  end;
  else
  begin
    logga('mainSemFlg: '||mainSemFlg,'replicaTrxCDB_CA');
  end;
  end if; --if ( mainSemFlg = '1' )

  EXCEPTION
    /*when updateCDBException then
        rollback;
        logga('Errore in replicaTrxCDB_CA: '|| SQLCODE || ', ' || SQLERRM,'replicaTrxCDB_CA');
        p_result := 0;*/
    when missingParamException then
        rollback;
        logga('Errore in replicaTrxCDB_CA. Parametro mancante in TMS00_CONFIG: '||missingParam,'replicaTrxCDB_CA');
        p_result := 0;

    when others then
        rollback;
        logga('Errore generico in replicaTrxCDB_CA: '|| SQLCODE || ', ' || SQLERRM,'replicaTrxCDB_CA');
        p_result := 0;

  end replicaTrxCDB_CA;

  procedure aggiornaStatoSuTabelleCDB
  (
    p_raw_xid_des   in  varchar2,
    p_prog_trns_des in  varchar2,
    p_sedeCod       in  varchar2,
    p_status        in  varchar2,
    p_err_des       in  varchar2,
    p_esito out number
  )
  is
    conta           number;
    --codiceSede      tcd0v_rp_gen_sede.cdl1_sede_cod%type;
  begin

  p_esito := 1;

  if (p_raw_xid_des is not null and p_prog_trns_des is not null and p_status is not null) then
  begin

    select count(*) into conta from tcd0v_rp_gen_sede where cd0v_raw_xid_des||cd0v_prog_trns_des = p_raw_xid_des||p_prog_trns_des;

    if ( conta = 1 ) then
    begin

        if ( p_status = 'L' ) then
        begin
            update 
                tcd0v_rp_gen_sede
            set
                cd0v_stat_recd_cod = p_status,
                cd0v_poll_dta = sysdate
            where 
                 cd0v_raw_xid_des||cd0v_prog_trns_des = p_raw_xid_des||p_prog_trns_des;
        end;
        else
        begin
            update 
                tcd0v_rp_gen_sede
            set
                cd0v_stat_recd_cod = p_status,
                cd0v_mlsf_upd_dta = sysdate,
                cd0v_err_des = p_err_des
            where 
                 cd0v_raw_xid_des||cd0v_prog_trns_des = p_raw_xid_des||p_prog_trns_des;
        end;
        end if; --if ( p_status = 'L' )

        /*update 
            tcd0z_rp_gen_customer
        set
            cd0z_stat_recd_cod = p_status
        where 
            cd0z_raw_xid_des = p_raw_xid_des;

        update 
            tcd0u_rp_gen_telefono
        set
            cd0u_stat_recd_cod = p_status
        where
            cd0u_raw_xid_des = p_raw_xid_des
            and cdl1_sede_cod = p_sedeCod;

        update
            tcd0t_rp_gen_mail
        set
            cd0t_stat_recd_cod = p_status
        where
            cd0t_raw_xid_des = p_raw_xid_des
            and cdl1_sede_cod = p_sedeCod;
        */
        commit;

    end;
    end if; --if ( conta = 1 )

  end;
  else
  begin
    p_esito := 0;
  end;
  end if;

  EXCEPTION
    when others then
    rollback;
    logga('Errore: '|| SQLCODE || ', ' || SQLERRM,'aggiornaStatoCDB');
    p_esito := 0;

  end aggiornaStatoSuTabelleCDB;

  procedure cleanLogs
  is
  begin
    logga('Start log cleaning...','cleanLogs');

    delete from tms01_log where ms01_log_dta <= sysdate - 0.3 and upper(ms01_log_msg) not like '%ERROR%';

    logga('Cleaned '|| SQL%ROWCOUNT || ' rows (tms01_log).','cleanLogs');
    
    delete from tcd01_dc_log where msg_date <= sysdate -2;

    logga('Cleaned '|| SQL%ROWCOUNT || ' rows (tcd01_dc_log).','cleanLogs');

    commit;

  end cleanLogs;
  
  procedure cleanData
  is
  begin
  
    UPDATE TMS10_ESITI_CA
    SET MS10_STAT_COD = 'O'
    WHERE MS10_STAT_COD = 'S'
    AND EXISTS (SELECT 1 FROM TMS11_STORICO_ESITI_CA WHERE MS10_RAW_XID_DES||MS10_PROG_TRNS_DES = MS11_RAW_XID_DES||MS11_PROG_TRNS_DES AND MS11_NEW_STAT_COD = 'O');
    
    logga('Ripulite '|| SQL%ROWCOUNT || ' trx (S -> O).','cleanData');
    
    UPDATE TMS10_ESITI_CA
    SET MS10_STAT_COD = 'A'
    WHERE MS10_STAT_COD in ('P','Q','R')
    AND MS10_UPD_DTA <= SYSDATE -0.5;
    
    logga('Rimesse in A '|| SQL%ROWCOUNT || ' trx (P/Q/R).','cleanData');
    
    COMMIT;
  
  end cleanData;

END MS_REP_UTILS;
--------------------------------------------------------
--  DDL for Package Body MS_TK_CA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "DBACD8ESB"."MS_TK_CA" AS 

  procedure logga
    (
      p_message    IN       VARCHAR2,
      p_user       IN       VARCHAR2
    ) 
    is
    begin
        ms_rep_utils.logga(p_message,p_user);
    end logga;

    procedure aggiornaConfig
    (
        p_key varchar2,
        p_val varchar2
    )
    is
    begin 
        ms_rep_utils.aggiornaconfig(p_key, p_val);
    end aggiornaConfig;

  procedure aggiornaStatoTk
  (
    p_tck_num       IN  TCD0S_RP_GEN_TICKET.CDW6_TCK_NUM%TYPE,
    p_status        IN  TCD0S_RP_GEN_TICKET.CD0S_STAT_RECD_COD%TYPE,
    p_msg           IN  TCD0S_RP_GEN_TICKET.CD0S_MLSF_MSG_DES%TYPE,
    p_mule_trx_id   IN  TCD0S_RP_GEN_TICKET.CD0S_MULE_TRX_ID%TYPE,
    p_result        OUT VARCHAR2,
    p_error_cod     OUT VARCHAR2,
    p_error_des     OUT VARCHAR2
  )
  is 
    invalidDataException    EXCEPTION;
    tkNotFoundException     EXCEPTION;
    conta                   NUMBER;
  begin

    if ( p_tck_num is not null and p_status is not null ) then
    begin

        select count(*) into conta from TCD0S_RP_GEN_TICKET where cdw6_tck_num = p_tck_num;
        if ( conta <> 1 ) then
            raise tkNotFoundException;
        end if;

        update 
            TCD0S_RP_GEN_TICKET
        set
            cd0s_stat_recd_cod = p_status,
            cd0s_poll_dta = case when p_status = 'P' then sysdate else cd0s_poll_dta end,
            cd0s_mule_trx_id = case when p_mule_trx_id is null then cd0s_mule_trx_id else p_mule_trx_id end,
            cd0s_mlsf_msg_des = p_msg
        where
            cdw6_tck_num = p_tck_num;

        p_result := 1;
        p_error_cod := 'OK';
        p_error_des := 'Tk ' || p_tck_num ||' aggiornato con successo allo stato ' || p_status;

        commit;

        logga('Tk '|| p_tck_num || ' aggiornato in stato ' || p_status || '(msg: '|| p_msg ||', mule_trx_id: '|| p_mule_trx_id ||')' ,'aggiornaStatoTk');

    end;
    else
    begin
        raise invalidDataException;
    end;
    end if;

    EXCEPTION

    WHEN invalidDataException THEN
        logga('Dati in input insufficienti (p_tck_num: '|| p_tck_num ||', p_status: '|| p_status ||')','aggiornaStatoTk');
        p_result := 0;
        p_error_cod := 'INVALID_DATA';
        p_error_des := 'Dati in input insufficienti (p_tck_num: '|| p_tck_num ||', p_status: '|| p_status ||')';

    WHEN tkNotFoundException THEN
        logga('Ticket: '|| p_tck_num ||' non trovato','aggiornaStatoTk');
        p_result := 0;
        p_error_cod := 'TK_NOT_FOUND';
        p_error_des := 'Ticket: '|| p_tck_num ||' non trovato';

    WHEN OTHERS THEN 
        logga('Errore generico in aggiornaStatoTk: '|| SQLCODE || ', ' || SQLERRM,'aggiornaStatoTk');
        p_error_cod := 'GENERIC_ERROR';
        p_error_des := 'Errore generico in aggiornaStatoTk: '|| SQLCODE || ', ' || SQLERRM;
        p_result := 0;

  end aggiornaStatoTk;

  procedure pollTk
  (
    p_tickets       OUT SYS_REFCURSOR
  )
  IS
    pollSize    number;
    mainSemFlg tms00_config.ms00_key_val%type;
    applSemFlg tms00_config.ms00_key_val%type;
    poll_dta    date;
    poll_guid   varchar2(32);
    missingParamException   EXCEPTION;
    missingParam tms00_config.ms00_key_val%type;
    updateRecordException   EXCEPTION;
  BEGIN
    logga('Start', 'pollTk');

    select sysdate, sys_guid() into poll_dta, poll_guid from dual;

    select ms00_key_val into mainSemFlg from tms00_config where ms00_key_cod = 'CA_TK_MAIN_SEM_FLG';

    if (mainSemFlg is null) then
    begin
        missingParam := 'CA_TK_MAIN_SEM_FLG';
        raise missingParamException;
    end;
    end if;

    if ( mainSemFlg = '1' ) then
    begin

        select ms00_key_val into applSemFlg from tms00_config where ms00_key_cod = 'CA_TK_APPL_SEM_FLG';

        if (applSemFlg is null) then
        begin
            missingParam := 'CA_TK_APPL_SEM_FLG';
            raise missingParamException;
        end;
        end if;

        if ( applSemFlg = '0' ) then
        begin

            aggiornaConfig('CA_TK_APPL_SEM_FLG','1');

            select ms00_key_val into pollSize from tms00_config where ms00_key_cod = 'CA_TK_SIZE_NUM';
            if (pollSize is null) then
            begin
                missingParam := 'CA_TK_SIZE_NUM';
                raise missingParamException;
            end;
            end if;

            logga('['|| poll_guid || '] Inizio estrazione (size='||pollSize||') da TCD0S_RP_GEN_TICKET','pollTk');

            for r in 
            (
                select 
                    a.* 
                from 
                    (
                    SELECT 
                        CDW6_TCK_NUM AS ticketNum,
                        CDW4_STAT_TCK_COD AS ticketStatus,
                        CDW6_CHIU_TCK_DTA AS closeDate,
                        CDW2_PROB_NUM AS problemCod,
                        CDW2_PROB_DES AS problemDes,
                        CD0S_NOTA_TCK_DES AS nota,
                        CDL0_CUST_COD AS customerCod,
                        CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                        CD0S_RECD_NUM AS recdNum,
                        CD0S_INSE_DTA AS createdDate,
                        dense_rank() OVER(ORDER BY CD0S_INSE_DTA, CD0S_RECD_NUM) AS rank
                    FROM 
                        TCD0S_RP_GEN_TICKET
                    WHERE 
                        CD0S_STAT_RECD_COD = 'A'
                    ) a
                where a.rank <= pollSize
                order by a.createdDate, a.recdNum
            )
            loop

                insert into tms13_polled_ca_tk (ms13_poll_guid, ms13_tk_num, ms13_poll_dta)
                values
                (
                    poll_guid,
                    r.ticketNum,
                    poll_dta
                );

                DECLARE
                  P_RESULT VARCHAR2(200);
                  P_ERROR_COD VARCHAR2(200);
                  P_ERROR_DES VARCHAR2(200);

                BEGIN
                  P_RESULT := NULL;
                  P_ERROR_COD := NULL;
                  P_ERROR_DES := NULL;

                  aggiornaStatoTk (  
                  r.ticketNum,
                  'P',
                  '',
                  '',
                    P_RESULT => P_RESULT,
                    P_ERROR_COD => P_ERROR_COD,
                    P_ERROR_DES => P_ERROR_DES) ;  

                if ( P_RESULT != 1 ) then
                begin
                    logga('['|| poll_guid || '] Errore in aggiornamento tk ' || r.ticketNum, 'pollTk');
                    raise updateRecordException;
                end;
                end if;

                END;

            end loop;

            open p_tickets for
                SELECT distinct
                    CDW6_TCK_NUM AS ticketNum,
                    CDW4_STAT_TCK_COD AS ticketStatus,
                    CDW6_CHIU_TCK_DTA AS closeDate,
                    CDW2_PROB_NUM AS problemCod,
                    CDW2_PROB_DES AS problemDes,
                    CD0S_NOTA_TCK_DES AS nota,
                    CDL0_CUST_COD AS customerCod,
                    CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                    CD0S_RECD_NUM AS recdNum,
                    CD0S_INSE_DTA AS createdDate,
                    ms13_poll_guid as pollGuid,
                    ms13_poll_dta as pollDta
                FROM 
                    TCD0S_RP_GEN_TICKET
                    join 
                        tms13_polled_ca_tk 
                            on ms13_poll_guid = poll_guid
                            and ms13_tk_num = CDW6_TCK_NUM

                order by 
                    CD0S_INSE_DTA, CD0S_RECD_NUM;

            delete from tms13_polled_ca_tk
            where MS13_POLL_GUID = poll_guid;

            logga('['|| poll_guid || '] Fine estrazione (size='||pollSize||')','pollTk');

            aggiornaConfig('CA_TK_APPL_SEM_FLG','0');

            commit;  

        end;
        else
        begin
            logga('['|| poll_guid || '] applSemFlg: '||applSemFlg,'pollTk');

            open p_tickets for
                SELECT 
                    CDW6_TCK_NUM AS ticketNum,
                    CDW4_STAT_TCK_COD AS ticketStatus,
                    CDW6_CHIU_TCK_DTA AS closeDate,
                    CDW2_PROB_NUM AS problemCod,
                    CDW2_PROB_DES AS problemDes,
                    CD0S_NOTA_TCK_DES AS nota,
                    CDL0_CUST_COD AS customerCod,
                    CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                    CD0S_RECD_NUM AS recdNum,
                    CD0S_INSE_DTA AS createdDate,
                    null as pollGuid,
                    null as pollDta
                FROM 
                    TCD0S_RP_GEN_TICKET
                    where 1 = 0
                order by 
                    CD0S_INSE_DTA, CD0S_RECD_NUM;

        end;
        end if;

    end;
    else
    begin
        logga('['|| poll_guid || '] mainSemFlg: '||mainSemFlg,'pollTk');

        open p_tickets for
                SELECT 
                    CDW6_TCK_NUM AS ticketNum,
                    CDW4_STAT_TCK_COD AS ticketStatus,
                    CDW6_CHIU_TCK_DTA AS closeDate,
                    CDW2_PROB_NUM AS problemCod,
                    CDW2_PROB_DES AS problemDes,
                    CD0S_NOTA_TCK_DES AS nota,
                    CDL0_CUST_COD AS customerCod,
                    CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                    CD0S_RECD_NUM AS recdNum,
                    CD0S_INSE_DTA AS createdDate,
                    null as pollGuid,
                    null as pollDta
                FROM 
                    TCD0S_RP_GEN_TICKET
                    where 1 = 0
                order by 
                    CD0S_INSE_DTA, CD0S_RECD_NUM;
    end;
    end if;

    EXCEPTION
        when missingParamException then
            rollback;
            logga('['|| poll_guid || '] Errore in pollTk. Parametro mancante in TMS00_CONFIG: '||missingParam,'pollTk');
            open p_tickets for
                SELECT 
                    CDW6_TCK_NUM AS ticketNum,
                    CDW4_STAT_TCK_COD AS ticketStatus,
                    CDW6_CHIU_TCK_DTA AS closeDate,
                    CDW2_PROB_NUM AS problemCod,
                    CDW2_PROB_DES AS problemDes,
                    CD0S_NOTA_TCK_DES AS nota,
                    CDL0_CUST_COD AS customerCod,
                    CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                    CD0S_RECD_NUM AS recdNum,
                    CD0S_INSE_DTA AS createdDate,
                    null as pollGuid,
                    null as pollDta
                FROM 
                    TCD0S_RP_GEN_TICKET
                    where 1 = 0
                order by 
                    CD0S_INSE_DTA, CD0S_RECD_NUM;

        when updateRecordException then
            rollback;
            logga('['|| poll_guid || '] Errore in pollTk. (Aggiornamento record)','pollTk');
            open p_tickets for
                SELECT 
                    CDW6_TCK_NUM AS ticketNum,
                    CDW4_STAT_TCK_COD AS ticketStatus,
                    CDW6_CHIU_TCK_DTA AS closeDate,
                    CDW2_PROB_NUM AS problemCod,
                    CDW2_PROB_DES AS problemDes,
                    CD0S_NOTA_TCK_DES AS nota,
                    CDL0_CUST_COD AS customerCod,
                    CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                    CD0S_RECD_NUM AS recdNum,
                    CD0S_INSE_DTA AS createdDate,
                    null as pollGuid,
                    null as pollDta
                FROM 
                    TCD0S_RP_GEN_TICKET
                    where 1 = 0
                order by 
                    CD0S_INSE_DTA, CD0S_RECD_NUM;

        when others then
            rollback;
            logga('['|| poll_guid || '] Errore generico in pollTk: '|| SQLCODE || ', ' || SQLERRM,'pollTk');
            open p_tickets for
                SELECT 
                    CDW6_TCK_NUM AS ticketNum,
                    CDW4_STAT_TCK_COD AS ticketStatus,
                    CDW6_CHIU_TCK_DTA AS closeDate,
                    CDW2_PROB_NUM AS problemCod,
                    CDW2_PROB_DES AS problemDes,
                    CD0S_NOTA_TCK_DES AS nota,
                    CDL0_CUST_COD AS customerCod,
                    CDL1_SEDE_COD||CDL1_NVER_SEDE_COD AS sedeCod,
                    CD0S_RECD_NUM AS recdNum,
                    CD0S_INSE_DTA AS createdDate,
                    null as pollGuid,
                    null as pollDta
                FROM 
                    TCD0S_RP_GEN_TICKET
                    where 1 = 0
                order by 
                    CD0S_INSE_DTA, CD0S_RECD_NUM;

  END pollTk;

END MS_TK_CA;
--------------------------------------------------------
--  DDL for Package Body SF_DATACHECK
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "DBACD8ESB"."SF_DATACHECK" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */

  procedure ReadAndReserveRUN
    (
        runData     OUT SYS_REFCURSOR
    )
    is
        runGuid tcd00_sfdc_input.cd00_run_guid%TYPE;
        runDate tcd00_sfdc_input.cd00_crtn_date%TYPE;
    begin
        loggamelo('[ReadAndReserveRUN] Start');
        
        with runs as
        (
            select distinct 
                cd00_run_guid, cd00_crtn_date, DENSE_RANK() OVER (ORDER BY cd00_crtn_date, cd00_run_guid) as ord
            from 
                tcd00_sfdc_input
            where 
                cd00_run_stat = 'NEW'
        )
        select cd00_run_guid, cd00_crtn_date into runGuid, runDate from runs WHERE ord = 1;
        
        loggamelo('[ReadAndReserveRUN] Trovato RUN (id='||runGuid||', data='||runDate||')');
        
        update 
            tcd00_sfdc_input
        set
            cd00_modf_date = sysdate,
            cd00_run_stat = 'PENDING'
        where 
            cd00_run_guid = runGuid;
        
        open runData for
            select distinct 
                cd00_run_guid as guid,
                cd00_data_typ as type,
                cd00_data_val as val,
                cd00_oper_val as oper,
                cd00_crtn_date
            from 
                tcd00_sfdc_input
            where
                cd00_run_guid = runGuid
            order by 
                cd00_crtn_date;
        
        commit;
    
    EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          --loggamelo('ERROR ReadAndReserveRUN: '|| SQLCODE || ', ' || SQLERRM);
          
          open runData for
            select distinct 
                cd00_run_guid as guid,
                cd00_data_typ as type,
                cd00_data_val as val,
                cd00_oper_val as oper,
                cd00_crtn_date
            from 
                tcd00_sfdc_input
            where
                cd00_run_guid = '-_-'
            order by 
                cd00_crtn_date;
        
    end ReadAndReserveRUN;
    
    PROCEDURE loggamelo 
    (
      p_message    IN       VARCHAR2
    )
    is
        PRAGMA AUTONOMOUS_TRANSACTION;
    begin
    
    INSERT INTO TCD01_DC_LOG(msg_date,msg_text)
    VALUES (sysdate, p_message);
    
    commit;
    
    end loggamelo;
    
    procedure UpdateRUNStatus
    (
        runGuid     tcd00_sfdc_input.cd00_run_guid%TYPE,
        runStatus   tcd00_sfdc_input.cd00_run_stat%TYPE
    )
    is
    begin
    
    loggamelo('[UpdateRUNStatus] Aggiorno RUN '||runGuid||' a stato '||runStatus);
    
        update 
            tcd00_sfdc_input
        set
            cd00_run_stat = runStatus,
            cd00_modf_date = sysdate,
            cd00_send_date = sysdate
        where 
            cd00_run_guid = runGuid;
        
        commit;
        
     EXCEPTION
          WHEN OTHERS THEN 
          ROLLBACK;
          loggamelo('ERROR UpdateRUNStatus: '|| SQLCODE || ', ' || SQLERRM);
    
    end UpdateRUNStatus;

END SF_DATACHECK;
