--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Function FIND_CHILD_TRANSACTIONS
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "DBACD1CDB"."FIND_CHILD_TRANSACTIONS" ( parent_Transaction in VARCHAR2 )
RETURN Varchar2
AS 
result varchar2(4000);
BEGIN 
result := '';

for r in 
    ( 
        select 
            distinct v.figlio, v.data 
        from 
            vcd0a_rp_sf_transactions_rel v
        where 
            v.padre = parent_Transaction
            and v.figlio <> parent_Transaction
        union
            select 
                p.padre, p.datapadre
            from 
                vcd0a_rp_sf_transactions_rel p
            where 
                p.padre = parent_Transaction
        order by 
            2
    )
    loop
        result := result || r.figlio || ',';
    end loop;

if length(result) > 0  
then
    select SUBSTR(result, 0, LENGTH(result) - 1)into result from dual;
end if;
return (result); 
END;
  GRANT EXECUTE ON "DBACD1CDB"."FIND_CHILD_TRANSACTIONS" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for Function FIND_CHILD_TRANSACTIONS_NEW
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "DBACD1CDB"."FIND_CHILD_TRANSACTIONS_NEW" ( parent_Transaction in VARCHAR2 )
RETURN Varchar2
AS 
result varchar2(4000);
BEGIN 
result := '';


select to_string(cast(collect(figlio) as ARRAY_TYPE_VARCHAR1000)) into result from (
        select 
            distinct v.figlio, v.data 
        from 
            vcd0a_rp_sf_transactions_rel v
        where 
            v.padre = parent_Transaction
            /*and v.figlio <> parent_Transaction
        union
            select 
                p.padre, p.datapadre
            from 
                vcd0a_rp_sf_transactions_rel p
            where 
                p.padre = parent_Transaction*/
        order by 
            2);

/*
if length(result) > 0  
then
    select SUBSTR(result, 0, LENGTH(result) - 1)into result from dual;
end if;*/
return (result); 
END;
  GRANT EXECUTE ON "DBACD1CDB"."FIND_CHILD_TRANSACTIONS_NEW" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for Function FIND_CHILD_TRANSACTIONS_P
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "DBACD1CDB"."FIND_CHILD_TRANSACTIONS_P" ( parent_Transaction in VARCHAR2 )
RETURN Varchar2
AS 
result varchar2(4000);
BEGIN 
result := '';

for r in 
    ( 
        select 
            distinct v.figlio, v.data 
        from 
            vcd0a_rp_sf_transactions_rel_p v
        where 
            v.padre = parent_Transaction
            and v.figlio <> parent_Transaction
        union
            select 
                p.padre, p.datapadre
            from 
                vcd0a_rp_sf_transactions_rel_p p
            where 
                p.padre = parent_Transaction
        order by 
            2
    )
    loop
        result := result || r.figlio || ',';
    end loop;

if length(result) > 0  
then
    select SUBSTR(result, 0, LENGTH(result) - 1)into result from dual;
end if;
return (result); 
END;
  GRANT EXECUTE ON "DBACD1CDB"."FIND_CHILD_TRANSACTIONS_P" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for Function FIND_CHILD_TRANSACTIONS_V3
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "DBACD1CDB"."FIND_CHILD_TRANSACTIONS_V3" ( CustomerId in VARCHAR2 )
RETURN Varchar2
AS 
result varchar2(4000);
BEGIN 
result := '';

select to_string(cast(collect(figlio) as ARRAY_TYPE_VARCHAR1000)) into result from 
(
    /*
    1. estraggo le transazioni per customer 2 (a e b)
    2. estraggo i customer per le transazioni a e b (1, 2 e 3)
    3. estraggo le transazioni per i customer del punto 2 (a, b e c)
    */
    SELECT figlio, min(data), min(num) FROM
    (
        --start 3.
        select 
            distinct x.CD0B_RAW_XID_DES||x.CD0B_PROG_TRNS_DES as figlio, x.CD0B_STRT_TRNS_TIM AS data, x.CD0B_RECD_NUM AS num
        from
            tcd0b_rp_sf_sede x
        where x.CDL0_CUST_COD in 
        (
            --start 2.
            select 
                distinct t.CDL0_CUST_COD 
            from 
                tcd0b_rp_sf_sede t 
            where 
                t.CD0B_RAW_XID_DES||t.CD0B_PROG_TRNS_DES in 
                (
                    --start 1.
                    SELECT DISTINCT s.CD0B_RAW_XID_DES||s.CD0B_PROG_TRNS_DES AS figlio--, s.CD0B_STRT_TRNS_TIM AS data, s.CD0B_RECD_NUM AS num
                    FROM TCD0B_RP_SF_SEDE s
                    WHERE s.CD0B_STAT_RECD_COD = 'A' AND s.CDL0_CUST_COD = CustomerId
                    --ORDER BY s.CD0B_RECD_NUM
                    --end 1.
                )
            --end 2.
        )
        and x.cd0b_stat_recd_cod = 'A'
        --order by x.CD0B_RECD_NUM
        -- end 3.
    )
    GROUP BY figlio
    ORDER BY 3
);

return (result); 
END;
  GRANT EXECUTE ON "DBACD1CDB"."FIND_CHILD_TRANSACTIONS_V3" TO "DBACD8ESB"
--------------------------------------------------------
--  DDL for Function IS_VALID_CSCT
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "DBACD1CDB"."IS_VALID_CSCT" ( raw_xid in tcd0b_rp_sf_sede.cd0b_raw_xid_des%TYPE, prog_trns in tcd0b_rp_sf_sede.cd0b_prog_trns_des%TYPE  )
RETURN CHAR
AS 
    result char;
    contaTelefoni number;
    contaMail     number;
BEGIN 
    result := '0';

    -- 1. pesca tutti gli id di telefoni collegati alla transazione in ingresso dalla tcd0h_rp_sf_tel_cessati
    -- 2. pesca tutte gli id di mail collegate alla transazione in ingresso dalla tcd0i_rp_sf_mail_cessate
    -- 3. cerca transazioni in stato diverso da O / W sulla tcd0h_rp_sf_tel_cessati e id nella lista del punto 1.
    -- 4. cerca transazioni in stato diverso da O / W sulla tcd0i_rp_sf_mail_cessate e id nella lista del punto 2.
    -- 5. result = '1' se e solo se al punto 3. ed al punto 4. non ho trovato nulla

    select 
        count(distinct t.CD0h_RAW_XID_DES || t.CD0h_PROG_TRNS_DES) into contaTelefoni
    from 
        tcd0h_rp_sf_tel_cessati t
    where t.CDL4_TELE_NUM in 
    (
        select distinct cdl4_tele_num from tcd0h_rp_sf_tel_cessati where cd0h_raw_xid_des = raw_xid and cd0h_prog_trns_des = prog_trns
    )
    and t.cd0h_stat_recd_cod in ('Q', 'P', 'K');

    select 
        count(distinct m.CD0i_RAW_XID_DES || m.CD0i_PROG_TRNS_DES) into contaMail
    from 
        tcd0i_rp_sf_mail_cessate m
    where m.cdl5_mail_num in 
    (
        select distinct cdl5_mail_num from tcd0i_rp_sf_mail_cessate where cd0i_raw_xid_des = raw_xid and cd0i_prog_trns_des = prog_trns
    )
    and m.cd0i_stat_recd_cod in ('Q', 'P', 'K');

    select case when contaTelefoni = 0 and contaMail = 0 then '1' else '0' end into result from dual;

    return (result); -- '1' = OK, '0' = KO

END;


--select IS_VALID_CSCT(v.raw_xid_des, v.prog_trns_des), v.* from vcd0e_rp_sf_cessazioni v;
--------------------------------------------------------
--  DDL for Function IS_VALID_TRANSACTION
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "DBACD1CDB"."IS_VALID_TRANSACTION" ( raw_xid in tcd0b_rp_sf_sede.cd0b_raw_xid_des%TYPE, prog_trns in tcd0b_rp_sf_sede.cd0b_prog_trns_des%TYPE  )
RETURN CHAR
AS 
    result char;
    evento tcd0b_rp_sf_sede.cd0b_tipo_even_cod%TYPE;
    numEventi number;
    found number;
    custPrecFCA number;
    contaPiva number;
BEGIN 
    
    result := '0';
    
        select count(distinct CD0B_TIPO_EVEN_COD) into numEventi from tcd0b_rp_sf_sede where cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns;
        
        if ( numEventi > 1 ) then
        begin
            result := '0';
            return (result);
        end;
        end if;
        
        select count(*) into found from tcd0a_rp_sf_customer c where c.CD0A_RAW_XID_DES = raw_xid and c.CD0A_PROG_TRNS_DES = prog_trns;
        
        if ( found = 1 ) then
        begin
            select 
            case
                when a.CDL0_PREC_CUST_COD is not null and a.CDL0_PREC_CUST_COD = 'C001061739' then 1
                else 0
            end into custPrecFCA
            from tcd0a_rp_sf_customer a where a.CD0A_RAW_XID_DES = raw_xid and a.CD0A_PROG_TRNS_DES = prog_trns;
            
            if ( custPrecFCA = 1 ) then
            begin
                result := '2';
                return (result);
            end;
            end if;
        end;
        end if;
        
        SELECT ((select count(*)  from TCDL0_VI_CUSTOMER l0, TCD0B_RP_SF_SEDE b
        where L0.CDL0_PIVA_COD like 'SYS%'
        and L0.CDL0_CUST_COD = B.CDL0_CUST_COD
        and B.CD0B_RAW_XID_DES = raw_xid
        and B.CD0B_PROG_TRNS_DES = prog_trns)
        +
        (SELECT count(*) FROM TCD0A_RP_SF_CUSTOMER WHERE CD0a_RAW_XID_DES||CD0a_PROG_TRNS_DES = (''||raw_xid)||prog_trns and CDL0_PIVA_COD like 'SYS%')) 
         INTO contaPiva 
         FROM dual;
        
        if ( contaPiva > 0 ) then
        begin
            result := '2';
            return (result);
        end;
        end if;
    
        select distinct CD0B_TIPO_EVEN_COD into evento from tcd0b_rp_sf_sede where cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns;
    
        if ( evento is not null and length(evento) > 0 ) then
        begin
            if ( evento like '%SP' or evento like '%CU' or evento in ('VRG', 'VDF') ) then -- CRCU, CRSP, VASP, VGCU, VGSP, VISP, VSCU, VSSP
            begin
                 select 
                     case 
                         when (
                             select 
                                 count(distinct s.CDL1_SEDE_COD||s.CDL1_NVER_SEDE_COD) 
                             from 
                                 tcd0b_rp_sf_sede s 
                                 join tcd0a_rp_sf_customer c 
                                     on s.cd0b_raw_xid_des = c.cd0a_raw_xid_des
                                     and s.cd0b_prog_trns_des = c.cd0a_prog_trns_des  
                                     and s.cdl0_cust_cod = c.cdl0_cust_cod
                             where
                                 s.cdl1_prim_sedc_flg = 'S'
                                 and cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns
                             ) > 0 then '1'
                         else '0'
                     end into result
                 from dual;
            end;
            elsif ( evento like '%SS' ) then --CRSS, VGSS, VISS, VSSS
            begin
                 select 
                     case 
                         when (
                             select 
                                 count(distinct s.CDL1_SEDE_COD||s.CDL1_NVER_SEDE_COD) 
                             from 
                                 tcd0b_rp_sf_sede s 
                                 join tcd0a_rp_sf_customer c 
                                     on s.cd0b_raw_xid_des = c.cd0a_raw_xid_des
                                     and s.cd0b_prog_trns_des = c.cd0a_prog_trns_des
                             where
                                 s.cdl1_prim_sedc_flg = 'S'
                                 and cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns
                             ) = 0 
                             and
                             (
                             select 
                                 count(distinct s.CDL1_SEDE_COD||s.CDL1_NVER_SEDE_COD) 
                             from 
                                 tcd0b_rp_sf_sede s 
                             where
                                 s.cdl1_prim_sedc_flg = 'N'
                                 and cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns
                             ) > 0 
                             and
                             (
                             select 
                                 count(distinct s.CDL1_SEDE_COD||s.CDL1_NVER_SEDE_COD) 
                             from 
                                 tcd0b_rp_sf_sede s 
                             where
                                 s.cdl1_prim_sedc_flg = 'S'
                                 and cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns
                             ) = 0 
                             then '1'
                         else '0'
                     end into result
                 from dual;
            end;
            elsif ( evento in ( 'SUB', 'SCO' ) ) then
            begin 
                select
                    case
                        when 
                        (
                            select 
                                 count(distinct s.CDL1_SEDE_COD||s.CDL1_NVER_SEDE_COD) 
                             from 
                                 tcd0b_rp_sf_sede s 
                             where
                                cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns
                                and s.cdl1_stat_cod = 'C'
                        ) > 0 then '1'
                        else '0'
                    end into result
                from dual;
                
                if (result = 1) then
                begin
                    select
                    case
                        when 
                        (
                            select 
                                 count(distinct s.cdl0_cust_cod) 
                             from 
                                 tcd0b_rp_sf_sede s 
                             where
                                cd0b_raw_xid_des = raw_xid and CD0B_PROG_TRNS_DES = prog_trns
                        ) <> 2 then '0'
                        else '1'
                    end into result
                from dual;
                end;
                end if;
            end;
            else -- VGEN
            begin
                select '1' into result from dual;
            end;
            end if;
            -- check pdr non nullo (su eventi elaborati ok)
            if ( result = 1 and evento NOT LIKE 'VS%' AND evento NOT IN ('VRG', 'VDF', 'VGEN') ) then
            begin
                select 
                    case
                        when 
                        (
                            select 
                                 count(distinct s.CDL1_SEDE_COD||s.CDL1_NVER_SEDE_COD) 
                             from 
                                 tcd0b_rp_sf_sede s 
                             where
                                s.cd0b_raw_xid_des = raw_xid and s.CD0B_PROG_TRNS_DES = prog_trns
                                and s.CDL2_PDR_COD is null and s.cdl1_stat_cod <> 'C'
                        ) > 0 then '0'
                        else '1'
                    end into result from dual;
            end;
            end if;
        end;
        end if;        
        
    /*end;
    end if;*/

    return (result); -- '1' = OK, '0' = KO

END;
