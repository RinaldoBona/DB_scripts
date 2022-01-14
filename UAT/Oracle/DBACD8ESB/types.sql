--------------------------------------------------------
--  File creato - venerdì-gennaio-14-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Type AGGSTATOTKCDB
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."AGGSTATOTKCDB" AS OBJECT (
    p_transactionid NUMBER(9,0),
    p_status        VARCHAR2(1),
    p_caseNumber    VARCHAR(20),
    p_msg           VARCHAR2(2000),
    p_mule_trx_id   VARCHAR2(50),
    p_force_flg     NUMBER
);

--create or replace type aggstatotkcdb_table as table of aggstatotkcdb;
--------------------------------------------------------
--  DDL for Type AGGSTATOTKCDB_TABLE
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."AGGSTATOTKCDB_TABLE" as table of aggstatotkcdb;
--------------------------------------------------------
--  DDL for Type AGGSTATOTRX
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."AGGSTATOTRX" as object (
    p_sys           VARCHAR2(10),
    p_trx           VARCHAR2(32),
    p_status        CHAR(1),
    p_user          VARCHAR2(10),
    p_reply_trx_id  VARCHAR2(50),
    p_mule_trx_id   VARCHAR2(50),
    p_msg           VARCHAR2(1000),
    p_force_flg     NUMBER(1)
);
--------------------------------------------------------
--  DDL for Type AGGSTATOTRX_TABLE
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."AGGSTATOTRX_TABLE" as table of aggStatoTrx;
--------------------------------------------------------
--  DDL for Type ARRAY_TYPE_VARCHAR100
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."ARRAY_TYPE_VARCHAR100" as table of varchar(100);
--------------------------------------------------------
--  DDL for Type RESULT_AGG
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."RESULT_AGG" as object (
    p_trx           VARCHAR2(32),
    p_result        char(1),
    p_error_cod     VARCHAR2(50),
    p_error_des     VARCHAR2(500)
);
--------------------------------------------------------
--  DDL for Type RESULT_AGG_TABLE
--------------------------------------------------------

  CREATE OR REPLACE TYPE "DBACD8ESB"."RESULT_AGG_TABLE" as table of result_agg;
