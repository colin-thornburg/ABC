
----DROP SPECIFIC PROCEDURE EDW_PROCESS.EDW_PROCESS_P_LOAD_DIM_PRODUCT;
--DROP PROCEDURE EDW_PROCESS.P_LOAD_DIM_PRODUCT( BIGINT,INT);

CREATE OR replace PROCEDURE EDW_PROCESS.P_LOAD_DIM_PRODUCT( 
     IN i_job_execution_id  BIGINT,
     IN i_load_type INT )
MODIFIES SQL DATA 
NOT DETERMINISTIC 
LANGUAGE SQL
SPECIFIC EDW_PROCESS_P_LOAD_DIM_PRODUCT

/*Stored Procedure Notes
    Developer: Joel Muckom
    Definition: 
    CALL EDW_PROCESS.P_LOAD_DIM_PRODUCT( IN i_job_execution_id BIGINT, IN i_load_type INT);
    CALL EDW_PROCESS.P_LOAD_DIM_PRODUCT( -256, 0);
  
    Execute Example:
    CALL EDW_PROCESS.P_LOAD_DIM_PRODUCT( -1, 0);
    
    Custom Views/Tables/Sequences/Procedures/Functions Used:
    EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION_H0
    EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 
    EDW_PROCESS.SEQ_DATAPIPELINE_PROCESS
    EDW_PROCESS.P_LOG_JOB_H0
    EDW_STAGING.HUB_LOAD_DIM_PRODUCT
    EDW_HUB.DIM_PRODUCT
    EDW_STAGING.CUR_EMP_DWEMP
    EDW_STAGING.CUR_IVB_DWWMSITEM
    EDW_STAGING.CUR_IVB_ECPARTS
    EDW_STAGING.CUR_IVB_IMASTER
    EDW_STAGING.CUR_IVB_IMASTNS
    EDW_STAGING.CUR_IVB_WHNONSTK
    EDW_STAGING.CUR_IVC_ICLINCTL
    EDW_STAGING.CUR_PMG_DWASGN
    EDW_STAGING.CUR_PRT_AAIAVEND
    EDW_STAGING.CUR_PRT_AAIA_AGE
    EDW_STAGING.CUR_PRT_CATEGORY
    EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION
    EDW_STAGING.CUR_PRT_CMISFILE
    EDW_STAGING.CUR_PRT_EXPCORGN
    EDW_STAGING.CUR_PRT_EXPCORIGIN
    EDW_STAGING.CUR_PRT_EXPECCN
    EDW_STAGING.CUR_PRT_EXPHTS
    EDW_STAGING.CUR_PRT_EXPSCDB
    EDW_STAGING.CUR_PRT_EXPUSML
    EDW_STAGING.CUR_PRT_HAZLION
    EDW_STAGING.CUR_PUR_PMATRIX
    EDW_STAGING.HUB_LOAD_DIM_DATE
    EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE
    EDW_STAGING.HUB_LOAD_DIM_SUPPLIER
    EDW_PROCESS.P_LOG_STATEMENT_H0
    Change Insert Order to IMASTER, IMASTNS, ECPARTS, WHNONSTK from the Opposite order - BRL
    
    Change Log:
    2021-04-15 Removed LINE_DESCRIPTION and PRODUCT_LINE_CODE_DESCRIPTION columns from processing
               Explicitly cast longer queries to clob to fix overflow error resulting from above changes
    
    Change Log: Jim Brooks
    2021-11-15 ADDED LOTS of fields
               Added come debugging based on SQLCODE
               for debugging add 1 to get all log output 0 for just SQLCODE logs
    2021-11-30 ADDED TRIM on 4 source ITEM/LINE joins
    2021-12-02 - update EMPLOYEE_ID/NUMBER joins
    2022-11-15 -- Update Catalog fields from deleted from source products  INFAETL-9890
    
    Change Log: Michael Shreeve
    2022.12.21 Converted left joins to inner joins to EDW_STAGING.HUB_LOAD_DIM_PRODUCT for MERGE (later to be UPDATES) x4
    2022.12.23 Refactored first merge from IMASTER into two parts: create a session table with the result set, then an merge
    2022.12.23 Include product_id on the session table, and use that instead of line/item so the update is co-located
    2022.12.23 for "first merge from IMASTER" refactored MERGE as an UPDATE
    2022.01.01.12 - Added proc P_LOG_STATEMENT_H0 to do all logging
    2022.01.11-12 - removed freshly deprecated logging logic
    2022.01.16 - Refactored IMASTNS, ECPARTS, and WHNONSTK to transform UPDATE only MERGEs to CTAS / UPDATE
    2022.01.17 - Added in call to P_LOG_JOB_H0 when v_sql_ok is false to indicate failure at the job level
    2023.01.18-20 - wrapped up sql_str_logging based on v_log_level (greater or equal to 2)
                  - changed all list_agg to change the order by to the column being aggregated (exception, country-name is ordered by country-code)
                  - changed any country-name-list calculations that are casting to VARCHAR(64) to VARCHAR(256)
    2023.10.11 (and following)
               - add new sources for category-manager; category-director and category-vp number and name for all reads (inserts and updates) - INFAETL-11515
               - add new field PRODUCT_LEVEL_CODE, with a hard-code of PRODUCT for all inserts and updates
               - change any lookups to either DIM_PRODUCT or HUB_LOAD_DIM_PRODUCT to include "AND PRODUCT_LEVEL_CODE = 'PRODUCT'" - INFAETL-11815
               - removed references to MLN32 as they were commented out in previous versions with differing logic for dev vs. prod.
               - commented out some variables that are not used in this proc so i will stop searching to see where they are used.
    Srini Usike
    2023.12.19 - Jira 322 Duplicates generated in join to DIM_EMPLOYEE using EMPLOYEE_USERID_CODE field
               -HOTFIX- changed second lookup (project coordinator) using DIM_EMPLOYEE.USERID
                       added criteria "AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''" (currently lines 971, 1562, 2624, 3153, 4124, 4564, 5572, 6007)

    PRODUCTION BOX TO DOs:
    1) recomment out the debugging for v_job_execution_starttime (around line 220) 
    2) change tablespace for new CTAS of temporary table to point back to MLN (around line 1444) NO! and reverting as part of INFAETL-11515 and INFAETL-11815. 
    3) change all EDWD to EDWT (and eventually EDWT to EDW) for the production deployment
    4) change proc-name in line 132 to remove UAT
    5) change the call to xxxx_PROCESS.P_LOG_STATEMENT_H0 to the right "process" schema (e.g. EDW_PROCESS, EDW_PROCESS; EDW_PROCESS)
    
    TO DOs:
    1) change all "single row" cursors into straightup Select blah into variable. -- still undecided, as cannot do an execute immediate
    2) replace remaining MERGEs with CTAS result-table, UPDATE sql (similar to IMASTER logic) --DONE!
    2) comment out the ROW_COUNT in the ERROR strings (JB's suggestion, still undecided
    3) create schema/table variables and use in the v_str_sql in the queries below:
          schemas: v_staging_read, v_staging_write, v_odata_read, v_hub_read (if ever used), etc.
          tables: one for each
    4) create either a single variable, or a parameter of "environment" D/T/P and IFs around all variances based on this variable / parameter to point to the right schema.
       E.g, pseudocode:
            if v_environment = 'D' 
               then set v_staging_write = 'EDWD_STAGING'
               elseif v_environment = 'T'
               then set v_staging_write = 'EDWT_STAGING'
               elseif v_environment = 'P'
               then set v_staging_write = 'EDW_STAGING'
             endif
       STARTED - currently a global variable; with logic in the local_log_statement internal procedure.
*/

/*Begin Stored Procedure*/

BEGIN 
              
        /*Local Variable Declaration*/
         --TODO: None     
             
        DECLARE v_environment char(1) DEFAULT 'P'; -- D = Development, T = UAT, P = Production. Used IN LOCAL_LOG_STATEMENT, getting max prod_id AND eventual SET OF databases below
        DECLARE v_timestamp timestamp; 
        DECLARE v_bignint bigint;
        DECLARE v_varchar varchar(128);
        DECLARE p_load_type bigint;
        DECLARE p_parent_job_execution_id BIGINT ;    
        DECLARE v_stored_procedure_execution_id BIGINT ;         
        DECLARE v_job_execution_name VARCHAR(128) ;
        DECLARE v_job_phase_id BIGINT;
--        DECLARE v_source_database_name VARCHAR(128) ; 
        DECLARE v_source_table_name VARCHAR(128) ; 
        DECLARE v_staging_table_name VARCHAR(128) ; 
        DECLARE p_source_table_id  BIGINT;  
        DECLARE v_target_database_name VARCHAR(128) ; 
        DECLARE v_target_table_name VARCHAR(128) ;  
        DECLARE v_hub_procedure_name VARCHAR(128) ; 
        DECLARE v_validation_flag BIGINT;
        DECLARE v_hub_database_name VARCHAR(128);        
        DECLARE v_process_database_name VARCHAR(128);
        DECLARE v_staging_database_name VARCHAR(128); 
--        DECLARE v_landing_database_name VARCHAR(128); 
        DECLARE v_str_sql CLOB(21474834);
        DECLARE v_sql_logging_str CLOB(21474834);
        DECLARE v_log_level     INTEGER DEFAULT 1;  -- to print out SQL
        DECLARE v_str_sql_debug CLOB(21474834);
        DECLARE v_str_sql_cursor CLOB(21474834);
        DECLARE v_str_sql_log_job CLOB(21474834); 
        DECLARE v_error_message CLOB(21474834);
        DECLARE p_processed_rows BIGINT;
        DECLARE p_failed_rows BIGINT;
        DECLARE p_processed_insert_rows BIGINT;
        DECLARE p_processed_update_rows BIGINT;
        DECLARE p_processed_delete_rows BIGINT; 
        DECLARE v_job_execution_starttime TIMESTAMP;
        DECLARE o_return_value INT; 
        DECLARE o_str_debug                 CLOB(21474834);
        DECLARE v_etl_source_table_1         VARCHAR(128);
        DECLARE v_etl_source_table_2         VARCHAR(128);
        DECLARE v_etl_source_table_3         VARCHAR(128);
        DECLARE v_etl_source_table_4         VARCHAR(128);
        DECLARE V_PROD_ID_MAX                 BIGINT ;
        DECLARE V_SQL_OK                     BOOLEAN;
        DECLARE V_SQL_CODE                     INTEGER                 DEFAULT 0;
        DECLARE V_RETURN_STATUS             INTEGER                 DEFAULT 0;
        DECLARE V_SQL_STATE                 CHAR(5 OCTETS)     ;
        DECLARE V_SQL_MSG                     VARCHAR(32672 OCTETS) ;
        DECLARE V_RCOUNT                     DECIMAL(31,0)        ;
        DECLARE V_SQL_ERR_STR                VARCHAR(2672 OCTETS) ;
        DECLARE SQLCODE                     INTEGER                 DEFAULT 0;
        DECLARE SQLSTATE                     CHAR(5 OCTETS)             DEFAULT '00000';

    -- ERROR handlers
        DECLARE c_table_cursor         CURSOR FOR c_table; 

        -- Declare SQL Exception Handler for Errors
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
            BEGIN
                GET DIAGNOSTICS EXCEPTION 1 V_SQL_MSG = MESSAGE_TEXT;
                
                SELECT SQLSTATE, SQLCODE 
                  INTO V_SQL_STATE, V_SQL_CODE
                  FROM SYSIBM.SYSDUMMY1;
            END;    

basic_start: 
BEGIN
    --  Internal helper function -- used to have the direct call to the logger rather than the usual standard of creating a clob-string and "EXECUTE IMMEDIATE"
        DECLARE PROCEDURE LOCAL_LOG_STATEMENT ( 
                    IN l_i_job_execution_id BIGINT, 
                    IN l_i_label VARCHAR(255),
                    IN l_i_metadata VARCHAR(4096),
                    IN l_i_message CLOB,
                    IN l_i_timestamp TIMESTAMP
            )
--         sample invocation: CALL LOCAL_LOG_STATEMENT(v_job_xecution_id, 'LABEL STRING', V_ERROR_MESSAGE, v_sql_str, v_timestamp)
          LOCAL_LOG_STATEMENT_BODY: BEGIN
            
              CASE 
                WHEN v_environment = 'D' 
                      THEN CALL EDWD_PROCESS.P_LOG_STATEMENT_H0( 
                                COALESCE(l_i_job_execution_id,-1), 
                                COALESCE(l_i_label,''),
                                COALESCE(l_i_metadata,''),
                                COALESCE(l_i_message,'no message')::clob,
                                COALESCE(l_i_timestamp, current_timestamp)
                               );
	            WHEN v_environment = 'T' 
                      THEN CALL EDWT_PROCESS.P_LOG_STATEMENT_H0( 
                                COALESCE(l_i_job_execution_id,-1), 
                                COALESCE(l_i_label,''),
                                COALESCE(l_i_metadata,''),
                                COALESCE(l_i_message,'no message')::clob,
                                COALESCE(l_i_timestamp, current_timestamp)
                               );
                WHEN v_environment = 'P' 
                      THEN CALL EDW_PROCESS.P_LOG_STATEMENT_H0( 
                                COALESCE(l_i_job_execution_id,-1), 
                                COALESCE(l_i_label,''),
                                COALESCE(l_i_metadata,''),
                                COALESCE(l_i_message,'no message')::clob,
                                COALESCE(l_i_timestamp, current_timestamp)
                               );
               END CASE;
        END LOCAL_LOG_STATEMENT_BODY;
       
         /*Local Variable Initialization*/
        SET v_timestamp = CURRENT_TIMESTAMP; 
        SET v_bignint = CAST(0 AS BIGINT);
        SET v_varchar = CAST('' AS varchar(128));
        SET p_load_type = i_load_type;
        SET p_parent_job_execution_id = i_job_execution_id; 
        SET p_source_table_id = -1;
        SET v_stored_procedure_execution_id = 0;
        SET v_hub_procedure_name = TRIM('P_LOAD_DIM_PRODUCT');
        SET v_staging_table_name = TRIM('HUB_LOAD_DIM_PRODUCT');
        SET v_source_table_name = TRIM('HUB_LOAD_DIM_PRODUCT');
        SET v_job_phase_id = 1;
        SET v_target_table_name = TRIM('DIM_PRODUCT'); 
        SET v_validation_flag = 0;
        SET v_hub_database_name = TRIM('EDW_HUB');
        SET v_process_database_name = TRIM('EDW_PROCESS');
--        SET v_landing_database_name = TRIM('EDW_STAGING'); -- does not appear to be used
        SET v_staging_database_name = TRIM('EDW_STAGING'); --where is hub_load_dim_product for this environment
        SET v_job_execution_name = TRIM(v_hub_procedure_name);
        SET v_target_database_name = TRIM(v_hub_database_name);   
--        SET v_source_database_name = TRIM(v_staging_database_name);    --does NOT appear TO be used
        SET v_error_message = TRIM('');    
        SET p_processed_rows = 0;
        SET p_failed_rows = 0;
        SET p_processed_insert_rows = 0;
        SET p_processed_update_rows = 0;
        SET p_processed_delete_rows = 0;
        SET v_str_sql = TRIM('');    
        SET v_str_sql_cursor = TRIM('');
        SET v_str_sql_log_job = TRIM('');
        SET v_str_sql_debug = TRIM('');
        SET o_return_value = 0; 
        SET o_str_debug = TRIM('');    
        SET v_etl_source_table_1 = 'IMASTER';
        SET v_etl_source_table_2 = 'IMASTNS';
        SET v_etl_source_table_3 = 'ECPARTS';
        SET v_etl_source_table_4 = 'WHNONSTK';
        SET v_log_level = 2;            --    0 logs only if error. 1 logs completion OF EACH step. 2 logs ALL SQL statements 3. logs additional debugging. 4 sends a debug flag TO logging procs (future)
          
          
              -- ERROR handlers
        SET V_RCOUNT         = 0;
        SET V_SQL_ERR_STR    = '';
        SET V_SQL_OK        = TRUE;
      
          /*README: 
        The section of code below involves calling the sequence SEQ_DATAPIPELINE_PROCESS_H0.NEXTVAL to get the next value to Populate v_stored_procedure_execution_id 
        for use within this stored procedure and for logging to AUDIT_LOG_JOB_EXECUTION_H0 table in EDW_PROCESS
        */
     
        --Get v_stored_procedure_execution_id  
        SET v_str_sql_cursor = 'SELECT ' || v_process_database_name || '.SEQ_DATAPIPELINE_PROCESS_H0.NEXTVAL FROM sysibm.sysdummy1;';
        SET o_str_debug = v_str_sql_cursor;
    
        PREPARE c_table
        FROM v_str_sql_cursor; 
        OPEN c_table_cursor USING v_process_database_name; 
        FETCH c_table_cursor INTO v_stored_procedure_execution_id; 
        CLOSE c_table_cursor;  
        SET v_str_sql =  'SELECT ' || v_process_database_name || '.SEQ_DATAPIPELINE_PROCESS_H0.NEXTVAL INTO v_stored_procedure_execution_id FROM sysibm.sysdummy1;';       
       

        GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
            
        IF (V_SQL_CODE <> 0) THEN  --  Warning
            SET V_SQL_OK = FALSE;
            SET V_RETURN_STATUS = V_SQL_CODE;
        END IF;
            
        IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN
                
           SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <GET V_STORED_PROCEDURE_EXECUTION_ID> '||
                        '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql_cursor) AS VARCHAR),'NSQLLEN') || '> ' ||
                        '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                        ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                        ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                        ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';
           IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
              THEN SET v_sql_logging_str = v_str_sql_cursor;
              ELSE SET v_sql_logging_str = 'no sql logged';
           END IF;

           CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '01. GET v_stored_procedure_execution_id', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
        END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
        
         --Populate EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION
         --TODO: Implement inter-process logging and pass PARAM values and dynamic sql called via EXECUTE IMMEDIATE
        SET v_job_phase_id = 2;
        SET v_error_message = 'Stored Procedure Message: RUNNING/STARTED > ' || v_hub_procedure_name || ' > JOB EXECUTION ID: ' || v_stored_procedure_execution_id || ' > Total Rows Processed: ' || p_processed_rows || '.';
        SET o_str_debug = v_error_message; 
        SET v_str_sql = 'CALL ' || v_process_database_name || '.P_LOG_JOB_H0( ' || quote_literal(v_job_execution_name) || ',' || v_stored_procedure_execution_id ||','|| quote_literal(v_error_message) || ',' || v_job_phase_id || ',' || p_parent_job_execution_id || ',' || p_source_table_id || ',' || quote_literal(v_target_table_name) || ',' || p_processed_rows || ',' || p_failed_rows || ',' || p_processed_insert_rows || ',' || p_processed_update_rows || ',' || p_processed_delete_rows || ');'; 
        SET o_str_debug =  v_str_sql;
        EXECUTE IMMEDIATE v_str_sql;
       
        GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

        IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN
                
           SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <P_LOG_JOB_H0 Start> '||
                        '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                        '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                        ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                        ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                        ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

           IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
              THEN SET v_sql_logging_str = v_str_sql;
              ELSE SET v_sql_logging_str = 'no sql logged';
           END IF;

           CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '02. P_LOG_JOB_H0 Start', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
        END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

             
        --Get v_job_execution_starttime; MDS: WHY are we using status_code = 0? that is the timestamp of the last successful completion
        SET v_str_sql_cursor = 'SELECT ADD_HOURS(aljeh.JOB_EXECUTION_STARTTIME, -3) AS JOB_EXECUTION_STARTTIME
                                        FROM ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_H0 aljeh 
                                        WHERE aljeh.JOB_EXECUTION_STATUS_CODE = 0 AND aljeh.JOB_EXECUTION_NAME = ' || quote_literal(v_job_execution_name) || ' ORDER BY aljeh.JOB_EXECUTION_STARTTIME DESC ;';
                        
        PREPARE c_table 
        FROM v_str_sql_cursor; 
        OPEN c_table_cursor USING v_process_database_name; 
        FETCH c_table_cursor INTO v_job_execution_starttime;
        CLOSE c_table_cursor;  

        GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
            
            IF (V_SQL_CODE <> 0) THEN  --  Warning
               SET V_SQL_OK = FALSE;
               SET V_RETURN_STATUS = V_SQL_CODE;
            END IF;
            
            IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

               SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <GET V_STORED_PROCEDURE_EXECUTION_ID> '||
                   '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql_cursor) AS VARCHAR),'NSQLLEN') || '> ' ||
                   '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                   ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                   ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                  ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                  THEN SET v_sql_logging_str = v_str_sql_cursor;
                  ELSE SET v_sql_logging_str = 'no sql logged';
               END IF;


               CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '03. GET v_job_execution_starttime', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
            END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

           
            IF v_job_execution_starttime IS NULL THEN 
               SET v_job_execution_starttime = CAST('1900-01-01-00.00.00.000000' AS TIMESTAMP);
            ELSE 
               SET v_job_execution_starttime = CAST(v_job_execution_starttime AS TIMESTAMP);
            END IF;
        
            /*  DEBUG fr VALIDATION - set date to past for not-updated-staging-tables. */
        
               --SET v_job_execution_starttime = CAST('1900-01-01-00.00.00.000000' AS TIMESTAMP);
            IF v_log_level >= 1
               THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '04. debug: v_job_execution_starttime', '', quote_literal(v_job_execution_starttime), current_timestamp);
           END IF;
          
              /*README: 
            The section of code below involves the clearing and loading the Hub Load Table with IF/THEN/END IF logic used to have the case with the Hub Table needs to be cleared and reloaded in
            full.
            */     
       
            IF p_load_type = 1
            THEN
                --Delete Hub Table
                SET v_str_sql = 'DELETE FROM ' || v_target_database_name ||'.'|| v_target_table_name || ' WHERE PRODUCT_ID >= 0;'; 
                SET o_str_debug =  v_str_sql;     
                EXECUTE IMMEDIATE  v_str_sql;    
               
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

                CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '05. delete/cleanout of dim_product', '', v_str_sql, current_timestamp);
            ELSE
                CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '05. skipped delete/cleanout of dim_product', '', 'no sql', current_timestamp);
            END IF;
            
        --Populate Hub Load Table - IMASTER 1ST INSERT
      
                SET v_str_sql =  CLOB('INSERT INTO ') || v_staging_database_name || '.' || v_staging_table_name || 
                                '(PRODUCT_ID, LINE_CODE, LINE_DESCRIPTION, ITEM_CODE, ITEM_DESCRIPTION, SEGMENT_NUMBER, SEGMENT_DESCRIPTION, SUB_CATEGORY_NUMBER, SUB_CATEGORY_DESCRIPTION, CATEGORY_NUMBER, 
                                  CATEGORY_DESCRIPTION, PRODUCT_LINE_CODE, SUB_CODE, MANUFACTURE_ITEM_NUMBER_CODE, SUPERSEDED_LINE_CODE, SUPERSEDED_ITEM_NUMBER_CODE, SORT_CONTROL_NUMBER, POINT_OF_SALE_DESCRIPTION, 
                                  POPULARITY_CODE, POPULARITY_CODE_DESCRIPTION, POPULARITY_TREND_CODE, POPULARITY_TREND_CODE_DESCRIPTION, LINE_IS_MARINE_SPECIFIC_FLAG, LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE, 
                                  LINE_IS_FLEET_SPECIFIC_CODE, LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE, LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG, JOBBER_SUPPLIER_CODE, JOBBER_UNIT_OF_MEASURE_CODE, 
                                  WAREHOUSE_UNIT_OF_MEASURE_CODE, WAREHOUSE_SELL_QUANTITY, RETAIL_WEIGHT, QUANTITY_PER_CAR, CASE_QUANTITY, STANDARD_PACKAGE, PAINT_BODY_AND_EQUIPMENT_PRICE, WAREHOUSE_JOBBER_PRICE, 
                                  WAREHOUSE_COST_WUM, WAREHOUSE_CORE_WUM, OREILLY_COST_PRICE, JOBBER_COST, JOBBER_CORE_PRICE, OUT_FRONT_MERCHANDISE_FLAG, ITEM_IS_TAXED_FLAG, QUANTITY_ORDER_ITEM_FLAG, 
                                  JOBBER_DIVIDE_QUANTITY, ITEM_DELETE_FLAG_RECORD_CODE, SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE, PRIMARY_UNIVERSAL_PRODUCT_CODE, WARRANTY_CODE, WARRANTY_CODE_DESCRIPTION, 
                                  INVOICE_COST_WUM_INVOICE_COST, INVOICE_CORE_WUM_CORE_COST, IS_CONSIGNMENT_ITEM_FLAG, WAREHOUSE_JOBBER_CORE_PRICE, ACQUISITION_FIELD_1_CODE, ACQUISITION_FIELD_2_CODE, BUY_MULTIPLE, 
                                  BUY_MULTIPLE_CODE, BUY_MULTIPLE_CODE_DESCRIPTION, SUPPLIER_CONVERSION_FACTOR_CODE, SUPPLIER_CONVERSION_QUANTITY, SUPPLIER_UNIT_OF_MEASURE_CODE, UNIT_OF_MEASURE_AMOUNT, 
                                  UNIT_OF_MEASURE_QUANTITY, UNIT_OF_MEASURE_DESCRIPTION, TAX_CLASSIFICATION_CODE, TAX_CLASSIFICATION_CODE_DESCRIPTION, TAX_CLASSIFICATION_REVIEW_STATUS_CODE, 
                                  DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION, DISTRIBUTION_CENTER_PICK_LENGTH, DISTRIBUTION_CENTER_PICK_WIDTH, 
                                  DISTRIBUTION_CENTER_PICK_HEIGHT, DISTRIBUTION_CENTER_PICK_WEIGHT, PICK_LENGTH_WIDTH_HEIGHT_CODE, CASE_QUANTITY_CODE, CASE_LENGTH, CASE_WIDTH, CASE_HEIGHT, CASE_WEIGHT, 
                                  CASE_LENGTH_WIDTH_HEIGHT_CODE, CASES_PER_PALLET, CASES_PER_PALLET_LAYER, PALLET_LENGTH, PALLET_WIDTH, PALLET_HEIGHT, PALLET_WEIGHT, PALLET_LENGTH_WIDTH_HEIGHT_CODE, SHIPMENT_CLASS_CODE, 
                                  DOT_CLASS_NUMBER, DOT_CLASS_FOR_MSDS_ID_NUMBER, CONTAINER_DESCRIPTION, KEEP_FROM_FREEZING_FLAG, FLIGHT_RESTRICTED_FLAG, ALLOW_NEW_RETURNS_FLAG, ALLOW_CORE_RETURNS_FLAG, 
                                  ALLOW_WARRANTY_RETURNS_FLAG, ALLOW_RECALL_RETURNS_FLAG, ALLOW_MANUAL_OTHER_RETURNS_FLAG, ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG, HAZARDOUS_UPDATE_DATE, PIECE_LENGTH, PIECE_WIDTH, 
                                  PIECE_HEIGHT, PIECE_WEIGHT, PIECES_INNER_PACK, IN_CATALOG_CODE, IN_CATALOG_CODE_DESCRIPTION, ALLOW_SPECIAL_ORDER_FLAG, ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG, 
                                  SUPPLIER_LIFE_CYCLE_CODE, SUPPLIER_LIFE_CYCLE_CHANGE_DATE, LONG_DESCRIPTION, ELECTRONIC_WASTE_FLAG, STORE_MINIMUM_SALE_QUANTITY, MANUFACTURER_SUGGESTED_RETAIL_PRICE, 
                                  MAXIMUM_CAR_QUANTITY, MINIMUM_CAR_QUANTITY, ESSENTIAL_HARD_PART_CODE, INNER_PACK_CODE, INNER_PACK_QUANTITY, INNER_PACK_LENGTH, INNER_PACK_WIDTH, INNER_PACK_HEIGHT, INNER_PACK_WEIGHT, 
                                  BRAND_CODE, PART_NUMBER_CODE, PART_NUMBER_DISPLAY_CODE, PART_NUMBER_DESCRIPTION, SPANISH_PART_NUMBER_DESCRIPTION, SUGGESTED_ORDER_QUANTITY, BRAND_TYPE_NAME, LOCATION_TYPE_NAME, 
                                  MANUFACTURING_CODE_DESCRIPTION, QUALITY_GRADE_CODE, PRIMARY_APPLICATION_NAME, 
                                  --INFAETL-11515 mds renamed / added the following line
                                  CATEGORY_MANAGER_NAME, CATEGORY_MANAGER_NUMBER, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER, CATEGORY_DIRECTOR_NAME, CATEGORY_DIRECTOR_NUMBER, CATEGORY_VP_NAME, CATEGORY_VP_NUMBER, 
                                  INACTIVATED_DATE, REVIEW_CODE, STOCKING_LINE_FLAG, OIL_LINE_FLAG, SPECIAL_REQUIREMENTS_LABEL, SUPPLIER_ACCOUNT_NUMBER, SUPPLIER_NUMBER, SUPPLIER_ID, BRAND_DESCRIPTION, 
                                  DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER, ACCOUNTS_PAYABLE_VENDOR_NUMBER, SALES_AREA_NAME, TEAM_NAME, CATEGORY_NAME, REPLENISHMENT_ANALYST_NAME, REPLENISHMENT_ANALYST_NUMBER, 
                                  REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER, REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID, SALES_AREA_NAME_SORT_NUMBER, TEAM_NAME_SORT_NUMBER, BUYER_CODE, BUYER_NAME, 
                                  BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE, BATTERY_PACKING_INSTRUCTIONS_CODE, BATTERY_MANUFACTURING_NAME, BATTERY_MANUFACTURING_ADDRESS_LINE_1, BATTERY_MANUFACTURING_ADDRESS_LINE_2, 
                                  BATTERY_MANUFACTURING_ADDRESS_LINE_3, BATTERY_MANUFACTURING_ADDRESS_LINE_4, BATTERY_MANUFACTURING_CITY_NAME, BATTERY_MANUFACTURING_POSTAL_TOWN_NAME, BATTERY_MANUFACTURING_STATE_NAME, 
                                  BATTERY_MANUFACTURING_ZIP_CODE, BATTERY_MANUFACTURING_COUNTRY_CODE, BATTERY_PHONE_NUMBER_CODE, BATTERY_WEIGHT_IN_GRAMS, BATTERY_GRAMS_OF_LITHIUM_PER_CELL, 
                                  BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY, BATTERY_WATT_HOURS_PER_CELL, BATTERY_WATT_HOURS_PER_BATTERY, BATTERY_CELLS_NUMBER, BATTERIES_PER_PACKAGE_NUMBER, BATTERIES_IN_EQUIPMENT_NUMBER, 
                                  BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG, BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG, COUNTRY_OF_ORIGIN_NAME_LIST, EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST, 
                                  HARMONIZED_TARIFF_SCHEDULE_CODE_LIST, SCHEDULE_B_CODE_LIST, UNITED_STATES_MUNITIONS_LIST_CODE, PROJECT_COORDINATOR_ID_CODE, PROJECT_COORDINATOR_EMPLOYEE_ID, 
                                  STOCK_ADJUSTMENT_MONTH_NUMBER, BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST, BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST, ALL_IN_COST, CANCEL_OR_BACKORDER_REMAINDER_CODE, CASE_LOT_DISCOUNT, 
                                  COMPANY_NUMBER, CONVENIENCE_PACK_QUANTITY, CONVENIENCE_PACK_DESCRIPTION, PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_CREATION_DATE, 
                                  PRODUCT_SOURCE_TABLE_CREATION_TIME, PRODUCT_SOURCE_TABLE_CREATION_USER_NAME, PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE, 
                                  PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME, DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE, 
                                  DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE, ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE, EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG, HAZARDOUS_UPDATE_PROGRAM_NAME, 
                                  HAZARDOUS_UPDATE_TIME, HAZARDOUS_UPDATE_USER_NAME, LIST_PRICE, LOW_USER_PRICE, MINIMUM_ADVERTISED_PRICE, MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE, MINIMUM_SELL_QUANTITY, 
                                  PACKAGE_SIZE_DESCRIPTION, PERCENTAGE_OF_SUPPLIER_FUNDING, PIECE_LENGTH_WIDTH_HEIGHT_FLAG, PRICING_COST, PROFESSIONAL_PRICE, RETAIL_CORE, RETAIL_HEIGHT, RETAIL_LENGTH, 
                                  RETAIL_UNIT_OF_MEASURE_DESCRIPTION, RETAIL_WIDTH, SALES_PACK_CODE, SCORE_FLAG, SHIPPING_DIMENSIONS_CODE, SUPPLIER_BASE_COST, SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE, 
                                  SUPPLIER_SUPERSEDED_LINE_CODE, CATEGORY_TABLE_CREATE_DATE, CATEGORY_TABLE_CREATE_PROGRAM_NAME, CATEGORY_TABLE_CREATE_TIME, CATEGORY_TABLE_CREATE_USER_NAME, CATEGORY_TABLE_UPDATE_DATE, 
                                  CATEGORY_TABLE_UPDATE_PROGRAM_NAME, CATEGORY_TABLE_UPDATE_TIME, CATEGORY_TABLE_UPDATE_USER_NAME, PRODUCT_SOURCE_TABLE_UPDATE_DATE, PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME, 
                                  PRODUCT_SOURCE_TABLE_UPDATE_TIME, PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME, VIP_JOBBER, WAREHOUSE_CORE, WAREHOUSE_COST, 
                                  --INFAETL-11815 mds added the following line
                                  PRODUCT_LEVEL_CODE, 
                                  ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS
                                ) ' || ' SELECT CAST(' || v_process_database_name|| '.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS PRODUCT_ID,
                                        CAST(TRIM(cur_IMASTER.ILINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                                        CAST(TRIM(cur_IMASTER.IITEM#) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IDESC), TRIM(cur_ECPARTS.SHORT_DESCRIPTION)) AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                                        CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                                        CAST(COALESCE(cur_CATEGORY.SUBCATNUM,-2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                                        CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                                        CAST(TRIM(cur_IMASTER.IPLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                                        CAST(cur_IMASTER.IPCODE AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                                        CAST(COALESCE(cur_IMASTER.IMFRI#, ''-2'' ) AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                                        CAST(COALESCE(cur_IMASTER.ISLINE, ''-2'' ) AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                                        CAST(COALESCE(cur_IMASTER.ISITM#, ''-2'' ) AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                                        CAST(COALESCE(cur_IMASTER.ICNTRL, -2) AS INTEGER) AS SORT_CONTROL_NUMBER,
                                        CAST(TRIM(cur_IMASTER.IPSDSC) AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IPOPCD), '''') AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                                        CAST(COALESCE(VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE_DESCRIPTION , ''DEFAULT'') AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                                        CAST(COALESCE(TRIM(cur_IMASTER.ITRNDC),'''') AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                                        CAST(COALESCE(VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE_DESCRIPTION , ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                                        CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                                        CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                                        CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                                        CAST(cur_IMASTER.IJSC AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                                        CAST(cur_IMASTER.IJUM AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                                        CAST(cur_IMASTER.IWUM AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                                        CAST(cur_IMASTER.IWSQTY AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                                        CAST(COALESCE(cur_IMASTER.IWGT, 0) AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                                        CAST(COALESCE(cur_IMASTER.IQTYPC, '''') AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                                        CAST(cur_IMASTER.ICSQTY AS DECIMAL(12,4)) AS CASE_QUANTITY,
                                        CAST(cur_IMASTER.ISTDPK AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                                        CAST(COALESCE(cur_IMASTER.IPBNEP, 0) AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                                        CAST(cur_IMASTER.IJOBRP AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                                        CAST(cur_IMASTER.ICOSTP AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                                        CAST(cur_IMASTER.ICOREP AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                                        CAST(COALESCE(cur_IMASTER.IORCST, 0) AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                                        CAST(cur_IMASTER.IJCOST AS DECIMAL(12,4)) AS JOBBER_COST,
                                        CAST(cur_IMASTER.IJCORE AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_IMASTER.IOFM) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_IMASTER.ITAXED) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                                        CAST(COALESCE(cur_IMASTER.IQORDR, '''') AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                                        CAST(COALESCE(cur_IMASTER.IJDQ, 1) AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IRECCD), '''') AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                                        CAST(cur_IMASTER.IMSDS AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                                        CAST(TRIM(cur_IMASTER.IBARC) AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IWARRC), ''NONE'') AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                                        CAST(COALESCE(VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE_DESCRIPTION, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                                        CAST(COALESCE(cur_IMASTER.IINVC, 0) AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                                        CAST(COALESCE(cur_IMASTER.ICORC, 0) AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                                        CAST(COALESCE(cur_IMASTER.ICONITM, ''N'') AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                                        CAST(COALESCE(cur_IMASTER.IWJCRP, 0) AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                                        CAST(cur_IMASTER.IACQ1 AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                                        CAST(cur_IMASTER.IACQ2 AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IBUYMULT), -2) AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IBUYDESC), '''') AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                                        CAST(COALESCE(
                                            CASE 
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''1'' THEN ''1 - BLANK''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''2'' THEN ''2 - BLANK''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''AS'' THEN ''ASSORTED''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BD'' THEN ''BUNDLE''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BG'' THEN ''BAG''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BP'' THEN ''BLISTER PACK''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BX'' THEN ''BOX''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''CD'' THEN ''CARDED''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''CS'' THEN ''CASE''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''EA'' THEN ''EACH''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''FT'' THEN ''FOOT''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''KT'' THEN ''KIT''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''PK'' THEN ''PACKAGE'' 
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''PR'' THEN ''PAIR''
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''RL'' THEN ''ROLL'' 
                                            WHEN TRIM(cur_IMASTER.IBUYDESC) = ''ST'' THEN ''SET''
                                            ELSE ''UNKNOWN''
                                            END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                                        CAST(cur_IMASTER.IVENCNVRF AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                                        CAST(COALESCE(cur_IMASTER.IVENCNVRQ, 1) AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                                        CAST(cur_IMASTER.IVENUOM AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                                        CAST(cur_IMASTER.IUOMAMT AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                                        CAST(cur_IMASTER.IUOMQTY AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                                        CAST(cur_IMASTER.IUOMDESC AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                                        CAST(cur_IMASTER.ITAXCLASS AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''ACC'' THEN ''NON CLOTHING ACCESSORIES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''AID'' THEN ''FIRST AID KITS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''APP'' THEN ''CLOTHING AND APPAREL''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B06'' THEN ''BATTERY UNDER 12 VOLTS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B06W'' THEN ''BATTERY UNDER 12 VOLTS UNDER WARRANTY''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B12'' THEN ''BATTERY 12 VOLTS OR HIGHER''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B12W'' THEN ''BATTERY 12 VOLTS OR HIGHER UNDER WARRANTY''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BAT'' THEN ''BATTERY KIOSK''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BCR'' THEN ''BATTERY CORE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BTL'' THEN ''BOTTLE DEPOSIT''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BTY'' THEN ''BATTERY FEE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CAB'' THEN ''POWER CABLES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CCD'' THEN ''COMMON CARRIER FOB DESTINATION''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CCO'' THEN ''COMMON CARRIER FOB ORGIN''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CDY'' THEN ''CANDY''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CEL'' THEN ''CELL PHONE BATTERIES AND CHARGERS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CHM'' THEN ''CHEMICALS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''COK'' THEN ''CARBONATED SOFT DRINKS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''DUC'' THEN ''DUCT TAPE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''EMS'' THEN ''EMISSIONS PARTS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''EXP'' THEN ''EXPORT TAX''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FEX'' THEN ''FIRE EXTINGUISHER''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRE'' THEN ''FREON W/DEPOSIT''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRM'' THEN ''FARM MACHINERY AND EQUIPMENT PARTS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRT'' THEN ''FREIGHT DELIVERY''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRZ'' THEN ''ANTIFREEZE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FSL'' THEN ''FLASHLIGHTS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''GAS'' THEN ''GAS OR DIESEL FUEL TANKS OR CONTAINERS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''GEN'' THEN ''PORTABLE GENERATORS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''GFT'' THEN ''GIFT CARDS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''H2C'' THEN ''WATER CASES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''H2O'' THEN ''BOTTLE WATER''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''HAZ'' THEN ''HAZARDOUS WASTE REMOVAL''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''HBT'' THEN ''HOUSEHOLD BATTERIES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''HND'' THEN ''HANDLING CHARGE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''LAB'' THEN ''REPAIR OF MOTOR VEHICLES & TPP''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''MFG'' THEN ''MANUFACTURING MACHINERY AND EQUIPMENT PARTS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''NCD'' THEN ''NONCARBONDATED SOFT DRINKS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''NEW'' THEN ''NEW ITEMS AND CORES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''OAL'' THEN ''MOTOR OIL AND TRANS FLUID EXEMPT IN AL''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''OIL'' THEN ''MOTOR OIL AND TRANS FLUID''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''PBF'' THEN ''PLASTIC BAG FEE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''PCR'' THEN ''PART CORE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''PNT'' THEN ''PAINT AND PAINT SUPPLIES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''RDO'' THEN ''RADIOS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''SHC'' THEN ''SHIPPING AND HANDLING COMBINED CHARGE''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''SNK'' THEN ''SNACK FOOD OTHER THAN CANDY''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''SUP'' THEN ''SUPPLIES''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TCK'' THEN ''TICKETS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TEC'' THEN ''SALES AND INSTALLER CLINICS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TIE'' THEN ''GROUND ANCHOR SYSTEM OR TIE DOWN KITS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TOL'' THEN ''TOOLS''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TPP'' THEN ''TANGIBLE PERSONAL PROPERTY''
                                            WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TRP'' THEN ''TARPAULINS OR WATERPROOF SHEETING''
                                            ELSE ''UNKNOWN''
                                            END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                                        CAST(cur_IMASTER.ITAXCLSRVW AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.PKTYP), '''') AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                                        CAST(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.PKTYP) = ''R'' THEN ''RETAIL'' 
                                            WHEN TRIM(cur_DWWMSITEM.PKTYP) = ''I'' THEN ''INNERPACK''
                                            WHEN TRIM(cur_DWWMSITEM.PKTYP) = ''C'' THEN ''CASE''
                                            ELSE ''UNKNOWN''
                                            END AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                                        CAST(COALESCE(cur_DWWMSITEM.PKLEN, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                                        CAST(COALESCE(cur_DWWMSITEM.PKWID, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                                        CAST(COALESCE(cur_DWWMSITEM.PKHGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                                        CAST(COALESCE(cur_DWWMSITEM.PKWGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.PKLWHF), '''') AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.CSQTYF), '''') AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                                        CAST(COALESCE(cur_DWWMSITEM.CSLEN, 0) AS DECIMAL(12,4)) AS CASE_LENGTH,
                                        CAST(COALESCE(cur_DWWMSITEM.CSWID, 0) AS DECIMAL(12,4)) AS CASE_WIDTH,
                                        CAST(COALESCE(cur_DWWMSITEM.CSHGT, 0) AS DECIMAL(12,4)) AS CASE_HEIGHT,
                                        CAST(COALESCE(cur_DWWMSITEM.CSWGT, 0) AS DECIMAL(12,4)) AS CASE_WEIGHT,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.CSLWHF), '''') AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                                        CAST(COALESCE(cur_DWWMSITEM.PTCSQTY, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                                        CAST(COALESCE(cur_DWWMSITEM.PTCSLYR, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                                        CAST(COALESCE(cur_DWWMSITEM.PTLEN, 0) AS DECIMAL(12,4)) AS PALLET_LENGTH,
                                        CAST(COALESCE(cur_DWWMSITEM.PTWID, 0) AS DECIMAL(12,4)) AS PALLET_WIDTH,
                                        CAST(COALESCE(cur_DWWMSITEM.PTHGT, 0) AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                                        CAST(COALESCE(cur_DWWMSITEM.PTWGT, 0) AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.PTLWHF), '''') AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.SHIPCLASS), '''') AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                                        CAST(COALESCE(cur_DWWMSITEM.DOTID, -2) AS INTEGER) AS DOT_CLASS_NUMBER,
                                        CAST(COALESCE(cur_DWWMSITEM.DOTID2, -2) AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                                        CAST(COALESCE(cur_DWWMSITEM.CNTDESC, '''') AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.KFF) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                                        CAST(CASE
                                            WHEN TRIM(cur_DWWMSITEM.FLR) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.RETNEW) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.RETCORE) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.RETWAR) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.RETREC) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.RETMANO) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                                        CAST(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.RETOSP) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                                        CAST(''1900-01-01'' AS DATE) AS HAZARDOUS_UPDATE_DATE,
                                        CAST(0 AS DECIMAL(12,4)) AS PIECE_LENGTH,
                                        CAST(0 AS DECIMAL(12,4)) AS PIECE_WIDTH,
                                        CAST(0 AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                                        CAST(0 AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                                        CAST(0 AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.INCATALOG),'''') AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                                        CAST(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''B'' THEN ''NOT LOADED TO ONLINE CATALOG/BRICK AND MORTAR - DISPLAY IN STORE ONLY''
                                            WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''D'' THEN ''ALLOWED ONLINE, PICK UP IN STORE''
                                            WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''O'' THEN ''ONLINE CATALOG''
                                            WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''P'' THEN ''PROFESSIONAL CATALOG''
                                            WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''R'' THEN ''RETAIL CATALOG''
                                            WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''Y'' THEN ''ALL CATALOGS''
                                            ELSE ''UNKNOWN''
                                            END AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.ALWSPECORD) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                                        CAST(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.SPCORDONLY) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.SUPLCCD),'''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                                        CAST(COALESCE(HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE, ''1900-01-01'') AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.LONGDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                                        CAST(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.EWASTE) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,' || '
                                        CAST(COALESCE(cur_DWWMSITEM.STRMINSALE,0) AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                                        CAST(COALESCE(cur_DWWMSITEM.MSRP,0) AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                                        CAST(COALESCE(cur_DWWMSITEM.MAXCARQTY,0) AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                                        CAST(COALESCE(cur_DWWMSITEM.MINCARQTY,0) AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.ESNTLHRDPT),'''') AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.IPFLG), '''') AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                                        CAST(COALESCE(cur_DWWMSITEM.IPQTY, 0) AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                                        CAST(COALESCE(cur_DWWMSITEM.IPLEN, 0) AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                                        CAST(COALESCE(cur_DWWMSITEM.IPWID, 0) AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                                        CAST(COALESCE(cur_DWWMSITEM.IPHGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                                        CAST(COALESCE(cur_DWWMSITEM.IPWGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                                        CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                                        CAST(TRIM(COALESCE(cur_ECPARTS.PART_NUMBER, cur_AAIAVEND.OCAT_PART_NUMBER, '''')) AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                                        CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DISPLAY),'''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                                        CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                                        CAST(COALESCE(TRIM(cur_ECPARTS.SPANISH_PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                                        CAST(COALESCE(cur_ECPARTS.SUGGESTED_ORDER_QTY, 0) AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                                        --INFAETL-11515 begin changes
                                        CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                                        CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                                        CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                                        CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                                        CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                                        CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                                        CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                                        CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                                        --INFAETL-11515 end changes
                                        CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                                        CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                                        CAST(COALESCE(
                                            CASE
                                                WHEN TRIM(cur_CMISFILE.LSTOCK) = '''' THEN ''Y''
                                                ELSE ''N''
                                                END, ''Y'') AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                                WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                                                ELSE ''N''
                                                END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                                        CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                                        CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                                        CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                                        CAST(COALESCE(cur_DWWMSITEM.DUNS#,-2) AS BIGINT) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                                        CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                                        CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                                        CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                                        CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                                        CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                                        CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                                        CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                                        CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                                        CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                                        CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                                        CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                                        CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                                        CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                                        CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                                        CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                                        CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                                        CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                                        CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                                        CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                                        CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                                        CAST(COALESCE(INT(trim(cur_HAZLION.NUMBER_BATT_IN_PACK)), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                                        CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                                        ' || ' CAST(COALESCE(
                                            CASE
                                                WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                                                ELSE ''N''
                                                END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                                        CAST(COALESCE(
                                            CASE
                                                WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                                                ELSE ''N''
                                                END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                                        CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                                        CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(256 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                                        CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(256 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                                        CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                                        CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                                        CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                                        CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                                        CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                                        CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                                        CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                                        CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                                        CAST(COALESCE(cur_IMASTER.ISDCL, 0) AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                                        CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                                        CAST(COALESCE(cur_IMASTER.ICNVCPACK, 0) AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                                        CAST(COALESCE(TRIM(cur_IMASTER.ICNVCDESC), '''') AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                                        CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                                        CAST(COALESCE(HUB_LOAD_DIM_DATE_IRDATE.FULL_DATE ,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                                        CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                                        CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                                        CAST(COALESCE(HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE ,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                                        CAST(COALESCE(HUB_LOAD_DIM_DATE_DIMUPDDTE.FULL_DATE,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                                        CAST(COALESCE(
                                             TIME(
                                                  CASE
                                                  WHEN cur_DWWMSITEM.DIMUPDTME = 0 THEN ''00:00:00''
                                                  ELSE LEFT(''0'' || cur_DWWMSITEM.DIMUPDTME, 2) || '':'' || SUBSTRING(RIGHT(''0'' || cur_DWWMSITEM.DIMUPDTME, 6), 3, 2) || '':'' || RIGHT(cur_DWWMSITEM.DIMUPDTME, 2) 
                                                  END
                                             ),
                                        ''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.DIMUPDUSR), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.FNRDNS), '''') AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.CNTRYOFORG), '''') AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.EAS), '''') AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                                        CAST(
                                            CASE
                                            WHEN TRIM(cur_DWWMSITEM.EXFRESHPOS) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                                        CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                                        CAST(''00:00:00'' AS TIME) AS HAZARDOUS_UPDATE_TIME,
                                        CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                                        CAST(COALESCE(cur_IMASTER.ILISTP, 0) AS DECIMAL(12,4)) AS LIST_PRICE,
                                        CAST(COALESCE(cur_IMASTER.IUSRP, 0) AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                                        CAST(COALESCE(cur_DWWMSITEM.MAPPRICE, 0) AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                                        CAST(COALESCE(cur_DWWMSITEM.MAPEFFDATE, ''1900-01-01'') AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                                        CAST(COALESCE(cur_IMASTER.IMINSQ, 0) AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                                        CAST(COALESCE(TRIM(cur_IMASTER.IPACKSIZE), '''') AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                                        CAST(0 AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                                        CAST(COALESCE(cur_IMASTER.IPRCCOST, 0) AS DECIMAL(12,4)) AS PRICING_COST,
                                        CAST(0 AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                                        CAST(0 AS DECIMAL(12,4)) AS RETAIL_CORE,
                                        CAST(COALESCE(cur_DWWMSITEM.RTHGT, 0) AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                                        CAST(COALESCE(cur_DWWMSITEM.RTLEN, 0) AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.RTUOMDSC), '''') AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                                        CAST(COALESCE(cur_DWWMSITEM.RTWID, 0) AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                                        CAST(COALESCE(TRIM(cur_IMASTER.ISLSPACK), '''') AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                                        CAST(COALESCE(
                                            CASE
                                            WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                                            ELSE ''N''
                                            END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                                        CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                                        CAST(COALESCE(cur_DWWMSITEM.BASECOST, 0) AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                                        CAST(COALESCE(cur_DWWMSITEM.SUPSITEM, '''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                                        CAST(COALESCE(cur_DWWMSITEM.SUPSLINE, '''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                                        CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                                        CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                                        CAST(COALESCE(cur_CATEGORY.LOADTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                                        CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                                        CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                                        CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                                        CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                                        CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                                        CAST(COALESCE(HUB_LOAD_DIM_DATE_UPDDTE.FULL_DATE,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                                        CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                                        CAST(COALESCE(
                                             TIME(
                                                  CASE
                                                  WHEN cur_DWWMSITEM.UPDTME = 0 THEN ''00:00:00''
                                                  ELSE LEFT(''0'' || cur_DWWMSITEM.UPDTME, 2) || '':'' || SUBSTRING(RIGHT(''0'' || cur_DWWMSITEM.UPDTME, 6), 3, 2) || '':'' || RIGHT(cur_DWWMSITEM.UPDTME, 2) 
                                                  END
                                             ),''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                                        CAST(COALESCE(TRIM(cur_DWWMSITEM.UPDUSR), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                                        CAST(COALESCE(cur_IMASTER.IFJOBR, 0) AS DECIMAL(12,4)) AS VIP_JOBBER,
                                        CAST(cur_IMASTER.IWHSCORE AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                                        CAST(cur_IMASTER.IWHSCOST AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                                        --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                                        ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                                        CAST(''N'' AS VARCHAR(1)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                                            '|| quote_literal(v_etl_source_table_1) || ' AS ETL_SOURCE_TABLE_NAME,
                                        CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                                        CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                                        ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                                        ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS
                                    FROM EDW_STAGING.CUR_IVB_IMASTER cur_IMASTER
                                    LEFT JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_IMASTER.ILINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_IMASTER.IITEM#)
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_IMASTER.ILINE AND cur_CATEGORY.ITEM = cur_IMASTER.IITEM#
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY 
                                    LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_IMASTER.ILINE AND cur_ICLINCTL.PLCD = cur_IMASTER.IPLCD AND cur_ICLINCTL.REGION = 0
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_IMASTER.ILINE 
                                                AND cur_DWASGN_DWEMP.PLCD = cur_IMASTER.IPLCD AND cur_DWASGN_DWEMP.SUBC = cur_IMASTER.IPCODE    
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_IMASTER.ILINE AND cur_REPLENISHMENT_ANALYST.PLCD = cur_IMASTER.IPLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_IMASTER.IPCODE     
                                    LEFT JOIN EDW_STAGING.CUR_IVB_DWWMSITEM AS cur_DWWMSITEM ON cur_DWWMSITEM.LINE = cur_IMASTER.ILINE AND cur_DWWMSITEM.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_IMASTER.ILINE AND cur_CMISFILE.LPLCD = cur_IMASTER.IPLCD AND cur_CMISFILE.LSUBC = cur_IMASTER.IPCODE
                                    LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX cur_PMATRIX on cur_IMASTER.ILINE = cur_PMATRIX.line and cast(cur_IMASTER.IPCODE as decimal) = cur_PMATRIX.subc and cast(cur_IMASTER.IPLCD as decimal) = cur_PMATRIX.plcd
                                    -- EDW_STAGING <-> EDW_STAGING 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_IMASTER.ILINE AND cur_HAZLION.ITEM_NAME = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, 
                                                      LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, 
                                                      LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                                                FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                                                GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_IMASTER.ILINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, 
                                                      LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                                                      LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                                                FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                                                GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_IMASTER.ILINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_IMASTER.ILINE AND cur_EXPECCN.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                                                FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                                                GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_IMASTER.ILINE AND cur_EXPHTS.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_IMASTER.ILINE AND cur_EXPUSML.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                                                FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                                                GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_IMASTER.ILINE AND cur_EXPSCDB_LA.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS ON cur_ECPARTS.LINE = cur_IMASTER.ILINE AND cur_ECPARTS.ITEMNUMBER = cur_IMASTER.IITEM# --?
                                    LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                                FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                                GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND  
                                                ON cur_AAIAVEND.OREILLY_LINE = cur_IMASTER.ILINE AND cur_AAIAVEND.KEY_ITEM = cur_IMASTER.IITEM# --114348766
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_LSTCOSTUPD ON HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE = cur_DWWMSITEM.LSTCOSTUPD
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DATELCCHG ON HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE = cur_DWWMSITEM.DATELCCHG
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_UPDDTE ON HUB_LOAD_DIM_DATE_UPDDTE.DATE_ID = cur_DWWMSITEM.UPDDTE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_IRDATE ON HUB_LOAD_DIM_DATE_IRDATE.DATE_ID = 
                                                (CASE 
                                                WHEN RIGHT(cur_IMASTER.IRDATE, 2) <= RIGHT(YEAR(CURRENT_DATE), 2) 
                                                    THEN ''20'' || RIGHT(cur_IMASTER.IRDATE, 2) || LEFT(''0'' || cur_IMASTER.IRDATE , 2) || ''01''
                                                ELSE ''19'' || RIGHT(cur_IMASTER.IRDATE, 2) || LEFT(''0'' || cur_IMASTER.IRDATE , 2) || ''01''
                                                END )
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DIMUPDDTE ON HUB_LOAD_DIM_DATE_DIMUPDDTE.DATE_ID = cur_DWWMSITEM.DIMUPDDTE         
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ACCOUNT_NUMBER= cur_CMISFILE.LVACCT
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = BIGINT(cur_PMATRIX.RA_TMN)
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                                    LEFT JOIN ODATA.VW_DIM_WARRANTY_CODE AS VW_DIM_WARRANTY_CODE_WARRANTY_CODE ON VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE = cur_IMASTER.IWARRC
                                    LEFT JOIN ODATA.VW_DIM_POPULARITY_CODE AS VW_DIM_POPULARITY_CODE_DESC ON VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE = TRIM(cur_IMASTER.IPOPCD)
                                    LEFT JOIN ODATA.VW_DIM_POPULARITY_TREND_CODE AS VW_DIM_POPULARITY_TREND_CODE_DESC ON VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE = TRIM(cur_IMASTER.ITRNDC)
                                    LEFT JOIN EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK ON cur_WHNONSTK.WHNLINE = cur_IMASTER.ILINE AND cur_WHNONSTK.WHNITEM = cur_IMASTER.IITEM# 
                                    --INFAETL-11515 add the following 3 joins
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                    WHERE hub_DIM_PRODUCT.PRODUCT_ID IS NULL
                                    AND (' || quote_literal(v_job_execution_starttime) || ' <= cur_IMASTER.CREATE_TIMESTAMP OR cur_IMASTER.CREATE_TIMESTAMP IS NULL
                                    OR ' || quote_literal(v_job_execution_starttime) || ' <= cur_IMASTER.LOAD_TIMESTAMP OR cur_IMASTER.LOAD_TIMESTAMP IS NULL)  WITH UR;';             

                        
                EXECUTE IMMEDIATE v_str_sql;
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

               /* Debugging Logging */
                IF (V_SQL_CODE <> 0) THEN  --  Warning
                   SET V_SQL_OK = FALSE;
                   SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
            
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                   SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTER Insert> '||
                       '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                       '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                       ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                       ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                       ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '06. IMASTER Insert', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
                    
                 --Populate Hub Load Table - IMASTER 1ST MERGE
        --1st merge step 1, drop "source" table

            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        

            IF V_SQL_OK THEN
               SET v_str_sql = 'DROP TABLE ' || v_staging_database_name || '.SESSION_TMP_IMASTER_UPDATE_SOURCE if exists;';
               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                IF (V_SQL_CODE <> 0) THEN  --  Warning
                   SET V_SQL_OK = FALSE;
                   SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
            
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                   SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTER DROP update table> '||
                       '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                       '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                       ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                       ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                       ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '07. IMASTER_UPDATE preprocess table DROP', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

            END IF; -- V_SQL_OK

            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        
        --1st Merge step 2, recreate "source" table   
            IF V_SQL_OK THEN            
               
               SET v_str_sql = clob('CREATE  TABLE ') ||  v_staging_database_name || '.SESSION_TMP_IMASTER_UPDATE_SOURCE
               AS (          SELECT hub_DIM_PRODUCT.PRODUCT_ID,
                                    CAST(TRIM(cur_IMASTER.ILINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTER.IITEM#) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IDESC), TRIM(cur_ECPARTS.SHORT_DESCRIPTION)) AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SUBCATNUM,-2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTER.IPLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                                    CAST(cur_IMASTER.IPCODE AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                                    CAST(COALESCE(cur_IMASTER."IMFRI#", ''-2'') AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                                    CAST(COALESCE(cur_IMASTER.ISLINE, ''-2'' ) AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_IMASTER.ISITM#, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST(cur_IMASTER.ICNTRL AS INTEGER) AS SORT_CONTROL_NUMBER,
                                    CAST(TRIM(cur_IMASTER.IPSDSC) AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IPOPCD), '''') AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                                    CAST(COALESCE(VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE_DESCRIPTION , ''DEFAULT'') AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTER.ITRNDC),'''') AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                                    CAST(COALESCE(VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE_DESCRIPTION , ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                                    CAST(cur_IMASTER.IJSC AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                                    CAST(cur_IMASTER.IJUM AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTER.IWUM AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTER.IWSQTY AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                                    CAST(COALESCE(cur_IMASTER.IWGT, 0) AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                                    CAST(COALESCE(cur_IMASTER.IQTYPC, '''') AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                                    CAST(cur_IMASTER.ICSQTY AS DECIMAL(12,4)) AS CASE_QUANTITY,
                                    CAST(cur_IMASTER.ISTDPK AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                                    CAST(COALESCE(cur_IMASTER.IPBNEP, 0) AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                                    CAST(cur_IMASTER.IJOBRP AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                                    CAST(cur_IMASTER.ICOSTP AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                                    CAST(cur_IMASTER.ICOREP AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                                    CAST(COALESCE(cur_IMASTER.IORCST, 0) AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                                    CAST(cur_IMASTER.IJCOST AS DECIMAL(12,4)) AS JOBBER_COST,
                                    CAST(cur_IMASTER.IJCORE AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_IMASTER.IOFM) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_IMASTER.ITAXED) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                                    CAST(COALESCE(cur_IMASTER.IQORDR, '''') AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                                    CAST(COALESCE(cur_IMASTER.IJDQ,1) AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IRECCD), '''') AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                                    CAST(cur_IMASTER.IMSDS AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                                    CAST(TRIM(cur_IMASTER.IBARC) AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IWARRC), ''NONE'') AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                                    CAST(COALESCE(VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE_DESCRIPTION, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                                    CAST(COALESCE(cur_IMASTER.IINVC, 0) AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                                    CAST(COALESCE(cur_IMASTER.ICORC, 0) AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                                    CAST(COALESCE(cur_IMASTER.ICONITM, '''') AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                                    CAST(cur_IMASTER.IWJCRP AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                                    CAST(cur_IMASTER.IACQ1 AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                                    CAST(cur_IMASTER.IACQ2 AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IBUYMULT), -2) AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IBUYDESC), '''') AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                                    CAST(COALESCE(
                                        CASE 
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''1'' THEN ''1 - BLANK''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''2'' THEN ''2 - BLANK''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''AS'' THEN ''ASSORTED''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BD'' THEN ''BUNDLE''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BG'' THEN ''BAG''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BP'' THEN ''BLISTER PACK''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''BX'' THEN ''BOX''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''CD'' THEN ''CARDED''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''CS'' THEN ''CASE''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''EA'' THEN ''EACH''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''FT'' THEN ''FOOT''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''KT'' THEN ''KIT''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''PK'' THEN ''PACKAGE'' 
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''PR'' THEN ''PAIR''
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''RL'' THEN ''ROLL'' 
                                        WHEN TRIM(cur_IMASTER.IBUYDESC) = ''ST'' THEN ''SET''
                                        ELSE ''UNKNOWN''
                                        END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                                    CAST(cur_IMASTER.IVENCNVRF AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                                    CAST(cur_IMASTER.IVENCNVRQ AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                                    CAST(cur_IMASTER.IVENUOM AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTER.IUOMAMT AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                                    CAST(cur_IMASTER.IUOMQTY AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                                    CAST(cur_IMASTER.IUOMDESC AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST(cur_IMASTER.ITAXCLASS AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''ACC'' THEN ''NON CLOTHING ACCESSORIES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''AID'' THEN ''FIRST AID KITS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''APP'' THEN ''CLOTHING AND APPAREL''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B06'' THEN ''BATTERY UNDER 12 VOLTS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B06W'' THEN ''BATTERY UNDER 12 VOLTS UNDER WARRANTY''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B12'' THEN ''BATTERY 12 VOLTS OR HIGHER''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''B12W'' THEN ''BATTERY 12 VOLTS OR HIGHER UNDER WARRANTY''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BAT'' THEN ''BATTERY KIOSK''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BCR'' THEN ''BATTERY CORE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BTL'' THEN ''BOTTLE DEPOSIT''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''BTY'' THEN ''BATTERY FEE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CAB'' THEN ''POWER CABLES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CCD'' THEN ''COMMON CARRIER FOB DESTINATION''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CCO'' THEN ''COMMON CARRIER FOB ORGIN''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CDY'' THEN ''CANDY''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CEL'' THEN ''CELL PHONE BATTERIES AND CHARGERS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''CHM'' THEN ''CHEMICALS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''COK'' THEN ''CARBONATED SOFT DRINKS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''DUC'' THEN ''DUCT TAPE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''EMS'' THEN ''EMISSIONS PARTS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''EXP'' THEN ''EXPORT TAX''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FEX'' THEN ''FIRE EXTINGUISHER''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRE'' THEN ''FREON W/DEPOSIT''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRM'' THEN ''FARM MACHINERY AND EQUIPMENT PARTS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRT'' THEN ''FREIGHT DELIVERY''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FRZ'' THEN ''ANTIFREEZE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''FSL'' THEN ''FLASHLIGHTS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''GAS'' THEN ''GAS OR DIESEL FUEL TANKS OR CONTAINERS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''GEN'' THEN ''PORTABLE GENERATORS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''GFT'' THEN ''GIFT CARDS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''H2C'' THEN ''WATER CASES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''H2O'' THEN ''BOTTLE WATER''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''HAZ'' THEN ''HAZARDOUS WASTE REMOVAL''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''HBT'' THEN ''HOUSEHOLD BATTERIES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''HND'' THEN ''HANDLING CHARGE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''LAB'' THEN ''REPAIR OF MOTOR VEHICLES & TPP''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''MFG'' THEN ''MANUFACTURING MACHINERY AND EQUIPMENT PARTS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''NCD'' THEN ''NONCARBONDATED SOFT DRINKS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''NEW'' THEN ''NEW ITEMS AND CORES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''OAL'' THEN ''MOTOR OIL AND TRANS FLUID EXEMPT IN AL''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''OIL'' THEN ''MOTOR OIL AND TRANS FLUID ''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''PBF'' THEN ''PLASTIC BAG FEE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''PCR'' THEN ''PART CORE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''PNT'' THEN ''PAINT AND PAINT SUPPLIES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''RDO'' THEN ''RADIOS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''SHC'' THEN ''SHIPPING AND HANDLING COMBINED CHARGE''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''SNK'' THEN ''SNACK FOOD OTHER THAN CANDY''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''SUP'' THEN ''SUPPLIES''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TCK'' THEN ''TICKETS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TEC'' THEN ''SALES AND INSTALLER CLINICS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TIE'' THEN ''GROUND ANCHOR SYSTEM OR TIE DOWN KITS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TOL'' THEN ''TOOLS''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TPP'' THEN ''TANGIBLE PERSONAL PROPERTY''
                                        WHEN TRIM(cur_IMASTER.ITAXCLASS) = ''TRP'' THEN ''TARPAULINS OR WATERPROOF SHEETING''
                                        ELSE ''UNKNOWN''
                                        END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                                    CAST(cur_IMASTER.ITAXCLSRVW AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.PKTYP), '''') AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.PKTYP) = ''R'' THEN ''RETAIL'' 
                                        WHEN TRIM(cur_DWWMSITEM.PKTYP) = ''I'' THEN ''INNERPACK''
                                        WHEN TRIM(cur_DWWMSITEM.PKTYP) = ''C'' THEN ''CASE''
                                        ELSE ''UNKNOWN''
                                        END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                                    CAST(COALESCE(cur_DWWMSITEM.PKLEN, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                                    CAST(COALESCE(cur_DWWMSITEM.PKWID, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                                    CAST(COALESCE(cur_DWWMSITEM.PKHGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                                    CAST(COALESCE(cur_DWWMSITEM.PKWGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.PKLWHF), '''') AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.CSQTYF), '''') AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                                    CAST(COALESCE(cur_DWWMSITEM.CSLEN, 0) AS DECIMAL(12,4)) AS CASE_LENGTH,
                                    CAST(COALESCE(cur_DWWMSITEM.CSWID, 0) AS DECIMAL(12,4)) AS CASE_WIDTH,
                                    CAST(COALESCE(cur_DWWMSITEM.CSHGT, 0) AS DECIMAL(12,4)) AS CASE_HEIGHT,
                                    CAST(COALESCE(cur_DWWMSITEM.CSWGT, 0) AS DECIMAL(12,4)) AS CASE_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.CSLWHF), '''') AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(cur_DWWMSITEM.PTCSQTY, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                                    CAST(COALESCE(cur_DWWMSITEM.PTCSLYR, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                                    CAST(COALESCE(cur_DWWMSITEM.PTLEN, 0) AS DECIMAL(12,4)) AS PALLET_LENGTH,
                                    CAST(COALESCE(cur_DWWMSITEM.PTWID, 0) AS DECIMAL(12,4)) AS PALLET_WIDTH,
                                    CAST(COALESCE(cur_DWWMSITEM.PTHGT, 0) AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                                    CAST(COALESCE(cur_DWWMSITEM.PTWGT, 0) AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.PTLWHF), '''') AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.SHIPCLASS), '''') AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                                    CAST(COALESCE(cur_DWWMSITEM.DOTID, -2) AS INTEGER) AS DOT_CLASS_NUMBER,
                                    CAST(COALESCE(cur_DWWMSITEM.DOTID2, -2) AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                                    CAST(COALESCE(cur_DWWMSITEM.CNTDESC, '''') AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.KFF) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                                    CAST(CASE
                                        WHEN TRIM(cur_DWWMSITEM.FLR) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.RETNEW) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.RETCORE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.RETWAR) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.RETREC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                                    CAST(COALESCE(CASE
                                        WHEN TRIM(cur_DWWMSITEM.RETMANO) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                                    CAST(CASE
                                        WHEN TRIM(cur_DWWMSITEM.RETOSP) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                                    CAST(''1900-01-01'' AS DATE) AS HAZARDOUS_UPDATE_DATE,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.INCATALOG),'''') AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                                    CAST(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''B'' THEN ''NOT LOADED TO ONLINE CATALOG/BRICK AND MORTAR - DISPLAY IN STORE ONLY''
                                        WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''D'' THEN ''ALLOWED ONLINE, PICK UP IN STORE''
                                        WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''O'' THEN ''ONLINE CATALOG''
                                        WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''P'' THEN ''PROFESSIONAL CATALOG''
                                        WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''R'' THEN ''RETAIL CATALOG''
                                        WHEN TRIM(cur_DWWMSITEM.INCATALOG) = ''Y'' THEN ''ALL CATALOGS''
                                        ELSE ''UNKNOWN''
                                        END AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,' || '
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.ALWSPECORD) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                                    CAST(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.SPCORDONLY) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.SUPLCCD),'''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE, ''1900-01-01'') AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.LONGDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                                    CAST(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.EWASTE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                                    CAST(COALESCE(cur_DWWMSITEM.STRMINSALE,0) AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                                    CAST(COALESCE(cur_DWWMSITEM.MSRP,0) AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                                    CAST(COALESCE(cur_DWWMSITEM.MAXCARQTY,0) AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                                    CAST(COALESCE(cur_DWWMSITEM.MINCARQTY,0) AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.ESNTLHRDPT), '''') AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.IPFLG), '''') AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                                    CAST(COALESCE(cur_DWWMSITEM.IPQTY, 0) AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                                    CAST(COALESCE(cur_DWWMSITEM.IPLEN, 0) AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                                    CAST(COALESCE(cur_DWWMSITEM.IPWID, 0) AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                                    CAST(COALESCE(cur_DWWMSITEM.IPHGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                                    CAST(COALESCE(cur_DWWMSITEM.IPWGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                                    CAST(TRIM(COALESCE(cur_ECPARTS.PART_NUMBER, cur_AAIAVEND.OCAT_PART_NUMBER, '''')) AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DISPLAY),'''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.SPANISH_PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(cur_ECPARTS.SUGGESTED_ORDER_QTY, 0) AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                                    --INFAETL-11515 begin changes
                                    CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                                    CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                                    CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                                    --INFAETL-11515 end changes
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.full_date, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_CMISFILE.LSTOCK) = '''' THEN ''Y''
                                        ELSE ''N''
                                        END, ''Y'') AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                                    CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                                    CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                                    CAST(COALESCE(cur_DWWMSITEM.DUNS#, -2) AS BIGINT) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                                    CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                                    CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                                    CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                                    CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                                    CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                                    CAST(COALESCE(INT(trim(cur_HAZLION.NUMBER_BATT_IN_PACK)), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                                    CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(256 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(256 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                                    CAST(COALESCE(cur_IMASTER.ISDCL, 0) AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                                    CAST(COALESCE(cur_IMASTER.ICNVCPACK, 0) AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTER.ICNVCDESC), '''') AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_IRDATE.FULL_DATE ,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE ,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                                    ' || ' CAST(COALESCE(HUB_LOAD_DIM_DATE_DIMUPDDTE.FULL_DATE,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                                    CAST(COALESCE(
                                         TIME(
                                              CASE
                                              WHEN cur_DWWMSITEM.DIMUPDTME = 0 THEN ''00:00:00''
                                              ELSE LEFT(''0'' || cur_DWWMSITEM.DIMUPDTME, 2) || '':'' || SUBSTRING(RIGHT(''0'' || cur_DWWMSITEM.DIMUPDTME, 6), 3, 2) || '':'' || RIGHT(cur_DWWMSITEM.DIMUPDTME, 2) 
                                              END
                                            ), ''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.DIMUPDUSR), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.FNRDNS), '''') AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.CNTRYOFORG), '''') AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.EAS), '''') AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                                    CAST(
                                        CASE
                                        WHEN TRIM(cur_DWWMSITEM.EXFRESHPOS) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                                    CAST(''00:00:00'' AS TIME) AS HAZARDOUS_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                                    CAST(COALESCE(cur_IMASTER.ILISTP, 0) AS DECIMAL(12,4)) AS LIST_PRICE,
                                    CAST(COALESCE(cur_IMASTER.IUSRP, 0) AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                                    CAST(COALESCE(cur_DWWMSITEM.MAPPRICE, 0) AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                                    CAST(COALESCE(cur_DWWMSITEM.MAPEFFDATE, ''1900-01-01'') AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                                    CAST(COALESCE(cur_IMASTER.IMINSQ, 0) AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTER.IPACKSIZE), '''') AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                                    CAST(COALESCE(cur_IMASTER.IPRCCOST, 0) AS DECIMAL(12,4)) AS PRICING_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_CORE,
                                    CAST(COALESCE(cur_DWWMSITEM.RTHGT, 0) AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                                    CAST(COALESCE(cur_DWWMSITEM.RTLEN, 0) AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.RTUOMDSC), '''') AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST(COALESCE(cur_DWWMSITEM.RTWID, 0) AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                                    CAST(COALESCE(TRIM(cur_IMASTER.ISLSPACK), '''') AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                                    CAST(COALESCE(cur_DWWMSITEM.BASECOST, 0) AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                                    CAST(COALESCE(cur_DWWMSITEM.SUPSITEM, '''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST(COALESCE(cur_DWWMSITEM.SUPSLINE, '''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.LOADTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_UPDDTE.FULL_DATE,''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(
                                         TIME(
                                              CASE
                                              WHEN cur_DWWMSITEM.UPDTME = 0 THEN ''00:00:00''
                                              ELSE LEFT(''0'' || cur_DWWMSITEM.UPDTME, 2) || '':'' || SUBSTRING(RIGHT(''0'' || cur_DWWMSITEM.UPDTME, 6), 3, 2) || '':'' || RIGHT(cur_DWWMSITEM.UPDTME, 2) 
                                              END
                                             ),
                                            ''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_DWWMSITEM.UPDUSR), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                                    CAST(COALESCE(cur_IMASTER.IFJOBR, 0) AS DECIMAL(12,4)) AS VIP_JOBBER,
                                    CAST(cur_IMASTER.IWHSCORE AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                                    CAST(cur_IMASTER.IWHSCOST AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                                    --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                                    ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                                    CAST(''N'' AS VARCHAR(1)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                                    ' || quote_literal(v_etl_source_table_1) || ' AS ETL_SOURCE_TABLE_NAME,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                                    ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                                    ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS
                                    FROM EDW_STAGING.CUR_IVB_IMASTER cur_IMASTER
                                    JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_IMASTER.ILINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_IMASTER.IITEM#) 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_IMASTER.ILINE AND cur_CATEGORY.ITEM = cur_IMASTER.IITEM#
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY 
                                    LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_IMASTER.ILINE AND cur_ICLINCTL.PLCD = cur_IMASTER.IPLCD AND cur_ICLINCTL.REGION = 0
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_IMASTER.ILINE 
                                                AND cur_DWASGN_DWEMP.PLCD = cur_IMASTER.IPLCD AND cur_DWASGN_DWEMP.SUBC = cur_IMASTER.IPCODE    
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_IMASTER.ILINE AND cur_REPLENISHMENT_ANALYST.PLCD = cur_IMASTER.IPLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_IMASTER.IPCODE     
                                    LEFT JOIN EDW_STAGING.CUR_IVB_DWWMSITEM AS cur_DWWMSITEM ON cur_DWWMSITEM.LINE = cur_IMASTER.ILINE AND cur_DWWMSITEM.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_IMASTER.ILINE AND cur_CMISFILE.LPLCD = cur_IMASTER.IPLCD AND cur_CMISFILE.LSUBC = cur_IMASTER.IPCODE
                                    LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX cur_PMATRIX on cur_IMASTER.ILINE = cur_PMATRIX.line and cast(cur_IMASTER.IPCODE as decimal) = cur_PMATRIX.subc and cast(cur_IMASTER.IPLCD as decimal) = cur_PMATRIX.plcd
                                    LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_IMASTER.ILINE AND cur_HAZLION.ITEM_NAME = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                                                FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                                                GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_IMASTER.ILINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                                                FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                                                GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_IMASTER.ILINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_IMASTER.ILINE AND cur_EXPECCN.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                                                FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                                                GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_IMASTER.ILINE AND cur_EXPHTS.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_IMASTER.ILINE AND cur_EXPUSML.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                                                FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                                                GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_IMASTER.ILINE AND cur_EXPSCDB_LA.ITEM = cur_IMASTER.IITEM# --337921
                                    LEFT JOIN EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS ON cur_ECPARTS.LINE = cur_IMASTER.ILINE AND cur_ECPARTS.ITEMNUMBER = cur_IMASTER.IITEM# --?
                                    LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                                FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                                GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND 
                                                ON cur_AAIAVEND.OREILLY_LINE = cur_IMASTER.ILINE AND cur_AAIAVEND.KEY_ITEM = cur_IMASTER.IITEM# --114348766
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_LSTCOSTUPD ON HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE = cur_DWWMSITEM.LSTCOSTUPD
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DATELCCHG ON HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE = cur_DWWMSITEM.DATELCCHG
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_UPDDTE ON HUB_LOAD_DIM_DATE_UPDDTE.DATE_ID = cur_DWWMSITEM.UPDDTE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_IRDATE ON HUB_LOAD_DIM_DATE_IRDATE.DATE_ID = 
                                                (CASE 
                                                WHEN RIGHT(cur_IMASTER.IRDATE, 2) <= RIGHT(YEAR(CURRENT_DATE), 2) 
                                                    THEN ''20'' || RIGHT(cur_IMASTER.IRDATE, 2) || LEFT(''0'' || cur_IMASTER.IRDATE , 2) || ''01''
                                                ELSE ''19'' || RIGHT(cur_IMASTER.IRDATE, 2) || LEFT(''0'' || cur_IMASTER.IRDATE , 2) || ''01''
                                                END )
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DIMUPDDTE ON HUB_LOAD_DIM_DATE_DIMUPDDTE.DATE_ID = cur_DWWMSITEM.DIMUPDDTE         
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = INTEGER(cur_PMATRIX.RA_TMN)
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON TRIM(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE) = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                                    LEFT JOIN ODATA.VW_DIM_WARRANTY_CODE AS VW_DIM_WARRANTY_CODE_WARRANTY_CODE ON VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE = cur_IMASTER.IWARRC
                                    LEFT JOIN ODATA.VW_DIM_POPULARITY_CODE AS VW_DIM_POPULARITY_CODE_DESC ON VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE = cur_IMASTER.IPOPCD
                                    LEFT JOIN ODATA.VW_DIM_POPULARITY_TREND_CODE AS VW_DIM_POPULARITY_TREND_CODE_DESC ON VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE = cur_IMASTER.ITRNDC
                                    LEFT JOIN EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK ON CUR_WHNONSTK.WHNLINE = cur_IMASTER.ILINE AND cur_WHNONSTK.WHNITEM = cur_IMASTER.IITEM#
                                    --INFAETL-11515 add the following 3 joins
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                                WHERE hub_DIM_PRODUCT.PRODUCT_ID IS NOT NULL
--                                                AND (' || quote_literal(v_job_execution_starttime) || ' <= cur_IMASTER.CREATE_TIMESTAMP OR cur_IMASTER.CREATE_TIMESTAMP IS NULL
--                                                OR ' || quote_literal(v_job_execution_starttime) || ' <= cur_IMASTER.LOAD_TIMESTAMP OR cur_IMASTER.LOAD_TIMESTAMP IS NULL)

                  ) WITH DATA
                  DISTRIBUTE ON HASH("PRODUCT_ID")
                  IN TS_EDW
                  ORGANIZE BY COLUMN;';

                                 /* Debugging Logging */
               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTER recreate update source table> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '08. IMASTER_UPDATE preprocess table CTAS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

            END IF; -- v_SQL_OK prior to source CTAS (near line 926)
            
            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        
            IF V_SQL_OK THEN
    
               SET v_str_sql =   CLOB('UPDATE ') || v_staging_database_name || '.' || v_staging_table_name || ' AS tgt ' || 
                 'SET 
                            tgt.LINE_DESCRIPTION=src.LINE_DESCRIPTION,
                            tgt.ITEM_DESCRIPTION=src.ITEM_DESCRIPTION,
                            tgt.SEGMENT_NUMBER=src.SEGMENT_NUMBER,
                            tgt.SEGMENT_DESCRIPTION=src.SEGMENT_DESCRIPTION,
                            tgt.SUB_CATEGORY_NUMBER=src.SUB_CATEGORY_NUMBER,
                            tgt.SUB_CATEGORY_DESCRIPTION=src.SUB_CATEGORY_DESCRIPTION,
                            tgt.CATEGORY_NUMBER=src.CATEGORY_NUMBER,
                            tgt.CATEGORY_DESCRIPTION=src.CATEGORY_DESCRIPTION,
                            tgt.PRODUCT_LINE_CODE=src.PRODUCT_LINE_CODE,
                            tgt.SUB_CODE=src.SUB_CODE,
                            tgt.MANUFACTURE_ITEM_NUMBER_CODE=src.MANUFACTURE_ITEM_NUMBER_CODE,
                            tgt.SUPERSEDED_LINE_CODE=src.SUPERSEDED_LINE_CODE,
                            tgt.SUPERSEDED_ITEM_NUMBER_CODE=src.SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SORT_CONTROL_NUMBER=src.SORT_CONTROL_NUMBER,
                            tgt.POINT_OF_SALE_DESCRIPTION=src.POINT_OF_SALE_DESCRIPTION,
                            tgt.POPULARITY_CODE=src.POPULARITY_CODE,
                            tgt.POPULARITY_CODE_DESCRIPTION=src.POPULARITY_CODE_DESCRIPTION,
                            tgt.POPULARITY_TREND_CODE=src.POPULARITY_TREND_CODE,
                            tgt.POPULARITY_TREND_CODE_DESCRIPTION=src.POPULARITY_TREND_CODE_DESCRIPTION,
                            tgt.LINE_IS_MARINE_SPECIFIC_FLAG=src.LINE_IS_MARINE_SPECIFIC_FLAG,
                            tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE=src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                            tgt.LINE_IS_FLEET_SPECIFIC_CODE=src.LINE_IS_FLEET_SPECIFIC_CODE,
                            tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE=src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                            tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG=src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                            tgt.JOBBER_SUPPLIER_CODE=src.JOBBER_SUPPLIER_CODE,
                            tgt.JOBBER_UNIT_OF_MEASURE_CODE=src.JOBBER_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE=src.WAREHOUSE_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_SELL_QUANTITY=src.WAREHOUSE_SELL_QUANTITY,
                            tgt.RETAIL_WEIGHT=src.RETAIL_WEIGHT,
                            tgt.QUANTITY_PER_CAR=src.QUANTITY_PER_CAR,
                            tgt.CASE_QUANTITY=src.CASE_QUANTITY,
                            tgt.STANDARD_PACKAGE=src.STANDARD_PACKAGE,
                            tgt.PAINT_BODY_AND_EQUIPMENT_PRICE=src.PAINT_BODY_AND_EQUIPMENT_PRICE,
                            tgt.WAREHOUSE_JOBBER_PRICE=src.WAREHOUSE_JOBBER_PRICE,
                            tgt.WAREHOUSE_COST_WUM=src.WAREHOUSE_COST_WUM,
                            tgt.WAREHOUSE_CORE_WUM=src.WAREHOUSE_CORE_WUM,
                            tgt.OREILLY_COST_PRICE=src.OREILLY_COST_PRICE,
                            tgt.JOBBER_COST=src.JOBBER_COST,
                            tgt.JOBBER_CORE_PRICE=src.JOBBER_CORE_PRICE,
                            tgt.OUT_FRONT_MERCHANDISE_FLAG=src.OUT_FRONT_MERCHANDISE_FLAG,
                            tgt.ITEM_IS_TAXED_FLAG=src.ITEM_IS_TAXED_FLAG,
                            tgt.QUANTITY_ORDER_ITEM_FLAG=src.QUANTITY_ORDER_ITEM_FLAG,
                            tgt.JOBBER_DIVIDE_QUANTITY=src.JOBBER_DIVIDE_QUANTITY,
                            tgt.ITEM_DELETE_FLAG_RECORD_CODE=src.ITEM_DELETE_FLAG_RECORD_CODE,
                            tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE=src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                            tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE=src.PRIMARY_UNIVERSAL_PRODUCT_CODE,
                            tgt.WARRANTY_CODE=src.WARRANTY_CODE,
                            tgt.WARRANTY_CODE_DESCRIPTION=src.WARRANTY_CODE_DESCRIPTION,
                            tgt.INVOICE_COST_WUM_INVOICE_COST=src.INVOICE_COST_WUM_INVOICE_COST,
                            tgt.INVOICE_CORE_WUM_CORE_COST=src.INVOICE_CORE_WUM_CORE_COST,
                            tgt.IS_CONSIGNMENT_ITEM_FLAG=src.IS_CONSIGNMENT_ITEM_FLAG,
                            tgt.WAREHOUSE_JOBBER_CORE_PRICE=src.WAREHOUSE_JOBBER_CORE_PRICE,
                            tgt.ACQUISITION_FIELD_1_CODE=src.ACQUISITION_FIELD_1_CODE,
                            tgt.ACQUISITION_FIELD_2_CODE=src.ACQUISITION_FIELD_2_CODE,
                            tgt.BUY_MULTIPLE=src.BUY_MULTIPLE,
                            tgt.BUY_MULTIPLE_CODE=src.BUY_MULTIPLE_CODE,
                            tgt.BUY_MULTIPLE_CODE_DESCRIPTION=src.BUY_MULTIPLE_CODE_DESCRIPTION,
                            tgt.SUPPLIER_CONVERSION_FACTOR_CODE=src.SUPPLIER_CONVERSION_FACTOR_CODE,
                            tgt.SUPPLIER_CONVERSION_QUANTITY=src.SUPPLIER_CONVERSION_QUANTITY,
                            tgt.SUPPLIER_UNIT_OF_MEASURE_CODE=src.SUPPLIER_UNIT_OF_MEASURE_CODE,
                            tgt.UNIT_OF_MEASURE_AMOUNT=src.UNIT_OF_MEASURE_AMOUNT,
                            tgt.UNIT_OF_MEASURE_QUANTITY=src.UNIT_OF_MEASURE_QUANTITY,
                            tgt.UNIT_OF_MEASURE_DESCRIPTION=src.UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_CODE=src.TAX_CLASSIFICATION_CODE,
                            tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION=src.TAX_CLASSIFICATION_CODE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE=src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                            tgt.DISTRIBUTION_CENTER_PICK_LENGTH=src.DISTRIBUTION_CENTER_PICK_LENGTH,
                            tgt.DISTRIBUTION_CENTER_PICK_WIDTH=src.DISTRIBUTION_CENTER_PICK_WIDTH,
                            tgt.DISTRIBUTION_CENTER_PICK_HEIGHT=src.DISTRIBUTION_CENTER_PICK_HEIGHT,
                            tgt.DISTRIBUTION_CENTER_PICK_WEIGHT=src.DISTRIBUTION_CENTER_PICK_WEIGHT,
                            tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE=src.PICK_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASE_QUANTITY_CODE=src.CASE_QUANTITY_CODE,
                            tgt.CASE_LENGTH=src.CASE_LENGTH,
                            tgt.CASE_WIDTH=src.CASE_WIDTH,
                            tgt.CASE_HEIGHT=src.CASE_HEIGHT,
                            tgt.CASE_WEIGHT=src.CASE_WEIGHT,
                            tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE=src.CASE_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASES_PER_PALLET=src.CASES_PER_PALLET,
                            tgt.CASES_PER_PALLET_LAYER=src.CASES_PER_PALLET_LAYER,
                            tgt.PALLET_LENGTH=src.PALLET_LENGTH,
                            tgt.PALLET_WIDTH=src.PALLET_WIDTH,
                            tgt.PALLET_HEIGHT=src.PALLET_HEIGHT,
                            tgt.PALLET_WEIGHT=src.PALLET_WEIGHT,
                            tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE=src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.SHIPMENT_CLASS_CODE=src.SHIPMENT_CLASS_CODE,
                            tgt.DOT_CLASS_NUMBER=src.DOT_CLASS_NUMBER,
                            tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER=src.DOT_CLASS_FOR_MSDS_ID_NUMBER,
                            tgt.CONTAINER_DESCRIPTION=src.CONTAINER_DESCRIPTION,
                            tgt.KEEP_FROM_FREEZING_FLAG=src.KEEP_FROM_FREEZING_FLAG,
                            tgt.FLIGHT_RESTRICTED_FLAG=src.FLIGHT_RESTRICTED_FLAG,
                            tgt.ALLOW_NEW_RETURNS_FLAG=src.ALLOW_NEW_RETURNS_FLAG,
                            tgt.ALLOW_CORE_RETURNS_FLAG=src.ALLOW_CORE_RETURNS_FLAG,
                            tgt.ALLOW_WARRANTY_RETURNS_FLAG=src.ALLOW_WARRANTY_RETURNS_FLAG,
                            tgt.ALLOW_RECALL_RETURNS_FLAG=src.ALLOW_RECALL_RETURNS_FLAG,
                            tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG=src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                            tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG=src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                            tgt.HAZARDOUS_UPDATE_DATE=src.HAZARDOUS_UPDATE_DATE,
                            tgt.PIECE_LENGTH=src.PIECE_LENGTH,
                            tgt.PIECE_WIDTH=src.PIECE_WIDTH,
                            tgt.PIECE_HEIGHT=src.PIECE_HEIGHT,
                            tgt.PIECE_WEIGHT=src.PIECE_WEIGHT,
                            tgt.PIECES_INNER_PACK=src.PIECES_INNER_PACK,
                            tgt.IN_CATALOG_CODE=src.IN_CATALOG_CODE,
                            tgt.IN_CATALOG_CODE_DESCRIPTION=src.IN_CATALOG_CODE_DESCRIPTION,
                            tgt.ALLOW_SPECIAL_ORDER_FLAG=src.ALLOW_SPECIAL_ORDER_FLAG,
                            tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG=src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                            tgt.SUPPLIER_LIFE_CYCLE_CODE=src.SUPPLIER_LIFE_CYCLE_CODE,
                            tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE=src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                            tgt.LONG_DESCRIPTION=src.LONG_DESCRIPTION,
                            tgt.ELECTRONIC_WASTE_FLAG=src.ELECTRONIC_WASTE_FLAG,
                            tgt.STORE_MINIMUM_SALE_QUANTITY=src.STORE_MINIMUM_SALE_QUANTITY,
                            tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE=src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                            tgt.MAXIMUM_CAR_QUANTITY=src.MAXIMUM_CAR_QUANTITY,
                            tgt.MINIMUM_CAR_QUANTITY=src.MINIMUM_CAR_QUANTITY,
                            tgt.ESSENTIAL_HARD_PART_CODE=src.ESSENTIAL_HARD_PART_CODE,
                            tgt.INNER_PACK_CODE=src.INNER_PACK_CODE,
                            tgt.INNER_PACK_QUANTITY=src.INNER_PACK_QUANTITY,
                            tgt.INNER_PACK_LENGTH=src.INNER_PACK_LENGTH,
                            tgt.INNER_PACK_WIDTH=src.INNER_PACK_WIDTH,
                            tgt.INNER_PACK_HEIGHT=src.INNER_PACK_HEIGHT,
                            tgt.INNER_PACK_WEIGHT=src.INNER_PACK_WEIGHT,
                            tgt.BRAND_CODE=src.BRAND_CODE,
                            tgt.PART_NUMBER_CODE=src.PART_NUMBER_CODE,
                            tgt.PART_NUMBER_DISPLAY_CODE=src.PART_NUMBER_DISPLAY_CODE,
                            tgt.PART_NUMBER_DESCRIPTION=src.PART_NUMBER_DESCRIPTION,
                            tgt.SPANISH_PART_NUMBER_DESCRIPTION=src.SPANISH_PART_NUMBER_DESCRIPTION,
                            tgt.SUGGESTED_ORDER_QUANTITY=src.SUGGESTED_ORDER_QUANTITY,
                            tgt.BRAND_TYPE_NAME=src.BRAND_TYPE_NAME,
                            tgt.LOCATION_TYPE_NAME=src.LOCATION_TYPE_NAME,
                            tgt.MANUFACTURING_CODE_DESCRIPTION=src.MANUFACTURING_CODE_DESCRIPTION,
                            tgt.QUALITY_GRADE_CODE=src.QUALITY_GRADE_CODE,
                            tgt.PRIMARY_APPLICATION_NAME=src.PRIMARY_APPLICATION_NAME,
                            --INFAETL-11515 begin change
                            tgt.CATEGORY_MANAGER_NAME=src.CATEGORY_MANAGER_NAME,
                            tgt.CATEGORY_MANAGER_NUMBER=src.CATEGORY_MANAGER_NUMBER,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                            tgt.CATEGORY_DIRECTOR_NAME=src.CATEGORY_DIRECTOR_NAME,
                            tgt.CATEGORY_DIRECTOR_NUMBER=src.CATEGORY_DIRECTOR_NUMBER,
                            tgt.CATEGORY_VP_NAME=src.CATEGORY_VP_NAME,
                            tgt.CATEGORY_VP_NUMBER=src.CATEGORY_VP_NUMBER,
                            --INFAETL-11515 end change
                            tgt.INACTIVATED_DATE=src.INACTIVATED_DATE,
                            tgt.REVIEW_CODE=src.REVIEW_CODE,
                            tgt.STOCKING_LINE_FLAG=src.STOCKING_LINE_FLAG,
                            tgt.OIL_LINE_FLAG=src.OIL_LINE_FLAG,
                            tgt.SPECIAL_REQUIREMENTS_LABEL=src.SPECIAL_REQUIREMENTS_LABEL,
                            tgt.SUPPLIER_ACCOUNT_NUMBER=src.SUPPLIER_ACCOUNT_NUMBER,
                            tgt.SUPPLIER_NUMBER=src.SUPPLIER_NUMBER,
                            tgt.SUPPLIER_ID=src.SUPPLIER_ID,
                            tgt.BRAND_DESCRIPTION=src.BRAND_DESCRIPTION,
                            tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER=src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                            tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER=src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                            tgt.SALES_AREA_NAME=src.SALES_AREA_NAME,
                            tgt.TEAM_NAME=src.TEAM_NAME,
                            tgt.CATEGORY_NAME=src.CATEGORY_NAME,
                            tgt.REPLENISHMENT_ANALYST_NAME=src.REPLENISHMENT_ANALYST_NAME,
                            tgt.REPLENISHMENT_ANALYST_NUMBER=src.REPLENISHMENT_ANALYST_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER=src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID=src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                            tgt.SALES_AREA_NAME_SORT_NUMBER=src.SALES_AREA_NAME_SORT_NUMBER,
                            tgt.TEAM_NAME_SORT_NUMBER=src.TEAM_NAME_SORT_NUMBER,
                            tgt.BUYER_CODE=src.BUYER_CODE,
                            tgt.BUYER_NAME=src.BUYER_NAME,
                            tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE=src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                            tgt.BATTERY_PACKING_INSTRUCTIONS_CODE=src.BATTERY_PACKING_INSTRUCTIONS_CODE,
                            tgt.BATTERY_MANUFACTURING_NAME=src.BATTERY_MANUFACTURING_NAME,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1=src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2=src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3=src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4=src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                            tgt.BATTERY_MANUFACTURING_CITY_NAME=src.BATTERY_MANUFACTURING_CITY_NAME,
                            tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME=src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                            tgt.BATTERY_MANUFACTURING_STATE_NAME=src.BATTERY_MANUFACTURING_STATE_NAME,
                            tgt.BATTERY_MANUFACTURING_ZIP_CODE=src.BATTERY_MANUFACTURING_ZIP_CODE,
                            tgt.BATTERY_MANUFACTURING_COUNTRY_CODE=src.BATTERY_MANUFACTURING_COUNTRY_CODE,
                            tgt.BATTERY_PHONE_NUMBER_CODE=src.BATTERY_PHONE_NUMBER_CODE,
                            tgt.BATTERY_WEIGHT_IN_GRAMS=src.BATTERY_WEIGHT_IN_GRAMS,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL=src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY=src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                            tgt.BATTERY_WATT_HOURS_PER_CELL=src.BATTERY_WATT_HOURS_PER_CELL,
                            tgt.BATTERY_WATT_HOURS_PER_BATTERY=src.BATTERY_WATT_HOURS_PER_BATTERY,
                            tgt.BATTERY_CELLS_NUMBER=src.BATTERY_CELLS_NUMBER,
                            tgt.BATTERIES_PER_PACKAGE_NUMBER=src.BATTERIES_PER_PACKAGE_NUMBER,
                            tgt.BATTERIES_IN_EQUIPMENT_NUMBER=src.BATTERIES_IN_EQUIPMENT_NUMBER,
                            tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG=src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                            tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG=src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                            tgt.COUNTRY_OF_ORIGIN_NAME_LIST=src.COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST=src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                            tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST=src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                            tgt.SCHEDULE_B_CODE_LIST=src.SCHEDULE_B_CODE_LIST,
                            tgt.UNITED_STATES_MUNITIONS_LIST_CODE=src.UNITED_STATES_MUNITIONS_LIST_CODE,
                            tgt.PROJECT_COORDINATOR_ID_CODE=src.PROJECT_COORDINATOR_ID_CODE,
                            tgt.PROJECT_COORDINATOR_EMPLOYEE_ID=src.PROJECT_COORDINATOR_EMPLOYEE_ID,
                            tgt.STOCK_ADJUSTMENT_MONTH_NUMBER=src.STOCK_ADJUSTMENT_MONTH_NUMBER,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.ALL_IN_COST=src.ALL_IN_COST,
                            tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE=src.CANCEL_OR_BACKORDER_REMAINDER_CODE,
                            tgt.CASE_LOT_DISCOUNT=src.CASE_LOT_DISCOUNT,
                            tgt.COMPANY_NUMBER=src.COMPANY_NUMBER,
                            tgt.CONVENIENCE_PACK_QUANTITY=src.CONVENIENCE_PACK_QUANTITY,
                            tgt.CONVENIENCE_PACK_DESCRIPTION=src.CONVENIENCE_PACK_DESCRIPTION,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE=src.PRODUCT_SOURCE_TABLE_CREATION_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME=src.PRODUCT_SOURCE_TABLE_CREATION_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE=src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                            tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE=src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                            tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE=src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                            tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE=src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                            tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG=src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                            tgt.HAZARDOUS_UPDATE_PROGRAM_NAME=src.HAZARDOUS_UPDATE_PROGRAM_NAME,
                            tgt.HAZARDOUS_UPDATE_TIME=src.HAZARDOUS_UPDATE_TIME,
                            tgt.HAZARDOUS_UPDATE_USER_NAME=src.HAZARDOUS_UPDATE_USER_NAME,
                            tgt.LIST_PRICE=src.LIST_PRICE,
                            tgt.LOW_USER_PRICE=src.LOW_USER_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE=src.MINIMUM_ADVERTISED_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE=src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                            tgt.MINIMUM_SELL_QUANTITY=src.MINIMUM_SELL_QUANTITY,
                            tgt.PACKAGE_SIZE_DESCRIPTION=src.PACKAGE_SIZE_DESCRIPTION,
                            tgt.PERCENTAGE_OF_SUPPLIER_FUNDING=src.PERCENTAGE_OF_SUPPLIER_FUNDING,
                            tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG=src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                            tgt.PRICING_COST=src.PRICING_COST,
                            tgt.PROFESSIONAL_PRICE=src.PROFESSIONAL_PRICE,
                            tgt.RETAIL_CORE=src.RETAIL_CORE,
                            tgt.RETAIL_HEIGHT=src.RETAIL_HEIGHT,
                            tgt.RETAIL_LENGTH=src.RETAIL_LENGTH,
                            tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION=src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.RETAIL_WIDTH=src.RETAIL_WIDTH,
                            tgt.SALES_PACK_CODE=src.SALES_PACK_CODE,
                            tgt.SCORE_FLAG=src.SCORE_FLAG,
                            tgt.SHIPPING_DIMENSIONS_CODE=src.SHIPPING_DIMENSIONS_CODE,
                            tgt.SUPPLIER_BASE_COST=src.SUPPLIER_BASE_COST,
                            tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE=src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SUPPLIER_SUPERSEDED_LINE_CODE=src.SUPPLIER_SUPERSEDED_LINE_CODE,
                            tgt.CATEGORY_TABLE_CREATE_DATE=src.CATEGORY_TABLE_CREATE_DATE,
                            tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME=src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_CREATE_TIME=src.CATEGORY_TABLE_CREATE_TIME,
                            tgt.CATEGORY_TABLE_CREATE_USER_NAME=src.CATEGORY_TABLE_CREATE_USER_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_DATE=src.CATEGORY_TABLE_UPDATE_DATE,
                            tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME=src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_TIME=src.CATEGORY_TABLE_UPDATE_TIME,
                            tgt.CATEGORY_TABLE_UPDATE_USER_NAME=src.CATEGORY_TABLE_UPDATE_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                            tgt.VIP_JOBBER=src.VIP_JOBBER,
                            tgt.WAREHOUSE_CORE=src.WAREHOUSE_CORE,
                            tgt.WAREHOUSE_COST=src.WAREHOUSE_COST,
                            --INFAETL-11815 added the next line 
                            tgt.PRODUCT_LEVEL_CODE = src.PRODUCT_LEVEL_CODE,
                            tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
                            tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
                            --tgt.ETL_CREATE_TIMESTAMP=src.ETL_CREATE_TIMESTAMP,  -- delete per peer review
                            tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
                            tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
                            tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS ' ||
                 'from '|| v_staging_database_name || '.SESSION_TMP_IMASTER_UPDATE_SOURCE SRC
                  WHERE tgt.product_id = src.product_id
                        AND (COALESCE(tgt.LINE_DESCRIPTION,''A'') <> COALESCE(src.LINE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.ITEM_DESCRIPTION,''A'') <> COALESCE(src.ITEM_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SEGMENT_NUMBER,0) <> COALESCE(src.SEGMENT_NUMBER,0)
                            OR COALESCE(tgt.SEGMENT_DESCRIPTION,''A'') <> COALESCE(src.SEGMENT_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SUB_CATEGORY_NUMBER,0) <> COALESCE(src.SUB_CATEGORY_NUMBER,0)
                            OR COALESCE(tgt.SUB_CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.SUB_CATEGORY_DESCRIPTION,''A'')
                            OR COALESCE(tgt.CATEGORY_NUMBER,0) <> COALESCE(src.CATEGORY_NUMBER,0)
                            OR COALESCE(tgt.CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.CATEGORY_DESCRIPTION,''A'')
                            OR COALESCE(tgt.PRODUCT_LINE_CODE,''A'') <> COALESCE(src.PRODUCT_LINE_CODE,''A'')
                            OR COALESCE(tgt.SUB_CODE,''A'') <> COALESCE(src.SUB_CODE,''A'')
                            OR COALESCE(tgt.MANUFACTURE_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.MANUFACTURE_ITEM_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPERSEDED_LINE_CODE,''A'')
                            OR COALESCE(tgt.SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.SORT_CONTROL_NUMBER,0) <> COALESCE(src.SORT_CONTROL_NUMBER,0)
                            OR COALESCE(tgt.POINT_OF_SALE_DESCRIPTION,''A'') <> COALESCE(src.POINT_OF_SALE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.POPULARITY_CODE,''A'') <> COALESCE(src.POPULARITY_CODE,''A'')
                            OR COALESCE(tgt.POPULARITY_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.POPULARITY_TREND_CODE,''A'') <> COALESCE(src.POPULARITY_TREND_CODE,''A'')
                            OR COALESCE(tgt.POPULARITY_TREND_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_TREND_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.LINE_IS_MARINE_SPECIFIC_FLAG,''A'') <> COALESCE(src.LINE_IS_MARINE_SPECIFIC_FLAG,''A'')
                            OR COALESCE(tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'')
                            OR COALESCE(tgt.LINE_IS_FLEET_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_FLEET_SPECIFIC_CODE,''A'')
                            OR COALESCE(tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'')
                            OR COALESCE(tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'')
                            OR COALESCE(tgt.JOBBER_SUPPLIER_CODE,''A'') <> COALESCE(src.JOBBER_SUPPLIER_CODE,''A'')
                            OR COALESCE(tgt.JOBBER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.JOBBER_UNIT_OF_MEASURE_CODE,''A'')
                            OR COALESCE(tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'')
                            OR COALESCE(tgt.WAREHOUSE_SELL_QUANTITY,0) <> COALESCE(src.WAREHOUSE_SELL_QUANTITY,0)
                            OR COALESCE(tgt.RETAIL_WEIGHT,0) <> COALESCE(src.RETAIL_WEIGHT,0)
                            OR COALESCE(tgt.QUANTITY_PER_CAR,0) <> COALESCE(src.QUANTITY_PER_CAR,0)
                            OR COALESCE(tgt.CASE_QUANTITY,0) <> COALESCE(src.CASE_QUANTITY,0)
                            OR COALESCE(tgt.STANDARD_PACKAGE,0) <> COALESCE(src.STANDARD_PACKAGE,0)
                            OR COALESCE(tgt.PAINT_BODY_AND_EQUIPMENT_PRICE,0) <> COALESCE(src.PAINT_BODY_AND_EQUIPMENT_PRICE,0)
                            OR COALESCE(tgt.WAREHOUSE_JOBBER_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_PRICE,0)
                            OR COALESCE(tgt.WAREHOUSE_COST_WUM,0) <> COALESCE(src.WAREHOUSE_COST_WUM,0)
                            OR COALESCE(tgt.WAREHOUSE_CORE_WUM,0) <> COALESCE(src.WAREHOUSE_CORE_WUM,0)
                            OR COALESCE(tgt.OREILLY_COST_PRICE,0) <> COALESCE(src.OREILLY_COST_PRICE,0)
                            OR COALESCE(tgt.JOBBER_COST,0) <> COALESCE(src.JOBBER_COST,0)
                            OR COALESCE(tgt.JOBBER_CORE_PRICE,0) <> COALESCE(src.JOBBER_CORE_PRICE,0)
                            OR COALESCE(tgt.OUT_FRONT_MERCHANDISE_FLAG,''A'') <> COALESCE(src.OUT_FRONT_MERCHANDISE_FLAG,''A'')
                            OR COALESCE(tgt.ITEM_IS_TAXED_FLAG,''A'') <> COALESCE(src.ITEM_IS_TAXED_FLAG,''A'')
                            OR COALESCE(tgt.QUANTITY_ORDER_ITEM_FLAG,''A'') <> COALESCE(src.QUANTITY_ORDER_ITEM_FLAG,''A'')
                            OR COALESCE(tgt.JOBBER_DIVIDE_QUANTITY,0) <> COALESCE(src.JOBBER_DIVIDE_QUANTITY,0)
                            OR COALESCE(tgt.ITEM_DELETE_FLAG_RECORD_CODE,''A'') <> COALESCE(src.ITEM_DELETE_FLAG_RECORD_CODE,''A'')
                            OR COALESCE(tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'') <> COALESCE(src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'')
                            OR COALESCE(tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'') <> COALESCE(src.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'')
                            OR COALESCE(tgt.WARRANTY_CODE,''A'') <> COALESCE(src.WARRANTY_CODE,''A'')
                            OR COALESCE(tgt.WARRANTY_CODE_DESCRIPTION,''A'') <> COALESCE(src.WARRANTY_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.INVOICE_COST_WUM_INVOICE_COST,0) <> COALESCE(src.INVOICE_COST_WUM_INVOICE_COST,0)
                            OR COALESCE(tgt.INVOICE_CORE_WUM_CORE_COST,0) <> COALESCE(src.INVOICE_CORE_WUM_CORE_COST,0)
                            OR COALESCE(tgt.IS_CONSIGNMENT_ITEM_FLAG,''A'') <> COALESCE(src.IS_CONSIGNMENT_ITEM_FLAG,''A'')
                            OR COALESCE(tgt.WAREHOUSE_JOBBER_CORE_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_CORE_PRICE,0)
                            OR COALESCE(tgt.ACQUISITION_FIELD_1_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_1_CODE,''A'')
                            OR COALESCE(tgt.ACQUISITION_FIELD_2_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_2_CODE,''A'')
                            OR COALESCE(tgt.BUY_MULTIPLE,0) <> COALESCE(src.BUY_MULTIPLE,0)
                            OR COALESCE(tgt.BUY_MULTIPLE_CODE,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE,''A'')
                            OR COALESCE(tgt.BUY_MULTIPLE_CODE_DESCRIPTION,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SUPPLIER_CONVERSION_FACTOR_CODE,''A'') <> COALESCE(src.SUPPLIER_CONVERSION_FACTOR_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_CONVERSION_QUANTITY,0) <> COALESCE(src.SUPPLIER_CONVERSION_QUANTITY,0)
                            OR COALESCE(tgt.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'')
                            OR COALESCE(tgt.UNIT_OF_MEASURE_AMOUNT,0) <> COALESCE(src.UNIT_OF_MEASURE_AMOUNT,0)
                            OR COALESCE(tgt.UNIT_OF_MEASURE_QUANTITY,0) <> COALESCE(src.UNIT_OF_MEASURE_QUANTITY,0)
                            OR COALESCE(tgt.UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.UNIT_OF_MEASURE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.TAX_CLASSIFICATION_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE,''A'')
                            OR COALESCE(tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'')
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'')
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_LENGTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_LENGTH,0)
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WIDTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WIDTH,0)
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_HEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_HEIGHT,0)
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WEIGHT,0)
                            OR COALESCE(tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                            OR COALESCE(tgt.CASE_QUANTITY_CODE,''A'') <> COALESCE(src.CASE_QUANTITY_CODE,''A'')
                            OR COALESCE(tgt.CASE_LENGTH,0) <> COALESCE(src.CASE_LENGTH,0)
                            OR COALESCE(tgt.CASE_WIDTH,0) <> COALESCE(src.CASE_WIDTH,0)
                            OR COALESCE(tgt.CASE_HEIGHT,0) <> COALESCE(src.CASE_HEIGHT,0)
                            OR COALESCE(tgt.CASE_WEIGHT,0) <> COALESCE(src.CASE_WEIGHT,0)
                            OR COALESCE(tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                            OR COALESCE(tgt.CASES_PER_PALLET,0) <> COALESCE(src.CASES_PER_PALLET,0)
                            OR COALESCE(tgt.CASES_PER_PALLET_LAYER,0) <> COALESCE(src.CASES_PER_PALLET_LAYER,0)
                            OR COALESCE(tgt.PALLET_LENGTH,0) <> COALESCE(src.PALLET_LENGTH,0)
                            OR COALESCE(tgt.PALLET_WIDTH,0) <> COALESCE(src.PALLET_WIDTH,0)
                            OR COALESCE(tgt.PALLET_HEIGHT,0) <> COALESCE(src.PALLET_HEIGHT,0)
                            OR COALESCE(tgt.PALLET_WEIGHT,0) <> COALESCE(src.PALLET_WEIGHT,0)
                            OR COALESCE(tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                            OR COALESCE(tgt.SHIPMENT_CLASS_CODE,''A'') <> COALESCE(src.SHIPMENT_CLASS_CODE,''A'')
                            OR COALESCE(tgt.DOT_CLASS_NUMBER,0) <> COALESCE(src.DOT_CLASS_NUMBER,0)
                            OR COALESCE(tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER,0) <> COALESCE(src.DOT_CLASS_FOR_MSDS_ID_NUMBER,0)
                            OR COALESCE(tgt.CONTAINER_DESCRIPTION,''A'') <> COALESCE(src.CONTAINER_DESCRIPTION,''A'')
                            OR COALESCE(tgt.KEEP_FROM_FREEZING_FLAG,''A'') <> COALESCE(src.KEEP_FROM_FREEZING_FLAG,''A'')
                            OR COALESCE(tgt.FLIGHT_RESTRICTED_FLAG,''A'') <> COALESCE(src.FLIGHT_RESTRICTED_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_NEW_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_NEW_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_CORE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_CORE_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_WARRANTY_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_WARRANTY_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_RECALL_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_RECALL_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.HAZARDOUS_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PIECE_LENGTH,0) <> COALESCE(src.PIECE_LENGTH,0)
                            OR COALESCE(tgt.PIECE_WIDTH,0) <> COALESCE(src.PIECE_WIDTH,0)
                            OR COALESCE(tgt.PIECE_HEIGHT,0) <> COALESCE(src.PIECE_HEIGHT,0)
                            OR COALESCE(tgt.PIECE_WEIGHT,0) <> COALESCE(src.PIECE_WEIGHT,0)
                            OR COALESCE(tgt.PIECES_INNER_PACK,0) <> COALESCE(src.PIECES_INNER_PACK,0)
                            OR COALESCE(tgt.IN_CATALOG_CODE,''A'') <> COALESCE(src.IN_CATALOG_CODE,''A'')
                            OR COALESCE(tgt.IN_CATALOG_CODE_DESCRIPTION,''A'') <> COALESCE(src.IN_CATALOG_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.ALLOW_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ALLOW_SPECIAL_ORDER_FLAG,''A'')
                            OR COALESCE(tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'')
                            OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CODE,''A'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.LONG_DESCRIPTION,''A'') <> COALESCE(src.LONG_DESCRIPTION,''A'')
                            OR COALESCE(tgt.ELECTRONIC_WASTE_FLAG,''A'') <> COALESCE(src.ELECTRONIC_WASTE_FLAG,''A'')
                            OR COALESCE(tgt.STORE_MINIMUM_SALE_QUANTITY,0) <> COALESCE(src.STORE_MINIMUM_SALE_QUANTITY,0)
                            OR COALESCE(tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0) <> COALESCE(src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0)
                            OR COALESCE(tgt.MAXIMUM_CAR_QUANTITY,0) <> COALESCE(src.MAXIMUM_CAR_QUANTITY,0)
                            OR COALESCE(tgt.MINIMUM_CAR_QUANTITY,0) <> COALESCE(src.MINIMUM_CAR_QUANTITY,0)
                            OR COALESCE(tgt.ESSENTIAL_HARD_PART_CODE,''A'') <> COALESCE(src.ESSENTIAL_HARD_PART_CODE,''A'')
                            OR COALESCE(tgt.INNER_PACK_CODE,''A'') <> COALESCE(src.INNER_PACK_CODE,''A'')
                            OR COALESCE(tgt.INNER_PACK_QUANTITY,0) <> COALESCE(src.INNER_PACK_QUANTITY,0)
                            OR COALESCE(tgt.INNER_PACK_LENGTH,0) <> COALESCE(src.INNER_PACK_LENGTH,0)
                            OR COALESCE(tgt.INNER_PACK_WIDTH,0) <> COALESCE(src.INNER_PACK_WIDTH,0)
                            OR COALESCE(tgt.INNER_PACK_HEIGHT,0) <> COALESCE(src.INNER_PACK_HEIGHT,0)
                            OR COALESCE(tgt.INNER_PACK_WEIGHT,0) <> COALESCE(src.INNER_PACK_WEIGHT,0)
                            OR COALESCE(tgt.BRAND_CODE,''A'') <> COALESCE(src.BRAND_CODE,''A'')
                            OR COALESCE(tgt.PART_NUMBER_CODE,''A'') <> COALESCE(src.PART_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.PART_NUMBER_DISPLAY_CODE,''A'') <> COALESCE(src.PART_NUMBER_DISPLAY_CODE,''A'')
                            OR COALESCE(tgt.PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.PART_NUMBER_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SPANISH_PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.SPANISH_PART_NUMBER_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SUGGESTED_ORDER_QUANTITY,0) <> COALESCE(src.SUGGESTED_ORDER_QUANTITY,0)
                            OR COALESCE(tgt.BRAND_TYPE_NAME,''A'') <> COALESCE(src.BRAND_TYPE_NAME,''A'')
                            OR COALESCE(tgt.LOCATION_TYPE_NAME,''A'') <> COALESCE(src.LOCATION_TYPE_NAME,''A'')
                            OR COALESCE(tgt.MANUFACTURING_CODE_DESCRIPTION,''A'') <> COALESCE(src.MANUFACTURING_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.QUALITY_GRADE_CODE,''A'') <> COALESCE(src.QUALITY_GRADE_CODE,''A'')
                            OR COALESCE(tgt.PRIMARY_APPLICATION_NAME,''A'') <> COALESCE(src.PRIMARY_APPLICATION_NAME,''A'')
                            --INFAETL-11515 begin change
                            OR COALESCE(tgt.CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.CATEGORY_MANAGER_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.CATEGORY_MANAGER_NUMBER,0)
                            OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'')
                            OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0)
                            OR COALESCE(tgt.CATEGORY_DIRECTOR_NAME,''A'') <> COALESCE(src.CATEGORY_DIRECTOR_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_DIRECTOR_NUMBER,0) <> COALESCE(src.CATEGORY_DIRECTOR_NUMBER,0)
                            OR COALESCE(tgt.CATEGORY_VP_NAME,''A'') <> COALESCE(src.CATEGORY_VP_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_VP_NUMBER,0) <> COALESCE(src.CATEGORY_VP_NUMBER,0)
                            --INFAETL-11515 end change
                            OR COALESCE(tgt.INACTIVATED_DATE,''1900-01-01'') <> COALESCE(src.INACTIVATED_DATE,''1900-01-01'')
                            OR COALESCE(tgt.REVIEW_CODE,''A'') <> COALESCE(src.REVIEW_CODE,''A'')
                            OR COALESCE(tgt.STOCKING_LINE_FLAG,''A'') <> COALESCE(src.STOCKING_LINE_FLAG,''A'')
                            OR COALESCE(tgt.OIL_LINE_FLAG,''A'') <> COALESCE(src.OIL_LINE_FLAG,''A'')
                            OR COALESCE(tgt.SPECIAL_REQUIREMENTS_LABEL,''A'') <> COALESCE(src.SPECIAL_REQUIREMENTS_LABEL,''A'')
                            OR COALESCE(tgt.SUPPLIER_ACCOUNT_NUMBER,0) <> COALESCE(src.SUPPLIER_ACCOUNT_NUMBER,0)
                            OR COALESCE(tgt.SUPPLIER_NUMBER,0) <> COALESCE(src.SUPPLIER_NUMBER,0)
                            OR COALESCE(tgt.SUPPLIER_ID,0) <> COALESCE(src.SUPPLIER_ID,0)
                            OR COALESCE(tgt.BRAND_DESCRIPTION,''A'') <> COALESCE(src.BRAND_DESCRIPTION,''A'')
                            OR COALESCE(tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0) <> COALESCE(src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0)
                            OR COALESCE(tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0) <> COALESCE(src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0)
                            OR COALESCE(tgt.SALES_AREA_NAME,''A'') <> COALESCE(src.SALES_AREA_NAME,''A'')
                            OR COALESCE(tgt.TEAM_NAME,''A'') <> COALESCE(src.TEAM_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_NAME,''A'') <> COALESCE(src.CATEGORY_NAME,''A'')
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_NAME,''A'') <> COALESCE(src.REPLENISHMENT_ANALYST_NAME,''A'')
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_NUMBER,0)
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0)
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0)
                            OR COALESCE(tgt.SALES_AREA_NAME_SORT_NUMBER,0) <> COALESCE(src.SALES_AREA_NAME_SORT_NUMBER,0)
                            OR COALESCE(tgt.TEAM_NAME_SORT_NUMBER,0) <> COALESCE(src.TEAM_NAME_SORT_NUMBER,0)
                            OR COALESCE(tgt.BUYER_CODE,''A'') <> COALESCE(src.BUYER_CODE,''A'')
                            OR COALESCE(tgt.BUYER_NAME,''A'') <> COALESCE(src.BUYER_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'') <> COALESCE(src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'') <> COALESCE(src.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_CITY_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_CITY_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_STATE_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_STATE_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ZIP_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ZIP_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_PHONE_NUMBER_CODE,''A'') <> COALESCE(src.BATTERY_PHONE_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_WEIGHT_IN_GRAMS,0) <> COALESCE(src.BATTERY_WEIGHT_IN_GRAMS,0)
                            OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0)
                            OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0)
                            OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_CELL,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_CELL,0)
                            OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_BATTERY,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_BATTERY,0)
                            OR COALESCE(tgt.BATTERY_CELLS_NUMBER,0) <> COALESCE(src.BATTERY_CELLS_NUMBER,0)
                            OR COALESCE(tgt.BATTERIES_PER_PACKAGE_NUMBER,0) <> COALESCE(src.BATTERIES_PER_PACKAGE_NUMBER,0)
                            OR COALESCE(tgt.BATTERIES_IN_EQUIPMENT_NUMBER,0) <> COALESCE(src.BATTERIES_IN_EQUIPMENT_NUMBER,0)
                            OR COALESCE(tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'') <> COALESCE(src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'')
                            OR COALESCE(tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'') <> COALESCE(src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'')
                            OR COALESCE(tgt.COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                            OR COALESCE(tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'') <> COALESCE(src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'')
                            OR COALESCE(tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'') <> COALESCE(src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'')
                            OR COALESCE(tgt.SCHEDULE_B_CODE_LIST,''A'') <> COALESCE(src.SCHEDULE_B_CODE_LIST,''A'')
                            OR COALESCE(tgt.UNITED_STATES_MUNITIONS_LIST_CODE,''A'') <> COALESCE(src.UNITED_STATES_MUNITIONS_LIST_CODE,''A'')
                            OR COALESCE(tgt.PROJECT_COORDINATOR_ID_CODE,''A'') <> COALESCE(src.PROJECT_COORDINATOR_ID_CODE,''A'')
                            OR COALESCE(tgt.PROJECT_COORDINATOR_EMPLOYEE_ID,0) <> COALESCE(src.PROJECT_COORDINATOR_EMPLOYEE_ID,0)
                            OR COALESCE(tgt.STOCK_ADJUSTMENT_MONTH_NUMBER,0) <> COALESCE(src.STOCK_ADJUSTMENT_MONTH_NUMBER,0)
                            OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'')
                            OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                            OR COALESCE(tgt.ALL_IN_COST,0) <> COALESCE(src.ALL_IN_COST,0)
                            OR COALESCE(tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'') <> COALESCE(src.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'')
                            OR COALESCE(tgt.CASE_LOT_DISCOUNT,0) <> COALESCE(src.CASE_LOT_DISCOUNT,0)
                            OR COALESCE(tgt.COMPANY_NUMBER,0) <> COALESCE(src.COMPANY_NUMBER,0)
                            OR COALESCE(tgt.CONVENIENCE_PACK_QUANTITY,0) <> COALESCE(src.CONVENIENCE_PACK_QUANTITY,0)
                            OR COALESCE(tgt.CONVENIENCE_PACK_DESCRIPTION,''A'') <> COALESCE(src.CONVENIENCE_PACK_DESCRIPTION,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'') <> COALESCE(src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'')
                            OR COALESCE(tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'') <> COALESCE(src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'')
                            OR COALESCE(tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'') <> COALESCE(src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'')
                            OR COALESCE(tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'') <> COALESCE(src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.HAZARDOUS_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_USER_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.LIST_PRICE,0) <> COALESCE(src.LIST_PRICE,0)
                            OR COALESCE(tgt.LOW_USER_PRICE,0) <> COALESCE(src.LOW_USER_PRICE,0)
                            OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE,0) <> COALESCE(src.MINIMUM_ADVERTISED_PRICE,0)
                            OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'') <> COALESCE(src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.MINIMUM_SELL_QUANTITY,0) <> COALESCE(src.MINIMUM_SELL_QUANTITY,0)
                            OR COALESCE(tgt.PACKAGE_SIZE_DESCRIPTION,''A'') <> COALESCE(src.PACKAGE_SIZE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.PERCENTAGE_OF_SUPPLIER_FUNDING,0) <> COALESCE(src.PERCENTAGE_OF_SUPPLIER_FUNDING,0)
                            OR COALESCE(tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'') <> COALESCE(src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'')
                            OR COALESCE(tgt.PRICING_COST,0) <> COALESCE(src.PRICING_COST,0)
                            OR COALESCE(tgt.PROFESSIONAL_PRICE,0) <> COALESCE(src.PROFESSIONAL_PRICE,0)
                            OR COALESCE(tgt.RETAIL_CORE,0) <> COALESCE(src.RETAIL_CORE,0)
                            OR COALESCE(tgt.RETAIL_HEIGHT,0) <> COALESCE(src.RETAIL_HEIGHT,0)
                            OR COALESCE(tgt.RETAIL_LENGTH,0) <> COALESCE(src.RETAIL_LENGTH,0)
                            OR COALESCE(tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.RETAIL_WIDTH,0) <> COALESCE(src.RETAIL_WIDTH,0)
                            OR COALESCE(tgt.SALES_PACK_CODE,''A'') <> COALESCE(src.SALES_PACK_CODE,''A'')
                            OR COALESCE(tgt.SCORE_FLAG,''A'') <> COALESCE(src.SCORE_FLAG,''A'')
                            OR COALESCE(tgt.SHIPPING_DIMENSIONS_CODE,''A'') <> COALESCE(src.SHIPPING_DIMENSIONS_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_BASE_COST,0) <> COALESCE(src.SUPPLIER_BASE_COST,0)
                            OR COALESCE(tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_LINE_CODE,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_USER_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.VIP_JOBBER,0) <> COALESCE(src.VIP_JOBBER,0)
                            OR COALESCE(tgt.WAREHOUSE_CORE,0) <> COALESCE(src.WAREHOUSE_CORE,0)
                            OR COALESCE(tgt.WAREHOUSE_COST,0) <> COALESCE(src.WAREHOUSE_COST,0)
                            --INFAETL-11815 adds the following line
                            OR COALESCE(tgt.PRODUCT_LEVEL_CODE,'''') <> COALESCE(src.PRODUCT_LEVEL_CODE,'''')
                            OR COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG,''A'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG,''A'')
                        ) 
                    WITH UR;';

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTER UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '09. IMASTER_UPDATE', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                        
                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

            END IF; --v_SQL_OK


                 --Populate Hub Load Table - IMASTNS 2nd INSERT             
            IF V_SQL_OK THEN 
            
               SET v_str_sql =  CLOB('INSERT INTO ') || v_staging_database_name || '.' || v_staging_table_name || 
                                '(PRODUCT_ID, LINE_CODE, LINE_DESCRIPTION, ITEM_CODE, ITEM_DESCRIPTION, SEGMENT_NUMBER, SEGMENT_DESCRIPTION, SUB_CATEGORY_NUMBER, SUB_CATEGORY_DESCRIPTION, CATEGORY_NUMBER, CATEGORY_DESCRIPTION, PRODUCT_LINE_CODE, SUB_CODE, MANUFACTURE_ITEM_NUMBER_CODE, SUPERSEDED_LINE_CODE, SUPERSEDED_ITEM_NUMBER_CODE, SORT_CONTROL_NUMBER, POINT_OF_SALE_DESCRIPTION, POPULARITY_CODE, POPULARITY_CODE_DESCRIPTION, POPULARITY_TREND_CODE, POPULARITY_TREND_CODE_DESCRIPTION, LINE_IS_MARINE_SPECIFIC_FLAG, LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE, LINE_IS_FLEET_SPECIFIC_CODE, LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE, LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG, JOBBER_SUPPLIER_CODE, JOBBER_UNIT_OF_MEASURE_CODE, WAREHOUSE_UNIT_OF_MEASURE_CODE, WAREHOUSE_SELL_QUANTITY, RETAIL_WEIGHT, QUANTITY_PER_CAR, CASE_QUANTITY, STANDARD_PACKAGE, PAINT_BODY_AND_EQUIPMENT_PRICE, WAREHOUSE_JOBBER_PRICE, WAREHOUSE_COST_WUM, WAREHOUSE_CORE_WUM, OREILLY_COST_PRICE, JOBBER_COST, JOBBER_CORE_PRICE, OUT_FRONT_MERCHANDISE_FLAG, ITEM_IS_TAXED_FLAG, QUANTITY_ORDER_ITEM_FLAG, JOBBER_DIVIDE_QUANTITY, ITEM_DELETE_FLAG_RECORD_CODE, SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE, PRIMARY_UNIVERSAL_PRODUCT_CODE, WARRANTY_CODE, WARRANTY_CODE_DESCRIPTION, INVOICE_COST_WUM_INVOICE_COST, INVOICE_CORE_WUM_CORE_COST, IS_CONSIGNMENT_ITEM_FLAG, WAREHOUSE_JOBBER_CORE_PRICE, ACQUISITION_FIELD_1_CODE, ACQUISITION_FIELD_2_CODE, BUY_MULTIPLE, BUY_MULTIPLE_CODE, BUY_MULTIPLE_CODE_DESCRIPTION, SUPPLIER_CONVERSION_FACTOR_CODE, SUPPLIER_CONVERSION_QUANTITY, SUPPLIER_UNIT_OF_MEASURE_CODE, UNIT_OF_MEASURE_AMOUNT, UNIT_OF_MEASURE_QUANTITY, UNIT_OF_MEASURE_DESCRIPTION, TAX_CLASSIFICATION_CODE, TAX_CLASSIFICATION_CODE_DESCRIPTION, TAX_CLASSIFICATION_REVIEW_STATUS_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION, DISTRIBUTION_CENTER_PICK_LENGTH, DISTRIBUTION_CENTER_PICK_WIDTH, DISTRIBUTION_CENTER_PICK_HEIGHT, DISTRIBUTION_CENTER_PICK_WEIGHT, PICK_LENGTH_WIDTH_HEIGHT_CODE, CASE_QUANTITY_CODE, CASE_LENGTH, CASE_WIDTH, CASE_HEIGHT, CASE_WEIGHT, CASE_LENGTH_WIDTH_HEIGHT_CODE, CASES_PER_PALLET, CASES_PER_PALLET_LAYER, PALLET_LENGTH, PALLET_WIDTH, PALLET_HEIGHT, PALLET_WEIGHT, PALLET_LENGTH_WIDTH_HEIGHT_CODE, SHIPMENT_CLASS_CODE, DOT_CLASS_NUMBER, DOT_CLASS_FOR_MSDS_ID_NUMBER, CONTAINER_DESCRIPTION, KEEP_FROM_FREEZING_FLAG, FLIGHT_RESTRICTED_FLAG, ALLOW_NEW_RETURNS_FLAG, ALLOW_CORE_RETURNS_FLAG, ALLOW_WARRANTY_RETURNS_FLAG, ALLOW_RECALL_RETURNS_FLAG, ALLOW_MANUAL_OTHER_RETURNS_FLAG, ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG, HAZARDOUS_UPDATE_DATE, PIECE_LENGTH, PIECE_WIDTH, PIECE_HEIGHT, PIECE_WEIGHT, PIECES_INNER_PACK, IN_CATALOG_CODE, IN_CATALOG_CODE_DESCRIPTION, ALLOW_SPECIAL_ORDER_FLAG, ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG, SUPPLIER_LIFE_CYCLE_CODE, SUPPLIER_LIFE_CYCLE_CHANGE_DATE, LONG_DESCRIPTION, ELECTRONIC_WASTE_FLAG, STORE_MINIMUM_SALE_QUANTITY, MANUFACTURER_SUGGESTED_RETAIL_PRICE, MAXIMUM_CAR_QUANTITY, MINIMUM_CAR_QUANTITY, ESSENTIAL_HARD_PART_CODE, INNER_PACK_CODE, INNER_PACK_QUANTITY, INNER_PACK_LENGTH, INNER_PACK_WIDTH, INNER_PACK_HEIGHT, INNER_PACK_WEIGHT, BRAND_CODE, PART_NUMBER_CODE,
                                PART_NUMBER_DISPLAY_CODE, PART_NUMBER_DESCRIPTION, SPANISH_PART_NUMBER_DESCRIPTION, SUGGESTED_ORDER_QUANTITY, BRAND_TYPE_NAME, LOCATION_TYPE_NAME, MANUFACTURING_CODE_DESCRIPTION, QUALITY_GRADE_CODE, PRIMARY_APPLICATION_NAME, 
                                --INFAETL-11515 replaced the following line
                                CATEGORY_MANAGER_NAME, CATEGORY_MANAGER_NUMBER, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER, CATEGORY_DIRECTOR_NAME, CATEGORY_DIRECTOR_NUMBER, CATEGORY_VP_NAME, CATEGORY_VP_NUMBER,
                                INACTIVATED_DATE, REVIEW_CODE, STOCKING_LINE_FLAG, OIL_LINE_FLAG, SPECIAL_REQUIREMENTS_LABEL, SUPPLIER_ACCOUNT_NUMBER, SUPPLIER_NUMBER, SUPPLIER_ID, BRAND_DESCRIPTION, DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER, ACCOUNTS_PAYABLE_VENDOR_NUMBER, SALES_AREA_NAME, TEAM_NAME, CATEGORY_NAME, REPLENISHMENT_ANALYST_NAME, REPLENISHMENT_ANALYST_NUMBER, REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER, REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID, SALES_AREA_NAME_SORT_NUMBER, TEAM_NAME_SORT_NUMBER, BUYER_CODE, BUYER_NAME, BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE, BATTERY_PACKING_INSTRUCTIONS_CODE, BATTERY_MANUFACTURING_NAME, BATTERY_MANUFACTURING_ADDRESS_LINE_1, BATTERY_MANUFACTURING_ADDRESS_LINE_2, BATTERY_MANUFACTURING_ADDRESS_LINE_3, BATTERY_MANUFACTURING_ADDRESS_LINE_4, BATTERY_MANUFACTURING_CITY_NAME, BATTERY_MANUFACTURING_POSTAL_TOWN_NAME, BATTERY_MANUFACTURING_STATE_NAME, BATTERY_MANUFACTURING_ZIP_CODE, BATTERY_MANUFACTURING_COUNTRY_CODE, BATTERY_PHONE_NUMBER_CODE, BATTERY_WEIGHT_IN_GRAMS, BATTERY_GRAMS_OF_LITHIUM_PER_CELL, BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY, BATTERY_WATT_HOURS_PER_CELL, BATTERY_WATT_HOURS_PER_BATTERY, BATTERY_CELLS_NUMBER, BATTERIES_PER_PACKAGE_NUMBER, BATTERIES_IN_EQUIPMENT_NUMBER, BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG, BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG, COUNTRY_OF_ORIGIN_NAME_LIST, EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST, HARMONIZED_TARIFF_SCHEDULE_CODE_LIST, SCHEDULE_B_CODE_LIST, UNITED_STATES_MUNITIONS_LIST_CODE, PROJECT_COORDINATOR_ID_CODE, PROJECT_COORDINATOR_EMPLOYEE_ID, STOCK_ADJUSTMENT_MONTH_NUMBER, BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST, BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST, ALL_IN_COST, CANCEL_OR_BACKORDER_REMAINDER_CODE, CASE_LOT_DISCOUNT, COMPANY_NUMBER, CONVENIENCE_PACK_QUANTITY, CONVENIENCE_PACK_DESCRIPTION, PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_CREATION_DATE, PRODUCT_SOURCE_TABLE_CREATION_TIME, PRODUCT_SOURCE_TABLE_CREATION_USER_NAME, PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME, DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE, DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE, ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE, EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG, HAZARDOUS_UPDATE_PROGRAM_NAME, HAZARDOUS_UPDATE_TIME, HAZARDOUS_UPDATE_USER_NAME, LIST_PRICE, LOW_USER_PRICE, MINIMUM_ADVERTISED_PRICE, MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE, MINIMUM_SELL_QUANTITY, PACKAGE_SIZE_DESCRIPTION, PERCENTAGE_OF_SUPPLIER_FUNDING, PIECE_LENGTH_WIDTH_HEIGHT_FLAG, PRICING_COST, PROFESSIONAL_PRICE, RETAIL_CORE, RETAIL_HEIGHT, RETAIL_LENGTH, RETAIL_UNIT_OF_MEASURE_DESCRIPTION, RETAIL_WIDTH, SALES_PACK_CODE, SCORE_FLAG, SHIPPING_DIMENSIONS_CODE, SUPPLIER_BASE_COST, SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE, SUPPLIER_SUPERSEDED_LINE_CODE, CATEGORY_TABLE_CREATE_DATE, CATEGORY_TABLE_CREATE_PROGRAM_NAME, CATEGORY_TABLE_CREATE_TIME, CATEGORY_TABLE_CREATE_USER_NAME, CATEGORY_TABLE_UPDATE_DATE, CATEGORY_TABLE_UPDATE_PROGRAM_NAME, CATEGORY_TABLE_UPDATE_TIME, CATEGORY_TABLE_UPDATE_USER_NAME, PRODUCT_SOURCE_TABLE_UPDATE_DATE, PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_UPDATE_TIME, PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME, VIP_JOBBER, WAREHOUSE_CORE, WAREHOUSE_COST, 
                                --INFAETL-11815 adds the following line
                                PRODUCT_LEVEL_CODE,
                                ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS
                                ) ' || ' SELECT CAST(' || v_process_database_name|| '.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS PRODUCT_ID,
                                    CAST(TRIM(cur_IMASTNS.LINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTNS.ITEM) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.DESC), TRIM(cur_ECPARTS.SHORT_DESCRIPTION)) AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SUBCATNUM, -2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTNS.PLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                                    CAST(cur_IMASTNS.PCODE AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                                    CAST(COALESCE(cur_IMASTNS.MFGITEM, ''-2'') AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SLINE, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SITEM, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST(0 AS INTEGER) AS SORT_CONTROL_NUMBER,
                                    CAST(TRIM(cur_IMASTNS.PSDSC) AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.POPCD), '''') AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                                    CAST(COALESCE(VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE_DESCRIPTION , ''DEFAULT'') AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.TRNDC), '''') AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                                    CAST(COALESCE(VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE_DESCRIPTION , ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                                    CAST(cur_IMASTNS.JUM AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTNS.WUM AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTNS.WSQTY AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.QTYPC, '''') AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                                    CAST(cur_IMASTNS.CSQTY AS DECIMAL(12,4)) AS CASE_QUANTITY,
                                    CAST(cur_IMASTNS.STDPK AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                                    CAST(0 AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                                    CAST(0 AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                                    CAST(COALESCE(cur_IMASTNS.JDQ,1) AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.RECCD), '''') AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                                    CAST(cur_IMASTNS.MSDS AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                                    CAST(TRIM(cur_IMASTNS.BARC) AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.WARRC), ''NONE'') AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                                    CAST(COALESCE(VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE_DESCRIPTION, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                                    CAST(0 AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                                    CAST(''DEFAULT'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                                    CAST(''DEFAULT'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.BUYMULT), -2) AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.BMDESC), '''') AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                                    CAST(COALESCE(
                                    CASE 
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''1'' THEN ''1 - BLANK''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''2'' THEN ''2 - BLANK''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''AS'' THEN ''ASSORTED''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BD'' THEN ''BUNDLE''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BG'' THEN ''BAG''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BP'' THEN ''BLISTER PACK''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BX'' THEN ''BOX''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''CD'' THEN ''CARDED''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''CS'' THEN ''CASE''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''EA'' THEN ''EACH''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''FT'' THEN ''FOOT''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''KT'' THEN ''KIT''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''PK'' THEN ''PACKAGE'' 
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''PR'' THEN ''PAIR''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''RL'' THEN ''ROLL'' 
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''ST'' THEN ''SET''
                                    ELSE ''UNKNOWN''
                                    END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                                    CAST(cur_IMASTNS.VENCNVRF AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                                    CAST(cur_IMASTNS.VENCNVRQ AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                                    CAST(cur_IMASTNS.VENUOM AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTNS.UOMAMT AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                                    CAST(cur_IMASTNS.UOMQTY AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                                    CAST(cur_IMASTNS.UOMDESC AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PKTYP), '''') AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.PKTYP) = ''R'' THEN ''RETAIL'' 
                                    WHEN TRIM(cur_IMASTNS.PKTYP) = ''I'' THEN ''INNERPACK''
                                    WHEN TRIM(cur_IMASTNS.PKTYP) = ''C'' THEN ''CASE''
                                    ELSE ''UNKNOWN''
                                    END AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                                    CAST(COALESCE(cur_IMASTNS.PKLEN, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.PKWID, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.PKHGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PKWGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PKLWHF), '''') AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CSQTYF), '''') AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                                    CAST(COALESCE(cur_IMASTNS.CSLEN, 0) AS DECIMAL(12,4)) AS CASE_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.CSWID, 0) AS DECIMAL(12,4)) AS CASE_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.CSHGT, 0) AS DECIMAL(12,4)) AS CASE_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.CSWGT, 0) AS DECIMAL(12,4)) AS CASE_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CSLWHF), '''') AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(cur_IMASTNS.PTCSQTY, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                                    CAST(COALESCE(cur_IMASTNS.PTCSLYR, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                                    CAST(COALESCE(cur_IMASTNS.PTLEN, 0) AS DECIMAL(12,4)) AS PALLET_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.PTWID, 0) AS DECIMAL(12,4)) AS PALLET_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.PTHGT, 0) AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PTWGT, 0) AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PTLWHF), '''') AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SHIPCLASS , '''')AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                                    CAST(COALESCE(cur_IMASTNS.DOTID, -2) AS INTEGER) AS DOT_CLASS_NUMBER,
                                    CAST(COALESCE(cur_IMASTNS.DOTID2, -2) AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                                    CAST(COALESCE(cur_IMASTNS.CNTDESC, '''') AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.KFF) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.FLR) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETNEW) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETCORE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETWAR) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETREC) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                                    CAST(COALESCE(CASE
                                    WHEN TRIM(cur_IMASTNS.RETMANO) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.RETOSP) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_HAZUPDDTE.FULL_DATE , ''1900-01-01'') AS DATE) AS HAZARDOUS_UPDATE_DATE, 
                                    CAST(COALESCE(cur_IMASTNS.PCLEN ,0) AS DECIMAL(12,4)) AS PIECE_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.PCWID ,0) AS DECIMAL(12,4)) AS PIECE_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.PCHGT ,0) AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PCWGT ,0) AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PKCSQTY ,0.0) AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.INCATALOG),'''') AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''B'' THEN ''NOT LOADED TO ONLINE CATALOG/BRICK AND MORTAR - DISPLAY IN STORE ONLY''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''D'' THEN ''ALLOWED ONLINE, PICK UP IN STORE''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''O'' THEN ''ONLINE CATALOG''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''P'' THEN ''PROFESSIONAL CATALOG''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''R'' THEN ''RETAIL CATALOG''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''Y'' THEN ''ALL CATALOGS''
                                    ELSE ''UNKNOWN''
                                    END AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.ALWSPECORD) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.SPCORDONLY) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.SUPLCCD),'''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE, ''1900-01-01'') AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE, 
                                    CAST(COALESCE(TRIM(cur_IMASTNS.LONGDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.EWASTE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                                    CAST(COALESCE(cur_IMASTNS.STRMINSALE,0) AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                                    CAST(COALESCE(cur_IMASTNS.MSRP,0) AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                                    CAST(COALESCE(cur_IMASTNS.MAXCARQTY,0) AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                                    CAST(COALESCE(cur_IMASTNS.MINCARQTY,0) AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.ESNTLHRDPT),'''') AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.IPFLG), '''') AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                                    CAST(COALESCE(cur_IMASTNS.IPQTY, 0) AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                                    CAST(COALESCE(cur_IMASTNS.IPLEN, 0) AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.IPWID, 0) AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.IPHGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.IPWGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                                    CAST(TRIM(COALESCE(cur_ECPARTS.PART_NUMBER, cur_AAIAVEND.OCAT_PART_NUMBER, '''')) AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DISPLAY),'''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.SPANISH_PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(cur_ECPARTS.SUGGESTED_ORDER_QTY, 0) AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                                    --INFAETL-11515 begin changes
                                    CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                                    CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                                    CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                                    --INFAETL-11515 end changes
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_CMISFILE.LSTOCK) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                                    CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                                    CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                                    CAST(COALESCE(INT(cur_IMASTNS.DUNS# ),-2) AS BIGINT) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                                    CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                                    CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                                    CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                                    CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,  
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                                    CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_PACK), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                                    ' || ' CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(16 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(64 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SDCL, 0) AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                                    CAST(0 AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CRTPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_CRTDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                                    CAST(TIME(cur_IMASTNS.CRTTIME) AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CRTUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_IMAST_DIMUPDDTE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                                    CAST(COALESCE(
                                         TIME(
                                              CASE
                                              WHEN cur_IMASTNS.DIMUPDTME = 0 THEN ''00:00:00''
                                              ELSE LEFT(''0'' || cur_IMASTNS.DIMUPDTME, 2) || '':'' || SUBSTRING(RIGHT(''0'' || cur_IMASTNS.DIMUPDTME, 6), 3, 2) || '':'' || RIGHT(cur_IMASTNS.DIMUPDTME, 2) 
                                              END
                                         ),
                                    ''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.DIMUPDUSR), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.FNRDNS), '''') AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.EXFRESHPOS) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.HAZUPDPGM), '''') AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                                    CAST(TRIM(cur_IMASTNS.HAZUPDTME) AS TIME) AS HAZARDOUS_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.HAZUPDUSR), '''') AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS LIST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                                    CAST(COALESCE(cur_IMASTNS.MAPPRICE, 0) AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                                    CAST(COALESCE(cur_IMASTNS.MAPEFFDATE, ''1900-01-01'') AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                                    CAST(COALESCE(cur_IMASTNS.MINSQ, 0) AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PACKSIZE), '''') AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                                    CAST(COALESCE(cur_IMASTNS.PERCNTVENF, 0) AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.PCLWHF) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS PRICING_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.SLSPACK), '''') AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.SHIPDIMS), '''') AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.LOADTIME, ''00:00:00'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_PST_UPDDTE.FULL_DATE , ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.UPDPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_IMASTNS.UPDTIME,''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.UPDUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS VIP_JOBBER,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                                    --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                                    ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                                    CAST(''N'' AS VARCHAR(1)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                                        '|| quote_literal(v_etl_source_table_2) || ' AS ETL_SOURCE_TABLE_NAME,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                                    ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                                    ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS    
                                    FROM EDW_STAGING.CUR_IVB_IMASTNS cur_IMASTNS
                                    -- EDW_STAGING <-> EDW_STAGING 
                                    LEFT JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_IMASTNS.LINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_IMASTNS.ITEM) 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_IMASTNS.LINE AND cur_CATEGORY.ITEM = cur_IMASTNS.ITEM
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY 
                                    LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_IMASTNS.LINE AND cur_ICLINCTL.PLCD = cur_IMASTNS.PLCD AND cur_ICLINCTL.REGION = 0
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_IMASTNS.LINE 
                                                AND cur_DWASGN_DWEMP.PLCD = cur_IMASTNS.PLCD AND cur_DWASGN_DWEMP.SUBC = cur_IMASTNS.PCODE    
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_IMASTNS.LINE AND cur_REPLENISHMENT_ANALYST.PLCD = cur_IMASTNS.PLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_IMASTNS.PCODE     
                                    LEFT JOIN EDW_STAGING.CUR_IVB_DWWMSITEM AS cur_DWWMSITEM ON cur_DWWMSITEM.LINE = cur_IMASTNS.LINE AND cur_DWWMSITEM.ITEM = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_IMASTNS.LINE AND cur_CMISFILE.LPLCD = cur_IMASTNS.PLCD AND cur_CMISFILE.LSUBC = cur_IMASTNS.PCODE
                                    LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX AS cur_PMATRIX ON cur_PMATRIX.LINE = cur_IMASTNS.LINE AND cur_PMATRIX.PLCD = cur_IMASTNS.PLCD AND cur_PMATRIX.SUBC = cur_IMASTNS.PCODE
                                -- EDW_STAGING <-> EDW_STAGING 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_IMASTNS.LINE AND cur_HAZLION.ITEM_NAME = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                                                FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                                                GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_IMASTNS.LINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                                                                                                    LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                                                FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                                                GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_IMASTNS.LINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_IMASTNS.LINE AND cur_EXPECCN.ITEM = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                                                FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                                                GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_IMASTNS.LINE AND cur_EXPHTS.ITEM = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_IMASTNS.LINE AND cur_EXPUSML.ITEM = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                                                FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                                                GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_IMASTNS.LINE AND cur_EXPSCDB_LA.ITEM = cur_IMASTNS.ITEM --337921
                                    LEFT JOIN EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS ON cur_ECPARTS.LINE = cur_IMASTNS.LINE AND cur_ECPARTS.ITEMNUMBER = cur_IMASTNS.ITEM --?
                                    LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                                FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                                GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND 
                                                ON cur_AAIAVEND.OREILLY_LINE = cur_IMASTNS.LINE AND cur_AAIAVEND.KEY_ITEM = cur_IMASTNS.ITEM --114348766
                                    LEFT JOIN ODATA.VW_DIM_WARRANTY_CODE AS VW_DIM_WARRANTY_CODE_WARRANTY_CODE ON VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE = cur_IMASTNS.WARRC
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_LSTCOSTUPD ON HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE = cur_DWWMSITEM.LSTCOSTUPD
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DATELCCHG ON HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE = cur_IMASTNS.DATELCCHG
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_UPDDTE ON HUB_LOAD_DIM_DATE_UPDDTE.DATE_ID = cur_DWWMSITEM.UPDDTE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_PST_UPDDTE ON HUB_LOAD_DIM_DATE_PST_UPDDTE.FULL_DATE = cur_IMASTNS.UPDDATE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_CRTDATE ON HUB_LOAD_DIM_DATE_CRTDATE.FULL_DATE = cur_IMASTNS.CRTDATE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_HAZUPDDTE ON HUB_LOAD_DIM_DATE_HAZUPDDTE.FULL_DATE = cur_IMASTNS.HAZUPDDTE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DIMUPDDTE ON HUB_LOAD_DIM_DATE_DIMUPDDTE.DATE_ID = cur_DWWMSITEM.DIMUPDDTE 
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_IMAST_DIMUPDDTE ON HUB_LOAD_DIM_DATE_IMAST_DIMUPDDTE.DATE_ID = cur_IMASTNS.DIMUPDDTE 
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = INTEGER(cur_PMATRIX.RA_TMN)
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                                    LEFT JOIN ODATA.VW_DIM_POPULARITY_CODE AS VW_DIM_POPULARITY_CODE_DESC ON VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE = cur_IMASTNS.POPCD
                                    LEFT JOIN ODATA.VW_DIM_POPULARITY_TREND_CODE AS VW_DIM_POPULARITY_TREND_CODE_DESC ON VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE = cur_IMASTNS.TRNDC
                                    LEFT JOIN EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK ON CUR_WHNONSTK.WHNLINE = cur_IMASTNS.LINE AND cur_WHNONSTK.WHNITEM = cur_IMASTNS.ITEM
                                    LEFT JOIN ( SELECT cur_IMASTER.ILINE AS LINE, cur_IMASTER.IITEM# AS ITEM
                                                        FROM EDW_STAGING.CUR_IVB_IMASTER AS cur_IMASTER  ) AS cur_UNION ON cur_UNION.LINE = cur_IMASTNS.LINE AND cur_UNION.ITEM = cur_IMASTNS.ITEM 
                                      --INFAETL-11515 add the following 3 joins
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                  WHERE NVL(cur_UNION.LINE, '''') = ''''
                                        AND hub_DIM_PRODUCT.PRODUCT_ID IS NULL AND (' || 
                                                quote_literal(v_job_execution_starttime) || 
                                                ' <= cur_IMASTNS.CREATE_TIMESTAMP OR cur_IMASTNS.CREATE_TIMESTAMP IS NULL OR ' || 
                                                quote_literal(v_job_execution_starttime) ||
                                                    ' <= cur_IMASTNS.LOAD_TIMESTAMP OR cur_IMASTNS.LOAD_TIMESTAMP IS NULL) ' ||
                                        ' WITH UR ' ;

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTNS 2nd INSERT SQL> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '10. IMASTNS Insert', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    
            END IF; -- V_SQL_OK
 
            
                 --Populate Hub Load Table - IMASTNS
        --Merge step 1, drop "source" table

            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        

            IF V_SQL_OK THEN
               SET v_str_sql = 'DROP TABLE ' || v_staging_database_name || '.SESSION_TMP_IMASTNS_UPDATE_SOURCE if exists;';
               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                IF (V_SQL_CODE <> 0) THEN  --  Warning
                   SET V_SQL_OK = FALSE;
                   SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
            
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                   SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTNS DROP update table> '||
                       '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                       '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                       ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                       ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                       ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '11. IMASTNS_UPDATE preprocess table DROP', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

            END IF; -- V_SQL_OK

            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        
        --1st Merge step 2, recreate "source" table   
            IF V_SQL_OK THEN            
               SET v_str_sql = CLOB('CREATE  TABLE ') ||  v_staging_database_name || '.SESSION_TMP_IMASTNS_UPDATE_SOURCE
               AS (          SELECT hub_DIM_PRODUCT.PRODUCT_ID,
                                    CAST(TRIM(cur_IMASTNS.LINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTNS.ITEM) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.DESC), TRIM(cur_ECPARTS.SHORT_DESCRIPTION)) AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SUBCATNUM, -2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTNS.PLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                                    CAST(cur_IMASTNS.PCODE AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                                    CAST(COALESCE(cur_IMASTNS.MFGITEM, ''-2'') AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SLINE, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SITEM, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST(0 AS INTEGER) AS SORT_CONTROL_NUMBER,
                                    CAST(TRIM(cur_IMASTNS.PSDSC) AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                                    CAST(TRIM(cur_IMASTNS.POPCD) AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                                    CAST(COALESCE(VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE_DESCRIPTION , ''DEFAULT'') AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.TRNDC), '''') AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                                    CAST(COALESCE(VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE_DESCRIPTION , ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                                    CAST(cur_IMASTNS.JUM AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTNS.WUM AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTNS.WSQTY AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.QTYPC, '''') AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                                    CAST(cur_IMASTNS.CSQTY AS DECIMAL(12,4)) AS CASE_QUANTITY,
                                    CAST(cur_IMASTNS.STDPK AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                                    CAST(0 AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                                    CAST(0 AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                                    CAST(COALESCE(cur_IMASTNS.JDQ,1) AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.RECCD), '''') AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                                    CAST(cur_IMASTNS.MSDS AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                                    CAST(TRIM(cur_IMASTNS.BARC) AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.WARRC), ''NONE'') AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                                    CAST(COALESCE(VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE_DESCRIPTION, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                                    CAST(0 AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                                    CAST(''DEFAULT'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                                    CAST(''DEFAULT'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.BUYMULT), -2) AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.BMDESC), '''') AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                                    CAST(COALESCE(
                                    CASE 
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''1'' THEN ''1 - BLANK''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''2'' THEN ''2 - BLANK''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''AS'' THEN ''ASSORTED''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BD'' THEN ''BUNDLE''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BG'' THEN ''BAG''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BP'' THEN ''BLISTER PACK''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''BX'' THEN ''BOX''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''CD'' THEN ''CARDED''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''CS'' THEN ''CASE''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''EA'' THEN ''EACH''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''FT'' THEN ''FOOT''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''KT'' THEN ''KIT''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''PK'' THEN ''PACKAGE'' 
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''PR'' THEN ''PAIR''
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''RL'' THEN ''ROLL'' 
                                    WHEN TRIM(cur_IMASTNS.BMDESC) = ''ST'' THEN ''SET''
                                    ELSE ''UNKNOWN''
                                    END, ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                                    CAST(cur_IMASTNS.VENCNVRF AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                                    CAST(cur_IMASTNS.VENCNVRQ AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                                    CAST(cur_IMASTNS.VENUOM AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                                    CAST(cur_IMASTNS.UOMAMT AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                                    CAST(cur_IMASTNS.UOMQTY AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                                    CAST(cur_IMASTNS.UOMDESC AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PKTYP), '''') AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.PKTYP) = ''R'' THEN ''RETAIL'' 
                                    WHEN TRIM(cur_IMASTNS.PKTYP) = ''I'' THEN ''INNERPACK''
                                    WHEN TRIM(cur_IMASTNS.PKTYP) = ''C'' THEN ''CASE''
                                    ELSE ''UNKNOWN''
                                    END AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                                    CAST(COALESCE(cur_IMASTNS.PKLEN, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.PKWID, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.PKHGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PKWGT, 0) AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PKLWHF), '''') AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CSQTYF), '''') AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                                    CAST(COALESCE(cur_IMASTNS.CSLEN, 0) AS DECIMAL(12,4)) AS CASE_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.CSWID, 0) AS DECIMAL(12,4)) AS CASE_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.CSHGT, 0) AS DECIMAL(12,4)) AS CASE_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.CSWGT, 0) AS DECIMAL(12,4)) AS CASE_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CSLWHF), '''') AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(cur_IMASTNS.PTCSQTY, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                                    CAST(COALESCE(cur_IMASTNS.PTCSLYR, 0) AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                                    CAST(COALESCE(cur_IMASTNS.PTLEN, 0) AS DECIMAL(12,4)) AS PALLET_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.PTWID, 0) AS DECIMAL(12,4)) AS PALLET_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.PTHGT, 0) AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PTWGT, 0) AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PTLWHF), '''') AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SHIPCLASS , '''')AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                                    CAST(COALESCE(cur_IMASTNS.DOTID, -2) AS INTEGER) AS DOT_CLASS_NUMBER,
                                    CAST(COALESCE(cur_IMASTNS.DOTID2, -2) AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                                    CAST(COALESCE(cur_IMASTNS.CNTDESC, '''') AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.KFF) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.FLR) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETNEW) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETCORE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETWAR) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.RETREC) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                                    CAST(COALESCE(CASE
                                    WHEN TRIM(cur_IMASTNS.RETMANO) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.RETOSP) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_HAZUPDDTE.FULL_DATE , ''1900-01-01'') AS DATE) AS HAZARDOUS_UPDATE_DATE, 
                                    CAST(COALESCE(cur_IMASTNS.PCLEN ,0) AS DECIMAL(12,4)) AS PIECE_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.PCWID ,0) AS DECIMAL(12,4)) AS PIECE_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.PCHGT ,0) AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PCWGT ,0) AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.PKCSQTY ,0.0) AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.INCATALOG),'''') AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''B'' THEN ''NOT LOADED TO ONLINE CATALOG/BRICK AND MORTAR - DISPLAY IN STORE ONLY''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''D'' THEN ''ALLOWED ONLINE, PICK UP IN STORE''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''O'' THEN ''ONLINE CATALOG''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''P'' THEN ''PROFESSIONAL CATALOG''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''R'' THEN ''RETAIL CATALOG''
                                    WHEN TRIM(cur_IMASTNS.INCATALOG) = ''Y'' THEN ''ALL CATALOGS''
                                    ELSE ''UNKNOWN''
                                    END AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.ALWSPECORD) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.SPCORDONLY) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.SUPLCCD),'''') AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE, ''1900-01-01'') AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE, 
                                    CAST(COALESCE(TRIM(cur_IMASTNS.LONGDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.EWASTE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                                    CAST(COALESCE(cur_IMASTNS.STRMINSALE,0) AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                                    CAST(COALESCE(cur_IMASTNS.MSRP,0) AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                                    CAST(COALESCE(cur_IMASTNS.MAXCARQTY,0) AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                                    CAST(COALESCE(cur_IMASTNS.MINCARQTY,0) AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.ESNTLHRDPT), '''') AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.IPFLG), '''') AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                                    CAST(COALESCE(cur_IMASTNS.IPQTY, 0) AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                                    CAST(COALESCE(cur_IMASTNS.IPLEN, 0) AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                                    CAST(COALESCE(cur_IMASTNS.IPWID, 0) AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                                    CAST(COALESCE(cur_IMASTNS.IPHGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                                    CAST(COALESCE(cur_IMASTNS.IPWGT, 0) AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                                    CAST(TRIM(COALESCE(cur_ECPARTS.PART_NUMBER, cur_AAIAVEND.OCAT_PART_NUMBER, '''')) AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DISPLAY),'''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.SPANISH_PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(cur_ECPARTS.SUGGESTED_ORDER_QTY, 0) AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                                    --INFAETL-11515 begin changes
                                    CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                                    CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                                    CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                                    --INFAETL-11515 end changes
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_CMISFILE.LSTOCK) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                                    CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                                    CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                                    CAST(COALESCE(INT(cur_IMASTNS.DUNS# ),-2) AS BIGINT) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                                    CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''')  AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                                    CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                                    CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                                    CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,  
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER)AS TEAM_NAME_SORT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                                    CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_PACK), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                                    ' || ' CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(16 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(64 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                                    CAST(COALESCE(cur_IMASTNS.SDCL, 0) AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                                    CAST(0 AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CRTPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_CRTDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                                    CAST(TIME(cur_IMASTNS.CRTTIME) AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.CRTUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_IMAST_DIMUPDDTE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                                    CAST(COALESCE(
                                         TIME(
                                              CASE
                                              WHEN cur_IMASTNS.DIMUPDTME = 0 THEN ''00:00:00''
                                              ELSE LEFT(''0'' || cur_IMASTNS.DIMUPDTME, 2) || '':'' || SUBSTRING(RIGHT(''0'' || cur_IMASTNS.DIMUPDTME, 6), 3, 2) || '':'' || RIGHT(cur_IMASTNS.DIMUPDTME, 2) 
                                              END
                                         ),
                                    ''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.DIMUPDUSR), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.FNRDNS), '''') AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                                    CAST(CASE
                                    WHEN TRIM(cur_IMASTNS.EXFRESHPOS) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.HAZUPDPGM), '''') AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                                    CAST(TRIM(cur_IMASTNS.HAZUPDTME) AS TIME) AS HAZARDOUS_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.HAZUPDUSR), '''') AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS LIST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                                    CAST(COALESCE(cur_IMASTNS.MAPPRICE, 0) AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                                    CAST(COALESCE(cur_IMASTNS.MAPEFFDATE, ''1900-01-01'') AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                                    CAST(COALESCE(cur_IMASTNS.MINSQ, 0) AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.PACKSIZE), '''') AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                                    CAST(COALESCE(cur_IMASTNS.PERCNTVENF, 0) AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_IMASTNS.PCLWHF) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''N'') AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS PRICING_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.SLSPACK), '''') AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                                    CAST(COALESCE(
                                    CASE
                                    WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                                    ELSE ''N''
                                    END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.SHIPDIMS), '''') AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.LOADTIME, ''00:00:00'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_PST_UPDDTE.FULL_DATE , ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.UPDPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_IMASTNS.UPDTIME,''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(TRIM(cur_IMASTNS.UPDUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS VIP_JOBBER,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                                    --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                                    ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                                    CAST(''N'' AS VARCHAR(1)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                                        '|| quote_literal(v_etl_source_table_2) || ' AS ETL_SOURCE_TABLE_NAME,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                                    ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                                    ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS    
                                        FROM EDW_STAGING.CUR_IVB_IMASTNS cur_IMASTNS
                                        -- EDW_STAGING <-> EDW_STAGING 
                                        JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_IMASTNS.LINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_IMASTNS.ITEM) 
                                        LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_IMASTNS.LINE AND cur_CATEGORY.ITEM = cur_IMASTNS.ITEM
                                        LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY 
                                        LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_IMASTNS.LINE AND cur_ICLINCTL.PLCD = cur_IMASTNS.PLCD AND cur_ICLINCTL.REGION = 0
                                        LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                    FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                    JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                    WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_IMASTNS.LINE 
                                                    AND cur_DWASGN_DWEMP.PLCD = cur_IMASTNS.PLCD AND cur_DWASGN_DWEMP.SUBC = cur_IMASTNS.PCODE    
                                        LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                    FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                    JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                    WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_IMASTNS.LINE AND cur_REPLENISHMENT_ANALYST.PLCD = cur_IMASTNS.PLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_IMASTNS.PCODE     
                                        LEFT JOIN EDW_STAGING.CUR_IVB_DWWMSITEM AS cur_DWWMSITEM ON cur_DWWMSITEM.LINE = cur_IMASTNS.LINE AND cur_DWWMSITEM.ITEM = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_IMASTNS.LINE AND cur_CMISFILE.LPLCD = cur_IMASTNS.PLCD AND cur_CMISFILE.LSUBC = cur_IMASTNS.PCODE
                                        LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX AS cur_PMATRIX ON cur_PMATRIX.LINE = cur_IMASTNS.LINE AND cur_PMATRIX.PLCD = cur_IMASTNS.PLCD AND cur_PMATRIX.SUBC = cur_IMASTNS.PCODE
                                    -- EDW_STAGING <-> EDW_STAGING 
                                        LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_IMASTNS.LINE AND cur_HAZLION.ITEM_NAME = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                                                    FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                                                    GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_IMASTNS.LINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                                                                                                        LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                                                    FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                                                    GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_IMASTNS.LINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_IMASTNS.LINE AND cur_EXPECCN.ITEM = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                                                    FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                                                    GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_IMASTNS.LINE AND cur_EXPHTS.ITEM = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_IMASTNS.LINE AND cur_EXPUSML.ITEM = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                                                    FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                                                    GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_IMASTNS.LINE AND cur_EXPSCDB_LA.ITEM = cur_IMASTNS.ITEM --337921
                                        LEFT JOIN EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS ON cur_ECPARTS.LINE = cur_IMASTNS.LINE AND cur_ECPARTS.ITEMNUMBER = cur_IMASTNS.ITEM --?
                                        LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                                    FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                                    GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND 
                                                    ON cur_AAIAVEND.OREILLY_LINE = cur_IMASTNS.LINE AND cur_AAIAVEND.KEY_ITEM = cur_IMASTNS.ITEM --114348766
                                        LEFT JOIN ODATA.VW_DIM_WARRANTY_CODE AS VW_DIM_WARRANTY_CODE_WARRANTY_CODE ON VW_DIM_WARRANTY_CODE_WARRANTY_CODE.WARRANTY_CODE = cur_IMASTNS.WARRC
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_LSTCOSTUPD ON HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE = cur_DWWMSITEM.LSTCOSTUPD
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DATELCCHG ON HUB_LOAD_DIM_DATE_DATELCCHG.FULL_DATE = cur_IMASTNS.DATELCCHG
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_UPDDTE ON HUB_LOAD_DIM_DATE_UPDDTE.DATE_ID = cur_DWWMSITEM.UPDDTE
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_PST_UPDDTE ON HUB_LOAD_DIM_DATE_PST_UPDDTE.FULL_DATE = cur_IMASTNS.UPDDATE
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_CRTDATE ON HUB_LOAD_DIM_DATE_CRTDATE.FULL_DATE = cur_IMASTNS.CRTDATE
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_HAZUPDDTE ON HUB_LOAD_DIM_DATE_HAZUPDDTE.FULL_DATE = cur_IMASTNS.HAZUPDDTE
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DIMUPDDTE ON HUB_LOAD_DIM_DATE_DIMUPDDTE.DATE_ID = cur_DWWMSITEM.DIMUPDDTE 
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_IMAST_DIMUPDDTE ON HUB_LOAD_DIM_DATE_IMAST_DIMUPDDTE.DATE_ID = cur_IMASTNS.DIMUPDDTE 
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = BIGINT(cur_PMATRIX.RA_TMN)
                                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                                        LEFT JOIN ODATA.VW_DIM_POPULARITY_CODE AS VW_DIM_POPULARITY_CODE_DESC ON VW_DIM_POPULARITY_CODE_DESC.POPULARITY_CODE = cur_IMASTNS.POPCD
                                        LEFT JOIN ODATA.VW_DIM_POPULARITY_TREND_CODE AS VW_DIM_POPULARITY_TREND_CODE_DESC ON VW_DIM_POPULARITY_TREND_CODE_DESC.POPULARITY_TREND_CODE = cur_IMASTNS.TRNDC
                                        LEFT JOIN EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK ON CUR_WHNONSTK.WHNLINE = cur_IMASTNS.LINE AND cur_WHNONSTK.WHNITEM = cur_IMASTNS.ITEM
                                        LEFT JOIN ( SELECT cur_IMASTER.ILINE AS LINE, cur_IMASTER.IITEM# AS ITEM
                                                FROM EDW_STAGING.CUR_IVB_IMASTER AS cur_IMASTER  ) AS cur_UNION ON cur_UNION.LINE = cur_IMASTNS.LINE AND cur_UNION.ITEM = cur_IMASTNS.ITEM 
                                        --INFAETL-11515 add the following 3 joins
                                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                                WHERE NVL(cur_UNION.LINE, '''') = ''''
                  --                                  AND hub_DIM_PRODUCT.PRODUCT_ID IS NOT NULL
                  ) WITH DATA
                  DISTRIBUTE ON HASH("PRODUCT_ID")
                  IN TS_EDW 
                  ORGANIZE BY COLUMN;';               

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTNS Source Update CTAS> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '12. IMASTNS UPDATE preprocess table CTAS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                        
                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

            END IF; --v_SQL_OK
            
            IF V_SQL_OK THEN
    
               SET v_str_sql =   CLOB('UPDATE ') || v_staging_database_name || '.' || v_staging_table_name || ' AS tgt ' || 
                 'SET tgt.LINE_DESCRIPTION=src.LINE_DESCRIPTION,
                            tgt.ITEM_DESCRIPTION=src.ITEM_DESCRIPTION,
                            tgt.SEGMENT_NUMBER=src.SEGMENT_NUMBER,
                            tgt.SEGMENT_DESCRIPTION=src.SEGMENT_DESCRIPTION,
                            tgt.SUB_CATEGORY_NUMBER=src.SUB_CATEGORY_NUMBER,
                            tgt.SUB_CATEGORY_DESCRIPTION=src.SUB_CATEGORY_DESCRIPTION,
                            tgt.CATEGORY_NUMBER=src.CATEGORY_NUMBER,
                            tgt.CATEGORY_DESCRIPTION=src.CATEGORY_DESCRIPTION,
                            tgt.PRODUCT_LINE_CODE=src.PRODUCT_LINE_CODE,
                            tgt.SUB_CODE=src.SUB_CODE,
                            tgt.MANUFACTURE_ITEM_NUMBER_CODE=src.MANUFACTURE_ITEM_NUMBER_CODE,
                            tgt.SUPERSEDED_LINE_CODE=src.SUPERSEDED_LINE_CODE,
                            tgt.SUPERSEDED_ITEM_NUMBER_CODE=src.SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SORT_CONTROL_NUMBER=src.SORT_CONTROL_NUMBER,
                            tgt.POINT_OF_SALE_DESCRIPTION=src.POINT_OF_SALE_DESCRIPTION,
                            tgt.POPULARITY_CODE=src.POPULARITY_CODE,
                            tgt.POPULARITY_CODE_DESCRIPTION=src.POPULARITY_CODE_DESCRIPTION,
                            tgt.POPULARITY_TREND_CODE=src.POPULARITY_TREND_CODE,
                            tgt.POPULARITY_TREND_CODE_DESCRIPTION=src.POPULARITY_TREND_CODE_DESCRIPTION,
                            tgt.LINE_IS_MARINE_SPECIFIC_FLAG=src.LINE_IS_MARINE_SPECIFIC_FLAG,
                            tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE=src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                            tgt.LINE_IS_FLEET_SPECIFIC_CODE=src.LINE_IS_FLEET_SPECIFIC_CODE,
                            tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE=src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                            tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG=src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                            tgt.JOBBER_SUPPLIER_CODE=src.JOBBER_SUPPLIER_CODE,
                            tgt.JOBBER_UNIT_OF_MEASURE_CODE=src.JOBBER_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE=src.WAREHOUSE_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_SELL_QUANTITY=src.WAREHOUSE_SELL_QUANTITY,
                            tgt.RETAIL_WEIGHT=src.RETAIL_WEIGHT,
                            tgt.QUANTITY_PER_CAR=src.QUANTITY_PER_CAR,
                            tgt.CASE_QUANTITY=src.CASE_QUANTITY,
                            tgt.STANDARD_PACKAGE=src.STANDARD_PACKAGE,
                            tgt.PAINT_BODY_AND_EQUIPMENT_PRICE=src.PAINT_BODY_AND_EQUIPMENT_PRICE,
                            tgt.WAREHOUSE_JOBBER_PRICE=src.WAREHOUSE_JOBBER_PRICE,
                            tgt.WAREHOUSE_COST_WUM=src.WAREHOUSE_COST_WUM,
                            tgt.WAREHOUSE_CORE_WUM=src.WAREHOUSE_CORE_WUM,
                            tgt.OREILLY_COST_PRICE=src.OREILLY_COST_PRICE,
                            tgt.JOBBER_COST=src.JOBBER_COST,
                            tgt.JOBBER_CORE_PRICE=src.JOBBER_CORE_PRICE,
                            tgt.OUT_FRONT_MERCHANDISE_FLAG=src.OUT_FRONT_MERCHANDISE_FLAG,
                            tgt.ITEM_IS_TAXED_FLAG=src.ITEM_IS_TAXED_FLAG,
                            tgt.QUANTITY_ORDER_ITEM_FLAG=src.QUANTITY_ORDER_ITEM_FLAG,
                            tgt.JOBBER_DIVIDE_QUANTITY=src.JOBBER_DIVIDE_QUANTITY,
                            tgt.ITEM_DELETE_FLAG_RECORD_CODE=src.ITEM_DELETE_FLAG_RECORD_CODE,
                            tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE=src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                            tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE=src.PRIMARY_UNIVERSAL_PRODUCT_CODE,
                            tgt.WARRANTY_CODE=src.WARRANTY_CODE,
                            tgt.WARRANTY_CODE_DESCRIPTION=src.WARRANTY_CODE_DESCRIPTION,
                            tgt.INVOICE_COST_WUM_INVOICE_COST=src.INVOICE_COST_WUM_INVOICE_COST,
                            tgt.INVOICE_CORE_WUM_CORE_COST=src.INVOICE_CORE_WUM_CORE_COST,
                            tgt.IS_CONSIGNMENT_ITEM_FLAG=src.IS_CONSIGNMENT_ITEM_FLAG,
                            tgt.WAREHOUSE_JOBBER_CORE_PRICE=src.WAREHOUSE_JOBBER_CORE_PRICE,
                            tgt.ACQUISITION_FIELD_1_CODE=src.ACQUISITION_FIELD_1_CODE,
                            tgt.ACQUISITION_FIELD_2_CODE=src.ACQUISITION_FIELD_2_CODE,
                            tgt.BUY_MULTIPLE=src.BUY_MULTIPLE,
                            tgt.BUY_MULTIPLE_CODE=src.BUY_MULTIPLE_CODE,
                            tgt.BUY_MULTIPLE_CODE_DESCRIPTION=src.BUY_MULTIPLE_CODE_DESCRIPTION,
                            tgt.SUPPLIER_CONVERSION_FACTOR_CODE=src.SUPPLIER_CONVERSION_FACTOR_CODE,
                            tgt.SUPPLIER_CONVERSION_QUANTITY=src.SUPPLIER_CONVERSION_QUANTITY,
                            tgt.SUPPLIER_UNIT_OF_MEASURE_CODE=src.SUPPLIER_UNIT_OF_MEASURE_CODE,
                            tgt.UNIT_OF_MEASURE_AMOUNT=src.UNIT_OF_MEASURE_AMOUNT,
                            tgt.UNIT_OF_MEASURE_QUANTITY=src.UNIT_OF_MEASURE_QUANTITY,
                            tgt.UNIT_OF_MEASURE_DESCRIPTION=src.UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_CODE=src.TAX_CLASSIFICATION_CODE,
                            tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION=src.TAX_CLASSIFICATION_CODE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE=src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                            tgt.DISTRIBUTION_CENTER_PICK_LENGTH=src.DISTRIBUTION_CENTER_PICK_LENGTH,
                            tgt.DISTRIBUTION_CENTER_PICK_WIDTH=src.DISTRIBUTION_CENTER_PICK_WIDTH,
                            tgt.DISTRIBUTION_CENTER_PICK_HEIGHT=src.DISTRIBUTION_CENTER_PICK_HEIGHT,
                            tgt.DISTRIBUTION_CENTER_PICK_WEIGHT=src.DISTRIBUTION_CENTER_PICK_WEIGHT,
                            tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE=src.PICK_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASE_QUANTITY_CODE=src.CASE_QUANTITY_CODE,
                            tgt.CASE_LENGTH=src.CASE_LENGTH,
                            tgt.CASE_WIDTH=src.CASE_WIDTH,
                            tgt.CASE_HEIGHT=src.CASE_HEIGHT,
                            tgt.CASE_WEIGHT=src.CASE_WEIGHT,
                            tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE=src.CASE_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASES_PER_PALLET=src.CASES_PER_PALLET,
                            tgt.CASES_PER_PALLET_LAYER=src.CASES_PER_PALLET_LAYER,
                            tgt.PALLET_LENGTH=src.PALLET_LENGTH,
                            tgt.PALLET_WIDTH=src.PALLET_WIDTH,
                            tgt.PALLET_HEIGHT=src.PALLET_HEIGHT,
                            tgt.PALLET_WEIGHT=src.PALLET_WEIGHT,
                            tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE=src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.SHIPMENT_CLASS_CODE=src.SHIPMENT_CLASS_CODE,
                            tgt.DOT_CLASS_NUMBER=src.DOT_CLASS_NUMBER,
                            tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER=src.DOT_CLASS_FOR_MSDS_ID_NUMBER,
                            tgt.CONTAINER_DESCRIPTION=src.CONTAINER_DESCRIPTION,
                            tgt.KEEP_FROM_FREEZING_FLAG=src.KEEP_FROM_FREEZING_FLAG,
                            tgt.FLIGHT_RESTRICTED_FLAG=src.FLIGHT_RESTRICTED_FLAG,
                            tgt.ALLOW_NEW_RETURNS_FLAG=src.ALLOW_NEW_RETURNS_FLAG,
                            tgt.ALLOW_CORE_RETURNS_FLAG=src.ALLOW_CORE_RETURNS_FLAG,
                            tgt.ALLOW_WARRANTY_RETURNS_FLAG=src.ALLOW_WARRANTY_RETURNS_FLAG,
                            tgt.ALLOW_RECALL_RETURNS_FLAG=src.ALLOW_RECALL_RETURNS_FLAG,
                            tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG=src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                            tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG=src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                            tgt.HAZARDOUS_UPDATE_DATE=src.HAZARDOUS_UPDATE_DATE,
                            tgt.PIECE_LENGTH=src.PIECE_LENGTH,
                            tgt.PIECE_WIDTH=src.PIECE_WIDTH,
                            tgt.PIECE_HEIGHT=src.PIECE_HEIGHT,
                            tgt.PIECE_WEIGHT=src.PIECE_WEIGHT,
                            tgt.PIECES_INNER_PACK=src.PIECES_INNER_PACK,
                            tgt.IN_CATALOG_CODE=src.IN_CATALOG_CODE,
                            tgt.IN_CATALOG_CODE_DESCRIPTION=src.IN_CATALOG_CODE_DESCRIPTION,
                            tgt.ALLOW_SPECIAL_ORDER_FLAG=src.ALLOW_SPECIAL_ORDER_FLAG,
                            tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG=src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                            tgt.SUPPLIER_LIFE_CYCLE_CODE=src.SUPPLIER_LIFE_CYCLE_CODE,
                            tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE=src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                            tgt.LONG_DESCRIPTION=src.LONG_DESCRIPTION,
                            tgt.ELECTRONIC_WASTE_FLAG=src.ELECTRONIC_WASTE_FLAG,
                            tgt.STORE_MINIMUM_SALE_QUANTITY=src.STORE_MINIMUM_SALE_QUANTITY,
                            tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE=src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                            tgt.MAXIMUM_CAR_QUANTITY=src.MAXIMUM_CAR_QUANTITY,
                            tgt.MINIMUM_CAR_QUANTITY=src.MINIMUM_CAR_QUANTITY,
                            tgt.ESSENTIAL_HARD_PART_CODE=src.ESSENTIAL_HARD_PART_CODE,
                            tgt.INNER_PACK_CODE=src.INNER_PACK_CODE,
                            tgt.INNER_PACK_QUANTITY=src.INNER_PACK_QUANTITY,
                            tgt.INNER_PACK_LENGTH=src.INNER_PACK_LENGTH,
                            tgt.INNER_PACK_WIDTH=src.INNER_PACK_WIDTH,
                            tgt.INNER_PACK_HEIGHT=src.INNER_PACK_HEIGHT,
                            tgt.INNER_PACK_WEIGHT=src.INNER_PACK_WEIGHT,
                            tgt.BRAND_CODE=src.BRAND_CODE,
                            tgt.PART_NUMBER_CODE=src.PART_NUMBER_CODE,
                            tgt.PART_NUMBER_DISPLAY_CODE=src.PART_NUMBER_DISPLAY_CODE,
                            tgt.PART_NUMBER_DESCRIPTION=src.PART_NUMBER_DESCRIPTION,
                            tgt.SPANISH_PART_NUMBER_DESCRIPTION=src.SPANISH_PART_NUMBER_DESCRIPTION,
                            tgt.SUGGESTED_ORDER_QUANTITY=src.SUGGESTED_ORDER_QUANTITY,
                            tgt.BRAND_TYPE_NAME=src.BRAND_TYPE_NAME,
                            tgt.LOCATION_TYPE_NAME=src.LOCATION_TYPE_NAME,
                            tgt.MANUFACTURING_CODE_DESCRIPTION=src.MANUFACTURING_CODE_DESCRIPTION,
                            tgt.QUALITY_GRADE_CODE=src.QUALITY_GRADE_CODE,
                            tgt.PRIMARY_APPLICATION_NAME=src.PRIMARY_APPLICATION_NAME,
                            --INFAETL-11515 begin change
                            tgt.CATEGORY_MANAGER_NAME=src.CATEGORY_MANAGER_NAME,
                            tgt.CATEGORY_MANAGER_NUMBER=src.CATEGORY_MANAGER_NUMBER,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                            tgt.CATEGORY_DIRECTOR_NAME=src.CATEGORY_DIRECTOR_NAME,
                            tgt.CATEGORY_DIRECTOR_NUMBER=src.CATEGORY_DIRECTOR_NUMBER,
                            tgt.CATEGORY_VP_NAME=src.CATEGORY_VP_NAME,
                            tgt.CATEGORY_VP_NUMBER=src.CATEGORY_VP_NUMBER,
                            --INFAETL-11515 end change
                            tgt.INACTIVATED_DATE=src.INACTIVATED_DATE,
                            tgt.REVIEW_CODE=src.REVIEW_CODE,
                            tgt.STOCKING_LINE_FLAG=src.STOCKING_LINE_FLAG,
                            tgt.OIL_LINE_FLAG=src.OIL_LINE_FLAG,
                            tgt.SPECIAL_REQUIREMENTS_LABEL=src.SPECIAL_REQUIREMENTS_LABEL,
                            tgt.SUPPLIER_ACCOUNT_NUMBER=src.SUPPLIER_ACCOUNT_NUMBER,
                            tgt.SUPPLIER_NUMBER=src.SUPPLIER_NUMBER,
                            tgt.SUPPLIER_ID=src.SUPPLIER_ID,
                            tgt.BRAND_DESCRIPTION=src.BRAND_DESCRIPTION,
                            tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER=src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                            tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER=src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                            tgt.SALES_AREA_NAME=src.SALES_AREA_NAME,
                            tgt.TEAM_NAME=src.TEAM_NAME,
                            tgt.CATEGORY_NAME=src.CATEGORY_NAME,
                            tgt.REPLENISHMENT_ANALYST_NAME=src.REPLENISHMENT_ANALYST_NAME,
                            tgt.REPLENISHMENT_ANALYST_NUMBER=src.REPLENISHMENT_ANALYST_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER=src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID=src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                            tgt.SALES_AREA_NAME_SORT_NUMBER=src.SALES_AREA_NAME_SORT_NUMBER,
                            tgt.TEAM_NAME_SORT_NUMBER=src.TEAM_NAME_SORT_NUMBER,
                            tgt.BUYER_CODE=src.BUYER_CODE,
                            tgt.BUYER_NAME=src.BUYER_NAME,
                            tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE=src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                            tgt.BATTERY_PACKING_INSTRUCTIONS_CODE=src.BATTERY_PACKING_INSTRUCTIONS_CODE,
                            tgt.BATTERY_MANUFACTURING_NAME=src.BATTERY_MANUFACTURING_NAME,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1=src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2=src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3=src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4=src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                            tgt.BATTERY_MANUFACTURING_CITY_NAME=src.BATTERY_MANUFACTURING_CITY_NAME,
                            tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME=src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                            tgt.BATTERY_MANUFACTURING_STATE_NAME=src.BATTERY_MANUFACTURING_STATE_NAME,
                            tgt.BATTERY_MANUFACTURING_ZIP_CODE=src.BATTERY_MANUFACTURING_ZIP_CODE,
                            tgt.BATTERY_MANUFACTURING_COUNTRY_CODE=src.BATTERY_MANUFACTURING_COUNTRY_CODE,
                            tgt.BATTERY_PHONE_NUMBER_CODE=src.BATTERY_PHONE_NUMBER_CODE,
                            tgt.BATTERY_WEIGHT_IN_GRAMS=src.BATTERY_WEIGHT_IN_GRAMS,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL=src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY=src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                            tgt.BATTERY_WATT_HOURS_PER_CELL=src.BATTERY_WATT_HOURS_PER_CELL,
                            tgt.BATTERY_WATT_HOURS_PER_BATTERY=src.BATTERY_WATT_HOURS_PER_BATTERY,
                            tgt.BATTERY_CELLS_NUMBER=src.BATTERY_CELLS_NUMBER,
                            tgt.BATTERIES_PER_PACKAGE_NUMBER=src.BATTERIES_PER_PACKAGE_NUMBER,
                            tgt.BATTERIES_IN_EQUIPMENT_NUMBER=src.BATTERIES_IN_EQUIPMENT_NUMBER,
                            tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG=src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                            tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG=src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                            tgt.COUNTRY_OF_ORIGIN_NAME_LIST=src.COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST=src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                            tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST=src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                            tgt.SCHEDULE_B_CODE_LIST=src.SCHEDULE_B_CODE_LIST,
                            tgt.UNITED_STATES_MUNITIONS_LIST_CODE=src.UNITED_STATES_MUNITIONS_LIST_CODE,
                            tgt.PROJECT_COORDINATOR_ID_CODE=src.PROJECT_COORDINATOR_ID_CODE,
                            tgt.PROJECT_COORDINATOR_EMPLOYEE_ID=src.PROJECT_COORDINATOR_EMPLOYEE_ID,
                            tgt.STOCK_ADJUSTMENT_MONTH_NUMBER=src.STOCK_ADJUSTMENT_MONTH_NUMBER,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.ALL_IN_COST=src.ALL_IN_COST,
                            tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE=src.CANCEL_OR_BACKORDER_REMAINDER_CODE,
                            tgt.CASE_LOT_DISCOUNT=src.CASE_LOT_DISCOUNT,
                            tgt.COMPANY_NUMBER=src.COMPANY_NUMBER,
                            tgt.CONVENIENCE_PACK_QUANTITY=src.CONVENIENCE_PACK_QUANTITY,
                            tgt.CONVENIENCE_PACK_DESCRIPTION=src.CONVENIENCE_PACK_DESCRIPTION,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE=src.PRODUCT_SOURCE_TABLE_CREATION_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME=src.PRODUCT_SOURCE_TABLE_CREATION_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE=src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                            tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE=src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                            tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE=src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                            tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE=src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                            tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG=src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                            tgt.HAZARDOUS_UPDATE_PROGRAM_NAME=src.HAZARDOUS_UPDATE_PROGRAM_NAME,
                            tgt.HAZARDOUS_UPDATE_TIME=src.HAZARDOUS_UPDATE_TIME,
                            tgt.HAZARDOUS_UPDATE_USER_NAME=src.HAZARDOUS_UPDATE_USER_NAME,
                            tgt.LIST_PRICE=src.LIST_PRICE,
                            tgt.LOW_USER_PRICE=src.LOW_USER_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE=src.MINIMUM_ADVERTISED_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE=src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                            tgt.MINIMUM_SELL_QUANTITY=src.MINIMUM_SELL_QUANTITY,
                            tgt.PACKAGE_SIZE_DESCRIPTION=src.PACKAGE_SIZE_DESCRIPTION,
                            tgt.PERCENTAGE_OF_SUPPLIER_FUNDING=src.PERCENTAGE_OF_SUPPLIER_FUNDING,
                            tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG=src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                            tgt.PRICING_COST=src.PRICING_COST,
                            tgt.PROFESSIONAL_PRICE=src.PROFESSIONAL_PRICE,
                            tgt.RETAIL_CORE=src.RETAIL_CORE,
                            tgt.RETAIL_HEIGHT=src.RETAIL_HEIGHT,
                            tgt.RETAIL_LENGTH=src.RETAIL_LENGTH,
                            tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION=src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.RETAIL_WIDTH=src.RETAIL_WIDTH,
                            tgt.SALES_PACK_CODE=src.SALES_PACK_CODE,
                            tgt.SCORE_FLAG=src.SCORE_FLAG,
                            tgt.SHIPPING_DIMENSIONS_CODE=src.SHIPPING_DIMENSIONS_CODE,
                            tgt.SUPPLIER_BASE_COST=src.SUPPLIER_BASE_COST,
                            tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE=src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SUPPLIER_SUPERSEDED_LINE_CODE=src.SUPPLIER_SUPERSEDED_LINE_CODE,
                            tgt.CATEGORY_TABLE_CREATE_DATE=src.CATEGORY_TABLE_CREATE_DATE,
                            tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME=src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_CREATE_TIME=src.CATEGORY_TABLE_CREATE_TIME,
                            tgt.CATEGORY_TABLE_CREATE_USER_NAME=src.CATEGORY_TABLE_CREATE_USER_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_DATE=src.CATEGORY_TABLE_UPDATE_DATE,
                            tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME=src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_TIME=src.CATEGORY_TABLE_UPDATE_TIME,
                            tgt.CATEGORY_TABLE_UPDATE_USER_NAME=src.CATEGORY_TABLE_UPDATE_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                            tgt.VIP_JOBBER=src.VIP_JOBBER,
                            tgt.WAREHOUSE_CORE=src.WAREHOUSE_CORE,
                            tgt.WAREHOUSE_COST=src.WAREHOUSE_COST,
                            --INFAETL-11815 added the next line 
                            tgt.PRODUCT_LEVEL_CODE = src.PRODUCT_LEVEL_CODE,
                            tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
                            tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
                            tgt.ETL_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP-CURRENT_TIMEZONE,
                            tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
                            tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS ' ||    '        
                 from '|| v_staging_database_name || '.SESSION_TMP_IMASTNS_UPDATE_SOURCE SRC 
                  WHERE tgt.product_id = src.product_id
                  AND (
                     COALESCE(tgt.LINE_DESCRIPTION,''A'') <> COALESCE(src.LINE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ITEM_DESCRIPTION,''A'') <> COALESCE(src.ITEM_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SEGMENT_NUMBER,0) <> COALESCE(src.SEGMENT_NUMBER,0)
                    OR COALESCE(tgt.SEGMENT_DESCRIPTION,''A'') <> COALESCE(src.SEGMENT_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUB_CATEGORY_NUMBER,0) <> COALESCE(src.SUB_CATEGORY_NUMBER,0)
                    OR COALESCE(tgt.SUB_CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.SUB_CATEGORY_DESCRIPTION,''A'')
                    OR COALESCE(tgt.CATEGORY_NUMBER,0) <> COALESCE(src.CATEGORY_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.CATEGORY_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PRODUCT_LINE_CODE,''A'') <> COALESCE(src.PRODUCT_LINE_CODE,''A'')
                    OR COALESCE(tgt.SUB_CODE,''A'') <> COALESCE(src.SUB_CODE,''A'')
                    OR COALESCE(tgt.MANUFACTURE_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.MANUFACTURE_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPERSEDED_LINE_CODE,''A'')
                    OR COALESCE(tgt.SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SORT_CONTROL_NUMBER,0) <> COALESCE(src.SORT_CONTROL_NUMBER,0)
                    OR COALESCE(tgt.POINT_OF_SALE_DESCRIPTION,''A'') <> COALESCE(src.POINT_OF_SALE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.POPULARITY_CODE,''A'') <> COALESCE(src.POPULARITY_CODE,''A'')
                    OR COALESCE(tgt.POPULARITY_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.POPULARITY_TREND_CODE,''A'') <> COALESCE(src.POPULARITY_TREND_CODE,''A'')
                    OR COALESCE(tgt.POPULARITY_TREND_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_TREND_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.LINE_IS_MARINE_SPECIFIC_FLAG,''A'') <> COALESCE(src.LINE_IS_MARINE_SPECIFIC_FLAG,''A'')
                    OR COALESCE(tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_FLEET_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_FLEET_SPECIFIC_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'')
                    OR COALESCE(tgt.JOBBER_SUPPLIER_CODE,''A'') <> COALESCE(src.JOBBER_SUPPLIER_CODE,''A'')
                    OR COALESCE(tgt.JOBBER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.JOBBER_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.WAREHOUSE_SELL_QUANTITY,0) <> COALESCE(src.WAREHOUSE_SELL_QUANTITY,0)
                    OR COALESCE(tgt.RETAIL_WEIGHT,0) <> COALESCE(src.RETAIL_WEIGHT,0)
                    OR COALESCE(tgt.QUANTITY_PER_CAR,0) <> COALESCE(src.QUANTITY_PER_CAR,0)
                    OR COALESCE(tgt.CASE_QUANTITY,0) <> COALESCE(src.CASE_QUANTITY,0)
                    OR COALESCE(tgt.STANDARD_PACKAGE,0) <> COALESCE(src.STANDARD_PACKAGE,0)
                    OR COALESCE(tgt.PAINT_BODY_AND_EQUIPMENT_PRICE,0) <> COALESCE(src.PAINT_BODY_AND_EQUIPMENT_PRICE,0)
                    OR COALESCE(tgt.WAREHOUSE_JOBBER_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_PRICE,0)
                    OR COALESCE(tgt.WAREHOUSE_COST_WUM,0) <> COALESCE(src.WAREHOUSE_COST_WUM,0)
                    OR COALESCE(tgt.WAREHOUSE_CORE_WUM,0) <> COALESCE(src.WAREHOUSE_CORE_WUM,0)
                    OR COALESCE(tgt.OREILLY_COST_PRICE,0) <> COALESCE(src.OREILLY_COST_PRICE,0)
                    OR COALESCE(tgt.JOBBER_COST,0) <> COALESCE(src.JOBBER_COST,0)
                    OR COALESCE(tgt.JOBBER_CORE_PRICE,0) <> COALESCE(src.JOBBER_CORE_PRICE,0)
                    OR COALESCE(tgt.OUT_FRONT_MERCHANDISE_FLAG,''A'') <> COALESCE(src.OUT_FRONT_MERCHANDISE_FLAG,''A'')
                    OR COALESCE(tgt.ITEM_IS_TAXED_FLAG,''A'') <> COALESCE(src.ITEM_IS_TAXED_FLAG,''A'')
                    OR COALESCE(tgt.QUANTITY_ORDER_ITEM_FLAG,''A'') <> COALESCE(src.QUANTITY_ORDER_ITEM_FLAG,''A'')
                    OR COALESCE(tgt.JOBBER_DIVIDE_QUANTITY,0) <> COALESCE(src.JOBBER_DIVIDE_QUANTITY,0)
                    OR COALESCE(tgt.ITEM_DELETE_FLAG_RECORD_CODE,''A'') <> COALESCE(src.ITEM_DELETE_FLAG_RECORD_CODE,''A'')
                    OR COALESCE(tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'') <> COALESCE(src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'')
                    OR COALESCE(tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'') <> COALESCE(src.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'')
                    OR COALESCE(tgt.WARRANTY_CODE,''A'') <> COALESCE(src.WARRANTY_CODE,''A'')
                    OR COALESCE(tgt.WARRANTY_CODE_DESCRIPTION,''A'') <> COALESCE(src.WARRANTY_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.INVOICE_COST_WUM_INVOICE_COST,0) <> COALESCE(src.INVOICE_COST_WUM_INVOICE_COST,0)
                    OR COALESCE(tgt.INVOICE_CORE_WUM_CORE_COST,0) <> COALESCE(src.INVOICE_CORE_WUM_CORE_COST,0)
                    OR COALESCE(tgt.IS_CONSIGNMENT_ITEM_FLAG,''A'') <> COALESCE(src.IS_CONSIGNMENT_ITEM_FLAG,''A'')
                    OR COALESCE(tgt.WAREHOUSE_JOBBER_CORE_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_CORE_PRICE,0)
                    OR COALESCE(tgt.ACQUISITION_FIELD_1_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_1_CODE,''A'')
                    OR COALESCE(tgt.ACQUISITION_FIELD_2_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_2_CODE,''A'')
                    OR COALESCE(tgt.BUY_MULTIPLE,0) <> COALESCE(src.BUY_MULTIPLE,0)
                    OR COALESCE(tgt.BUY_MULTIPLE_CODE,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE,''A'')
                    OR COALESCE(tgt.BUY_MULTIPLE_CODE_DESCRIPTION,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUPPLIER_CONVERSION_FACTOR_CODE,''A'') <> COALESCE(src.SUPPLIER_CONVERSION_FACTOR_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_CONVERSION_QUANTITY,0) <> COALESCE(src.SUPPLIER_CONVERSION_QUANTITY,0)
                    OR COALESCE(tgt.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.UNIT_OF_MEASURE_AMOUNT,0) <> COALESCE(src.UNIT_OF_MEASURE_AMOUNT,0)
                    OR COALESCE(tgt.UNIT_OF_MEASURE_QUANTITY,0) <> COALESCE(src.UNIT_OF_MEASURE_QUANTITY,0)
                    OR COALESCE(tgt.UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.UNIT_OF_MEASURE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_LENGTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_LENGTH,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WIDTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WIDTH,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_HEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_HEIGHT,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WEIGHT,0)
                    OR COALESCE(tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.CASE_QUANTITY_CODE,''A'') <> COALESCE(src.CASE_QUANTITY_CODE,''A'')
                    OR COALESCE(tgt.CASE_LENGTH,0) <> COALESCE(src.CASE_LENGTH,0)
                    OR COALESCE(tgt.CASE_WIDTH,0) <> COALESCE(src.CASE_WIDTH,0)
                    OR COALESCE(tgt.CASE_HEIGHT,0) <> COALESCE(src.CASE_HEIGHT,0)
                    OR COALESCE(tgt.CASE_WEIGHT,0) <> COALESCE(src.CASE_WEIGHT,0)
                    OR COALESCE(tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.CASES_PER_PALLET,0) <> COALESCE(src.CASES_PER_PALLET,0)
                    OR COALESCE(tgt.CASES_PER_PALLET_LAYER,0) <> COALESCE(src.CASES_PER_PALLET_LAYER,0)
                    OR COALESCE(tgt.PALLET_LENGTH,0) <> COALESCE(src.PALLET_LENGTH,0)
                    OR COALESCE(tgt.PALLET_WIDTH,0) <> COALESCE(src.PALLET_WIDTH,0)
                    OR COALESCE(tgt.PALLET_HEIGHT,0) <> COALESCE(src.PALLET_HEIGHT,0)
                    OR COALESCE(tgt.PALLET_WEIGHT,0) <> COALESCE(src.PALLET_WEIGHT,0)
                    OR COALESCE(tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.SHIPMENT_CLASS_CODE,''A'') <> COALESCE(src.SHIPMENT_CLASS_CODE,''A'')
                    OR COALESCE(tgt.DOT_CLASS_NUMBER,0) <> COALESCE(src.DOT_CLASS_NUMBER,0)
                    OR COALESCE(tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER,0) <> COALESCE(src.DOT_CLASS_FOR_MSDS_ID_NUMBER,0)
                    OR COALESCE(tgt.CONTAINER_DESCRIPTION,''A'') <> COALESCE(src.CONTAINER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.KEEP_FROM_FREEZING_FLAG,''A'') <> COALESCE(src.KEEP_FROM_FREEZING_FLAG,''A'')
                    OR COALESCE(tgt.FLIGHT_RESTRICTED_FLAG,''A'') <> COALESCE(src.FLIGHT_RESTRICTED_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_NEW_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_NEW_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_CORE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_CORE_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_WARRANTY_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_WARRANTY_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_RECALL_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_RECALL_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.HAZARDOUS_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PIECE_LENGTH,0) <> COALESCE(src.PIECE_LENGTH,0)
                    OR COALESCE(tgt.PIECE_WIDTH,0) <> COALESCE(src.PIECE_WIDTH,0)
                    OR COALESCE(tgt.PIECE_HEIGHT,0) <> COALESCE(src.PIECE_HEIGHT,0)
                    OR COALESCE(tgt.PIECE_WEIGHT,0) <> COALESCE(src.PIECE_WEIGHT,0)
                    OR COALESCE(tgt.PIECES_INNER_PACK,0) <> COALESCE(src.PIECES_INNER_PACK,0)
                    OR COALESCE(tgt.IN_CATALOG_CODE,''A'') <> COALESCE(src.IN_CATALOG_CODE,''A'')
                    OR COALESCE(tgt.IN_CATALOG_CODE_DESCRIPTION,''A'') <> COALESCE(src.IN_CATALOG_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ALLOW_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ALLOW_SPECIAL_ORDER_FLAG,''A'')
                    OR COALESCE(tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'')
                    OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CODE,''A'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.LONG_DESCRIPTION,''A'') <> COALESCE(src.LONG_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ELECTRONIC_WASTE_FLAG,''A'') <> COALESCE(src.ELECTRONIC_WASTE_FLAG,''A'')
                    OR COALESCE(tgt.STORE_MINIMUM_SALE_QUANTITY,0) <> COALESCE(src.STORE_MINIMUM_SALE_QUANTITY,0)
                    OR COALESCE(tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0) <> COALESCE(src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0)
                    OR COALESCE(tgt.MAXIMUM_CAR_QUANTITY,0) <> COALESCE(src.MAXIMUM_CAR_QUANTITY,0)
                    OR COALESCE(tgt.MINIMUM_CAR_QUANTITY,0) <> COALESCE(src.MINIMUM_CAR_QUANTITY,0)
                    OR COALESCE(tgt.ESSENTIAL_HARD_PART_CODE,''A'') <> COALESCE(src.ESSENTIAL_HARD_PART_CODE,''A'')
                    OR COALESCE(tgt.INNER_PACK_CODE,''A'') <> COALESCE(src.INNER_PACK_CODE,''A'')
                    OR COALESCE(tgt.INNER_PACK_QUANTITY,0) <> COALESCE(src.INNER_PACK_QUANTITY,0)
                    OR COALESCE(tgt.INNER_PACK_LENGTH,0) <> COALESCE(src.INNER_PACK_LENGTH,0)
                    OR COALESCE(tgt.INNER_PACK_WIDTH,0) <> COALESCE(src.INNER_PACK_WIDTH,0)
                    OR COALESCE(tgt.INNER_PACK_HEIGHT,0) <> COALESCE(src.INNER_PACK_HEIGHT,0)
                    OR COALESCE(tgt.INNER_PACK_WEIGHT,0) <> COALESCE(src.INNER_PACK_WEIGHT,0)
                    OR COALESCE(tgt.BRAND_CODE,''A'') <> COALESCE(src.BRAND_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_CODE,''A'') <> COALESCE(src.PART_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_DISPLAY_CODE,''A'') <> COALESCE(src.PART_NUMBER_DISPLAY_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.PART_NUMBER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SPANISH_PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.SPANISH_PART_NUMBER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUGGESTED_ORDER_QUANTITY,0) <> COALESCE(src.SUGGESTED_ORDER_QUANTITY,0)
                    OR COALESCE(tgt.BRAND_TYPE_NAME,''A'') <> COALESCE(src.BRAND_TYPE_NAME,''A'')
                    OR COALESCE(tgt.LOCATION_TYPE_NAME,''A'') <> COALESCE(src.LOCATION_TYPE_NAME,''A'')
                    OR COALESCE(tgt.MANUFACTURING_CODE_DESCRIPTION,''A'') <> COALESCE(src.MANUFACTURING_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.QUALITY_GRADE_CODE,''A'') <> COALESCE(src.QUALITY_GRADE_CODE,''A'')
                    OR COALESCE(tgt.PRIMARY_APPLICATION_NAME,''A'') <> COALESCE(src.PRIMARY_APPLICATION_NAME,''A'')
                    --INFAETL-11515 begin change
                    OR COALESCE(tgt.CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.CATEGORY_MANAGER_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.CATEGORY_MANAGER_NUMBER,0)
                    OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'')
                    OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NAME,''A'') <> COALESCE(src.CATEGORY_DIRECTOR_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NUMBER,0) <> COALESCE(src.CATEGORY_DIRECTOR_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_VP_NAME,''A'') <> COALESCE(src.CATEGORY_VP_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_VP_NUMBER,0) <> COALESCE(src.CATEGORY_VP_NUMBER,0)
                    --INFAETL-11515 end change
                    OR COALESCE(tgt.INACTIVATED_DATE,''1900-01-01'') <> COALESCE(src.INACTIVATED_DATE,''1900-01-01'')
                    OR COALESCE(tgt.REVIEW_CODE,''A'') <> COALESCE(src.REVIEW_CODE,''A'')
                    OR COALESCE(tgt.STOCKING_LINE_FLAG,''A'') <> COALESCE(src.STOCKING_LINE_FLAG,''A'')
                    OR COALESCE(tgt.OIL_LINE_FLAG,''A'') <> COALESCE(src.OIL_LINE_FLAG,''A'')
                    OR COALESCE(tgt.SPECIAL_REQUIREMENTS_LABEL,''A'') <> COALESCE(src.SPECIAL_REQUIREMENTS_LABEL,''A'')
                    OR COALESCE(tgt.SUPPLIER_ACCOUNT_NUMBER,0) <> COALESCE(src.SUPPLIER_ACCOUNT_NUMBER,0)
                    OR COALESCE(tgt.SUPPLIER_NUMBER,0) <> COALESCE(src.SUPPLIER_NUMBER,0)
                    OR COALESCE(tgt.SUPPLIER_ID,0) <> COALESCE(src.SUPPLIER_ID,0)
                    OR COALESCE(tgt.BRAND_DESCRIPTION,''A'') <> COALESCE(src.BRAND_DESCRIPTION,''A'')
                    OR COALESCE(tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0) <> COALESCE(src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0)
                    OR COALESCE(tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0) <> COALESCE(src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0)
                    OR COALESCE(tgt.SALES_AREA_NAME,''A'') <> COALESCE(src.SALES_AREA_NAME,''A'')
                    OR COALESCE(tgt.TEAM_NAME,''A'') <> COALESCE(src.TEAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_NAME,''A'') <> COALESCE(src.CATEGORY_NAME,''A'')
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_NAME,''A'') <> COALESCE(src.REPLENISHMENT_ANALYST_NAME,''A'')
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_NUMBER,0)
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0)
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0)
                    OR COALESCE(tgt.SALES_AREA_NAME_SORT_NUMBER,0) <> COALESCE(src.SALES_AREA_NAME_SORT_NUMBER,0)
                    OR COALESCE(tgt.TEAM_NAME_SORT_NUMBER,0) <> COALESCE(src.TEAM_NAME_SORT_NUMBER,0)
                    OR COALESCE(tgt.BUYER_CODE,''A'') <> COALESCE(src.BUYER_CODE,''A'')
                    OR COALESCE(tgt.BUYER_NAME,''A'') <> COALESCE(src.BUYER_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'') <> COALESCE(src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'') <> COALESCE(src.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_CITY_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_CITY_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_STATE_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_STATE_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ZIP_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ZIP_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_PHONE_NUMBER_CODE,''A'') <> COALESCE(src.BATTERY_PHONE_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_WEIGHT_IN_GRAMS,0) <> COALESCE(src.BATTERY_WEIGHT_IN_GRAMS,0)
                    OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0)
                    OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0)
                    OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_CELL,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_CELL,0)
                    OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_BATTERY,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_BATTERY,0)
                    OR COALESCE(tgt.BATTERY_CELLS_NUMBER,0) <> COALESCE(src.BATTERY_CELLS_NUMBER,0)
                    OR COALESCE(tgt.BATTERIES_PER_PACKAGE_NUMBER,0) <> COALESCE(src.BATTERIES_PER_PACKAGE_NUMBER,0)
                    OR COALESCE(tgt.BATTERIES_IN_EQUIPMENT_NUMBER,0) <> COALESCE(src.BATTERIES_IN_EQUIPMENT_NUMBER,0)
                    OR COALESCE(tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'') <> COALESCE(src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'')
                    OR COALESCE(tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'') <> COALESCE(src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'')
                    OR COALESCE(tgt.COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                    OR COALESCE(tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'') <> COALESCE(src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'')
                    OR COALESCE(tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'') <> COALESCE(src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'')
                    OR COALESCE(tgt.SCHEDULE_B_CODE_LIST,''A'') <> COALESCE(src.SCHEDULE_B_CODE_LIST,''A'')
                    OR COALESCE(tgt.UNITED_STATES_MUNITIONS_LIST_CODE,''A'') <> COALESCE(src.UNITED_STATES_MUNITIONS_LIST_CODE,''A'')
                    OR COALESCE(tgt.PROJECT_COORDINATOR_ID_CODE,''A'') <> COALESCE(src.PROJECT_COORDINATOR_ID_CODE,''A'')
                    OR COALESCE(tgt.PROJECT_COORDINATOR_EMPLOYEE_ID,0) <> COALESCE(src.PROJECT_COORDINATOR_EMPLOYEE_ID,0)
                    OR COALESCE(tgt.STOCK_ADJUSTMENT_MONTH_NUMBER,0) <> COALESCE(src.STOCK_ADJUSTMENT_MONTH_NUMBER,0)
                    OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'')
                    OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                    OR COALESCE(tgt.ALL_IN_COST,0) <> COALESCE(src.ALL_IN_COST,0)
                    OR COALESCE(tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'') <> COALESCE(src.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'')
                    OR COALESCE(tgt.CASE_LOT_DISCOUNT,0) <> COALESCE(src.CASE_LOT_DISCOUNT,0)
                    OR COALESCE(tgt.COMPANY_NUMBER,0) <> COALESCE(src.COMPANY_NUMBER,0)
                    OR COALESCE(tgt.CONVENIENCE_PACK_QUANTITY,0) <> COALESCE(src.CONVENIENCE_PACK_QUANTITY,0)
                    OR COALESCE(tgt.CONVENIENCE_PACK_DESCRIPTION,''A'') <> COALESCE(src.CONVENIENCE_PACK_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'') <> COALESCE(src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'')
                    OR COALESCE(tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'') <> COALESCE(src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'')
                    OR COALESCE(tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'') <> COALESCE(src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'')
                    OR COALESCE(tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'') <> COALESCE(src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.HAZARDOUS_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_USER_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.LIST_PRICE,0) <> COALESCE(src.LIST_PRICE,0)
                    OR COALESCE(tgt.LOW_USER_PRICE,0) <> COALESCE(src.LOW_USER_PRICE,0)
                    OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE,0) <> COALESCE(src.MINIMUM_ADVERTISED_PRICE,0)
                    OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'') <> COALESCE(src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.MINIMUM_SELL_QUANTITY,0) <> COALESCE(src.MINIMUM_SELL_QUANTITY,0)
                    OR COALESCE(tgt.PACKAGE_SIZE_DESCRIPTION,''A'') <> COALESCE(src.PACKAGE_SIZE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PERCENTAGE_OF_SUPPLIER_FUNDING,0) <> COALESCE(src.PERCENTAGE_OF_SUPPLIER_FUNDING,0)
                    OR COALESCE(tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'') <> COALESCE(src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'')
                    OR COALESCE(tgt.PRICING_COST,0) <> COALESCE(src.PRICING_COST,0)
                    OR COALESCE(tgt.PROFESSIONAL_PRICE,0) <> COALESCE(src.PROFESSIONAL_PRICE,0)
                    OR COALESCE(tgt.RETAIL_CORE,0) <> COALESCE(src.RETAIL_CORE,0)
                    OR COALESCE(tgt.RETAIL_HEIGHT,0) <> COALESCE(src.RETAIL_HEIGHT,0)
                    OR COALESCE(tgt.RETAIL_LENGTH,0) <> COALESCE(src.RETAIL_LENGTH,0)
                    OR COALESCE(tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.RETAIL_WIDTH,0) <> COALESCE(src.RETAIL_WIDTH,0)
                    OR COALESCE(tgt.SALES_PACK_CODE,''A'') <> COALESCE(src.SALES_PACK_CODE,''A'')
                    OR COALESCE(tgt.SCORE_FLAG,''A'') <> COALESCE(src.SCORE_FLAG,''A'')
                    OR COALESCE(tgt.SHIPPING_DIMENSIONS_CODE,''A'') <> COALESCE(src.SHIPPING_DIMENSIONS_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_BASE_COST,0) <> COALESCE(src.SUPPLIER_BASE_COST,0)
                    OR COALESCE(tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_LINE_CODE,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_USER_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.VIP_JOBBER,0) <> COALESCE(src.VIP_JOBBER,0)
                    OR COALESCE(tgt.WAREHOUSE_CORE,0) <> COALESCE(src.WAREHOUSE_CORE,0)
                    OR COALESCE(tgt.WAREHOUSE_COST,0) <> COALESCE(src.WAREHOUSE_COST,0)
                    --INFAETL-11815 adds the following line
                    OR COALESCE(tgt.PRODUCT_LEVEL_CODE,'''') <> COALESCE(src.PRODUCT_LEVEL_CODE,'''')
                    OR COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG,''A'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG,''A'')
                        ) 
                    WITH UR;';

               EXECUTE IMMEDIATE v_str_sql;

               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTNS UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '13. IMASTNS UPDATE from CTAS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

            END IF; -- V_SQL_OK
                            
                --Populate Hub Load Table - ECPARTS 3nd INSERT             
            IF V_SQL_OK THEN
            
                SET v_str_sql =  CLOB('INSERT INTO ') || v_staging_database_name || '.' || v_staging_table_name || 
                                '(PRODUCT_ID, LINE_CODE, LINE_DESCRIPTION, ITEM_CODE, ITEM_DESCRIPTION, SEGMENT_NUMBER, SEGMENT_DESCRIPTION, SUB_CATEGORY_NUMBER, SUB_CATEGORY_DESCRIPTION, CATEGORY_NUMBER, CATEGORY_DESCRIPTION, PRODUCT_LINE_CODE, SUB_CODE, MANUFACTURE_ITEM_NUMBER_CODE, SUPERSEDED_LINE_CODE, SUPERSEDED_ITEM_NUMBER_CODE, SORT_CONTROL_NUMBER, POINT_OF_SALE_DESCRIPTION, POPULARITY_CODE, POPULARITY_CODE_DESCRIPTION, POPULARITY_TREND_CODE, POPULARITY_TREND_CODE_DESCRIPTION, LINE_IS_MARINE_SPECIFIC_FLAG, LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE, LINE_IS_FLEET_SPECIFIC_CODE, LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE, LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG, JOBBER_SUPPLIER_CODE, JOBBER_UNIT_OF_MEASURE_CODE, WAREHOUSE_UNIT_OF_MEASURE_CODE, WAREHOUSE_SELL_QUANTITY, RETAIL_WEIGHT, QUANTITY_PER_CAR, CASE_QUANTITY, STANDARD_PACKAGE, PAINT_BODY_AND_EQUIPMENT_PRICE, WAREHOUSE_JOBBER_PRICE, WAREHOUSE_COST_WUM, WAREHOUSE_CORE_WUM, OREILLY_COST_PRICE, JOBBER_COST, JOBBER_CORE_PRICE, OUT_FRONT_MERCHANDISE_FLAG, ITEM_IS_TAXED_FLAG, QUANTITY_ORDER_ITEM_FLAG, JOBBER_DIVIDE_QUANTITY, ITEM_DELETE_FLAG_RECORD_CODE, SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE, PRIMARY_UNIVERSAL_PRODUCT_CODE, WARRANTY_CODE, WARRANTY_CODE_DESCRIPTION, INVOICE_COST_WUM_INVOICE_COST, INVOICE_CORE_WUM_CORE_COST, IS_CONSIGNMENT_ITEM_FLAG, WAREHOUSE_JOBBER_CORE_PRICE, ACQUISITION_FIELD_1_CODE, ACQUISITION_FIELD_2_CODE, BUY_MULTIPLE, BUY_MULTIPLE_CODE, BUY_MULTIPLE_CODE_DESCRIPTION, SUPPLIER_CONVERSION_FACTOR_CODE, SUPPLIER_CONVERSION_QUANTITY, SUPPLIER_UNIT_OF_MEASURE_CODE, UNIT_OF_MEASURE_AMOUNT, UNIT_OF_MEASURE_QUANTITY, UNIT_OF_MEASURE_DESCRIPTION, TAX_CLASSIFICATION_CODE, TAX_CLASSIFICATION_CODE_DESCRIPTION, TAX_CLASSIFICATION_REVIEW_STATUS_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION, DISTRIBUTION_CENTER_PICK_LENGTH, DISTRIBUTION_CENTER_PICK_WIDTH, DISTRIBUTION_CENTER_PICK_HEIGHT, DISTRIBUTION_CENTER_PICK_WEIGHT, PICK_LENGTH_WIDTH_HEIGHT_CODE, CASE_QUANTITY_CODE, CASE_LENGTH, CASE_WIDTH, CASE_HEIGHT, CASE_WEIGHT, CASE_LENGTH_WIDTH_HEIGHT_CODE, CASES_PER_PALLET, CASES_PER_PALLET_LAYER, PALLET_LENGTH, PALLET_WIDTH, PALLET_HEIGHT, PALLET_WEIGHT, PALLET_LENGTH_WIDTH_HEIGHT_CODE, SHIPMENT_CLASS_CODE, DOT_CLASS_NUMBER, DOT_CLASS_FOR_MSDS_ID_NUMBER, CONTAINER_DESCRIPTION, KEEP_FROM_FREEZING_FLAG, FLIGHT_RESTRICTED_FLAG, ALLOW_NEW_RETURNS_FLAG, ALLOW_CORE_RETURNS_FLAG, ALLOW_WARRANTY_RETURNS_FLAG, ALLOW_RECALL_RETURNS_FLAG, ALLOW_MANUAL_OTHER_RETURNS_FLAG, ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG, HAZARDOUS_UPDATE_DATE, PIECE_LENGTH, PIECE_WIDTH, PIECE_HEIGHT, PIECE_WEIGHT, PIECES_INNER_PACK, IN_CATALOG_CODE, IN_CATALOG_CODE_DESCRIPTION, ALLOW_SPECIAL_ORDER_FLAG, ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG, SUPPLIER_LIFE_CYCLE_CODE, SUPPLIER_LIFE_CYCLE_CHANGE_DATE, LONG_DESCRIPTION, ELECTRONIC_WASTE_FLAG, STORE_MINIMUM_SALE_QUANTITY, MANUFACTURER_SUGGESTED_RETAIL_PRICE, MAXIMUM_CAR_QUANTITY, MINIMUM_CAR_QUANTITY, ESSENTIAL_HARD_PART_CODE, INNER_PACK_CODE, INNER_PACK_QUANTITY, INNER_PACK_LENGTH, INNER_PACK_WIDTH, INNER_PACK_HEIGHT, INNER_PACK_WEIGHT, BRAND_CODE, PART_NUMBER_CODE,
                                  PART_NUMBER_DISPLAY_CODE, PART_NUMBER_DESCRIPTION, SPANISH_PART_NUMBER_DESCRIPTION, SUGGESTED_ORDER_QUANTITY, BRAND_TYPE_NAME, LOCATION_TYPE_NAME, MANUFACTURING_CODE_DESCRIPTION, QUALITY_GRADE_CODE, PRIMARY_APPLICATION_NAME, 
                                  --INFAETL-11515 mds renamed / added the following line
                                  CATEGORY_MANAGER_NAME, CATEGORY_MANAGER_NUMBER, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER, CATEGORY_DIRECTOR_NAME, CATEGORY_DIRECTOR_NUMBER, CATEGORY_VP_NAME, CATEGORY_VP_NUMBER, 
                                  INACTIVATED_DATE, REVIEW_CODE, STOCKING_LINE_FLAG, OIL_LINE_FLAG, SPECIAL_REQUIREMENTS_LABEL, SUPPLIER_ACCOUNT_NUMBER, SUPPLIER_NUMBER, SUPPLIER_ID, BRAND_DESCRIPTION, DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER, ACCOUNTS_PAYABLE_VENDOR_NUMBER, SALES_AREA_NAME, TEAM_NAME, CATEGORY_NAME, REPLENISHMENT_ANALYST_NAME, REPLENISHMENT_ANALYST_NUMBER, REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER, REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID, SALES_AREA_NAME_SORT_NUMBER, TEAM_NAME_SORT_NUMBER, BUYER_CODE, BUYER_NAME, BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE, BATTERY_PACKING_INSTRUCTIONS_CODE, BATTERY_MANUFACTURING_NAME, BATTERY_MANUFACTURING_ADDRESS_LINE_1, BATTERY_MANUFACTURING_ADDRESS_LINE_2, BATTERY_MANUFACTURING_ADDRESS_LINE_3, BATTERY_MANUFACTURING_ADDRESS_LINE_4, BATTERY_MANUFACTURING_CITY_NAME, BATTERY_MANUFACTURING_POSTAL_TOWN_NAME, BATTERY_MANUFACTURING_STATE_NAME, BATTERY_MANUFACTURING_ZIP_CODE, BATTERY_MANUFACTURING_COUNTRY_CODE, BATTERY_PHONE_NUMBER_CODE, BATTERY_WEIGHT_IN_GRAMS, BATTERY_GRAMS_OF_LITHIUM_PER_CELL, BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY, BATTERY_WATT_HOURS_PER_CELL, BATTERY_WATT_HOURS_PER_BATTERY, BATTERY_CELLS_NUMBER, BATTERIES_PER_PACKAGE_NUMBER, BATTERIES_IN_EQUIPMENT_NUMBER, BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG, BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG, COUNTRY_OF_ORIGIN_NAME_LIST, EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST, HARMONIZED_TARIFF_SCHEDULE_CODE_LIST, SCHEDULE_B_CODE_LIST, UNITED_STATES_MUNITIONS_LIST_CODE, PROJECT_COORDINATOR_ID_CODE, PROJECT_COORDINATOR_EMPLOYEE_ID, STOCK_ADJUSTMENT_MONTH_NUMBER, BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST, BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST, ALL_IN_COST, CANCEL_OR_BACKORDER_REMAINDER_CODE, CASE_LOT_DISCOUNT, COMPANY_NUMBER, CONVENIENCE_PACK_QUANTITY, CONVENIENCE_PACK_DESCRIPTION, PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_CREATION_DATE, PRODUCT_SOURCE_TABLE_CREATION_TIME, PRODUCT_SOURCE_TABLE_CREATION_USER_NAME, PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME, DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE, DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE, ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE, EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG, HAZARDOUS_UPDATE_PROGRAM_NAME, HAZARDOUS_UPDATE_TIME, HAZARDOUS_UPDATE_USER_NAME, LIST_PRICE, LOW_USER_PRICE, MINIMUM_ADVERTISED_PRICE, MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE, MINIMUM_SELL_QUANTITY, PACKAGE_SIZE_DESCRIPTION, PERCENTAGE_OF_SUPPLIER_FUNDING, PIECE_LENGTH_WIDTH_HEIGHT_FLAG, PRICING_COST, PROFESSIONAL_PRICE, RETAIL_CORE, RETAIL_HEIGHT, RETAIL_LENGTH, RETAIL_UNIT_OF_MEASURE_DESCRIPTION, RETAIL_WIDTH, SALES_PACK_CODE, SCORE_FLAG, SHIPPING_DIMENSIONS_CODE, SUPPLIER_BASE_COST, SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE, SUPPLIER_SUPERSEDED_LINE_CODE, CATEGORY_TABLE_CREATE_DATE, CATEGORY_TABLE_CREATE_PROGRAM_NAME, CATEGORY_TABLE_CREATE_TIME, CATEGORY_TABLE_CREATE_USER_NAME, CATEGORY_TABLE_UPDATE_DATE, CATEGORY_TABLE_UPDATE_PROGRAM_NAME, CATEGORY_TABLE_UPDATE_TIME, CATEGORY_TABLE_UPDATE_USER_NAME, PRODUCT_SOURCE_TABLE_UPDATE_DATE, PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_UPDATE_TIME, PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME, VIP_JOBBER, WAREHOUSE_CORE, WAREHOUSE_COST, 
                                  --INFAETL-11815 mds added the following line
                                  PRODUCT_LEVEL_CODE, 
                                  ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS
                                ) ' || ' SELECT CAST(' || v_process_database_name|| '.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS PRODUCT_ID,                       
                                    CAST(TRIM(cur_ECPARTS.LINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                                    CAST(TRIM(cur_ECPARTS.ITEMNUMBER) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                                    CAST(TRIM(cur_ECPARTS.SHORT_DESCRIPTION) AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SUBCATNUM,-2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                                    CAST(TRIM(cur_ECPARTS.PLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                                    CAST(cur_ECPARTS.SUBC AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                                    CAST(''-2'' AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                                    CAST(COALESCE(cur_ECPARTS.LINE, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_ECPARTS.ITEMNUMBER, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST(-1 AS INTEGER) AS SORT_CONTROL_NUMBER,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                                    CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                                    CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                                    CAST(0 AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                                    CAST(0 AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                                    CAST(1 AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                                    CAST(''NONE'' AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                                    CAST('''' AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                                    CAST(''UNKNOWN'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                                    CAST(-2 AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                                    CAST(1 AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                                    CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_WEIGHT,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                                    CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                                    CAST('''' AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                                    CAST(-2 AS INTEGER) AS DOT_CLASS_NUMBER,
                                    CAST(-2 AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                                    CAST(''1900-01-01'' AS DATE) AS HAZARDOUS_UPDATE_DATE,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                                    CAST(''1900-01-01'' AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                                    ' || ' CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                                    CAST(TRIM(COALESCE(cur_ECPARTS.PART_NUMBER, cur_AAIAVEND.OCAT_PART_NUMBER, '''')) AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DISPLAY),'''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.SPANISH_PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(cur_ECPARTS.SUGGESTED_ORDER_QTY, 0) AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                                    --INFAETL-11515 begin changes
                                    CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                                    CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                                    CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                                    --INFAETL-11515 end changes
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                                    CAST(CASE
                                        WHEN TRIM(cur_CMISFILE.LSTOCK) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                                    CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                                    CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                                    CAST(-2 AS INTEGER) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                                    CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                                    CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                                    CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                                    CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(64 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                                    CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_PACK), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                                    CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(256 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(256 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                                    CAST(0 AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                                    CAST(''00:00:00'' AS TIME) AS HAZARDOUS_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS LIST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                                    CAST(''1900-01-01'' AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS PRICING_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                                    CAST(COALESCE(
                                    CASE
                                        WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.LOADTIME, ''00:00:00'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS VIP_JOBBER,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                                    --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                                    ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                                        '|| quote_literal(v_etl_source_table_3) || ' AS ETL_SOURCE_TABLE_NAME,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                                    ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                                    ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS    
                                    FROM EDW_STAGING.CUR_IVB_ECPARTS cur_ECPARTS
                                    LEFT JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_ECPARTS.LINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_ECPARTS.ITEMNUMBER)
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_ECPARTS.LINE AND cur_CATEGORY.ITEM = cur_ECPARTS.ITEMNUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY  
                                    LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_ECPARTS.LINE AND cur_ICLINCTL.PLCD = cur_ECPARTS.PLCD AND cur_ICLINCTL.REGION = 0
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                            FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                            JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                            WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_ECPARTS.LINE 
                                            AND cur_DWASGN_DWEMP.PLCD = cur_ECPARTS.PLCD AND cur_DWASGN_DWEMP.SUBC = cur_ECPARTS.SUBC                                      
                                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                            FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                            JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                            WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_ECPARTS.LINE 
                                            AND cur_REPLENISHMENT_ANALYST.PLCD = cur_ECPARTS.PLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_ECPARTS.SUBC                          
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_ECPARTS.LINE AND cur_CMISFILE.LPLCD = cur_ECPARTS.PLCD AND cur_CMISFILE.LSUBC = cur_ECPARTS.SUBC 
                                    LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX AS cur_PMATRIX ON cur_PMATRIX.LINE = cur_ECPARTS.LINE AND cur_PMATRIX.PLCD = cur_ECPARTS.PLCD AND cur_PMATRIX.SUBC = cur_ECPARTS.SUBC
                                    LEFT JOIN EDW_STAGING.CUR_IVB_DWWMSITEM AS cur_DWWMSITEM ON cur_DWWMSITEM.LINE = cur_ECPARTS.LINE AND cur_DWWMSITEM.ITEM = cur_ECPARTS.ITEMNUMBER --337921
                                    -- EDW_STAGING <-> EDW_STAGING 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_ECPARTS.LINE AND cur_HAZLION.ITEM_NAME = cur_ECPARTS.ITEMNUMBER
                                    LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') 
                                            WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                                            FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                                            LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                                            GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_ECPARTS.LINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_ECPARTS.ITEMNUMBER 
                                    LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, 
                                            LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                                            LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                                            FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                                            LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                                            GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_ECPARTS.LINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_ECPARTS.ITEMNUMBER 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_ECPARTS.LINE AND cur_EXPECCN.ITEM = cur_ECPARTS.ITEMNUMBER 
                                    LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                                            FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                                            GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_ECPARTS.LINE AND cur_EXPHTS.ITEM = cur_ECPARTS.ITEMNUMBER 
                                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_ECPARTS.LINE AND cur_EXPUSML.ITEM = cur_ECPARTS.ITEMNUMBER 
                                    LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                                            FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                                            GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_ECPARTS.LINE AND cur_EXPSCDB_LA.ITEM = cur_ECPARTS.ITEMNUMBER
                                    LEFT JOIN EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK ON cur_WHNONSTK.WHNLINE = cur_ECPARTS.LINE AND cur_WHNONSTK.WHNITEM = cur_ECPARTS.ITEMNUMBER
                                    LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                                FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                                GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND 
                                                ON cur_AAIAVEND.OREILLY_LINE = cur_ECPARTS.LINE AND cur_AAIAVEND.KEY_ITEM = cur_ECPARTS.ITEMNUMBER
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_LSTCOSTUPD ON HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE = cur_DWWMSITEM.LSTCOSTUPD
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_UPDDTE ON HUB_LOAD_DIM_DATE_UPDDTE.DATE_ID = cur_DWWMSITEM.UPDDTE
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DIMUPDDTE ON HUB_LOAD_DIM_DATE_DIMUPDDTE.DATE_ID = cur_DWWMSITEM.DIMUPDDTE         
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = BIGINT(cur_PMATRIX.RA_TMN)
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                                    LEFT JOIN ( SELECT cur_IMASTER.ILINE AS LINE, cur_IMASTER.IITEM# AS ITEM
                                                FROM EDW_STAGING.CUR_IVB_IMASTER AS cur_IMASTER  
                                                UNION 
                                                SELECT cur_IMASTNS.LINE AS LINE, cur_IMASTNS.ITEM AS ITEM
                                                FROM EDW_STAGING.CUR_IVB_IMASTNS AS cur_IMASTNS) AS cur_UNION ON cur_UNION.LINE = cur_ECPARTS.LINE AND cur_UNION.ITEM = cur_ECPARTS.ITEMNUMBER 
                                    --INFAETL-11515 add the following 3 joins
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                    WHERE NVL(cur_UNION.LINE, '''') = ''''
                                        AND hub_DIM_PRODUCT.PRODUCT_ID IS NULL
                                        AND (' || quote_literal(v_job_execution_starttime) || ' <= cur_ECPARTS.CREATE_TIMESTAMP OR cur_ECPARTS.CREATE_TIMESTAMP IS NULL OR 
                                            ' || quote_literal(v_job_execution_starttime) || ' <= cur_ECPARTS.LOAD_TIMESTAMP OR cur_ECPARTS.LOAD_TIMESTAMP IS NULL) WITH UR;';
    
               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <ECPARTS INSERT> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '14. ECPARTS INSERT', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                                            
                                        

                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

             END IF; -- V_SQL_OK
           
             --Populate Hub Load Table - ECPARTS 3rd UPDATE
             -- drop update-stage table             
        --3rd merge step 1, drop "source" table

            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        

            IF V_SQL_OK THEN
               SET v_str_sql = 'DROP TABLE ' || v_staging_database_name || '.SESSION_TMP_ECPARTS_UPDATE_SOURCE if exists;';
               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                IF (V_SQL_CODE <> 0) THEN  --  Warning
                   SET V_SQL_OK = FALSE;
                   SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
            
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                   SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <ECPARTS DROP update table> '||
                       '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                       '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                       ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                       ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                       ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '15. ECPARTS_UPDATE preprocess table DROP', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

            END IF; -- V_SQL_OK

        --1st Merge step 2, recreate "source" table   
            IF V_SQL_OK THEN            
               SET v_str_sql = CLOB('CREATE  TABLE ') ||  v_staging_database_name || '.SESSION_TMP_ECPARTS_UPDATE_SOURCE
               AS (          SELECT hub_DIM_PRODUCT.PRODUCT_ID,
                                    CAST(TRIM(cur_ECPARTS.LINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                                    CAST(TRIM(cur_ECPARTS.ITEMNUMBER) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                                    CAST(TRIM(cur_ECPARTS.SHORT_DESCRIPTION) AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.SUBCATNUM,-2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                                    CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                                    CAST(TRIM(cur_ECPARTS.PLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                                    CAST(cur_ECPARTS.SUBC AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                                    CAST(''-2'' AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                                    CAST(COALESCE(cur_ECPARTS.LINE, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_ECPARTS.ITEMNUMBER, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST(-1 AS INTEGER) AS SORT_CONTROL_NUMBER,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                                    CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                                    CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                                    CAST(0 AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                                    CAST(0 AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                                    CAST(0 AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                                    CAST(1 AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                                    CAST(''NONE'' AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                                    CAST('''' AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                                    CAST(''UNKNOWN'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                                    CAST(-2 AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                                    CAST(1 AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                                    CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_WEIGHT,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                                    CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                                    CAST('''' AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                                    CAST(-2 AS INTEGER) AS DOT_CLASS_NUMBER,
                                    CAST(-2 AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                                    CAST(''1900-01-01'' AS DATE) AS HAZARDOUS_UPDATE_DATE,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                                    CAST(''1900-01-01'' AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                                    CAST(TRIM(COALESCE(cur_ECPARTS.PART_NUMBER, cur_AAIAVEND.OCAT_PART_NUMBER, '''')) AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DISPLAY),'''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_ECPARTS.SPANISH_PART_NUMBER_DESCRIPTION),'''') AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                                    CAST(COALESCE(cur_ECPARTS.SUGGESTED_ORDER_QTY, 0) AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                                    --INFAETL-11515 begin changes
                                    CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                                    CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                                    CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                                    CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                                    CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                                    --INFAETL-11515 end changes
                                    CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                                    CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_CMISFILE.LSTOCK) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                                    CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                                    CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                                    CAST(-2 AS INTEGER) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                                    CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                                    CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                                    CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                                    CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                                    CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(64 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                                    CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_PACK), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                                    CAST(COALESCE(
                                        CASE
                                        WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                                    CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(256 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(256 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                                    CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                                    CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                                    CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                                    CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                                    CAST(0 AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                                    CAST(''00:00:00'' AS TIME) AS HAZARDOUS_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS LIST_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                                    CAST(''1900-01-01'' AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                                    CAST(0 AS DECIMAL(12,4)) AS PRICING_COST,
                                    CAST(0 AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                                    CAST('''' AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                                    CAST(COALESCE(
                                    CASE
                                        WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                                        ELSE ''N''
                                        END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                                    CAST(0 AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                                    CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.LOADTIME, ''00:00:00'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                                    CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                                    CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                                    CAST(0 AS DECIMAL(12,4)) AS VIP_JOBBER,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                                    --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                                    ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                                        '|| quote_literal(v_etl_source_table_3) || ' AS ETL_SOURCE_TABLE_NAME,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                                    ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                                    ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS    
                                                FROM EDW_STAGING.CUR_IVB_ECPARTS cur_ECPARTS
                                                JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_ECPARTS.LINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_ECPARTS.ITEMNUMBER) 
                                                LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_ECPARTS.LINE AND cur_CATEGORY.ITEM = cur_ECPARTS.ITEMNUMBER
                                                LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY  
                                                LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_ECPARTS.LINE AND cur_ICLINCTL.PLCD = cur_ECPARTS.PLCD AND cur_ICLINCTL.REGION = 0
                                                LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                        FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                        JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                        WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_ECPARTS.LINE 
                                                        AND cur_DWASGN_DWEMP.PLCD = cur_ECPARTS.PLCD AND cur_DWASGN_DWEMP.SUBC = cur_ECPARTS.SUBC                                      
                                                LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                                                        FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                                                        JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                                                        WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_ECPARTS.LINE 
                                                        AND cur_REPLENISHMENT_ANALYST.PLCD = cur_ECPARTS.PLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_ECPARTS.SUBC                          
                                                --LEFT JOIN EDW_STAGING.CUR_PRT_AAIA_AGE AS cur_AAIA_AGE ON cur_AAIA_AGE.OREILLY_LINE = cur_ECPARTS.LINE AND cur_AAIA_AGE.KEY_ITEM = cur_ECPARTS.ITEMNUMBER
                                                LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_ECPARTS.LINE AND cur_CMISFILE.LPLCD = cur_ECPARTS.PLCD AND cur_CMISFILE.LSUBC = cur_ECPARTS.SUBC 
                                                LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX AS cur_PMATRIX ON cur_PMATRIX.LINE = cur_ECPARTS.LINE AND cur_PMATRIX.PLCD = cur_ECPARTS.PLCD AND cur_PMATRIX.SUBC = cur_ECPARTS.SUBC
                                                LEFT JOIN EDW_STAGING.CUR_IVB_DWWMSITEM AS cur_DWWMSITEM ON cur_DWWMSITEM.LINE = cur_ECPARTS.LINE AND cur_DWWMSITEM.ITEM = cur_ECPARTS.ITEMNUMBER --337921
                                                -- EDW_STAGING <-> EDW_STAGING 
                                                --LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER ON HUB_LOAD_DIM_SUPPLIER.SUPPLIER_ACCOUNT_NUMBER = cur_CMISFILE.LVACCT    
                                                LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_ECPARTS.LINE AND cur_HAZLION.ITEM_NAME = cur_ECPARTS.ITEMNUMBER
                                                LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') 
                                                        WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                                                        FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                                                        LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                                                        GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_ECPARTS.LINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_ECPARTS.ITEMNUMBER 
                                                LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, 
                                                        LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                                                        LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                                                        FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                                                        LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                                                        GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_ECPARTS.LINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_ECPARTS.ITEMNUMBER 
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_ECPARTS.LINE AND cur_EXPECCN.ITEM = cur_ECPARTS.ITEMNUMBER 
                                                LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                                                        FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                                                        GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_ECPARTS.LINE AND cur_EXPHTS.ITEM = cur_ECPARTS.ITEMNUMBER 
                                                LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_ECPARTS.LINE AND cur_EXPUSML.ITEM = cur_ECPARTS.ITEMNUMBER 
                                                LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                                                        FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                                                        GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_ECPARTS.LINE AND cur_EXPSCDB_LA.ITEM = cur_ECPARTS.ITEMNUMBER
                                                LEFT JOIN EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK ON cur_WHNONSTK.WHNLINE = cur_ECPARTS.LINE AND cur_WHNONSTK.WHNITEM = cur_ECPARTS.ITEMNUMBER
                                                LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                                            FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                                            GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND  
                                                            ON cur_AAIAVEND.OREILLY_LINE = cur_ECPARTS.LINE AND cur_AAIAVEND.KEY_ITEM = cur_ECPARTS.ITEMNUMBER
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_LSTCOSTUPD ON HUB_LOAD_DIM_DATE_LSTCOSTUPD.FULL_DATE = cur_DWWMSITEM.LSTCOSTUPD
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_UPDDTE ON HUB_LOAD_DIM_DATE_UPDDTE.DATE_ID = cur_DWWMSITEM.UPDDTE
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_DIMUPDDTE ON HUB_LOAD_DIM_DATE_DIMUPDDTE.DATE_ID = cur_DWWMSITEM.DIMUPDDTE         
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = BIGINT(cur_PMATRIX.RA_TMN)
                                                LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                                        LEFT JOIN ( SELECT cur_IMASTER.ILINE AS LINE, cur_IMASTER.IITEM# AS ITEM
                                                    FROM EDW_STAGING.CUR_IVB_IMASTER AS cur_IMASTER  
                                                    UNION 
                                                    SELECT cur_IMASTNS.LINE AS LINE, cur_IMASTNS.ITEM AS ITEM
                                                    FROM EDW_STAGING.CUR_IVB_IMASTNS AS cur_IMASTNS) AS cur_UNION ON cur_UNION.LINE = cur_ECPARTS.LINE AND cur_UNION.ITEM = cur_ECPARTS.ITEMNUMBER 
                                        --INFAETL-11515 add the following 3 joins
                                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                        WHERE NVL(cur_UNION.LINE, '''') = ''''
                                                AND hub_DIM_PRODUCT.PRODUCT_ID IS NOT NULL
--                                                AND (' || quote_literal(v_job_execution_starttime) || ' <= cur_ECPARTS.CREATE_TIMESTAMP OR cur_ECPARTS.CREATE_TIMESTAMP IS NULL
--                                                    OR ' || quote_literal(v_job_execution_starttime) || ' <= cur_ECPARTS.LOAD_TIMESTAMP OR cur_ECPARTS.LOAD_TIMESTAMP IS NULL)
                   ) WITH DATA
                  DISTRIBUTE ON HASH(PRODUCT_ID)
                  IN TS_EDW
                  ORGANIZE BY COLUMN
--                  WITH UR
;';
                 
               IF v_log_level >= 3
                  THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '16a. ECPARTS UPDATE CTAS source', 'diagnostic', v_str_sql, current_timestamp);
               END IF;

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

               IF v_log_level >= 3
                  THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '16b. ECPARTS UPDATE CTAS source', 'diagnostic2', v_str_sql, current_timestamp);
               END IF;
              
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;

               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <ECPARTS Source Update CTAS> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '16. ECPARTS UPDATE preprocess table CTAS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                        
                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

            END IF; --v_SQL_OK
            
            IF V_SQL_OK THEN
    
               SET v_str_sql =   CLOB('UPDATE ') || v_staging_database_name || '.' || v_staging_table_name || ' AS tgt ' || 
                 'SET tgt.LINE_DESCRIPTION=src.LINE_DESCRIPTION,
                            tgt.ITEM_DESCRIPTION=src.ITEM_DESCRIPTION,
                            tgt.SEGMENT_NUMBER=src.SEGMENT_NUMBER,
                            tgt.SEGMENT_DESCRIPTION=src.SEGMENT_DESCRIPTION,
                            tgt.SUB_CATEGORY_NUMBER=src.SUB_CATEGORY_NUMBER,
                            tgt.SUB_CATEGORY_DESCRIPTION=src.SUB_CATEGORY_DESCRIPTION,
                            tgt.CATEGORY_NUMBER=src.CATEGORY_NUMBER,
                            tgt.CATEGORY_DESCRIPTION=src.CATEGORY_DESCRIPTION,
                            tgt.PRODUCT_LINE_CODE=src.PRODUCT_LINE_CODE,
                            tgt.SUB_CODE=src.SUB_CODE,
                            tgt.MANUFACTURE_ITEM_NUMBER_CODE=src.MANUFACTURE_ITEM_NUMBER_CODE,
                            tgt.SUPERSEDED_LINE_CODE=src.SUPERSEDED_LINE_CODE,
                            tgt.SUPERSEDED_ITEM_NUMBER_CODE=src.SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SORT_CONTROL_NUMBER=src.SORT_CONTROL_NUMBER,
                            tgt.POINT_OF_SALE_DESCRIPTION=src.POINT_OF_SALE_DESCRIPTION,
                            tgt.POPULARITY_CODE=src.POPULARITY_CODE,
                            tgt.POPULARITY_CODE_DESCRIPTION=src.POPULARITY_CODE_DESCRIPTION,
                            tgt.POPULARITY_TREND_CODE=src.POPULARITY_TREND_CODE,
                            tgt.POPULARITY_TREND_CODE_DESCRIPTION=src.POPULARITY_TREND_CODE_DESCRIPTION,
                            tgt.LINE_IS_MARINE_SPECIFIC_FLAG=src.LINE_IS_MARINE_SPECIFIC_FLAG,
                            tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE=src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                            tgt.LINE_IS_FLEET_SPECIFIC_CODE=src.LINE_IS_FLEET_SPECIFIC_CODE,
                            tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE=src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                            tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG=src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                            tgt.JOBBER_SUPPLIER_CODE=src.JOBBER_SUPPLIER_CODE,
                            tgt.JOBBER_UNIT_OF_MEASURE_CODE=src.JOBBER_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE=src.WAREHOUSE_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_SELL_QUANTITY=src.WAREHOUSE_SELL_QUANTITY,
                            tgt.RETAIL_WEIGHT=src.RETAIL_WEIGHT,
                            tgt.QUANTITY_PER_CAR=src.QUANTITY_PER_CAR,
                            tgt.CASE_QUANTITY=src.CASE_QUANTITY,
                            tgt.STANDARD_PACKAGE=src.STANDARD_PACKAGE,
                            tgt.PAINT_BODY_AND_EQUIPMENT_PRICE=src.PAINT_BODY_AND_EQUIPMENT_PRICE,
                            tgt.WAREHOUSE_JOBBER_PRICE=src.WAREHOUSE_JOBBER_PRICE,
                            tgt.WAREHOUSE_COST_WUM=src.WAREHOUSE_COST_WUM,
                            tgt.WAREHOUSE_CORE_WUM=src.WAREHOUSE_CORE_WUM,
                            tgt.OREILLY_COST_PRICE=src.OREILLY_COST_PRICE,
                            tgt.JOBBER_COST=src.JOBBER_COST,
                            tgt.JOBBER_CORE_PRICE=src.JOBBER_CORE_PRICE,
                            tgt.OUT_FRONT_MERCHANDISE_FLAG=src.OUT_FRONT_MERCHANDISE_FLAG,
                            tgt.ITEM_IS_TAXED_FLAG=src.ITEM_IS_TAXED_FLAG,
                            tgt.QUANTITY_ORDER_ITEM_FLAG=src.QUANTITY_ORDER_ITEM_FLAG,
                            tgt.JOBBER_DIVIDE_QUANTITY=src.JOBBER_DIVIDE_QUANTITY,
                            tgt.ITEM_DELETE_FLAG_RECORD_CODE=src.ITEM_DELETE_FLAG_RECORD_CODE,
                            tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE=src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                            tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE=src.PRIMARY_UNIVERSAL_PRODUCT_CODE,
                            tgt.WARRANTY_CODE=src.WARRANTY_CODE,
                            tgt.WARRANTY_CODE_DESCRIPTION=src.WARRANTY_CODE_DESCRIPTION,
                            tgt.INVOICE_COST_WUM_INVOICE_COST=src.INVOICE_COST_WUM_INVOICE_COST,
                            tgt.INVOICE_CORE_WUM_CORE_COST=src.INVOICE_CORE_WUM_CORE_COST,
                            tgt.IS_CONSIGNMENT_ITEM_FLAG=src.IS_CONSIGNMENT_ITEM_FLAG,
                            tgt.WAREHOUSE_JOBBER_CORE_PRICE=src.WAREHOUSE_JOBBER_CORE_PRICE,
                            tgt.ACQUISITION_FIELD_1_CODE=src.ACQUISITION_FIELD_1_CODE,
                            tgt.ACQUISITION_FIELD_2_CODE=src.ACQUISITION_FIELD_2_CODE,
                            tgt.BUY_MULTIPLE=src.BUY_MULTIPLE,
                            tgt.BUY_MULTIPLE_CODE=src.BUY_MULTIPLE_CODE,
                            tgt.BUY_MULTIPLE_CODE_DESCRIPTION=src.BUY_MULTIPLE_CODE_DESCRIPTION,
                            tgt.SUPPLIER_CONVERSION_FACTOR_CODE=src.SUPPLIER_CONVERSION_FACTOR_CODE,
                            tgt.SUPPLIER_CONVERSION_QUANTITY=src.SUPPLIER_CONVERSION_QUANTITY,
                            tgt.SUPPLIER_UNIT_OF_MEASURE_CODE=src.SUPPLIER_UNIT_OF_MEASURE_CODE,
                            tgt.UNIT_OF_MEASURE_AMOUNT=src.UNIT_OF_MEASURE_AMOUNT,
                            tgt.UNIT_OF_MEASURE_QUANTITY=src.UNIT_OF_MEASURE_QUANTITY,
                            tgt.UNIT_OF_MEASURE_DESCRIPTION=src.UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_CODE=src.TAX_CLASSIFICATION_CODE,
                            tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION=src.TAX_CLASSIFICATION_CODE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE=src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                            tgt.DISTRIBUTION_CENTER_PICK_LENGTH=src.DISTRIBUTION_CENTER_PICK_LENGTH,
                            tgt.DISTRIBUTION_CENTER_PICK_WIDTH=src.DISTRIBUTION_CENTER_PICK_WIDTH,
                            tgt.DISTRIBUTION_CENTER_PICK_HEIGHT=src.DISTRIBUTION_CENTER_PICK_HEIGHT,
                            tgt.DISTRIBUTION_CENTER_PICK_WEIGHT=src.DISTRIBUTION_CENTER_PICK_WEIGHT,
                            tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE=src.PICK_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASE_QUANTITY_CODE=src.CASE_QUANTITY_CODE,
                            tgt.CASE_LENGTH=src.CASE_LENGTH,
                            tgt.CASE_WIDTH=src.CASE_WIDTH,
                            tgt.CASE_HEIGHT=src.CASE_HEIGHT,
                            tgt.CASE_WEIGHT=src.CASE_WEIGHT,
                            tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE=src.CASE_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASES_PER_PALLET=src.CASES_PER_PALLET,
                            tgt.CASES_PER_PALLET_LAYER=src.CASES_PER_PALLET_LAYER,
                            tgt.PALLET_LENGTH=src.PALLET_LENGTH,
                            tgt.PALLET_WIDTH=src.PALLET_WIDTH,
                            tgt.PALLET_HEIGHT=src.PALLET_HEIGHT,
                            tgt.PALLET_WEIGHT=src.PALLET_WEIGHT,
                            tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE=src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.SHIPMENT_CLASS_CODE=src.SHIPMENT_CLASS_CODE,
                            tgt.DOT_CLASS_NUMBER=src.DOT_CLASS_NUMBER,
                            tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER=src.DOT_CLASS_FOR_MSDS_ID_NUMBER,
                            tgt.CONTAINER_DESCRIPTION=src.CONTAINER_DESCRIPTION,
                            tgt.KEEP_FROM_FREEZING_FLAG=src.KEEP_FROM_FREEZING_FLAG,
                            tgt.FLIGHT_RESTRICTED_FLAG=src.FLIGHT_RESTRICTED_FLAG,
                            tgt.ALLOW_NEW_RETURNS_FLAG=src.ALLOW_NEW_RETURNS_FLAG,
                            tgt.ALLOW_CORE_RETURNS_FLAG=src.ALLOW_CORE_RETURNS_FLAG,
                            tgt.ALLOW_WARRANTY_RETURNS_FLAG=src.ALLOW_WARRANTY_RETURNS_FLAG,
                            tgt.ALLOW_RECALL_RETURNS_FLAG=src.ALLOW_RECALL_RETURNS_FLAG,
                            tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG=src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                            tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG=src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                            tgt.HAZARDOUS_UPDATE_DATE=src.HAZARDOUS_UPDATE_DATE,
                            tgt.PIECE_LENGTH=src.PIECE_LENGTH,
                            tgt.PIECE_WIDTH=src.PIECE_WIDTH,
                            tgt.PIECE_HEIGHT=src.PIECE_HEIGHT,
                            tgt.PIECE_WEIGHT=src.PIECE_WEIGHT,
                            tgt.PIECES_INNER_PACK=src.PIECES_INNER_PACK,
                            tgt.IN_CATALOG_CODE=src.IN_CATALOG_CODE,
                            tgt.IN_CATALOG_CODE_DESCRIPTION=src.IN_CATALOG_CODE_DESCRIPTION,
                            tgt.ALLOW_SPECIAL_ORDER_FLAG=src.ALLOW_SPECIAL_ORDER_FLAG,
                            tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG=src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                            tgt.SUPPLIER_LIFE_CYCLE_CODE=src.SUPPLIER_LIFE_CYCLE_CODE,
                            tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE=src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                            tgt.LONG_DESCRIPTION=src.LONG_DESCRIPTION,
                            tgt.ELECTRONIC_WASTE_FLAG=src.ELECTRONIC_WASTE_FLAG,
                            tgt.STORE_MINIMUM_SALE_QUANTITY=src.STORE_MINIMUM_SALE_QUANTITY,
                            tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE=src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                            tgt.MAXIMUM_CAR_QUANTITY=src.MAXIMUM_CAR_QUANTITY,
                            tgt.MINIMUM_CAR_QUANTITY=src.MINIMUM_CAR_QUANTITY,
                            tgt.ESSENTIAL_HARD_PART_CODE=src.ESSENTIAL_HARD_PART_CODE,
                            tgt.INNER_PACK_CODE=src.INNER_PACK_CODE,
                            tgt.INNER_PACK_QUANTITY=src.INNER_PACK_QUANTITY,
                            tgt.INNER_PACK_LENGTH=src.INNER_PACK_LENGTH,
                            tgt.INNER_PACK_WIDTH=src.INNER_PACK_WIDTH,
                            tgt.INNER_PACK_HEIGHT=src.INNER_PACK_HEIGHT,
                            tgt.INNER_PACK_WEIGHT=src.INNER_PACK_WEIGHT,
                            tgt.BRAND_CODE=src.BRAND_CODE,
                            tgt.PART_NUMBER_CODE=src.PART_NUMBER_CODE,
                            tgt.PART_NUMBER_DISPLAY_CODE=src.PART_NUMBER_DISPLAY_CODE,
                            tgt.PART_NUMBER_DESCRIPTION=src.PART_NUMBER_DESCRIPTION,
                            tgt.SPANISH_PART_NUMBER_DESCRIPTION=src.SPANISH_PART_NUMBER_DESCRIPTION,
                            tgt.SUGGESTED_ORDER_QUANTITY=src.SUGGESTED_ORDER_QUANTITY,
                            tgt.BRAND_TYPE_NAME=src.BRAND_TYPE_NAME,
                            tgt.LOCATION_TYPE_NAME=src.LOCATION_TYPE_NAME,
                            tgt.MANUFACTURING_CODE_DESCRIPTION=src.MANUFACTURING_CODE_DESCRIPTION,
                            tgt.QUALITY_GRADE_CODE=src.QUALITY_GRADE_CODE,
                            tgt.PRIMARY_APPLICATION_NAME=src.PRIMARY_APPLICATION_NAME,
                            --INFAETL-11515 begin change
                            tgt.CATEGORY_MANAGER_NAME=src.CATEGORY_MANAGER_NAME,
                            tgt.CATEGORY_MANAGER_NUMBER=src.CATEGORY_MANAGER_NUMBER,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                            tgt.CATEGORY_DIRECTOR_NAME=src.CATEGORY_DIRECTOR_NAME,
                            tgt.CATEGORY_DIRECTOR_NUMBER=src.CATEGORY_DIRECTOR_NUMBER,
                            tgt.CATEGORY_VP_NAME=src.CATEGORY_VP_NAME,
                            tgt.CATEGORY_VP_NUMBER=src.CATEGORY_VP_NUMBER,
                            --INFAETL-11515 end change
                            tgt.INACTIVATED_DATE=src.INACTIVATED_DATE,
                            tgt.REVIEW_CODE=src.REVIEW_CODE,
                            tgt.STOCKING_LINE_FLAG=src.STOCKING_LINE_FLAG,
                            tgt.OIL_LINE_FLAG=src.OIL_LINE_FLAG,
                            tgt.SPECIAL_REQUIREMENTS_LABEL=src.SPECIAL_REQUIREMENTS_LABEL,
                            tgt.SUPPLIER_ACCOUNT_NUMBER=src.SUPPLIER_ACCOUNT_NUMBER,
                            tgt.SUPPLIER_NUMBER=src.SUPPLIER_NUMBER,
                            tgt.SUPPLIER_ID=src.SUPPLIER_ID,
                            tgt.BRAND_DESCRIPTION=src.BRAND_DESCRIPTION,
                            tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER=src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                            tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER=src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                            tgt.SALES_AREA_NAME=src.SALES_AREA_NAME,
                            tgt.TEAM_NAME=src.TEAM_NAME,
                            tgt.CATEGORY_NAME=src.CATEGORY_NAME,
                            tgt.REPLENISHMENT_ANALYST_NAME=src.REPLENISHMENT_ANALYST_NAME,
                            tgt.REPLENISHMENT_ANALYST_NUMBER=src.REPLENISHMENT_ANALYST_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER=src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID=src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                            tgt.SALES_AREA_NAME_SORT_NUMBER=src.SALES_AREA_NAME_SORT_NUMBER,
                            tgt.TEAM_NAME_SORT_NUMBER=src.TEAM_NAME_SORT_NUMBER,
                            tgt.BUYER_CODE=src.BUYER_CODE,
                            tgt.BUYER_NAME=src.BUYER_NAME,
                            tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE=src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                            tgt.BATTERY_PACKING_INSTRUCTIONS_CODE=src.BATTERY_PACKING_INSTRUCTIONS_CODE,
                            tgt.BATTERY_MANUFACTURING_NAME=src.BATTERY_MANUFACTURING_NAME,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1=src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2=src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3=src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4=src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                            tgt.BATTERY_MANUFACTURING_CITY_NAME=src.BATTERY_MANUFACTURING_CITY_NAME,
                            tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME=src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                            tgt.BATTERY_MANUFACTURING_STATE_NAME=src.BATTERY_MANUFACTURING_STATE_NAME,
                            tgt.BATTERY_MANUFACTURING_ZIP_CODE=src.BATTERY_MANUFACTURING_ZIP_CODE,
                            tgt.BATTERY_MANUFACTURING_COUNTRY_CODE=src.BATTERY_MANUFACTURING_COUNTRY_CODE,
                            tgt.BATTERY_PHONE_NUMBER_CODE=src.BATTERY_PHONE_NUMBER_CODE,
                            tgt.BATTERY_WEIGHT_IN_GRAMS=src.BATTERY_WEIGHT_IN_GRAMS,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL=src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY=src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                            tgt.BATTERY_WATT_HOURS_PER_CELL=src.BATTERY_WATT_HOURS_PER_CELL,
                            tgt.BATTERY_WATT_HOURS_PER_BATTERY=src.BATTERY_WATT_HOURS_PER_BATTERY,
                            tgt.BATTERY_CELLS_NUMBER=src.BATTERY_CELLS_NUMBER,
                            tgt.BATTERIES_PER_PACKAGE_NUMBER=src.BATTERIES_PER_PACKAGE_NUMBER,
                            tgt.BATTERIES_IN_EQUIPMENT_NUMBER=src.BATTERIES_IN_EQUIPMENT_NUMBER,
                            tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG=src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                            tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG=src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                            tgt.COUNTRY_OF_ORIGIN_NAME_LIST=src.COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST=src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                            tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST=src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                            tgt.SCHEDULE_B_CODE_LIST=src.SCHEDULE_B_CODE_LIST,
                            tgt.UNITED_STATES_MUNITIONS_LIST_CODE=src.UNITED_STATES_MUNITIONS_LIST_CODE,
                            tgt.PROJECT_COORDINATOR_ID_CODE=src.PROJECT_COORDINATOR_ID_CODE,
                            tgt.PROJECT_COORDINATOR_EMPLOYEE_ID=src.PROJECT_COORDINATOR_EMPLOYEE_ID,
                            tgt.STOCK_ADJUSTMENT_MONTH_NUMBER=src.STOCK_ADJUSTMENT_MONTH_NUMBER,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.ALL_IN_COST=src.ALL_IN_COST,
                            tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE=src.CANCEL_OR_BACKORDER_REMAINDER_CODE,
                            tgt.CASE_LOT_DISCOUNT=src.CASE_LOT_DISCOUNT,
                            tgt.COMPANY_NUMBER=src.COMPANY_NUMBER,
                            tgt.CONVENIENCE_PACK_QUANTITY=src.CONVENIENCE_PACK_QUANTITY,
                            tgt.CONVENIENCE_PACK_DESCRIPTION=src.CONVENIENCE_PACK_DESCRIPTION,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE=src.PRODUCT_SOURCE_TABLE_CREATION_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME=src.PRODUCT_SOURCE_TABLE_CREATION_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE=src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                            tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE=src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                            tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE=src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                            tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE=src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                            tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG=src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                            tgt.HAZARDOUS_UPDATE_PROGRAM_NAME=src.HAZARDOUS_UPDATE_PROGRAM_NAME,
                            tgt.HAZARDOUS_UPDATE_TIME=src.HAZARDOUS_UPDATE_TIME,
                            tgt.HAZARDOUS_UPDATE_USER_NAME=src.HAZARDOUS_UPDATE_USER_NAME,
                            tgt.LIST_PRICE=src.LIST_PRICE,
                            tgt.LOW_USER_PRICE=src.LOW_USER_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE=src.MINIMUM_ADVERTISED_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE=src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                            tgt.MINIMUM_SELL_QUANTITY=src.MINIMUM_SELL_QUANTITY,
                            tgt.PACKAGE_SIZE_DESCRIPTION=src.PACKAGE_SIZE_DESCRIPTION,
                            tgt.PERCENTAGE_OF_SUPPLIER_FUNDING=src.PERCENTAGE_OF_SUPPLIER_FUNDING,
                            tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG=src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                            tgt.PRICING_COST=src.PRICING_COST,
                            tgt.PROFESSIONAL_PRICE=src.PROFESSIONAL_PRICE,
                            tgt.RETAIL_CORE=src.RETAIL_CORE,
                            tgt.RETAIL_HEIGHT=src.RETAIL_HEIGHT,
                            tgt.RETAIL_LENGTH=src.RETAIL_LENGTH,
                            tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION=src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.RETAIL_WIDTH=src.RETAIL_WIDTH,
                            tgt.SALES_PACK_CODE=src.SALES_PACK_CODE,
                            tgt.SCORE_FLAG=src.SCORE_FLAG,
                            tgt.SHIPPING_DIMENSIONS_CODE=src.SHIPPING_DIMENSIONS_CODE,
                            tgt.SUPPLIER_BASE_COST=src.SUPPLIER_BASE_COST,
                            tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE=src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SUPPLIER_SUPERSEDED_LINE_CODE=src.SUPPLIER_SUPERSEDED_LINE_CODE,
                            tgt.CATEGORY_TABLE_CREATE_DATE=src.CATEGORY_TABLE_CREATE_DATE,
                            tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME=src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_CREATE_TIME=src.CATEGORY_TABLE_CREATE_TIME,
                            tgt.CATEGORY_TABLE_CREATE_USER_NAME=src.CATEGORY_TABLE_CREATE_USER_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_DATE=src.CATEGORY_TABLE_UPDATE_DATE,
                            tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME=src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_TIME=src.CATEGORY_TABLE_UPDATE_TIME,
                            tgt.CATEGORY_TABLE_UPDATE_USER_NAME=src.CATEGORY_TABLE_UPDATE_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                            tgt.VIP_JOBBER=src.VIP_JOBBER,
                            tgt.WAREHOUSE_CORE=src.WAREHOUSE_CORE,
                            tgt.WAREHOUSE_COST=src.WAREHOUSE_COST,
                            --INFAETL-11815 added the next line 
                            tgt.PRODUCT_LEVEL_CODE = src.PRODUCT_LEVEL_CODE,
                            tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
                            tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
                            --tgt.ETL_CREATE_TIMESTAMP=src.ETL_CREATE_TIMESTAMP
                            tgt.ETL_UPDATE_TIMESTAMP= CURRENT_TIMESTAMP-CURRENT_TIMEZONE,
                            tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
                            tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
                 from '|| v_staging_database_name || '.SESSION_TMP_ECPARTS_UPDATE_SOURCE SRC 
                  WHERE tgt.product_id = src.product_id
                  AND (     COALESCE(tgt.LINE_DESCRIPTION,''A'') <> COALESCE(src.LINE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.ITEM_DESCRIPTION,''A'') <> COALESCE(src.ITEM_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SEGMENT_NUMBER,0) <> COALESCE(src.SEGMENT_NUMBER,0)
                            OR COALESCE(tgt.SEGMENT_DESCRIPTION,''A'') <> COALESCE(src.SEGMENT_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SUB_CATEGORY_NUMBER,0) <> COALESCE(src.SUB_CATEGORY_NUMBER,0)
                            OR COALESCE(tgt.SUB_CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.SUB_CATEGORY_DESCRIPTION,''A'')
                            OR COALESCE(tgt.CATEGORY_NUMBER,0) <> COALESCE(src.CATEGORY_NUMBER,0)
                            OR COALESCE(tgt.CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.CATEGORY_DESCRIPTION,''A'')
                            OR COALESCE(tgt.PRODUCT_LINE_CODE,''A'') <> COALESCE(src.PRODUCT_LINE_CODE,''A'')
                            OR COALESCE(tgt.SUB_CODE,''A'') <> COALESCE(src.SUB_CODE,''A'')
                            OR COALESCE(tgt.MANUFACTURE_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.MANUFACTURE_ITEM_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPERSEDED_LINE_CODE,''A'')
                            OR COALESCE(tgt.SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.SORT_CONTROL_NUMBER,0) <> COALESCE(src.SORT_CONTROL_NUMBER,0)
                            OR COALESCE(tgt.POINT_OF_SALE_DESCRIPTION,''A'') <> COALESCE(src.POINT_OF_SALE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.POPULARITY_CODE,''A'') <> COALESCE(src.POPULARITY_CODE,''A'')
                            OR COALESCE(tgt.POPULARITY_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.POPULARITY_TREND_CODE,''A'') <> COALESCE(src.POPULARITY_TREND_CODE,''A'')
                            OR COALESCE(tgt.POPULARITY_TREND_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_TREND_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.LINE_IS_MARINE_SPECIFIC_FLAG,''A'') <> COALESCE(src.LINE_IS_MARINE_SPECIFIC_FLAG,''A'')
                            OR COALESCE(tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'')
                            OR COALESCE(tgt.LINE_IS_FLEET_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_FLEET_SPECIFIC_CODE,''A'')
                            OR COALESCE(tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'')
                            OR COALESCE(tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'')
                            OR COALESCE(tgt.JOBBER_SUPPLIER_CODE,''A'') <> COALESCE(src.JOBBER_SUPPLIER_CODE,''A'')
                            OR COALESCE(tgt.JOBBER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.JOBBER_UNIT_OF_MEASURE_CODE,''A'')
                            OR COALESCE(tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'')
                            OR COALESCE(tgt.WAREHOUSE_SELL_QUANTITY,0) <> COALESCE(src.WAREHOUSE_SELL_QUANTITY,0)
                            OR COALESCE(tgt.RETAIL_WEIGHT,0) <> COALESCE(src.RETAIL_WEIGHT,0)
                            OR COALESCE(tgt.QUANTITY_PER_CAR,0) <> COALESCE(src.QUANTITY_PER_CAR,0)
                            OR COALESCE(tgt.CASE_QUANTITY,0) <> COALESCE(src.CASE_QUANTITY,0)
                            OR COALESCE(tgt.STANDARD_PACKAGE,0) <> COALESCE(src.STANDARD_PACKAGE,0)
                            OR COALESCE(tgt.PAINT_BODY_AND_EQUIPMENT_PRICE,0) <> COALESCE(src.PAINT_BODY_AND_EQUIPMENT_PRICE,0)
                            OR COALESCE(tgt.WAREHOUSE_JOBBER_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_PRICE,0)
                            OR COALESCE(tgt.WAREHOUSE_COST_WUM,0) <> COALESCE(src.WAREHOUSE_COST_WUM,0)
                            OR COALESCE(tgt.WAREHOUSE_CORE_WUM,0) <> COALESCE(src.WAREHOUSE_CORE_WUM,0)
                            OR COALESCE(tgt.OREILLY_COST_PRICE,0) <> COALESCE(src.OREILLY_COST_PRICE,0)
                            OR COALESCE(tgt.JOBBER_COST,0) <> COALESCE(src.JOBBER_COST,0)
                            OR COALESCE(tgt.JOBBER_CORE_PRICE,0) <> COALESCE(src.JOBBER_CORE_PRICE,0)
                            OR COALESCE(tgt.OUT_FRONT_MERCHANDISE_FLAG,''A'') <> COALESCE(src.OUT_FRONT_MERCHANDISE_FLAG,''A'')
                            OR COALESCE(tgt.ITEM_IS_TAXED_FLAG,''A'') <> COALESCE(src.ITEM_IS_TAXED_FLAG,''A'')
                            OR COALESCE(tgt.QUANTITY_ORDER_ITEM_FLAG,''A'') <> COALESCE(src.QUANTITY_ORDER_ITEM_FLAG,''A'')
                            OR COALESCE(tgt.JOBBER_DIVIDE_QUANTITY,0) <> COALESCE(src.JOBBER_DIVIDE_QUANTITY,0)
                            OR COALESCE(tgt.ITEM_DELETE_FLAG_RECORD_CODE,''A'') <> COALESCE(src.ITEM_DELETE_FLAG_RECORD_CODE,''A'')
                            OR COALESCE(tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'') <> COALESCE(src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'')
                            OR COALESCE(tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'') <> COALESCE(src.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'')
                            OR COALESCE(tgt.WARRANTY_CODE,''A'') <> COALESCE(src.WARRANTY_CODE,''A'')
                            OR COALESCE(tgt.WARRANTY_CODE_DESCRIPTION,''A'') <> COALESCE(src.WARRANTY_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.INVOICE_COST_WUM_INVOICE_COST,0) <> COALESCE(src.INVOICE_COST_WUM_INVOICE_COST,0)
                            OR COALESCE(tgt.INVOICE_CORE_WUM_CORE_COST,0) <> COALESCE(src.INVOICE_CORE_WUM_CORE_COST,0)
                            OR COALESCE(tgt.IS_CONSIGNMENT_ITEM_FLAG,''A'') <> COALESCE(src.IS_CONSIGNMENT_ITEM_FLAG,''A'')
                            OR COALESCE(tgt.WAREHOUSE_JOBBER_CORE_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_CORE_PRICE,0)
                            OR COALESCE(tgt.ACQUISITION_FIELD_1_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_1_CODE,''A'')
                            OR COALESCE(tgt.ACQUISITION_FIELD_2_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_2_CODE,''A'')
                            OR COALESCE(tgt.BUY_MULTIPLE,0) <> COALESCE(src.BUY_MULTIPLE,0)
                            OR COALESCE(tgt.BUY_MULTIPLE_CODE,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE,''A'')
                            OR COALESCE(tgt.BUY_MULTIPLE_CODE_DESCRIPTION,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SUPPLIER_CONVERSION_FACTOR_CODE,''A'') <> COALESCE(src.SUPPLIER_CONVERSION_FACTOR_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_CONVERSION_QUANTITY,0) <> COALESCE(src.SUPPLIER_CONVERSION_QUANTITY,0)
                            OR COALESCE(tgt.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'')
                            OR COALESCE(tgt.UNIT_OF_MEASURE_AMOUNT,0) <> COALESCE(src.UNIT_OF_MEASURE_AMOUNT,0)
                            OR COALESCE(tgt.UNIT_OF_MEASURE_QUANTITY,0) <> COALESCE(src.UNIT_OF_MEASURE_QUANTITY,0)
                            OR COALESCE(tgt.UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.UNIT_OF_MEASURE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.TAX_CLASSIFICATION_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE,''A'')
                            OR COALESCE(tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'')
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'')
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_LENGTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_LENGTH,0)
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WIDTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WIDTH,0)
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_HEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_HEIGHT,0)
                            OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WEIGHT,0)
                            OR COALESCE(tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                            OR COALESCE(tgt.CASE_QUANTITY_CODE,''A'') <> COALESCE(src.CASE_QUANTITY_CODE,''A'')
                            OR COALESCE(tgt.CASE_LENGTH,0) <> COALESCE(src.CASE_LENGTH,0)
                            OR COALESCE(tgt.CASE_WIDTH,0) <> COALESCE(src.CASE_WIDTH,0)
                            OR COALESCE(tgt.CASE_HEIGHT,0) <> COALESCE(src.CASE_HEIGHT,0)
                            OR COALESCE(tgt.CASE_WEIGHT,0) <> COALESCE(src.CASE_WEIGHT,0)
                            OR COALESCE(tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                            OR COALESCE(tgt.CASES_PER_PALLET,0) <> COALESCE(src.CASES_PER_PALLET,0)
                            OR COALESCE(tgt.CASES_PER_PALLET_LAYER,0) <> COALESCE(src.CASES_PER_PALLET_LAYER,0)
                            OR COALESCE(tgt.PALLET_LENGTH,0) <> COALESCE(src.PALLET_LENGTH,0)
                            OR COALESCE(tgt.PALLET_WIDTH,0) <> COALESCE(src.PALLET_WIDTH,0)
                            OR COALESCE(tgt.PALLET_HEIGHT,0) <> COALESCE(src.PALLET_HEIGHT,0)
                            OR COALESCE(tgt.PALLET_WEIGHT,0) <> COALESCE(src.PALLET_WEIGHT,0)
                            OR COALESCE(tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                            OR COALESCE(tgt.SHIPMENT_CLASS_CODE,''A'') <> COALESCE(src.SHIPMENT_CLASS_CODE,''A'')
                            OR COALESCE(tgt.DOT_CLASS_NUMBER,0) <> COALESCE(src.DOT_CLASS_NUMBER,0)
                            OR COALESCE(tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER,0) <> COALESCE(src.DOT_CLASS_FOR_MSDS_ID_NUMBER,0)
                            OR COALESCE(tgt.CONTAINER_DESCRIPTION,''A'') <> COALESCE(src.CONTAINER_DESCRIPTION,''A'')
                            OR COALESCE(tgt.KEEP_FROM_FREEZING_FLAG,''A'') <> COALESCE(src.KEEP_FROM_FREEZING_FLAG,''A'')
                            OR COALESCE(tgt.FLIGHT_RESTRICTED_FLAG,''A'') <> COALESCE(src.FLIGHT_RESTRICTED_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_NEW_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_NEW_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_CORE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_CORE_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_WARRANTY_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_WARRANTY_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_RECALL_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_RECALL_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.HAZARDOUS_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PIECE_LENGTH,0) <> COALESCE(src.PIECE_LENGTH,0)
                            OR COALESCE(tgt.PIECE_WIDTH,0) <> COALESCE(src.PIECE_WIDTH,0)
                            OR COALESCE(tgt.PIECE_HEIGHT,0) <> COALESCE(src.PIECE_HEIGHT,0)
                            OR COALESCE(tgt.PIECE_WEIGHT,0) <> COALESCE(src.PIECE_WEIGHT,0)
                            OR COALESCE(tgt.PIECES_INNER_PACK,0) <> COALESCE(src.PIECES_INNER_PACK,0)
                            OR COALESCE(tgt.IN_CATALOG_CODE,''A'') <> COALESCE(src.IN_CATALOG_CODE,''A'')
                            OR COALESCE(tgt.IN_CATALOG_CODE_DESCRIPTION,''A'') <> COALESCE(src.IN_CATALOG_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.ALLOW_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ALLOW_SPECIAL_ORDER_FLAG,''A'')
                            OR COALESCE(tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'')
                            OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CODE,''A'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.LONG_DESCRIPTION,''A'') <> COALESCE(src.LONG_DESCRIPTION,''A'')
                            OR COALESCE(tgt.ELECTRONIC_WASTE_FLAG,''A'') <> COALESCE(src.ELECTRONIC_WASTE_FLAG,''A'')
                            OR COALESCE(tgt.STORE_MINIMUM_SALE_QUANTITY,0) <> COALESCE(src.STORE_MINIMUM_SALE_QUANTITY,0)
                            OR COALESCE(tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0) <> COALESCE(src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0)
                            OR COALESCE(tgt.MAXIMUM_CAR_QUANTITY,0) <> COALESCE(src.MAXIMUM_CAR_QUANTITY,0)
                            OR COALESCE(tgt.MINIMUM_CAR_QUANTITY,0) <> COALESCE(src.MINIMUM_CAR_QUANTITY,0)
                            OR COALESCE(tgt.ESSENTIAL_HARD_PART_CODE,''A'') <> COALESCE(src.ESSENTIAL_HARD_PART_CODE,''A'')
                            OR COALESCE(tgt.INNER_PACK_CODE,''A'') <> COALESCE(src.INNER_PACK_CODE,''A'')
                            OR COALESCE(tgt.INNER_PACK_QUANTITY,0) <> COALESCE(src.INNER_PACK_QUANTITY,0)
                            OR COALESCE(tgt.INNER_PACK_LENGTH,0) <> COALESCE(src.INNER_PACK_LENGTH,0)
                            OR COALESCE(tgt.INNER_PACK_WIDTH,0) <> COALESCE(src.INNER_PACK_WIDTH,0)
                            OR COALESCE(tgt.INNER_PACK_HEIGHT,0) <> COALESCE(src.INNER_PACK_HEIGHT,0)
                            OR COALESCE(tgt.INNER_PACK_WEIGHT,0) <> COALESCE(src.INNER_PACK_WEIGHT,0)
                            OR COALESCE(tgt.BRAND_CODE,''A'') <> COALESCE(src.BRAND_CODE,''A'')
                            OR COALESCE(tgt.PART_NUMBER_CODE,''A'') <> COALESCE(src.PART_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.PART_NUMBER_DISPLAY_CODE,''A'') <> COALESCE(src.PART_NUMBER_DISPLAY_CODE,''A'')
                            OR COALESCE(tgt.PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.PART_NUMBER_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SPANISH_PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.SPANISH_PART_NUMBER_DESCRIPTION,''A'')
                            OR COALESCE(tgt.SUGGESTED_ORDER_QUANTITY,0) <> COALESCE(src.SUGGESTED_ORDER_QUANTITY,0)
                            OR COALESCE(tgt.BRAND_TYPE_NAME,''A'') <> COALESCE(src.BRAND_TYPE_NAME,''A'')
                            OR COALESCE(tgt.LOCATION_TYPE_NAME,''A'') <> COALESCE(src.LOCATION_TYPE_NAME,''A'')
                            OR COALESCE(tgt.MANUFACTURING_CODE_DESCRIPTION,''A'') <> COALESCE(src.MANUFACTURING_CODE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.QUALITY_GRADE_CODE,''A'') <> COALESCE(src.QUALITY_GRADE_CODE,''A'')
                            OR COALESCE(tgt.PRIMARY_APPLICATION_NAME,''A'') <> COALESCE(src.PRIMARY_APPLICATION_NAME,''A'')
                            --INFAETL-11515 begin change
                            OR COALESCE(tgt.CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.CATEGORY_MANAGER_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.CATEGORY_MANAGER_NUMBER,0)
                            OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'')
                            OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0)
                            OR COALESCE(tgt.CATEGORY_DIRECTOR_NAME,''A'') <> COALESCE(src.CATEGORY_DIRECTOR_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_DIRECTOR_NUMBER,0) <> COALESCE(src.CATEGORY_DIRECTOR_NUMBER,0)
                            OR COALESCE(tgt.CATEGORY_VP_NAME,''A'') <> COALESCE(src.CATEGORY_VP_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_VP_NUMBER,0) <> COALESCE(src.CATEGORY_VP_NUMBER,0)
                            --INFAETL-11515 end change
                            OR COALESCE(tgt.INACTIVATED_DATE,''1900-01-01'') <> COALESCE(src.INACTIVATED_DATE,''1900-01-01'')
                            OR COALESCE(tgt.REVIEW_CODE,''A'') <> COALESCE(src.REVIEW_CODE,''A'')
                            OR COALESCE(tgt.STOCKING_LINE_FLAG,''A'') <> COALESCE(src.STOCKING_LINE_FLAG,''A'')
                            OR COALESCE(tgt.OIL_LINE_FLAG,''A'') <> COALESCE(src.OIL_LINE_FLAG,''A'')
                            OR COALESCE(tgt.SPECIAL_REQUIREMENTS_LABEL,''A'') <> COALESCE(src.SPECIAL_REQUIREMENTS_LABEL,''A'')
                            OR COALESCE(tgt.SUPPLIER_ACCOUNT_NUMBER,0) <> COALESCE(src.SUPPLIER_ACCOUNT_NUMBER,0)
                            OR COALESCE(tgt.SUPPLIER_NUMBER,0) <> COALESCE(src.SUPPLIER_NUMBER,0)
                            OR COALESCE(tgt.SUPPLIER_ID,0) <> COALESCE(src.SUPPLIER_ID,0)
                            OR COALESCE(tgt.BRAND_DESCRIPTION,''A'') <> COALESCE(src.BRAND_DESCRIPTION,''A'')
                            OR COALESCE(tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0) <> COALESCE(src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0)
                            OR COALESCE(tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0) <> COALESCE(src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0)
                            OR COALESCE(tgt.SALES_AREA_NAME,''A'') <> COALESCE(src.SALES_AREA_NAME,''A'')
                            OR COALESCE(tgt.TEAM_NAME,''A'') <> COALESCE(src.TEAM_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_NAME,''A'') <> COALESCE(src.CATEGORY_NAME,''A'')
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_NAME,''A'') <> COALESCE(src.REPLENISHMENT_ANALYST_NAME,''A'')
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_NUMBER,0)
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0)
                            OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0)
                            OR COALESCE(tgt.SALES_AREA_NAME_SORT_NUMBER,0) <> COALESCE(src.SALES_AREA_NAME_SORT_NUMBER,0)
                            OR COALESCE(tgt.TEAM_NAME_SORT_NUMBER,0) <> COALESCE(src.TEAM_NAME_SORT_NUMBER,0)
                            OR COALESCE(tgt.BUYER_CODE,''A'') <> COALESCE(src.BUYER_CODE,''A'')
                            OR COALESCE(tgt.BUYER_NAME,''A'') <> COALESCE(src.BUYER_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'') <> COALESCE(src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'') <> COALESCE(src.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_CITY_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_CITY_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_STATE_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_STATE_NAME,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_ZIP_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ZIP_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_PHONE_NUMBER_CODE,''A'') <> COALESCE(src.BATTERY_PHONE_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.BATTERY_WEIGHT_IN_GRAMS,0) <> COALESCE(src.BATTERY_WEIGHT_IN_GRAMS,0)
                            OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0)
                            OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0)
                            OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_CELL,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_CELL,0)
                            OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_BATTERY,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_BATTERY,0)
                            OR COALESCE(tgt.BATTERY_CELLS_NUMBER,0) <> COALESCE(src.BATTERY_CELLS_NUMBER,0)
                            OR COALESCE(tgt.BATTERIES_PER_PACKAGE_NUMBER,0) <> COALESCE(src.BATTERIES_PER_PACKAGE_NUMBER,0)
                            OR COALESCE(tgt.BATTERIES_IN_EQUIPMENT_NUMBER,0) <> COALESCE(src.BATTERIES_IN_EQUIPMENT_NUMBER,0)
                            OR COALESCE(tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'') <> COALESCE(src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'')
                            OR COALESCE(tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'') <> COALESCE(src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'')
                            OR COALESCE(tgt.COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                            OR COALESCE(tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'') <> COALESCE(src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'')
                            OR COALESCE(tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'') <> COALESCE(src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'')
                            OR COALESCE(tgt.SCHEDULE_B_CODE_LIST,''A'') <> COALESCE(src.SCHEDULE_B_CODE_LIST,''A'')
                            OR COALESCE(tgt.UNITED_STATES_MUNITIONS_LIST_CODE,''A'') <> COALESCE(src.UNITED_STATES_MUNITIONS_LIST_CODE,''A'')
                            OR COALESCE(tgt.PROJECT_COORDINATOR_ID_CODE,''A'') <> COALESCE(src.PROJECT_COORDINATOR_ID_CODE,''A'')
                            OR COALESCE(tgt.PROJECT_COORDINATOR_EMPLOYEE_ID,0) <> COALESCE(src.PROJECT_COORDINATOR_EMPLOYEE_ID,0)
                            OR COALESCE(tgt.STOCK_ADJUSTMENT_MONTH_NUMBER,0) <> COALESCE(src.STOCK_ADJUSTMENT_MONTH_NUMBER,0)
                            OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'')
                            OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                            OR COALESCE(tgt.ALL_IN_COST,0) <> COALESCE(src.ALL_IN_COST,0)
                            OR COALESCE(tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'') <> COALESCE(src.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'')
                            OR COALESCE(tgt.CASE_LOT_DISCOUNT,0) <> COALESCE(src.CASE_LOT_DISCOUNT,0)
                            OR COALESCE(tgt.COMPANY_NUMBER,0) <> COALESCE(src.COMPANY_NUMBER,0)
                            OR COALESCE(tgt.CONVENIENCE_PACK_QUANTITY,0) <> COALESCE(src.CONVENIENCE_PACK_QUANTITY,0)
                            OR COALESCE(tgt.CONVENIENCE_PACK_DESCRIPTION,''A'') <> COALESCE(src.CONVENIENCE_PACK_DESCRIPTION,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'') <> COALESCE(src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'')
                            OR COALESCE(tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'') <> COALESCE(src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'')
                            OR COALESCE(tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'') <> COALESCE(src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'')
                            OR COALESCE(tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'') <> COALESCE(src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.HAZARDOUS_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.HAZARDOUS_UPDATE_USER_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.LIST_PRICE,0) <> COALESCE(src.LIST_PRICE,0)
                            OR COALESCE(tgt.LOW_USER_PRICE,0) <> COALESCE(src.LOW_USER_PRICE,0)
                            OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE,0) <> COALESCE(src.MINIMUM_ADVERTISED_PRICE,0)
                            OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'') <> COALESCE(src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.MINIMUM_SELL_QUANTITY,0) <> COALESCE(src.MINIMUM_SELL_QUANTITY,0)
                            OR COALESCE(tgt.PACKAGE_SIZE_DESCRIPTION,''A'') <> COALESCE(src.PACKAGE_SIZE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.PERCENTAGE_OF_SUPPLIER_FUNDING,0) <> COALESCE(src.PERCENTAGE_OF_SUPPLIER_FUNDING,0)
                            OR COALESCE(tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'') <> COALESCE(src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'')
                            OR COALESCE(tgt.PRICING_COST,0) <> COALESCE(src.PRICING_COST,0)
                            OR COALESCE(tgt.PROFESSIONAL_PRICE,0) <> COALESCE(src.PROFESSIONAL_PRICE,0)
                            OR COALESCE(tgt.RETAIL_CORE,0) <> COALESCE(src.RETAIL_CORE,0)
                            OR COALESCE(tgt.RETAIL_HEIGHT,0) <> COALESCE(src.RETAIL_HEIGHT,0)
                            OR COALESCE(tgt.RETAIL_LENGTH,0) <> COALESCE(src.RETAIL_LENGTH,0)
                            OR COALESCE(tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'')
                            OR COALESCE(tgt.RETAIL_WIDTH,0) <> COALESCE(src.RETAIL_WIDTH,0)
                            OR COALESCE(tgt.SALES_PACK_CODE,''A'') <> COALESCE(src.SALES_PACK_CODE,''A'')
                            OR COALESCE(tgt.SCORE_FLAG,''A'') <> COALESCE(src.SCORE_FLAG,''A'')
                            OR COALESCE(tgt.SHIPPING_DIMENSIONS_CODE,''A'') <> COALESCE(src.SHIPPING_DIMENSIONS_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_BASE_COST,0) <> COALESCE(src.SUPPLIER_BASE_COST,0)
                            OR COALESCE(tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                            OR COALESCE(tgt.SUPPLIER_SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_LINE_CODE,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.CATEGORY_TABLE_CREATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_USER_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'')
                            OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'')
                            OR COALESCE(tgt.VIP_JOBBER,0) <> COALESCE(src.VIP_JOBBER,0)
                            OR COALESCE(tgt.WAREHOUSE_CORE,0) <> COALESCE(src.WAREHOUSE_CORE,0)
                            OR COALESCE(tgt.WAREHOUSE_COST,0) <> COALESCE(src.WAREHOUSE_COST,0)
                            OR COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG,''A'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG,''A'')
                            ) 
                            WITH UR;';
                           
               IF v_log_level >= 3
                  THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17a. ECPARTS UPDATE diagnostic', 'diagnostic', v_str_sql, current_timestamp);
               END IF;
             
               EXECUTE IMMEDIATE v_str_sql;

               IF v_log_level >= 3
                  THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17b. ECPARTS UPDATE diagnostic', 'diag: '||v_sql_code, v_str_sql, current_timestamp);
               END IF;

               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

               IF v_log_level >= 3
                  THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17c. ECPARTS UPDATE diagnostic - after get diagnositcs', 'diag: '||v_sql_code, v_str_sql, current_timestamp);
               END IF;
           
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  IF v_log_level >= 3
                     THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17d. ECPARTS UPDATE diagnostic - in SQL<>0 block', 'diag: '||v_sql_code, v_str_sql, current_timestamp);
                  END IF;
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN
                  IF v_log_level >= 3
                     THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17e. ECPARTS UPDATE diagnostic - in SQL<>0 /v_logging block', 'diag: '||v_sql_code, v_str_sql, current_timestamp);
                  END IF;

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <ECPARTS Update> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';
                  IF v_log_level >= 3
                     THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17f. ECPARTS UPDATE diagnostic - about to call standard log', 'diag: '||v_sql_code, v_str_sql, current_timestamp);
                  END IF;

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17. ECPARTS UPDATE', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);

                  IF v_log_level >= 3
                     THEN CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '17g. ECPARTS UPDATE diagnostic - after call standard log', 'diag: '||v_sql_code, v_str_sql, current_timestamp);
                  END IF;
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                        
                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

            END IF; --v_SQL_OK
           
             

            
                --Populate Hub Load Table - WHNONSTK 4th INSERT
            IF V_SQL_OK THEN
                    SET v_str_sql =  CLOB('INSERT INTO ') || v_staging_database_name || '.' || v_staging_table_name || 
                                '(PRODUCT_ID, LINE_CODE, LINE_DESCRIPTION, ITEM_CODE, ITEM_DESCRIPTION, SEGMENT_NUMBER, SEGMENT_DESCRIPTION, SUB_CATEGORY_NUMBER, SUB_CATEGORY_DESCRIPTION, CATEGORY_NUMBER, CATEGORY_DESCRIPTION, PRODUCT_LINE_CODE, SUB_CODE, MANUFACTURE_ITEM_NUMBER_CODE, SUPERSEDED_LINE_CODE, SUPERSEDED_ITEM_NUMBER_CODE, SORT_CONTROL_NUMBER, POINT_OF_SALE_DESCRIPTION, POPULARITY_CODE, POPULARITY_CODE_DESCRIPTION, POPULARITY_TREND_CODE, POPULARITY_TREND_CODE_DESCRIPTION, LINE_IS_MARINE_SPECIFIC_FLAG, LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE, LINE_IS_FLEET_SPECIFIC_CODE, LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE, LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG, JOBBER_SUPPLIER_CODE, JOBBER_UNIT_OF_MEASURE_CODE, WAREHOUSE_UNIT_OF_MEASURE_CODE, WAREHOUSE_SELL_QUANTITY, RETAIL_WEIGHT, QUANTITY_PER_CAR, CASE_QUANTITY, STANDARD_PACKAGE, PAINT_BODY_AND_EQUIPMENT_PRICE, WAREHOUSE_JOBBER_PRICE, WAREHOUSE_COST_WUM, WAREHOUSE_CORE_WUM, OREILLY_COST_PRICE, JOBBER_COST, JOBBER_CORE_PRICE, OUT_FRONT_MERCHANDISE_FLAG, ITEM_IS_TAXED_FLAG, QUANTITY_ORDER_ITEM_FLAG, JOBBER_DIVIDE_QUANTITY, ITEM_DELETE_FLAG_RECORD_CODE, SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE, PRIMARY_UNIVERSAL_PRODUCT_CODE, WARRANTY_CODE, WARRANTY_CODE_DESCRIPTION, INVOICE_COST_WUM_INVOICE_COST, INVOICE_CORE_WUM_CORE_COST, IS_CONSIGNMENT_ITEM_FLAG, WAREHOUSE_JOBBER_CORE_PRICE, ACQUISITION_FIELD_1_CODE, ACQUISITION_FIELD_2_CODE, BUY_MULTIPLE, BUY_MULTIPLE_CODE, BUY_MULTIPLE_CODE_DESCRIPTION, SUPPLIER_CONVERSION_FACTOR_CODE, SUPPLIER_CONVERSION_QUANTITY, SUPPLIER_UNIT_OF_MEASURE_CODE, UNIT_OF_MEASURE_AMOUNT, UNIT_OF_MEASURE_QUANTITY, UNIT_OF_MEASURE_DESCRIPTION, TAX_CLASSIFICATION_CODE, TAX_CLASSIFICATION_CODE_DESCRIPTION, TAX_CLASSIFICATION_REVIEW_STATUS_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION, DISTRIBUTION_CENTER_PICK_LENGTH, DISTRIBUTION_CENTER_PICK_WIDTH, DISTRIBUTION_CENTER_PICK_HEIGHT, DISTRIBUTION_CENTER_PICK_WEIGHT, PICK_LENGTH_WIDTH_HEIGHT_CODE, CASE_QUANTITY_CODE, CASE_LENGTH, CASE_WIDTH, CASE_HEIGHT, CASE_WEIGHT, CASE_LENGTH_WIDTH_HEIGHT_CODE, CASES_PER_PALLET, CASES_PER_PALLET_LAYER, PALLET_LENGTH, PALLET_WIDTH, PALLET_HEIGHT, PALLET_WEIGHT, PALLET_LENGTH_WIDTH_HEIGHT_CODE, SHIPMENT_CLASS_CODE, DOT_CLASS_NUMBER, DOT_CLASS_FOR_MSDS_ID_NUMBER, CONTAINER_DESCRIPTION, KEEP_FROM_FREEZING_FLAG, FLIGHT_RESTRICTED_FLAG, ALLOW_NEW_RETURNS_FLAG, ALLOW_CORE_RETURNS_FLAG, ALLOW_WARRANTY_RETURNS_FLAG, ALLOW_RECALL_RETURNS_FLAG, ALLOW_MANUAL_OTHER_RETURNS_FLAG, ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG, HAZARDOUS_UPDATE_DATE, PIECE_LENGTH, PIECE_WIDTH, PIECE_HEIGHT, PIECE_WEIGHT, PIECES_INNER_PACK, IN_CATALOG_CODE, IN_CATALOG_CODE_DESCRIPTION, ALLOW_SPECIAL_ORDER_FLAG, ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG, SUPPLIER_LIFE_CYCLE_CODE, SUPPLIER_LIFE_CYCLE_CHANGE_DATE, LONG_DESCRIPTION, ELECTRONIC_WASTE_FLAG, STORE_MINIMUM_SALE_QUANTITY, MANUFACTURER_SUGGESTED_RETAIL_PRICE, MAXIMUM_CAR_QUANTITY, MINIMUM_CAR_QUANTITY, ESSENTIAL_HARD_PART_CODE, INNER_PACK_CODE, INNER_PACK_QUANTITY, INNER_PACK_LENGTH, INNER_PACK_WIDTH, INNER_PACK_HEIGHT, INNER_PACK_WEIGHT, BRAND_CODE, PART_NUMBER_CODE,
                                PART_NUMBER_DISPLAY_CODE, PART_NUMBER_DESCRIPTION, SPANISH_PART_NUMBER_DESCRIPTION, SUGGESTED_ORDER_QUANTITY, BRAND_TYPE_NAME, LOCATION_TYPE_NAME, MANUFACTURING_CODE_DESCRIPTION, QUALITY_GRADE_CODE, PRIMARY_APPLICATION_NAME, 
                                --INFAETL-11515 mds renamed / added the following line
                                CATEGORY_MANAGER_NAME, CATEGORY_MANAGER_NUMBER, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER, CATEGORY_DIRECTOR_NAME, CATEGORY_DIRECTOR_NUMBER, CATEGORY_VP_NAME, CATEGORY_VP_NUMBER, 
                                INACTIVATED_DATE, REVIEW_CODE, STOCKING_LINE_FLAG, OIL_LINE_FLAG, SPECIAL_REQUIREMENTS_LABEL, SUPPLIER_ACCOUNT_NUMBER, SUPPLIER_NUMBER, SUPPLIER_ID, BRAND_DESCRIPTION, DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER, ACCOUNTS_PAYABLE_VENDOR_NUMBER, SALES_AREA_NAME, TEAM_NAME, CATEGORY_NAME, REPLENISHMENT_ANALYST_NAME, REPLENISHMENT_ANALYST_NUMBER, REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER, REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID, SALES_AREA_NAME_SORT_NUMBER, TEAM_NAME_SORT_NUMBER, BUYER_CODE, BUYER_NAME, BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE, BATTERY_PACKING_INSTRUCTIONS_CODE, BATTERY_MANUFACTURING_NAME, BATTERY_MANUFACTURING_ADDRESS_LINE_1, BATTERY_MANUFACTURING_ADDRESS_LINE_2, BATTERY_MANUFACTURING_ADDRESS_LINE_3, BATTERY_MANUFACTURING_ADDRESS_LINE_4, BATTERY_MANUFACTURING_CITY_NAME, BATTERY_MANUFACTURING_POSTAL_TOWN_NAME, BATTERY_MANUFACTURING_STATE_NAME, BATTERY_MANUFACTURING_ZIP_CODE, BATTERY_MANUFACTURING_COUNTRY_CODE, BATTERY_PHONE_NUMBER_CODE, BATTERY_WEIGHT_IN_GRAMS, BATTERY_GRAMS_OF_LITHIUM_PER_CELL, BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY, BATTERY_WATT_HOURS_PER_CELL, BATTERY_WATT_HOURS_PER_BATTERY, BATTERY_CELLS_NUMBER, BATTERIES_PER_PACKAGE_NUMBER, BATTERIES_IN_EQUIPMENT_NUMBER, BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG, BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG, COUNTRY_OF_ORIGIN_NAME_LIST, EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST, HARMONIZED_TARIFF_SCHEDULE_CODE_LIST, SCHEDULE_B_CODE_LIST, UNITED_STATES_MUNITIONS_LIST_CODE, PROJECT_COORDINATOR_ID_CODE, PROJECT_COORDINATOR_EMPLOYEE_ID, STOCK_ADJUSTMENT_MONTH_NUMBER, BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST, BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST, ALL_IN_COST, CANCEL_OR_BACKORDER_REMAINDER_CODE, CASE_LOT_DISCOUNT, COMPANY_NUMBER, CONVENIENCE_PACK_QUANTITY, CONVENIENCE_PACK_DESCRIPTION, PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_CREATION_DATE, PRODUCT_SOURCE_TABLE_CREATION_TIME, PRODUCT_SOURCE_TABLE_CREATION_USER_NAME, PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME, DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE, DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE, ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE, EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG, HAZARDOUS_UPDATE_PROGRAM_NAME, HAZARDOUS_UPDATE_TIME, HAZARDOUS_UPDATE_USER_NAME, LIST_PRICE, LOW_USER_PRICE, MINIMUM_ADVERTISED_PRICE, MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE, MINIMUM_SELL_QUANTITY, PACKAGE_SIZE_DESCRIPTION, PERCENTAGE_OF_SUPPLIER_FUNDING, PIECE_LENGTH_WIDTH_HEIGHT_FLAG, PRICING_COST, PROFESSIONAL_PRICE, RETAIL_CORE, RETAIL_HEIGHT, RETAIL_LENGTH, RETAIL_UNIT_OF_MEASURE_DESCRIPTION, RETAIL_WIDTH, SALES_PACK_CODE, SCORE_FLAG, SHIPPING_DIMENSIONS_CODE, SUPPLIER_BASE_COST, SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE, SUPPLIER_SUPERSEDED_LINE_CODE, CATEGORY_TABLE_CREATE_DATE, CATEGORY_TABLE_CREATE_PROGRAM_NAME, CATEGORY_TABLE_CREATE_TIME, CATEGORY_TABLE_CREATE_USER_NAME, CATEGORY_TABLE_UPDATE_DATE, CATEGORY_TABLE_UPDATE_PROGRAM_NAME, CATEGORY_TABLE_UPDATE_TIME, CATEGORY_TABLE_UPDATE_USER_NAME, PRODUCT_SOURCE_TABLE_UPDATE_DATE, PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_UPDATE_TIME, PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME, VIP_JOBBER, WAREHOUSE_CORE, WAREHOUSE_COST, 
                                --INFAETL-11815 mds added the following line
                                PRODUCT_LEVEL_CODE, 
                                ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS
                                ) ' ||   
                        ' SELECT CAST(' || v_process_database_name|| '.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS PRODUCT_ID,
                        CAST(TRIM(cur_WHNONSTK.WHNLINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                        CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                        CAST(TRIM(cur_WHNONSTK.WHNITEM) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                        CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                        CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                        CAST(COALESCE(cur_CATEGORY.SUBCATNUM,-2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                        CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                        CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                        CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                        CAST(TRIM(cur_WHNONSTK.WHNPLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                        CAST(cur_WHNONSTK.WHNSUBC AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                        CAST(''-2'' AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                        CAST(COALESCE(cur_WHNONSTK.WHNLINE, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                        CAST(COALESCE(cur_WHNONSTK.WHNITEM, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                        CAST(-1 AS INTEGER) AS SORT_CONTROL_NUMBER,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                        CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                        CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                        CAST(COALESCE(
                        CASE
                        WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                        CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                        CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                        CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                        CAST(COALESCE(
                        CASE
                        WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                        CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                        CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                        CAST(0 AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                        CAST(0 AS DECIMAL(12,4)) AS CASE_QUANTITY,
                        CAST(0 AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                        CAST(COALESCE(cur_WHNONSTK.WHNPBNEP, 0) AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                        CAST(cur_WHNONSTK.WHNJOBRP AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                        CAST(cur_WHNONSTK.WHNCOSTP AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                        CAST(cur_WHNONSTK.WHNCOREP AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                        CAST(COALESCE(cur_WHNONSTK.WHNORCST, 0) AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                        CAST(cur_WHNONSTK.WHNJCOST AS DECIMAL(12,4)) AS JOBBER_COST,
                        CAST(0 AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                        CAST(0 AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                        CAST(1 AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                        CAST(''NONE'' AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                        CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                        CAST(cur_WHNONSTK.WHNINVC AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                        CAST(cur_WHNONSTK.WHNCORC AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                        CAST('''' AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                        CAST(COALESCE(cur_WHNONSTK.WHNWJCRP, 0) AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                        CAST(-2 AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                        CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                        CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                        CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                        CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                        CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                        CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                        CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS CASE_LENGTH,
                        CAST(0 AS DECIMAL(12,4)) AS CASE_WIDTH,
                        CAST(0 AS DECIMAL(12,4)) AS CASE_HEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS CASE_WEIGHT,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                        CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                        CAST(0 AS DECIMAL(12,4)) AS PALLET_LENGTH,
                        CAST(0 AS DECIMAL(12,4)) AS PALLET_WIDTH,
                        CAST(0 AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                        CAST('''' AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                        CAST(-2 AS INTEGER) AS DOT_CLASS_NUMBER,
                        CAST(-2 AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                        CAST(''1900-01-01'' AS DATE) AS HAZARDOUS_UPDATE_DATE,
                        CAST(0 AS DECIMAL(12,4)) AS PIECE_LENGTH,
                        CAST(0 AS DECIMAL(12,4)) AS PIECE_WIDTH,
                        CAST(0 AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                        CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                        CAST(''1900-01-01'' AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                        CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                        CAST(0 AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                        CAST(0 AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                        CAST(0 AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                        CAST(0 AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                        CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                        CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                        CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                        CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                        CAST(COALESCE(TRIM(cur_AAIAVEND.OCAT_PART_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                        CAST(0 AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                        CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                        CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                        CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                        CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                        CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                        --INFAETL-11515 begin changes
                        CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                        CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                        CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                        CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                        CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                        CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                        CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                        CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                        --INFAETL-11515 end changes
                        CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                        CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                        CAST(CASE
                        WHEN TRIM(cur_CMISFILE.LSTOCK) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                        CAST(COALESCE(
                        CASE
                        WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                        CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                        CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                        CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                        CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                        CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                        CAST(-2 AS INTEGER) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                        CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                        CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                        CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                        CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                        CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                        CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                        CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                        CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                        CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                        CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                        CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                        CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                        CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                        CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                        CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                        CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                        CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(64 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                        CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                        CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                        CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                        CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                        CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                        CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                        CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_PACK), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                        CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                        CAST(COALESCE(
                        CASE
                        WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                        CAST(COALESCE(
                        CASE
                        WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                        CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                        CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(256 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                        CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(256 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                        CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                        CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                        CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                        CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                        CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                        CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                        CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                        CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                        CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                        CAST(0 AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                        CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                        CAST(0 AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                        CAST(COALESCE(TRIM(cur_WHNONSTK.WHNLODPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                        CAST(COALESCE(HUB_LOAD_DIM_DATE_WHNLODDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                        CAST(TIME(cur_WHNONSTK.WHNLODTIME) AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                        CAST(COALESCE(TRIM(cur_WHNONSTK.WHNLODUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                        CAST(COALESCE(HUB_LOAD_DIM_DATE_WHNLSTCUPD.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                        CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                        CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                        CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                        CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                        CAST(''00:00:00'' AS TIME) AS HAZARDOUS_UPDATE_TIME,
                        CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                        CAST(COALESCE(cur_WHNONSTK.WHNLISTP, 0) AS DECIMAL(12,4)) AS LIST_PRICE,
                        CAST(COALESCE(cur_WHNONSTK.WHNUSR, 0) AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                        CAST(0 AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                        CAST(''1900-01-01'' AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                        CAST(0 AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                        CAST(0 AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                        CAST(''N'' AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                        CAST(0 AS DECIMAL(12,4)) AS PRICING_COST,
                        CAST(COALESCE(cur_WHNONSTK.WHNINSTL, 0) AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                        CAST(COALESCE(cur_WHNONSTK.WHNSTRC, 0) AS DECIMAL(12,4)) AS RETAIL_CORE,
                        CAST(0 AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                        CAST(0 AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                        CAST('''' AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                        CAST(0 AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                        CAST(COALESCE(
                        CASE
                        WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                        ELSE ''N''
                        END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                        CAST(COALESCE(cur_WHNONSTK.WHNBCOST, 0) AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                        CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                        CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                        CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                        CAST(COALESCE(cur_CATEGORY.LOADTIME, ''00:00:00'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                        CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                        CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                        CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                        CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                        CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                        CAST(COALESCE(HUB_LOAD_DIM_DATE_WHNUPDDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                        CAST(COALESCE(TRIM(cur_WHNONSTK.WHNUPDPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                        CAST(COALESCE(cur_WHNONSTK.WHNUPDTIME ,''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                        CAST(COALESCE(TRIM(cur_WHNONSTK.WHNUPDUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                        CAST(COALESCE(cur_WHNONSTK.WHNFJOBR,0) AS DECIMAL(12,4)) AS VIP_JOBBER,
                        CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                        CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                        --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                        ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                        CAST(''N'' AS VARCHAR(1)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                        ' || quote_literal(v_etl_source_table_4) || ' AS ETL_SOURCE_TABLE_NAME,
                        CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                        CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP,
                        ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                        ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS
                        FROM EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK
                        LEFT JOIN EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS ON cur_ECPARTS.LINE = cur_WHNONSTK.WHNLINE AND cur_ECPARTS.ITEMNUMBER = cur_WHNONSTK.WHNITEM
                        LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                    FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                    GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND 
                                    ON cur_AAIAVEND.OREILLY_LINE = cur_WHNONSTK.WHNLINE AND cur_AAIAVEND.KEY_ITEM = cur_WHNONSTK.WHNITEM
                        LEFT JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_WHNONSTK.WHNLINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_WHNONSTK.WHNITEM) 
                        LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_WHNONSTK.WHNLINE AND cur_CATEGORY.ITEM = cur_WHNONSTK.WHNITEM
                        LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY 
                        LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_WHNONSTK.WHNLINE AND cur_ICLINCTL.PLCD = cur_WHNONSTK.WHNPLCD AND cur_ICLINCTL.REGION = 0 
                        LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                           FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                           JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                           WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_WHNONSTK.WHNLINE AND cur_DWASGN_DWEMP.PLCD = cur_WHNONSTK.WHNPLCD AND cur_DWASGN_DWEMP.SUBC = cur_WHNONSTK.WHNSUBC 
                        LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                           FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                           JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                           WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_WHNONSTK.WHNLINE AND cur_REPLENISHMENT_ANALYST.PLCD = cur_WHNONSTK.WHNPLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_WHNONSTK.WHNSUBC  
                        LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_WHNONSTK.WHNLINE AND cur_CMISFILE.LPLCD = cur_WHNONSTK.WHNPLCD AND cur_CMISFILE.LSUBC = cur_WHNONSTK.WHNSUBC  
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT  
                        LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_WHNONSTK.WHNLINE AND cur_HAZLION.ITEM_NAME = cur_WHNONSTK.WHNITEM
                        LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, 
                                LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, 
                                LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                           FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                           LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                           GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_WHNONSTK.WHNITEM
                        LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, 
                                LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                                LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                           FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                           LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                           GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_WHNONSTK.WHNLINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_WHNONSTK.WHNITEM 
                        LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPECCN.ITEM = cur_WHNONSTK.WHNITEM
                        LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                           FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                           GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPHTS.ITEM = cur_WHNONSTK.WHNITEM 
                        LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPUSML.ITEM = cur_WHNONSTK.WHNITEM 
                        LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                           FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                           GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPSCDB_LA.ITEM = cur_WHNONSTK.WHNITEM
                        LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX AS cur_PMATRIX ON cur_PMATRIX.LINE = cur_WHNONSTK.WHNLINE AND cur_PMATRIX.PLCD = cur_WHNONSTK.WHNPLCD AND cur_PMATRIX.SUBC = cur_WHNONSTK.WHNSUBC
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_WHNLODDATE ON HUB_LOAD_DIM_DATE_WHNLODDATE.FULL_DATE = cur_WHNONSTK.WHNLODDATE
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_WHNLSTCUPD ON HUB_LOAD_DIM_DATE_WHNLSTCUPD.FULL_DATE = cur_WHNONSTK.WHNLSTCUPD
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_WHNUPDDATE ON HUB_LOAD_DIM_DATE_WHNUPDDATE.FULL_DATE = cur_WHNONSTK.WHNUPDDATE
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = BIGINT(cur_PMATRIX.RA_TMN)
                        LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
                        LEFT JOIN ( SELECT cur_IMASTER.ILINE AS LINE, cur_IMASTER.IITEM# AS ITEM
                                    FROM EDW_STAGING.CUR_IVB_IMASTER AS cur_IMASTER  
                                    UNION 
                                    SELECT cur_IMASTNS.LINE AS LINE, cur_IMASTNS.ITEM AS ITEM
                                    FROM EDW_STAGING.CUR_IVB_IMASTNS AS cur_IMASTNS
                                    UNION 
                                    SELECT cur_ECPARTS.LINE AS LINE, cur_ECPARTS.ITEMNUMBER AS ITEM
                                    FROM EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS) AS cur_UNION ON cur_UNION.LINE = cur_WHNONSTK.WHNLINE AND cur_UNION.ITEM = cur_WHNONSTK.WHNITEM --427137 
                        --INFAETL-11515 add the following 3 joins
                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                        LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                        WHERE NVL(cur_UNION.LINE, '''') = ''''
                            AND hub_DIM_PRODUCT.PRODUCT_ID IS NULL
                            AND (' || quote_literal(v_job_execution_starttime) || ' <= cur_WHNONSTK.CREATE_TIMESTAMP OR cur_WHNONSTK.CREATE_TIMESTAMP IS NULL
                                OR ' || quote_literal(v_job_execution_starttime) || ' <= cur_WHNONSTK.LOAD_TIMESTAMP OR cur_WHNONSTK.LOAD_TIMESTAMP IS NULL ) WITH UR;';

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN
                  SELECT MAX(PRODUCT_ID) INTO V_PROD_ID_MAX FROM EDW_STAGING.HUB_LOAD_DIM_PRODUCT;

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <WHNONSTK INSERT> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< PROD_ID_MAX = ' || COALESCE(CAST( V_PROD_ID_MAX AS VARCHAR),'NPRDIDMAX') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                     
                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '18. WHNONSTK INSERT', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 


                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    
               

             END IF; -- V_SQL_OK

            
                --Populate Hub Load Table - WHNONSTK 4th MERGE
            IF V_SQL_OK THEN
               SET v_str_sql = 'DROP TABLE ' || v_staging_database_name || '.SESSION_TMP_WHNONSTK_UPDATE_SOURCE if exists;';
               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                IF (V_SQL_CODE <> 0) THEN  --  Warning
                   SET V_SQL_OK = FALSE;
                   SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
            
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                   SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <WHNONSTK DROP update table> '||
                       '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                       '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                       ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                       ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                       ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '19. WHNONSTK_UPDATE preprocess table DROP', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    

            END IF; -- V_SQL_OK

        --1st Merge step 2, recreate "source" table   
            IF V_SQL_OK THEN            
               SET v_str_sql = CLOB('CREATE  TABLE ') ||  v_staging_database_name || '.SESSION_TMP_WHNONSTK_UPDATE_SOURCE
               AS (          SELECT hub_DIM_PRODUCT.PRODUCT_ID,
                    CAST(TRIM(cur_WHNONSTK.WHNLINE) AS VARCHAR(16 OCTETS)) AS LINE_CODE,
                    CAST(COALESCE(TRIM(cur_CMISFILE.LDESC), '''') AS VARCHAR(512 OCTETS)) AS LINE_DESCRIPTION,
                    CAST(TRIM(cur_WHNONSTK.WHNITEM) AS VARCHAR(16 OCTETS)) AS ITEM_CODE,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS ITEM_DESCRIPTION,
                    CAST(COALESCE(cur_CATEGORY.SEGNUM, -2) AS INTEGER) AS SEGMENT_NUMBER,
                    CAST(COALESCE(TRIM(cur_CATEGORY.SEGMENT), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SEGMENT_DESCRIPTION,
                    CAST(COALESCE(cur_CATEGORY.SUBCATNUM,-2) AS INTEGER) AS SUB_CATEGORY_NUMBER,
                    CAST(COALESCE(TRIM(cur_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS SUB_CATEGORY_DESCRIPTION,
                    CAST(COALESCE(cur_CATEGORY.CATEGORY_NUMBER, -2) AS INTEGER) AS CATEGORY_NUMBER,
                    CAST(COALESCE(TRIM(cur_CATEGORY.CATEGORY), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS CATEGORY_DESCRIPTION,
                    CAST(TRIM(cur_WHNONSTK.WHNPLCD) AS VARCHAR(16 OCTETS)) AS PRODUCT_LINE_CODE,
                    CAST(cur_WHNONSTK.WHNSUBC AS VARCHAR(16 OCTETS)) AS SUB_CODE,
                    CAST(''-2'' AS VARCHAR(16 OCTETS)) AS MANUFACTURE_ITEM_NUMBER_CODE,
                    CAST(COALESCE(cur_WHNONSTK.WHNLINE, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_LINE_CODE,
                    CAST(COALESCE(cur_WHNONSTK.WHNITEM, ''-2'') AS VARCHAR(16 OCTETS)) AS SUPERSEDED_ITEM_NUMBER_CODE,
                    CAST(-1 AS INTEGER) AS SORT_CONTROL_NUMBER,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS POINT_OF_SALE_DESCRIPTION,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_CODE,
                    CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_CODE_DESCRIPTION,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS POPULARITY_TREND_CODE,
                    CAST(''DEFAULT'' AS VARCHAR(512 OCTETS)) AS POPULARITY_TREND_CODE_DESCRIPTION,
                    CAST(COALESCE(
                    CASE
                    WHEN TRIM(cur_ICLINCTL.MARINE) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_MARINE_SPECIFIC_FLAG,
                    CAST(COALESCE(TRIM(cur_ICLINCTL.AGRI), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                    CAST(COALESCE(TRIM(cur_ICLINCTL.FLEET), ''N'') AS VARCHAR(16 OCTETS)) AS LINE_IS_FLEET_SPECIFIC_CODE,
                    CAST(COALESCE(TRIM(cur_ICLINCTL.PAINT), ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                    CAST(COALESCE(
                    CASE
                    WHEN TRIM(cur_ICLINCTL.HYDRAULIC) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END, ''N'') AS VARCHAR(1 OCTETS)) AS LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_SUPPLIER_CODE,
                    CAST(0 AS VARCHAR(16 OCTETS)) AS JOBBER_UNIT_OF_MEASURE_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS WAREHOUSE_UNIT_OF_MEASURE_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_SELL_QUANTITY,
                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS QUANTITY_PER_CAR,
                    CAST(0 AS DECIMAL(12,4)) AS CASE_QUANTITY,
                    CAST(0 AS DECIMAL(12,4)) AS STANDARD_PACKAGE,
                    CAST(COALESCE(cur_WHNONSTK.WHNPBNEP, 0) AS DECIMAL(12,4)) AS PAINT_BODY_AND_EQUIPMENT_PRICE,
                    CAST(cur_WHNONSTK.WHNJOBRP AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_PRICE,
                    CAST(cur_WHNONSTK.WHNCOSTP AS DECIMAL(12,4)) AS WAREHOUSE_COST_WUM,
                    CAST(cur_WHNONSTK.WHNCOREP AS DECIMAL(12,4)) AS WAREHOUSE_CORE_WUM,
                    CAST(COALESCE(cur_WHNONSTK.WHNORCST, 0) AS DECIMAL(12,4)) AS OREILLY_COST_PRICE,
                    CAST(cur_WHNONSTK.WHNJCOST AS DECIMAL(12,4)) AS JOBBER_COST,
                    CAST(0 AS DECIMAL(12,4)) AS JOBBER_CORE_PRICE,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS OUT_FRONT_MERCHANDISE_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_IS_TAXED_FLAG,
                    CAST(0 AS VARCHAR(1 OCTETS)) AS QUANTITY_ORDER_ITEM_FLAG,
                    CAST(1 AS DECIMAL(12,4)) AS JOBBER_DIVIDE_QUANTITY,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS ITEM_DELETE_FLAG_RECORD_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS PRIMARY_UNIVERSAL_PRODUCT_CODE,
                    CAST(''NONE'' AS VARCHAR(16 OCTETS)) AS WARRANTY_CODE,
                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS WARRANTY_CODE_DESCRIPTION,
                    CAST(cur_WHNONSTK.WHNINVC AS DECIMAL(12,4)) AS INVOICE_COST_WUM_INVOICE_COST,
                    CAST(cur_WHNONSTK.WHNCORC AS DECIMAL(12,4)) AS INVOICE_CORE_WUM_CORE_COST,
                    CAST('''' AS VARCHAR(1 OCTETS)) AS IS_CONSIGNMENT_ITEM_FLAG,
                    CAST(COALESCE(cur_WHNONSTK.WHNWJCRP, 0) AS DECIMAL(12,4)) AS WAREHOUSE_JOBBER_CORE_PRICE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_1_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS ACQUISITION_FIELD_2_CODE,
                    CAST(-2 AS DECIMAL(12,4)) AS BUY_MULTIPLE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS BUY_MULTIPLE_CODE,
                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS BUY_MULTIPLE_CODE_DESCRIPTION,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_CONVERSION_FACTOR_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS SUPPLIER_CONVERSION_QUANTITY,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_UNIT_OF_MEASURE_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_AMOUNT,
                    CAST(0 AS DECIMAL(12,4)) AS UNIT_OF_MEASURE_QUANTITY,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS UNIT_OF_MEASURE_DESCRIPTION,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_CODE,
                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS TAX_CLASSIFICATION_CODE_DESCRIPTION,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_LENGTH,
                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WIDTH,
                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_HEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS DISTRIBUTION_CENTER_PICK_WEIGHT,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS PICK_LENGTH_WIDTH_HEIGHT_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_QUANTITY_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS CASE_LENGTH,
                    CAST(0 AS DECIMAL(12,4)) AS CASE_WIDTH,
                    CAST(0 AS DECIMAL(12,4)) AS CASE_HEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS CASE_WEIGHT,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS CASE_LENGTH_WIDTH_HEIGHT_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET,
                    CAST(0 AS DECIMAL(12,4)) AS CASES_PER_PALLET_LAYER,
                    CAST(0 AS DECIMAL(12,4)) AS PALLET_LENGTH,
                    CAST(0 AS DECIMAL(12,4)) AS PALLET_WIDTH,
                    CAST(0 AS DECIMAL(12,4)) AS PALLET_HEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS PALLET_WEIGHT,
                    CAST('''' AS VARCHAR(1 OCTETS)) AS PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPMENT_CLASS_CODE,
                    CAST(-2 AS INTEGER) AS DOT_CLASS_NUMBER,
                    CAST(-2 AS INTEGER) AS DOT_CLASS_FOR_MSDS_ID_NUMBER,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONTAINER_DESCRIPTION,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS KEEP_FROM_FREEZING_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS FLIGHT_RESTRICTED_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_NEW_RETURNS_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_CORE_RETURNS_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_WARRANTY_RETURNS_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_RECALL_RETURNS_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                    CAST(''1900-01-01'' AS DATE) AS HAZARDOUS_UPDATE_DATE,
                    CAST(0 AS DECIMAL(12,4)) AS PIECE_LENGTH,
                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WIDTH,
                    CAST(0 AS DECIMAL(12,4)) AS PIECE_HEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS PIECE_WEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS PIECES_INNER_PACK,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS IN_CATALOG_CODE,
                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS IN_CATALOG_CODE_DESCRIPTION,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ALLOW_SPECIAL_ORDER_FLAG,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_LIFE_CYCLE_CODE,
                    CAST(''1900-01-01'' AS DATE) AS SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                    CAST(''UNKNOWN'' AS VARCHAR(512 OCTETS)) AS LONG_DESCRIPTION,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS ELECTRONIC_WASTE_FLAG,
                    CAST(0 AS DECIMAL(12,4)) AS STORE_MINIMUM_SALE_QUANTITY,
                    CAST(0 AS DECIMAL(12,4)) AS MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                    CAST(0 AS DECIMAL(12,4)) AS MAXIMUM_CAR_QUANTITY,
                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_CAR_QUANTITY,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS ESSENTIAL_HARD_PART_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS INNER_PACK_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_QUANTITY,
                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_LENGTH,
                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WIDTH,
                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_HEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS INNER_PACK_WEIGHT,
                    CAST(COALESCE(TRIM(cur_ECPARTS.BRAND_CODE), '''') AS VARCHAR(16 OCTETS)) AS BRAND_CODE,
                    CAST(COALESCE(TRIM(cur_AAIAVEND.OCAT_PART_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS PART_NUMBER_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS PART_NUMBER_DISPLAY_CODE,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS PART_NUMBER_DESCRIPTION,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS SPANISH_PART_NUMBER_DESCRIPTION,
                    CAST(0 AS DECIMAL(12,4)) AS SUGGESTED_ORDER_QUANTITY,
                    CAST(COALESCE(TRIM(cur_CATEGORY.BRAND_TYPE),'''') AS VARCHAR(128 OCTETS)) AS BRAND_TYPE_NAME,
                    CAST(COALESCE(TRIM(cur_CATEGORY.LOCATION_TYPE), '''') AS VARCHAR(128 OCTETS)) AS LOCATION_TYPE_NAME,
                    CAST(COALESCE(TRIM(cur_CATEGORY.MFG_TYPE), '''') AS VARCHAR(512 OCTETS)) AS MANUFACTURING_CODE_DESCRIPTION,
                    CAST(COALESCE(TRIM(cur_CATEGORY.QUALITY_GRADE), '''') AS VARCHAR(16 OCTETS)) AS QUALITY_GRADE_CODE,
                    CAST(COALESCE(TRIM(cur_CATEGORY.PRIMARY_APPLICATION), '''') AS VARCHAR(256 OCTETS)) AS PRIMARY_APPLICATION_NAME,
                    --INFAETL-11515 begin changes
                    CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME,
                    CAST(COALESCE(cur_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER,
                    CAST(COALESCE(TRIM(cur_DWASGN_DWEMP.DEUSRD), '''') AS VARCHAR(256 OCTETS)) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                    CAST(COALESCE(cur_DWASGN_DWEMP.EMP#, -2) AS INTEGER) AS ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                    CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME,
                    CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER,
                    CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME,
                    CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER,
                    --INFAETL-11515 end changes
                    CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE,
                    CAST(COALESCE(TRIM(cur_CATEGORY.REVIEW), '''') AS VARCHAR(1 OCTETS)) AS REVIEW_CODE,
                    CAST(CASE
                    WHEN TRIM(cur_CMISFILE.LSTOCK) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END AS VARCHAR(1 OCTETS)) AS STOCKING_LINE_FLAG,
                    CAST(COALESCE(
                    CASE
                    WHEN TRIM(cur_CMISFILE.LOIL) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END, ''N'') AS VARCHAR(1 OCTETS)) AS OIL_LINE_FLAG,
                    CAST(COALESCE(TRIM(cur_CMISFILE.LSPCRQ), '''') AS VARCHAR(128 OCTETS)) AS SPECIAL_REQUIREMENTS_LABEL,
                    CAST(COALESCE(cur_CMISFILE.LVACCT, -2) AS INTEGER) AS SUPPLIER_ACCOUNT_NUMBER,
                    CAST(COALESCE(cur_CMISFILE.LSUPR#, -2) AS INTEGER) AS SUPPLIER_NUMBER,
                    CAST(COALESCE(HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID , -2) AS BIGINT) AS SUPPLIER_ID,
                    CAST(COALESCE(TRIM(cur_CMISFILE.YDESC), ''UNKNOWN'') AS VARCHAR(512 OCTETS)) AS BRAND_DESCRIPTION,
                    CAST(-2 AS INTEGER) AS DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                    CAST(COALESCE(INT(cur_CMISFILE.VNDNBR),-2) AS INTEGER) AS ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME,
                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME,
                    CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME,
                    CAST(COALESCE(TRIM(cur_REPLENISHMENT_ANALYST.DEUSRD) , '''') AS VARCHAR(256 OCTETS)) AS REPLENISHMENT_ANALYST_NAME,
                    CAST(COALESCE(INT(cur_REPLENISHMENT_ANALYST.EMP#), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_NUMBER,
                    CAST(COALESCE(INT(cur_PMATRIX.RA_TMN), -2) AS INTEGER) AS REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_ID, -2) AS BIGINT) AS REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER,
                    CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER,
                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYCD) , '''') AS VARCHAR(16 OCTETS)) AS BUYER_CODE,
                    CAST(COALESCE(TRIM(cur_CMISFILE.LBUYNM), '''') AS VARCHAR(256 OCTETS)) AS BUYER_NAME,
                    CAST(COALESCE(TRIM(cur_HAZLION.UN_NUMBER), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                    CAST(COALESCE(TRIM(cur_HAZLION.PACKING_INST_CODE), '''') AS VARCHAR(16 OCTETS)) AS BATTERY_PACKING_INSTRUCTIONS_CODE,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_NAME,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR1) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR2) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR3), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ADDR4) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_CITY) , '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_CITY_NAME,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_POSTTWN), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_STATE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_MANUFACTURING_STATE_NAME,
                    CAST(COALESCE(TRIM(cur_HAZLION.MANUFACTURING_ZIPCODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_ZIP_CODE,
                    CAST(COALESCE(TRIM(cur_HAZLION.COUNTRY_CODE) , '''') AS VARCHAR(16 OCTETS)) AS BATTERY_MANUFACTURING_COUNTRY_CODE,
                    CAST(COALESCE(TRIM(cur_HAZLION.PHONE_NUMBER), '''') AS VARCHAR(64 OCTETS)) AS BATTERY_PHONE_NUMBER_CODE,
                    CAST(COALESCE(cur_HAZLION.WEIGHT_IN_GRAMS, 0) AS DECIMAL(12,4)) AS BATTERY_WEIGHT_IN_GRAMS,
                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                    CAST(COALESCE(cur_HAZLION.LITHIUM_METAL_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_CELL, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_CELL,
                    CAST(COALESCE(cur_HAZLION.LITHIUM_ION_W_P_BATT, 0) AS DECIMAL(12,4)) AS BATTERY_WATT_HOURS_PER_BATTERY,
                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_CELLSPER_BATT), 0) AS INTEGER) AS BATTERY_CELLS_NUMBER,
                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_PACK), 0) AS INTEGER) AS BATTERIES_PER_PACKAGE_NUMBER,
                    CAST(COALESCE(INT(cur_HAZLION.NUMBER_BATT_IN_EQUIP), 0) AS INTEGER) AS BATTERIES_IN_EQUIPMENT_NUMBER,
                    CAST(COALESCE(
                    CASE
                    WHEN TRIM(cur_HAZLION.LESS_THEN_30P_SOC) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                    CAST(COALESCE(
                    CASE
                    WHEN TRIM(cur_HAZLION.UN_TESTEDDOC_PROVID) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END, ''N'') AS VARCHAR(1 OCTETS)) AS BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                    CAST(COALESCE(TRIM(cur_EXPCORGN_EXPCORIGIN.COUNTRY_NAME), '''') AS VARCHAR(256 OCTETS)) AS COUNTRY_OF_ORIGIN_NAME_LIST,
                    CAST(COALESCE(TRIM(cur_EXPECCN.ECCN_CODE), '''') AS VARCHAR(256 OCTETS)) AS EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                    CAST(COALESCE(TRIM(cur_EXPHTS.HTS_CODE), '''') AS VARCHAR(256 OCTETS)) AS HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                    CAST(COALESCE(TRIM(cur_EXPSCDB_LA.SCHEDULE_B_CODE_LIST), '''') AS VARCHAR(256 OCTETS)) AS SCHEDULE_B_CODE_LIST,
                    CAST(COALESCE(TRIM(cur_EXPUSML.USML_CODE), '''') AS VARCHAR(16 OCTETS)) AS UNITED_STATES_MUNITIONS_LIST_CODE,
                    CAST(COALESCE(TRIM(cur_ICLINCTL.PRJCOORD), '''') AS VARCHAR(16 OCTETS)) AS PROJECT_COORDINATOR_ID_CODE,
                    CAST(COALESCE(HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_ID, -2) AS BIGINT) AS PROJECT_COORDINATOR_EMPLOYEE_ID,
                    CAST(COALESCE(INT(cur_ICLINCTL.STKADJMO), -2) AS INTEGER) AS STOCK_ADJUSTMENT_MONTH_NUMBER,
                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_CODE), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                    CAST(COALESCE(TRIM(cur_HAZLION_EXPCORIGIN.BATTERY_COUNTRY_OF_ORIGIN_NAME), '''') AS VARCHAR(256 OCTETS)) AS BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                    CAST(COALESCE(cur_WHNONSTK.WHNAICST, 0) AS DECIMAL(12,4)) AS ALL_IN_COST,
                    CAST(COALESCE(TRIM(cur_CMISFILE.LCANBO), '''') AS VARCHAR(16 OCTETS)) AS CANCEL_OR_BACKORDER_REMAINDER_CODE,
                    CAST(0 AS DECIMAL(12,4)) AS CASE_LOT_DISCOUNT,
                    CAST(COALESCE(TRIM(cur_CMISFILE.CONUM), -2) AS INTEGER) AS COMPANY_NUMBER,
                    CAST(0 AS DECIMAL(12,4)) AS CONVENIENCE_PACK_QUANTITY,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS CONVENIENCE_PACK_DESCRIPTION,
                    CAST(COALESCE(TRIM(cur_WHNONSTK.WHNLODPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                    CAST(COALESCE(HUB_LOAD_DIM_DATE_WHNLODDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_CREATION_DATE,
                    CAST(TIME(cur_WHNONSTK.WHNLODTIME) AS TIME) AS PRODUCT_SOURCE_TABLE_CREATION_TIME,
                    CAST(COALESCE(TRIM(cur_WHNONSTK.WHNLODUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                    CAST(COALESCE(HUB_LOAD_DIM_DATE_WHNLSTCUPD.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                    CAST(''1900-01-01'' AS DATE) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                    CAST(''00:00:00'' AS TIME) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                    CAST('''' AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_PROGRAM_NAME,
                    CAST(''00:00:00'' AS TIME) AS HAZARDOUS_UPDATE_TIME,
                    CAST('''' AS VARCHAR(128 OCTETS)) AS HAZARDOUS_UPDATE_USER_NAME,
                    CAST(COALESCE(cur_WHNONSTK.WHNLISTP, 0) AS DECIMAL(12,4)) AS LIST_PRICE,
                    CAST(COALESCE(cur_WHNONSTK.WHNUSR, 0) AS DECIMAL(12,4)) AS LOW_USER_PRICE,
                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_ADVERTISED_PRICE,
                    CAST(''1900-01-01'' AS DATE) AS MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                    CAST(0 AS DECIMAL(12,4)) AS MINIMUM_SELL_QUANTITY,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS PACKAGE_SIZE_DESCRIPTION,
                    CAST(0 AS DECIMAL(12,4)) AS PERCENTAGE_OF_SUPPLIER_FUNDING,
                    CAST(''N'' AS VARCHAR(1 OCTETS)) AS PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                    CAST(0 AS DECIMAL(12,4)) AS PRICING_COST,
                    CAST(COALESCE(cur_WHNONSTK.WHNINSTL, 0) AS DECIMAL(12,4)) AS PROFESSIONAL_PRICE,
                    CAST(COALESCE(cur_WHNONSTK.WHNSTRC, 0) AS DECIMAL(12,4)) AS RETAIL_CORE,
                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_HEIGHT,
                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_LENGTH,
                    CAST('''' AS VARCHAR(512 OCTETS)) AS RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                    CAST(0 AS DECIMAL(12,4)) AS RETAIL_WIDTH,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SALES_PACK_CODE,
                    CAST(COALESCE(
                    CASE
                    WHEN TRIM(cur_CMISFILE.LSCORE) = ''Y'' THEN ''Y''
                    ELSE ''N''
                    END, ''Y'') AS VARCHAR(1 OCTETS)) AS SCORE_FLAG,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SHIPPING_DIMENSIONS_CODE,
                    CAST(COALESCE(cur_WHNONSTK.WHNBCOST, 0) AS DECIMAL(12,4)) AS SUPPLIER_BASE_COST,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                    CAST('''' AS VARCHAR(16 OCTETS)) AS SUPPLIER_SUPERSEDED_LINE_CODE,
                    CAST(COALESCE(cur_CATEGORY.LOADDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_CREATE_DATE,
                    CAST('''' AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                    CAST(COALESCE(cur_CATEGORY.LOADTIME, ''00:00:00'') AS TIME) AS CATEGORY_TABLE_CREATE_TIME,
                    CAST(COALESCE(cur_CATEGORY.LOADUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_CREATE_USER_NAME,
                    CAST(COALESCE(cur_CATEGORY.UPDDATE, ''1900-01-01'') AS DATE) AS CATEGORY_TABLE_UPDATE_DATE,
                    CAST(COALESCE(cur_CATEGORY.UPDPGM, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                    CAST(COALESCE(cur_CATEGORY.UPDTIME, ''1900-01-01'') AS TIME) AS CATEGORY_TABLE_UPDATE_TIME,
                    CAST(COALESCE(cur_CATEGORY.UPDUSER, '''') AS VARCHAR(128 OCTETS)) AS CATEGORY_TABLE_UPDATE_USER_NAME,
                    CAST(COALESCE(HUB_LOAD_DIM_DATE_WHNUPDDATE.FULL_DATE, ''1900-01-01'') AS DATE) AS PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                    CAST(COALESCE(TRIM(cur_WHNONSTK.WHNUPDPGM), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                    CAST(COALESCE(cur_WHNONSTK.WHNUPDTIME ,''00:00:00'') AS TIME) AS PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                    CAST(COALESCE(TRIM(cur_WHNONSTK.WHNUPDUSER), '''') AS VARCHAR(128 OCTETS)) AS PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                    CAST(COALESCE(cur_WHNONSTK.WHNFJOBR,0) AS DECIMAL(12,4)) AS VIP_JOBBER,
                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_CORE,
                    CAST(0 AS DECIMAL(12,4)) AS WAREHOUSE_COST,
                    CAST(''N'' AS VARCHAR(1)) AS ETL_SOURCE_DATA_DELETED_FLAG,
                    --infaetl-11815 Add PRODUCT_LEVEL_CODE on the next line
                    ''PRODUCT'' AS PRODUCT_LEVEL_CODE,
                    '|| quote_literal(v_etl_source_table_4) || ' AS ETL_SOURCE_TABLE_NAME,
                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_CREATE_TIMESTAMP,
                    CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS ETL_UPDATE_TIMESTAMP, 
                    ' || v_stored_procedure_execution_id || ' AS ETL_MODIFIED_BY_JOB_ID,
                    ' || quote_literal(v_hub_procedure_name) || ' AS ETL_MODIFIED_BY_PROCESS
                    FROM EDW_STAGING.CUR_IVB_WHNONSTK AS cur_WHNONSTK
                    LEFT JOIN EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS ON cur_ECPARTS.LINE = cur_WHNONSTK.WHNLINE AND cur_ECPARTS.ITEMNUMBER = cur_WHNONSTK.WHNITEM
                    LEFT JOIN (SELECT cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER
                                FROM EDW_STAGING.CUR_PRT_AAIAVEND AS cur_AAIAVEND
                                GROUP BY cur_AAIAVEND.OREILLY_LINE, cur_AAIAVEND.KEY_ITEM, cur_AAIAVEND.OCAT_PART_NUMBER) AS cur_AAIAVEND 
                                ON cur_AAIAVEND.OREILLY_LINE = cur_WHNONSTK.WHNLINE AND cur_AAIAVEND.KEY_ITEM = cur_WHNONSTK.WHNITEM
                    JOIN '||v_staging_database_name||'.'||v_source_table_name||'  AS hub_DIM_PRODUCT ON hub_DIM_PRODUCT.LINE_CODE = TRIM(cur_WHNONSTK.WHNLINE) AND hub_DIM_PRODUCT.ITEM_CODE = TRIM(cur_WHNONSTK.WHNITEM) 
                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY AS cur_CATEGORY ON cur_CATEGORY.LINE = cur_WHNONSTK.WHNLINE AND cur_CATEGORY.ITEM = cur_WHNONSTK.WHNITEM
                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = cur_CATEGORY.CATEGORY 
                    LEFT JOIN EDW_STAGING.CUR_IVC_ICLINCTL AS cur_ICLINCTL ON cur_ICLINCTL.LINE = cur_WHNONSTK.WHNLINE AND cur_ICLINCTL.PLCD = cur_WHNONSTK.WHNPLCD AND cur_ICLINCTL.REGION = 0 
                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                       FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                       JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                       WHERE cur_DWEMP.DEPT# = 2) AS cur_DWASGN_DWEMP ON cur_DWASGN_DWEMP.LINE = cur_WHNONSTK.WHNLINE AND cur_DWASGN_DWEMP.PLCD = cur_WHNONSTK.WHNPLCD AND cur_DWASGN_DWEMP.SUBC = cur_WHNONSTK.WHNSUBC 
                    LEFT JOIN (SELECT cur_DWEMP.DMUSER, cur_DWEMP.DEUSRD, cur_DWEMP.EMP#, cur_DWASGN.LINE, cur_DWASGN.PLCD, cur_DWASGN.SUBC
                       FROM EDW_STAGING.CUR_PMG_DWASGN AS cur_DWASGN
                       JOIN EDW_STAGING.CUR_EMP_DWEMP AS cur_DWEMP ON cur_DWEMP.EMP# = cur_DWASGN.EMP# AND cur_DWEMP.DEPT# = cur_DWASGN.DEPT#
                       WHERE cur_DWEMP.DEPT# = 1) AS cur_REPLENISHMENT_ANALYST ON cur_REPLENISHMENT_ANALYST.LINE = cur_WHNONSTK.WHNLINE AND cur_REPLENISHMENT_ANALYST.PLCD = cur_WHNONSTK.WHNPLCD AND cur_REPLENISHMENT_ANALYST.SUBC = cur_WHNONSTK.WHNSUBC  
                    LEFT JOIN EDW_STAGING.CUR_PRT_CMISFILE AS cur_CMISFILE ON cur_CMISFILE.LLINE = cur_WHNONSTK.WHNLINE AND cur_CMISFILE.LPLCD = cur_WHNONSTK.WHNPLCD AND cur_CMISFILE.LSUBC = cur_WHNONSTK.WHNSUBC  
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_SUPPLIER AS HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID ON HUB_LOAD_DIM_SUPPLIER_SUPPLIER_ID.SUPPLIER_ID = cur_CMISFILE.LVACCT  
                    LEFT JOIN EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION ON cur_HAZLION.LINE_CODE = cur_WHNONSTK.WHNLINE AND cur_HAZLION.ITEM_NAME = cur_WHNONSTK.WHNITEM
                    LEFT JOIN (SELECT cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM, 
                            LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_OF_ORIGIN, 
                            LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS COUNTRY_NAME
                       FROM EDW_STAGING.CUR_PRT_EXPCORGN AS cur_EXPCORGN
                       LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_EXPCORGN.COUNTRY_OF_ORIGIN
                       GROUP BY cur_EXPCORGN.LINE, cur_EXPCORGN.ITEM) AS cur_EXPCORGN_EXPCORIGIN ON cur_EXPCORGN_EXPCORIGIN.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPCORGN_EXPCORIGIN.ITEM = cur_WHNONSTK.WHNITEM
                    LEFT JOIN (SELECT cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME, 
                            LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_OF_ORIGIN), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_CODE, 
                            LISTAGG(TRIM(cur_EXPCORIGIN.COUNTRY_NAME), '', '') WITHIN GROUP(ORDER BY cur_EXPCORIGIN.COUNTRY_OF_ORIGIN) AS BATTERY_COUNTRY_OF_ORIGIN_NAME
                       FROM EDW_STAGING.CUR_PRT_HAZLION AS cur_HAZLION
                       LEFT JOIN EDW_STAGING.CUR_PRT_EXPCORIGIN AS cur_EXPCORIGIN ON cur_EXPCORIGIN.COUNTRY_OF_ORIGIN = cur_HAZLION.COUNTRY_CODE
                       GROUP BY cur_HAZLION.LINE_CODE, cur_HAZLION.ITEM_NAME) AS cur_HAZLION_EXPCORIGIN ON cur_HAZLION_EXPCORIGIN.LINE_CODE = cur_WHNONSTK.WHNLINE AND cur_HAZLION_EXPCORIGIN.ITEM_NAME = cur_WHNONSTK.WHNITEM 
                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPECCN AS cur_EXPECCN ON cur_EXPECCN.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPECCN.ITEM = cur_WHNONSTK.WHNITEM
                    LEFT JOIN (SELECT cur_EXPHTS.LINE, cur_EXPHTS.ITEM, LISTAGG(TRIM(cur_EXPHTS.HTS_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPHTS.HTS_CODE) AS HTS_CODE
                       FROM EDW_STAGING.CUR_PRT_EXPHTS AS cur_EXPHTS
                       GROUP BY cur_EXPHTS.LINE, cur_EXPHTS.ITEM) AS cur_EXPHTS ON cur_EXPHTS.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPHTS.ITEM = cur_WHNONSTK.WHNITEM 
                    LEFT JOIN EDW_STAGING.CUR_PRT_EXPUSML AS cur_EXPUSML ON cur_EXPUSML.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPUSML.ITEM = cur_WHNONSTK.WHNITEM 
                    LEFT JOIN (SELECT cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM, LISTAGG(TRIM(cur_EXPSCDB.SCHEDULE_B_CODE), '', '') WITHIN GROUP(ORDER BY cur_EXPSCDB.SCHEDULE_B_CODE) AS SCHEDULE_B_CODE_LIST
                       FROM EDW_STAGING.CUR_PRT_EXPSCDB AS cur_EXPSCDB
                       GROUP BY cur_EXPSCDB.LINE, cur_EXPSCDB.ITEM) AS cur_EXPSCDB_LA ON cur_EXPSCDB_LA.LINE = cur_WHNONSTK.WHNLINE AND cur_EXPSCDB_LA.ITEM = cur_WHNONSTK.WHNITEM
                    LEFT JOIN EDW_STAGING.CUR_PUR_PMATRIX AS cur_PMATRIX ON cur_PMATRIX.LINE = cur_WHNONSTK.WHNLINE AND cur_PMATRIX.PLCD = cur_WHNONSTK.WHNPLCD AND cur_PMATRIX.SUBC = cur_WHNONSTK.WHNSUBC
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_WHNLODDATE ON HUB_LOAD_DIM_DATE_WHNLODDATE.FULL_DATE = cur_WHNONSTK.WHNLODDATE
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_WHNLSTCUPD ON HUB_LOAD_DIM_DATE_WHNLSTCUPD.FULL_DATE = cur_WHNONSTK.WHNLSTCUPD
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_WHNUPDDATE ON HUB_LOAD_DIM_DATE_WHNUPDDATE.FULL_DATE = cur_WHNONSTK.WHNUPDDATE
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = cur_CATEGORY.INACTIVEDATE
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID ON HUB_LOAD_DIM_EMPLOYEE_EMPLOYEE_ID.EMPLOYEE_NUMBER = BIGINT(cur_PMATRIX.RA_TMN)
                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_EMPLOYEE AS HUB_LOAD_DIM_EMPLOYEE_USERID_CODE ON HUB_LOAD_DIM_EMPLOYEE_USERID_CODE.EMPLOYEE_USERID_CODE = TRIM(cur_ICLINCTL.PRJCOORD) AND TRIM(cur_ICLINCTL.PRJCOORD) <> ''''
        LEFT JOIN ( SELECT cur_IMASTER.ILINE AS LINE, cur_IMASTER.IITEM# AS ITEM
                    FROM EDW_STAGING.CUR_IVB_IMASTER AS cur_IMASTER  
                    UNION 
                    SELECT cur_IMASTNS.LINE AS LINE, cur_IMASTNS.ITEM AS ITEM
                    FROM EDW_STAGING.CUR_IVB_IMASTNS AS cur_IMASTNS
                    UNION 
                    SELECT cur_ECPARTS.LINE AS LINE, cur_ECPARTS.ITEMNUMBER AS ITEM
                    FROM EDW_STAGING.CUR_IVB_ECPARTS AS cur_ECPARTS) AS cur_UNION ON cur_UNION.LINE = cur_WHNONSTK.WHNLINE AND cur_UNION.ITEM = cur_WHNONSTK.WHNITEM --427137 
         --INFAETL-11515 add the following 3 joins
         LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
         LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
         LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
        WHERE NVL(cur_UNION.LINE, '''') = ''''
                AND hub_DIM_PRODUCT.PRODUCT_ID IS NOT NULL
--                AND ( ' || quote_literal(v_job_execution_starttime) || ' <= cur_WHNONSTK.CREATE_TIMESTAMP OR cur_WHNONSTK.CREATE_TIMESTAMP IS NULL
--                    OR ' || quote_literal(v_job_execution_starttime) || '  <= cur_WHNONSTK.LOAD_TIMESTAMP OR cur_WHNONSTK.LOAD_TIMESTAMP IS NULL )
                   ) WITH DATA
                  DISTRIBUTE ON HASH("PRODUCT_ID")
                  IN TS_EDW
                  ORGANIZE BY COLUMN
--                  WITH UR
;';               

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                IF (V_SQL_CODE <> 0) THEN  --  Warning
                   SET V_SQL_OK = FALSE;
                   SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
            
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                   SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <WHNONSTK CTAS update table> '||
                       '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                       '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                       ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                       ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                       ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '20. WHNONSTK UPDATE preprocess table CTAS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

                --reset SQL status messages for the next calls
                SET V_SQL_CODE = 0;
                SET V_SQL_STATE = 0;
                SET V_SQL_MSG = 0;    
            END IF; -- V_SQL_OK

            IF V_SQL_OK THEN
               SET v_str_sql =   CLOB('UPDATE ') || v_staging_database_name || '.' || v_staging_table_name || ' AS tgt ' || 
                 'SET tgt.LINE_DESCRIPTION=src.LINE_DESCRIPTION,
                            tgt.ITEM_DESCRIPTION=src.ITEM_DESCRIPTION,
                            tgt.SEGMENT_NUMBER=src.SEGMENT_NUMBER,
                            tgt.SEGMENT_DESCRIPTION=src.SEGMENT_DESCRIPTION,
                            tgt.SUB_CATEGORY_NUMBER=src.SUB_CATEGORY_NUMBER,
                            tgt.SUB_CATEGORY_DESCRIPTION=src.SUB_CATEGORY_DESCRIPTION,
                            tgt.CATEGORY_NUMBER=src.CATEGORY_NUMBER,
                            tgt.CATEGORY_DESCRIPTION=src.CATEGORY_DESCRIPTION,
                            tgt.PRODUCT_LINE_CODE=src.PRODUCT_LINE_CODE,
                            tgt.SUB_CODE=src.SUB_CODE,
                            tgt.MANUFACTURE_ITEM_NUMBER_CODE=src.MANUFACTURE_ITEM_NUMBER_CODE,
                            tgt.SUPERSEDED_LINE_CODE=src.SUPERSEDED_LINE_CODE,
                            tgt.SUPERSEDED_ITEM_NUMBER_CODE=src.SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SORT_CONTROL_NUMBER=src.SORT_CONTROL_NUMBER,
                            tgt.POINT_OF_SALE_DESCRIPTION=src.POINT_OF_SALE_DESCRIPTION,
                            tgt.POPULARITY_CODE=src.POPULARITY_CODE,
                            tgt.POPULARITY_CODE_DESCRIPTION=src.POPULARITY_CODE_DESCRIPTION,
                            tgt.POPULARITY_TREND_CODE=src.POPULARITY_TREND_CODE,
                            tgt.POPULARITY_TREND_CODE_DESCRIPTION=src.POPULARITY_TREND_CODE_DESCRIPTION,
                            tgt.LINE_IS_MARINE_SPECIFIC_FLAG=src.LINE_IS_MARINE_SPECIFIC_FLAG,
                            tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE=src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                            tgt.LINE_IS_FLEET_SPECIFIC_CODE=src.LINE_IS_FLEET_SPECIFIC_CODE,
                            tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE=src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                            tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG=src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                            tgt.JOBBER_SUPPLIER_CODE=src.JOBBER_SUPPLIER_CODE,
                            tgt.JOBBER_UNIT_OF_MEASURE_CODE=src.JOBBER_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE=src.WAREHOUSE_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_SELL_QUANTITY=src.WAREHOUSE_SELL_QUANTITY,
                            tgt.RETAIL_WEIGHT=src.RETAIL_WEIGHT,
                            tgt.QUANTITY_PER_CAR=src.QUANTITY_PER_CAR,
                            tgt.CASE_QUANTITY=src.CASE_QUANTITY,
                            tgt.STANDARD_PACKAGE=src.STANDARD_PACKAGE,
                            tgt.PAINT_BODY_AND_EQUIPMENT_PRICE=src.PAINT_BODY_AND_EQUIPMENT_PRICE,
                            tgt.WAREHOUSE_JOBBER_PRICE=src.WAREHOUSE_JOBBER_PRICE,
                            tgt.WAREHOUSE_COST_WUM=src.WAREHOUSE_COST_WUM,
                            tgt.WAREHOUSE_CORE_WUM=src.WAREHOUSE_CORE_WUM,
                            tgt.OREILLY_COST_PRICE=src.OREILLY_COST_PRICE,
                            tgt.JOBBER_COST=src.JOBBER_COST,
                            tgt.JOBBER_CORE_PRICE=src.JOBBER_CORE_PRICE,
                            tgt.OUT_FRONT_MERCHANDISE_FLAG=src.OUT_FRONT_MERCHANDISE_FLAG,
                            tgt.ITEM_IS_TAXED_FLAG=src.ITEM_IS_TAXED_FLAG,
                            tgt.QUANTITY_ORDER_ITEM_FLAG=src.QUANTITY_ORDER_ITEM_FLAG,
                            tgt.JOBBER_DIVIDE_QUANTITY=src.JOBBER_DIVIDE_QUANTITY,
                            tgt.ITEM_DELETE_FLAG_RECORD_CODE=src.ITEM_DELETE_FLAG_RECORD_CODE,
                            tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE=src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                            tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE=src.PRIMARY_UNIVERSAL_PRODUCT_CODE,
                            tgt.WARRANTY_CODE=src.WARRANTY_CODE,
                            tgt.WARRANTY_CODE_DESCRIPTION=src.WARRANTY_CODE_DESCRIPTION,
                            tgt.INVOICE_COST_WUM_INVOICE_COST=src.INVOICE_COST_WUM_INVOICE_COST,
                            tgt.INVOICE_CORE_WUM_CORE_COST=src.INVOICE_CORE_WUM_CORE_COST,
                            tgt.IS_CONSIGNMENT_ITEM_FLAG=src.IS_CONSIGNMENT_ITEM_FLAG,
                            tgt.WAREHOUSE_JOBBER_CORE_PRICE=src.WAREHOUSE_JOBBER_CORE_PRICE,
                            tgt.ACQUISITION_FIELD_1_CODE=src.ACQUISITION_FIELD_1_CODE,
                            tgt.ACQUISITION_FIELD_2_CODE=src.ACQUISITION_FIELD_2_CODE,
                            tgt.BUY_MULTIPLE=src.BUY_MULTIPLE,
                            tgt.BUY_MULTIPLE_CODE=src.BUY_MULTIPLE_CODE,
                            tgt.BUY_MULTIPLE_CODE_DESCRIPTION=src.BUY_MULTIPLE_CODE_DESCRIPTION,
                            tgt.SUPPLIER_CONVERSION_FACTOR_CODE=src.SUPPLIER_CONVERSION_FACTOR_CODE,
                            tgt.SUPPLIER_CONVERSION_QUANTITY=src.SUPPLIER_CONVERSION_QUANTITY,
                            tgt.SUPPLIER_UNIT_OF_MEASURE_CODE=src.SUPPLIER_UNIT_OF_MEASURE_CODE,
                            tgt.UNIT_OF_MEASURE_AMOUNT=src.UNIT_OF_MEASURE_AMOUNT,
                            tgt.UNIT_OF_MEASURE_QUANTITY=src.UNIT_OF_MEASURE_QUANTITY,
                            tgt.UNIT_OF_MEASURE_DESCRIPTION=src.UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_CODE=src.TAX_CLASSIFICATION_CODE,
                            tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION=src.TAX_CLASSIFICATION_CODE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE=src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                            tgt.DISTRIBUTION_CENTER_PICK_LENGTH=src.DISTRIBUTION_CENTER_PICK_LENGTH,
                            tgt.DISTRIBUTION_CENTER_PICK_WIDTH=src.DISTRIBUTION_CENTER_PICK_WIDTH,
                            tgt.DISTRIBUTION_CENTER_PICK_HEIGHT=src.DISTRIBUTION_CENTER_PICK_HEIGHT,
                            tgt.DISTRIBUTION_CENTER_PICK_WEIGHT=src.DISTRIBUTION_CENTER_PICK_WEIGHT,
                            tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE=src.PICK_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASE_QUANTITY_CODE=src.CASE_QUANTITY_CODE,
                            tgt.CASE_LENGTH=src.CASE_LENGTH,
                            tgt.CASE_WIDTH=src.CASE_WIDTH,
                            tgt.CASE_HEIGHT=src.CASE_HEIGHT,
                            tgt.CASE_WEIGHT=src.CASE_WEIGHT,
                            tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE=src.CASE_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASES_PER_PALLET=src.CASES_PER_PALLET,
                            tgt.CASES_PER_PALLET_LAYER=src.CASES_PER_PALLET_LAYER,
                            tgt.PALLET_LENGTH=src.PALLET_LENGTH,
                            tgt.PALLET_WIDTH=src.PALLET_WIDTH,
                            tgt.PALLET_HEIGHT=src.PALLET_HEIGHT,
                            tgt.PALLET_WEIGHT=src.PALLET_WEIGHT,
                            tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE=src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.SHIPMENT_CLASS_CODE=src.SHIPMENT_CLASS_CODE,
                            tgt.DOT_CLASS_NUMBER=src.DOT_CLASS_NUMBER,
                            tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER=src.DOT_CLASS_FOR_MSDS_ID_NUMBER,
                            tgt.CONTAINER_DESCRIPTION=src.CONTAINER_DESCRIPTION,
                            tgt.KEEP_FROM_FREEZING_FLAG=src.KEEP_FROM_FREEZING_FLAG,
                            tgt.FLIGHT_RESTRICTED_FLAG=src.FLIGHT_RESTRICTED_FLAG,
                            tgt.ALLOW_NEW_RETURNS_FLAG=src.ALLOW_NEW_RETURNS_FLAG,
                            tgt.ALLOW_CORE_RETURNS_FLAG=src.ALLOW_CORE_RETURNS_FLAG,
                            tgt.ALLOW_WARRANTY_RETURNS_FLAG=src.ALLOW_WARRANTY_RETURNS_FLAG,
                            tgt.ALLOW_RECALL_RETURNS_FLAG=src.ALLOW_RECALL_RETURNS_FLAG,
                            tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG=src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                            tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG=src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                            tgt.HAZARDOUS_UPDATE_DATE=src.HAZARDOUS_UPDATE_DATE,
                            tgt.PIECE_LENGTH=src.PIECE_LENGTH,
                            tgt.PIECE_WIDTH=src.PIECE_WIDTH,
                            tgt.PIECE_HEIGHT=src.PIECE_HEIGHT,
                            tgt.PIECE_WEIGHT=src.PIECE_WEIGHT,
                            tgt.PIECES_INNER_PACK=src.PIECES_INNER_PACK,
                            tgt.IN_CATALOG_CODE=src.IN_CATALOG_CODE,
                            tgt.IN_CATALOG_CODE_DESCRIPTION=src.IN_CATALOG_CODE_DESCRIPTION,
                            tgt.ALLOW_SPECIAL_ORDER_FLAG=src.ALLOW_SPECIAL_ORDER_FLAG,
                            tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG=src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                            tgt.SUPPLIER_LIFE_CYCLE_CODE=src.SUPPLIER_LIFE_CYCLE_CODE,
                            tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE=src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                            tgt.LONG_DESCRIPTION=src.LONG_DESCRIPTION,
                            tgt.ELECTRONIC_WASTE_FLAG=src.ELECTRONIC_WASTE_FLAG,
                            tgt.STORE_MINIMUM_SALE_QUANTITY=src.STORE_MINIMUM_SALE_QUANTITY,
                            tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE=src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                            tgt.MAXIMUM_CAR_QUANTITY=src.MAXIMUM_CAR_QUANTITY,
                            tgt.MINIMUM_CAR_QUANTITY=src.MINIMUM_CAR_QUANTITY,
                            tgt.ESSENTIAL_HARD_PART_CODE=src.ESSENTIAL_HARD_PART_CODE,
                            tgt.INNER_PACK_CODE=src.INNER_PACK_CODE,
                            tgt.INNER_PACK_QUANTITY=src.INNER_PACK_QUANTITY,
                            tgt.INNER_PACK_LENGTH=src.INNER_PACK_LENGTH,
                            tgt.INNER_PACK_WIDTH=src.INNER_PACK_WIDTH,
                            tgt.INNER_PACK_HEIGHT=src.INNER_PACK_HEIGHT,
                            tgt.INNER_PACK_WEIGHT=src.INNER_PACK_WEIGHT,
                            tgt.BRAND_CODE=src.BRAND_CODE,
                            tgt.PART_NUMBER_CODE=src.PART_NUMBER_CODE,
                            tgt.PART_NUMBER_DISPLAY_CODE=src.PART_NUMBER_DISPLAY_CODE,
                            tgt.PART_NUMBER_DESCRIPTION=src.PART_NUMBER_DESCRIPTION,
                            tgt.SPANISH_PART_NUMBER_DESCRIPTION=src.SPANISH_PART_NUMBER_DESCRIPTION,
                            tgt.SUGGESTED_ORDER_QUANTITY=src.SUGGESTED_ORDER_QUANTITY,
                            tgt.BRAND_TYPE_NAME=src.BRAND_TYPE_NAME,
                            tgt.LOCATION_TYPE_NAME=src.LOCATION_TYPE_NAME,
                            tgt.MANUFACTURING_CODE_DESCRIPTION=src.MANUFACTURING_CODE_DESCRIPTION,
                            tgt.QUALITY_GRADE_CODE=src.QUALITY_GRADE_CODE,
                            tgt.PRIMARY_APPLICATION_NAME=src.PRIMARY_APPLICATION_NAME,
                            --INFAETL-11515 begin change
                            tgt.CATEGORY_MANAGER_NAME=src.CATEGORY_MANAGER_NAME,
                            tgt.CATEGORY_MANAGER_NUMBER=src.CATEGORY_MANAGER_NUMBER,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                            tgt.CATEGORY_DIRECTOR_NAME=src.CATEGORY_DIRECTOR_NAME,
                            tgt.CATEGORY_DIRECTOR_NUMBER=src.CATEGORY_DIRECTOR_NUMBER,
                            tgt.CATEGORY_VP_NAME=src.CATEGORY_VP_NAME,
                            tgt.CATEGORY_VP_NUMBER=src.CATEGORY_VP_NUMBER,
                            --INFAETL-11515 end change
                            tgt.INACTIVATED_DATE=src.INACTIVATED_DATE,
                            tgt.REVIEW_CODE=src.REVIEW_CODE,
                            tgt.STOCKING_LINE_FLAG=src.STOCKING_LINE_FLAG,
                            tgt.OIL_LINE_FLAG=src.OIL_LINE_FLAG,
                            tgt.SPECIAL_REQUIREMENTS_LABEL=src.SPECIAL_REQUIREMENTS_LABEL,
                            tgt.SUPPLIER_ACCOUNT_NUMBER=src.SUPPLIER_ACCOUNT_NUMBER,
                            tgt.SUPPLIER_NUMBER=src.SUPPLIER_NUMBER,
                            tgt.SUPPLIER_ID=src.SUPPLIER_ID,
                            tgt.BRAND_DESCRIPTION=src.BRAND_DESCRIPTION,
                            tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER=src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                            tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER=src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                            tgt.SALES_AREA_NAME=src.SALES_AREA_NAME,
                            tgt.TEAM_NAME=src.TEAM_NAME,
                            tgt.CATEGORY_NAME=src.CATEGORY_NAME,
                            tgt.REPLENISHMENT_ANALYST_NAME=src.REPLENISHMENT_ANALYST_NAME,
                            tgt.REPLENISHMENT_ANALYST_NUMBER=src.REPLENISHMENT_ANALYST_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER=src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID=src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                            tgt.SALES_AREA_NAME_SORT_NUMBER=src.SALES_AREA_NAME_SORT_NUMBER,
                            tgt.TEAM_NAME_SORT_NUMBER=src.TEAM_NAME_SORT_NUMBER,
                            tgt.BUYER_CODE=src.BUYER_CODE,
                            tgt.BUYER_NAME=src.BUYER_NAME,
                            tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE=src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                            tgt.BATTERY_PACKING_INSTRUCTIONS_CODE=src.BATTERY_PACKING_INSTRUCTIONS_CODE,
                            tgt.BATTERY_MANUFACTURING_NAME=src.BATTERY_MANUFACTURING_NAME,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1=src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2=src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3=src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4=src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                            tgt.BATTERY_MANUFACTURING_CITY_NAME=src.BATTERY_MANUFACTURING_CITY_NAME,
                            tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME=src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                            tgt.BATTERY_MANUFACTURING_STATE_NAME=src.BATTERY_MANUFACTURING_STATE_NAME,
                            tgt.BATTERY_MANUFACTURING_ZIP_CODE=src.BATTERY_MANUFACTURING_ZIP_CODE,
                            tgt.BATTERY_MANUFACTURING_COUNTRY_CODE=src.BATTERY_MANUFACTURING_COUNTRY_CODE,
                            tgt.BATTERY_PHONE_NUMBER_CODE=src.BATTERY_PHONE_NUMBER_CODE,
                            tgt.BATTERY_WEIGHT_IN_GRAMS=src.BATTERY_WEIGHT_IN_GRAMS,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL=src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY=src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                            tgt.BATTERY_WATT_HOURS_PER_CELL=src.BATTERY_WATT_HOURS_PER_CELL,
                            tgt.BATTERY_WATT_HOURS_PER_BATTERY=src.BATTERY_WATT_HOURS_PER_BATTERY,
                            tgt.BATTERY_CELLS_NUMBER=src.BATTERY_CELLS_NUMBER,
                            tgt.BATTERIES_PER_PACKAGE_NUMBER=src.BATTERIES_PER_PACKAGE_NUMBER,
                            tgt.BATTERIES_IN_EQUIPMENT_NUMBER=src.BATTERIES_IN_EQUIPMENT_NUMBER,
                            tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG=src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                            tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG=src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                            tgt.COUNTRY_OF_ORIGIN_NAME_LIST=src.COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST=src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                            tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST=src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                            tgt.SCHEDULE_B_CODE_LIST=src.SCHEDULE_B_CODE_LIST,
                            tgt.UNITED_STATES_MUNITIONS_LIST_CODE=src.UNITED_STATES_MUNITIONS_LIST_CODE,
                            tgt.PROJECT_COORDINATOR_ID_CODE=src.PROJECT_COORDINATOR_ID_CODE,
                            tgt.PROJECT_COORDINATOR_EMPLOYEE_ID=src.PROJECT_COORDINATOR_EMPLOYEE_ID,
                            tgt.STOCK_ADJUSTMENT_MONTH_NUMBER=src.STOCK_ADJUSTMENT_MONTH_NUMBER,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.ALL_IN_COST=src.ALL_IN_COST,
                            tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE=src.CANCEL_OR_BACKORDER_REMAINDER_CODE,
                            tgt.CASE_LOT_DISCOUNT=src.CASE_LOT_DISCOUNT,
                            tgt.COMPANY_NUMBER=src.COMPANY_NUMBER,
                            tgt.CONVENIENCE_PACK_QUANTITY=src.CONVENIENCE_PACK_QUANTITY,
                            tgt.CONVENIENCE_PACK_DESCRIPTION=src.CONVENIENCE_PACK_DESCRIPTION,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE=src.PRODUCT_SOURCE_TABLE_CREATION_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME=src.PRODUCT_SOURCE_TABLE_CREATION_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE=src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                            tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE=src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                            tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE=src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                            tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE=src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                            tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG=src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                            tgt.HAZARDOUS_UPDATE_PROGRAM_NAME=src.HAZARDOUS_UPDATE_PROGRAM_NAME,
                            tgt.HAZARDOUS_UPDATE_TIME=src.HAZARDOUS_UPDATE_TIME,
                            tgt.HAZARDOUS_UPDATE_USER_NAME=src.HAZARDOUS_UPDATE_USER_NAME,
                            tgt.LIST_PRICE=src.LIST_PRICE,
                            tgt.LOW_USER_PRICE=src.LOW_USER_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE=src.MINIMUM_ADVERTISED_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE=src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                            tgt.MINIMUM_SELL_QUANTITY=src.MINIMUM_SELL_QUANTITY,
                            tgt.PACKAGE_SIZE_DESCRIPTION=src.PACKAGE_SIZE_DESCRIPTION,
                            tgt.PERCENTAGE_OF_SUPPLIER_FUNDING=src.PERCENTAGE_OF_SUPPLIER_FUNDING,
                            tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG=src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                            tgt.PRICING_COST=src.PRICING_COST,
                            tgt.PROFESSIONAL_PRICE=src.PROFESSIONAL_PRICE,
                            tgt.RETAIL_CORE=src.RETAIL_CORE,
                            tgt.RETAIL_HEIGHT=src.RETAIL_HEIGHT,
                            tgt.RETAIL_LENGTH=src.RETAIL_LENGTH,
                            tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION=src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.RETAIL_WIDTH=src.RETAIL_WIDTH,
                            tgt.SALES_PACK_CODE=src.SALES_PACK_CODE,
                            tgt.SCORE_FLAG=src.SCORE_FLAG,
                            tgt.SHIPPING_DIMENSIONS_CODE=src.SHIPPING_DIMENSIONS_CODE,
                            tgt.SUPPLIER_BASE_COST=src.SUPPLIER_BASE_COST,
                            tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE=src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SUPPLIER_SUPERSEDED_LINE_CODE=src.SUPPLIER_SUPERSEDED_LINE_CODE,
                            tgt.CATEGORY_TABLE_CREATE_DATE=src.CATEGORY_TABLE_CREATE_DATE,
                            tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME=src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_CREATE_TIME=src.CATEGORY_TABLE_CREATE_TIME,
                            tgt.CATEGORY_TABLE_CREATE_USER_NAME=src.CATEGORY_TABLE_CREATE_USER_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_DATE=src.CATEGORY_TABLE_UPDATE_DATE,
                            tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME=src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_TIME=src.CATEGORY_TABLE_UPDATE_TIME,
                            tgt.CATEGORY_TABLE_UPDATE_USER_NAME=src.CATEGORY_TABLE_UPDATE_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                            tgt.VIP_JOBBER=src.VIP_JOBBER,
                            tgt.WAREHOUSE_CORE=src.WAREHOUSE_CORE,
                            tgt.WAREHOUSE_COST=src.WAREHOUSE_COST,
                            --INFAETL-11815 added the next line 
                            tgt.PRODUCT_LEVEL_CODE = src.PRODUCT_LEVEL_CODE,
                            tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
                            tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
                            --tgt.ETL_CREATE_TIMESTAMP=src.ETL_CREATE_TIMESTAMP
                            tgt.ETL_UPDATE_TIMESTAMP= CURRENT_TIMESTAMP-CURRENT_TIMEZONE,
                            tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
                            tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
                 from '|| v_staging_database_name || '.SESSION_TMP_WHNONSTK_UPDATE_SOURCE SRC 
                  WHERE tgt.product_id = src.product_id
                  AND (     COALESCE(tgt.LINE_DESCRIPTION,''A'') <> COALESCE(src.LINE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ITEM_DESCRIPTION,''A'') <> COALESCE(src.ITEM_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SEGMENT_NUMBER,0) <> COALESCE(src.SEGMENT_NUMBER,0)
                    OR COALESCE(tgt.SEGMENT_DESCRIPTION,''A'') <> COALESCE(src.SEGMENT_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUB_CATEGORY_NUMBER,0) <> COALESCE(src.SUB_CATEGORY_NUMBER,0)
                    OR COALESCE(tgt.SUB_CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.SUB_CATEGORY_DESCRIPTION,''A'')
                    OR COALESCE(tgt.CATEGORY_NUMBER,0) <> COALESCE(src.CATEGORY_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.CATEGORY_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PRODUCT_LINE_CODE,''A'') <> COALESCE(src.PRODUCT_LINE_CODE,''A'')
                    OR COALESCE(tgt.SUB_CODE,''A'') <> COALESCE(src.SUB_CODE,''A'')
                    OR COALESCE(tgt.MANUFACTURE_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.MANUFACTURE_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPERSEDED_LINE_CODE,''A'')
                    OR COALESCE(tgt.SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SORT_CONTROL_NUMBER,0) <> COALESCE(src.SORT_CONTROL_NUMBER,0)
                    OR COALESCE(tgt.POINT_OF_SALE_DESCRIPTION,''A'') <> COALESCE(src.POINT_OF_SALE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.POPULARITY_CODE,''A'') <> COALESCE(src.POPULARITY_CODE,''A'')
                    OR COALESCE(tgt.POPULARITY_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.POPULARITY_TREND_CODE,''A'') <> COALESCE(src.POPULARITY_TREND_CODE,''A'')
                    OR COALESCE(tgt.POPULARITY_TREND_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_TREND_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.LINE_IS_MARINE_SPECIFIC_FLAG,''A'') <> COALESCE(src.LINE_IS_MARINE_SPECIFIC_FLAG,''A'')
                    OR COALESCE(tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_FLEET_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_FLEET_SPECIFIC_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'')
                    OR COALESCE(tgt.JOBBER_SUPPLIER_CODE,''A'') <> COALESCE(src.JOBBER_SUPPLIER_CODE,''A'')
                    OR COALESCE(tgt.JOBBER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.JOBBER_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.WAREHOUSE_SELL_QUANTITY,0) <> COALESCE(src.WAREHOUSE_SELL_QUANTITY,0)
                    OR COALESCE(tgt.RETAIL_WEIGHT,0) <> COALESCE(src.RETAIL_WEIGHT,0)
                    OR COALESCE(tgt.QUANTITY_PER_CAR,0) <> COALESCE(src.QUANTITY_PER_CAR,0)
                    OR COALESCE(tgt.CASE_QUANTITY,0) <> COALESCE(src.CASE_QUANTITY,0)
                    OR COALESCE(tgt.STANDARD_PACKAGE,0) <> COALESCE(src.STANDARD_PACKAGE,0)
                    OR COALESCE(tgt.PAINT_BODY_AND_EQUIPMENT_PRICE,0) <> COALESCE(src.PAINT_BODY_AND_EQUIPMENT_PRICE,0)
                    OR COALESCE(tgt.WAREHOUSE_JOBBER_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_PRICE,0)
                    OR COALESCE(tgt.WAREHOUSE_COST_WUM,0) <> COALESCE(src.WAREHOUSE_COST_WUM,0)
                    OR COALESCE(tgt.WAREHOUSE_CORE_WUM,0) <> COALESCE(src.WAREHOUSE_CORE_WUM,0)
                    OR COALESCE(tgt.OREILLY_COST_PRICE,0) <> COALESCE(src.OREILLY_COST_PRICE,0)
                    OR COALESCE(tgt.JOBBER_COST,0) <> COALESCE(src.JOBBER_COST,0)
                    OR COALESCE(tgt.JOBBER_CORE_PRICE,0) <> COALESCE(src.JOBBER_CORE_PRICE,0)
                    OR COALESCE(tgt.OUT_FRONT_MERCHANDISE_FLAG,''A'') <> COALESCE(src.OUT_FRONT_MERCHANDISE_FLAG,''A'')
                    OR COALESCE(tgt.ITEM_IS_TAXED_FLAG,''A'') <> COALESCE(src.ITEM_IS_TAXED_FLAG,''A'')
                    OR COALESCE(tgt.QUANTITY_ORDER_ITEM_FLAG,''A'') <> COALESCE(src.QUANTITY_ORDER_ITEM_FLAG,''A'')
                    OR COALESCE(tgt.JOBBER_DIVIDE_QUANTITY,0) <> COALESCE(src.JOBBER_DIVIDE_QUANTITY,0)
                    OR COALESCE(tgt.ITEM_DELETE_FLAG_RECORD_CODE,''A'') <> COALESCE(src.ITEM_DELETE_FLAG_RECORD_CODE,''A'')
                    OR COALESCE(tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'') <> COALESCE(src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'')
                    OR COALESCE(tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'') <> COALESCE(src.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'')
                    OR COALESCE(tgt.WARRANTY_CODE,''A'') <> COALESCE(src.WARRANTY_CODE,''A'')
                    OR COALESCE(tgt.WARRANTY_CODE_DESCRIPTION,''A'') <> COALESCE(src.WARRANTY_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.INVOICE_COST_WUM_INVOICE_COST,0) <> COALESCE(src.INVOICE_COST_WUM_INVOICE_COST,0)
                    OR COALESCE(tgt.INVOICE_CORE_WUM_CORE_COST,0) <> COALESCE(src.INVOICE_CORE_WUM_CORE_COST,0)
                    OR COALESCE(tgt.IS_CONSIGNMENT_ITEM_FLAG,''A'') <> COALESCE(src.IS_CONSIGNMENT_ITEM_FLAG,''A'')
                    OR COALESCE(tgt.WAREHOUSE_JOBBER_CORE_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_CORE_PRICE,0)
                    OR COALESCE(tgt.ACQUISITION_FIELD_1_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_1_CODE,''A'')
                    OR COALESCE(tgt.ACQUISITION_FIELD_2_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_2_CODE,''A'')
                    OR COALESCE(tgt.BUY_MULTIPLE,0) <> COALESCE(src.BUY_MULTIPLE,0)
                    OR COALESCE(tgt.BUY_MULTIPLE_CODE,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE,''A'')
                    OR COALESCE(tgt.BUY_MULTIPLE_CODE_DESCRIPTION,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUPPLIER_CONVERSION_FACTOR_CODE,''A'') <> COALESCE(src.SUPPLIER_CONVERSION_FACTOR_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_CONVERSION_QUANTITY,0) <> COALESCE(src.SUPPLIER_CONVERSION_QUANTITY,0)
                    OR COALESCE(tgt.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.UNIT_OF_MEASURE_AMOUNT,0) <> COALESCE(src.UNIT_OF_MEASURE_AMOUNT,0)
                    OR COALESCE(tgt.UNIT_OF_MEASURE_QUANTITY,0) <> COALESCE(src.UNIT_OF_MEASURE_QUANTITY,0)
                    OR COALESCE(tgt.UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.UNIT_OF_MEASURE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_LENGTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_LENGTH,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WIDTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WIDTH,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_HEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_HEIGHT,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WEIGHT,0)
                    OR COALESCE(tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.CASE_QUANTITY_CODE,''A'') <> COALESCE(src.CASE_QUANTITY_CODE,''A'')
                    OR COALESCE(tgt.CASE_LENGTH,0) <> COALESCE(src.CASE_LENGTH,0)
                    OR COALESCE(tgt.CASE_WIDTH,0) <> COALESCE(src.CASE_WIDTH,0)
                    OR COALESCE(tgt.CASE_HEIGHT,0) <> COALESCE(src.CASE_HEIGHT,0)
                    OR COALESCE(tgt.CASE_WEIGHT,0) <> COALESCE(src.CASE_WEIGHT,0)
                    OR COALESCE(tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.CASES_PER_PALLET,0) <> COALESCE(src.CASES_PER_PALLET,0)
                    OR COALESCE(tgt.CASES_PER_PALLET_LAYER,0) <> COALESCE(src.CASES_PER_PALLET_LAYER,0)
                    OR COALESCE(tgt.PALLET_LENGTH,0) <> COALESCE(src.PALLET_LENGTH,0)
                    OR COALESCE(tgt.PALLET_WIDTH,0) <> COALESCE(src.PALLET_WIDTH,0)
                    OR COALESCE(tgt.PALLET_HEIGHT,0) <> COALESCE(src.PALLET_HEIGHT,0)
                    OR COALESCE(tgt.PALLET_WEIGHT,0) <> COALESCE(src.PALLET_WEIGHT,0)
                    OR COALESCE(tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.SHIPMENT_CLASS_CODE,''A'') <> COALESCE(src.SHIPMENT_CLASS_CODE,''A'')
                    OR COALESCE(tgt.DOT_CLASS_NUMBER,0) <> COALESCE(src.DOT_CLASS_NUMBER,0)
                    OR COALESCE(tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER,0) <> COALESCE(src.DOT_CLASS_FOR_MSDS_ID_NUMBER,0)
                    OR COALESCE(tgt.CONTAINER_DESCRIPTION,''A'') <> COALESCE(src.CONTAINER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.KEEP_FROM_FREEZING_FLAG,''A'') <> COALESCE(src.KEEP_FROM_FREEZING_FLAG,''A'')
                    OR COALESCE(tgt.FLIGHT_RESTRICTED_FLAG,''A'') <> COALESCE(src.FLIGHT_RESTRICTED_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_NEW_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_NEW_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_CORE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_CORE_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_WARRANTY_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_WARRANTY_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_RECALL_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_RECALL_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.HAZARDOUS_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PIECE_LENGTH,0) <> COALESCE(src.PIECE_LENGTH,0)
                    OR COALESCE(tgt.PIECE_WIDTH,0) <> COALESCE(src.PIECE_WIDTH,0)
                    OR COALESCE(tgt.PIECE_HEIGHT,0) <> COALESCE(src.PIECE_HEIGHT,0)
                    OR COALESCE(tgt.PIECE_WEIGHT,0) <> COALESCE(src.PIECE_WEIGHT,0)
                    OR COALESCE(tgt.PIECES_INNER_PACK,0) <> COALESCE(src.PIECES_INNER_PACK,0)
                    OR COALESCE(tgt.IN_CATALOG_CODE,''A'') <> COALESCE(src.IN_CATALOG_CODE,''A'')
                    OR COALESCE(tgt.IN_CATALOG_CODE_DESCRIPTION,''A'') <> COALESCE(src.IN_CATALOG_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ALLOW_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ALLOW_SPECIAL_ORDER_FLAG,''A'')
                    OR COALESCE(tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'')
                    OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CODE,''A'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.LONG_DESCRIPTION,''A'') <> COALESCE(src.LONG_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ELECTRONIC_WASTE_FLAG,''A'') <> COALESCE(src.ELECTRONIC_WASTE_FLAG,''A'')
                    OR COALESCE(tgt.STORE_MINIMUM_SALE_QUANTITY,0) <> COALESCE(src.STORE_MINIMUM_SALE_QUANTITY,0)
                    OR COALESCE(tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0) <> COALESCE(src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0)
                    OR COALESCE(tgt.MAXIMUM_CAR_QUANTITY,0) <> COALESCE(src.MAXIMUM_CAR_QUANTITY,0)
                    OR COALESCE(tgt.MINIMUM_CAR_QUANTITY,0) <> COALESCE(src.MINIMUM_CAR_QUANTITY,0)
                    OR COALESCE(tgt.ESSENTIAL_HARD_PART_CODE,''A'') <> COALESCE(src.ESSENTIAL_HARD_PART_CODE,''A'')
                    OR COALESCE(tgt.INNER_PACK_CODE,''A'') <> COALESCE(src.INNER_PACK_CODE,''A'')
                    OR COALESCE(tgt.INNER_PACK_QUANTITY,0) <> COALESCE(src.INNER_PACK_QUANTITY,0)
                    OR COALESCE(tgt.INNER_PACK_LENGTH,0) <> COALESCE(src.INNER_PACK_LENGTH,0)
                    OR COALESCE(tgt.INNER_PACK_WIDTH,0) <> COALESCE(src.INNER_PACK_WIDTH,0)
                    OR COALESCE(tgt.INNER_PACK_HEIGHT,0) <> COALESCE(src.INNER_PACK_HEIGHT,0)
                    OR COALESCE(tgt.INNER_PACK_WEIGHT,0) <> COALESCE(src.INNER_PACK_WEIGHT,0)
                    OR COALESCE(tgt.BRAND_CODE,''A'') <> COALESCE(src.BRAND_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_CODE,''A'') <> COALESCE(src.PART_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_DISPLAY_CODE,''A'') <> COALESCE(src.PART_NUMBER_DISPLAY_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.PART_NUMBER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SPANISH_PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.SPANISH_PART_NUMBER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUGGESTED_ORDER_QUANTITY,0) <> COALESCE(src.SUGGESTED_ORDER_QUANTITY,0)
                    OR COALESCE(tgt.BRAND_TYPE_NAME,''A'') <> COALESCE(src.BRAND_TYPE_NAME,''A'')
                    OR COALESCE(tgt.LOCATION_TYPE_NAME,''A'') <> COALESCE(src.LOCATION_TYPE_NAME,''A'')
                    OR COALESCE(tgt.MANUFACTURING_CODE_DESCRIPTION,''A'') <> COALESCE(src.MANUFACTURING_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.QUALITY_GRADE_CODE,''A'') <> COALESCE(src.QUALITY_GRADE_CODE,''A'')
                    OR COALESCE(tgt.PRIMARY_APPLICATION_NAME,''A'') <> COALESCE(src.PRIMARY_APPLICATION_NAME,''A'')
                    --INFAETL-11515 begin change
                    OR COALESCE(tgt.CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.CATEGORY_MANAGER_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.CATEGORY_MANAGER_NUMBER,0)
                    OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'')
                    OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NAME,''A'') <> COALESCE(src.CATEGORY_DIRECTOR_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NUMBER,0) <> COALESCE(src.CATEGORY_DIRECTOR_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_VP_NAME,''A'') <> COALESCE(src.CATEGORY_VP_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_VP_NUMBER,0) <> COALESCE(src.CATEGORY_VP_NUMBER,0)
                    --INFAETL-11515 end change
                    OR COALESCE(tgt.INACTIVATED_DATE,''1900-01-01'') <> COALESCE(src.INACTIVATED_DATE,''1900-01-01'')
                    OR COALESCE(tgt.REVIEW_CODE,''A'') <> COALESCE(src.REVIEW_CODE,''A'')
                    OR COALESCE(tgt.STOCKING_LINE_FLAG,''A'') <> COALESCE(src.STOCKING_LINE_FLAG,''A'')
                    OR COALESCE(tgt.OIL_LINE_FLAG,''A'') <> COALESCE(src.OIL_LINE_FLAG,''A'')
                    OR COALESCE(tgt.SPECIAL_REQUIREMENTS_LABEL,''A'') <> COALESCE(src.SPECIAL_REQUIREMENTS_LABEL,''A'')
                    OR COALESCE(tgt.SUPPLIER_ACCOUNT_NUMBER,0) <> COALESCE(src.SUPPLIER_ACCOUNT_NUMBER,0)
                    OR COALESCE(tgt.SUPPLIER_NUMBER,0) <> COALESCE(src.SUPPLIER_NUMBER,0)
                    OR COALESCE(tgt.SUPPLIER_ID,0) <> COALESCE(src.SUPPLIER_ID,0)
                    OR COALESCE(tgt.BRAND_DESCRIPTION,''A'') <> COALESCE(src.BRAND_DESCRIPTION,''A'')
                    OR COALESCE(tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0) <> COALESCE(src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0)
                    OR COALESCE(tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0) <> COALESCE(src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0)
                    OR COALESCE(tgt.SALES_AREA_NAME,''A'') <> COALESCE(src.SALES_AREA_NAME,''A'')
                    OR COALESCE(tgt.TEAM_NAME,''A'') <> COALESCE(src.TEAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_NAME,''A'') <> COALESCE(src.CATEGORY_NAME,''A'')
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_NAME,''A'') <> COALESCE(src.REPLENISHMENT_ANALYST_NAME,''A'')
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_NUMBER,0)
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0)
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0)
                    OR COALESCE(tgt.SALES_AREA_NAME_SORT_NUMBER,0) <> COALESCE(src.SALES_AREA_NAME_SORT_NUMBER,0)
                    OR COALESCE(tgt.TEAM_NAME_SORT_NUMBER,0) <> COALESCE(src.TEAM_NAME_SORT_NUMBER,0)
                    OR COALESCE(tgt.BUYER_CODE,''A'') <> COALESCE(src.BUYER_CODE,''A'')
                    OR COALESCE(tgt.BUYER_NAME,''A'') <> COALESCE(src.BUYER_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'') <> COALESCE(src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'') <> COALESCE(src.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_CITY_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_CITY_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_STATE_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_STATE_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ZIP_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ZIP_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_PHONE_NUMBER_CODE,''A'') <> COALESCE(src.BATTERY_PHONE_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_WEIGHT_IN_GRAMS,0) <> COALESCE(src.BATTERY_WEIGHT_IN_GRAMS,0)
                    OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0)
                    OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0)
                    OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_CELL,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_CELL,0)
                    OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_BATTERY,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_BATTERY,0)
                    OR COALESCE(tgt.BATTERY_CELLS_NUMBER,0) <> COALESCE(src.BATTERY_CELLS_NUMBER,0)
                    OR COALESCE(tgt.BATTERIES_PER_PACKAGE_NUMBER,0) <> COALESCE(src.BATTERIES_PER_PACKAGE_NUMBER,0)
                    OR COALESCE(tgt.BATTERIES_IN_EQUIPMENT_NUMBER,0) <> COALESCE(src.BATTERIES_IN_EQUIPMENT_NUMBER,0)
                    OR COALESCE(tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'') <> COALESCE(src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'')
                    OR COALESCE(tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'') <> COALESCE(src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'')
                    OR COALESCE(tgt.COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                    OR COALESCE(tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'') <> COALESCE(src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'')
                    OR COALESCE(tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'') <> COALESCE(src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'')
                    OR COALESCE(tgt.SCHEDULE_B_CODE_LIST,''A'') <> COALESCE(src.SCHEDULE_B_CODE_LIST,''A'')
                    OR COALESCE(tgt.UNITED_STATES_MUNITIONS_LIST_CODE,''A'') <> COALESCE(src.UNITED_STATES_MUNITIONS_LIST_CODE,''A'')
                    OR COALESCE(tgt.PROJECT_COORDINATOR_ID_CODE,''A'') <> COALESCE(src.PROJECT_COORDINATOR_ID_CODE,''A'')
                    OR COALESCE(tgt.PROJECT_COORDINATOR_EMPLOYEE_ID,0) <> COALESCE(src.PROJECT_COORDINATOR_EMPLOYEE_ID,0)
                    OR COALESCE(tgt.STOCK_ADJUSTMENT_MONTH_NUMBER,0) <> COALESCE(src.STOCK_ADJUSTMENT_MONTH_NUMBER,0)
                    OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'')
                    OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                    OR COALESCE(tgt.ALL_IN_COST,0) <> COALESCE(src.ALL_IN_COST,0)
                    OR COALESCE(tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'') <> COALESCE(src.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'')
                    OR COALESCE(tgt.CASE_LOT_DISCOUNT,0) <> COALESCE(src.CASE_LOT_DISCOUNT,0)
                    OR COALESCE(tgt.COMPANY_NUMBER,0) <> COALESCE(src.COMPANY_NUMBER,0)
                    OR COALESCE(tgt.CONVENIENCE_PACK_QUANTITY,0) <> COALESCE(src.CONVENIENCE_PACK_QUANTITY,0)
                    OR COALESCE(tgt.CONVENIENCE_PACK_DESCRIPTION,''A'') <> COALESCE(src.CONVENIENCE_PACK_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'') <> COALESCE(src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'')
                    OR COALESCE(tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'') <> COALESCE(src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'')
                    OR COALESCE(tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'') <> COALESCE(src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'')
                    OR COALESCE(tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'') <> COALESCE(src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.HAZARDOUS_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_USER_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.LIST_PRICE,0) <> COALESCE(src.LIST_PRICE,0)
                    OR COALESCE(tgt.LOW_USER_PRICE,0) <> COALESCE(src.LOW_USER_PRICE,0)
                    OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE,0) <> COALESCE(src.MINIMUM_ADVERTISED_PRICE,0)
                    OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'') <> COALESCE(src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.MINIMUM_SELL_QUANTITY,0) <> COALESCE(src.MINIMUM_SELL_QUANTITY,0)
                    OR COALESCE(tgt.PACKAGE_SIZE_DESCRIPTION,''A'') <> COALESCE(src.PACKAGE_SIZE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PERCENTAGE_OF_SUPPLIER_FUNDING,0) <> COALESCE(src.PERCENTAGE_OF_SUPPLIER_FUNDING,0)
                    OR COALESCE(tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'') <> COALESCE(src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'')
                    OR COALESCE(tgt.PRICING_COST,0) <> COALESCE(src.PRICING_COST,0)
                    OR COALESCE(tgt.PROFESSIONAL_PRICE,0) <> COALESCE(src.PROFESSIONAL_PRICE,0)
                    OR COALESCE(tgt.RETAIL_CORE,0) <> COALESCE(src.RETAIL_CORE,0)
                    OR COALESCE(tgt.RETAIL_HEIGHT,0) <> COALESCE(src.RETAIL_HEIGHT,0)
                    OR COALESCE(tgt.RETAIL_LENGTH,0) <> COALESCE(src.RETAIL_LENGTH,0)
                    OR COALESCE(tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.RETAIL_WIDTH,0) <> COALESCE(src.RETAIL_WIDTH,0)
                    OR COALESCE(tgt.SALES_PACK_CODE,''A'') <> COALESCE(src.SALES_PACK_CODE,''A'')
                    OR COALESCE(tgt.SCORE_FLAG,''A'') <> COALESCE(src.SCORE_FLAG,''A'')
                    OR COALESCE(tgt.SHIPPING_DIMENSIONS_CODE,''A'') <> COALESCE(src.SHIPPING_DIMENSIONS_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_BASE_COST,0) <> COALESCE(src.SUPPLIER_BASE_COST,0)
                    OR COALESCE(tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_LINE_CODE,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_USER_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.VIP_JOBBER,0) <> COALESCE(src.VIP_JOBBER,0)
                    OR COALESCE(tgt.WAREHOUSE_CORE,0) <> COALESCE(src.WAREHOUSE_CORE,0)
                    OR COALESCE(tgt.WAREHOUSE_COST,0) <> COALESCE(src.WAREHOUSE_COST,0)
                    --INFAETL-11815 adds the following line
                    OR COALESCE(tgt.PRODUCT_LEVEL_CODE,'''') <> COALESCE(src.PRODUCT_LEVEL_CODE,'''')
                    OR COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG,''A'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG,''A'')
                    )
                    with UR;';
            
               EXECUTE IMMEDIATE v_str_sql;

               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <WHNONSTK UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '21. WHNONSTK UPDATE', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

            END IF; -- V_SQL_OK
            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    

 /* README
 * This section sets the category fields for other-source deleted records to maintain their attributes from the CATEGORY source
 */                        
            IF V_SQL_OK THEN            
                SET v_str_sql =   CLOB('MERGE INTO ') || v_staging_database_name || '.' || v_staging_table_name || ' AS tgt ' || 
                                    'USING
                                    (SELECT TRIM(CUR_PRT_CATEGORY.LINE) AS LINE_CODE
                                    ,TRIM(CUR_PRT_CATEGORY.ITEM) AS ITEM_CODE 
                                    ,CUR_PRT_CATEGORY.LOAD_TIMESTAMP CLT
                                    ,DIM_PROD.ETL_UPDATE_TIMESTAMP DPT
                                    ,COALESCE(CUR_PRT_CATEGORY.SEGNUM, -2) AS SEGMENT_NUMBER
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.SEGMENT), ''UNKNOWN'') AS SEGMENT_DESCRIPTION
                                    ,COALESCE(CUR_PRT_CATEGORY.SUBCATNUM, -2) AS SUB_CATEGORY_NUMBER
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.SUB_CATEGORY), ''UNKNOWN'') AS SUB_CATEGORY_DESCRIPTION
                                    ,COALESCE(CUR_PRT_CATEGORY.CATEGORY_NUMBER, -2) AS CATEGORY_NUMBER
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.CATEGORY), ''UNKNOWN'') AS CATEGORY_DESCRIPTION
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.BRAND_TYPE),'''') AS BRAND_TYPE_NAME
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.LOCATION_TYPE), '''') AS LOCATION_TYPE_NAME
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.MFG_TYPE), '''') AS MANUFACTURING_CODE_DESCRIPTION
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.QUALITY_GRADE), '''') AS QUALITY_GRADE_CODE
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.PRIMARY_APPLICATION), '''') AS PRIMARY_APPLICATION_NAME
                                    ,CAST(COALESCE(UPPER(TRIM(CUR_PM.PRODUCT_MGR_NAME)),''UNKNOWN'') AS VARCHAR(256 OCTETS)) AS CATEGORY_MANAGER_NAME
                                    ,CAST(COALESCE(CUR_PRT_CATEGORY.PMNUM, -2) AS INTEGER) AS CATEGORY_MANAGER_NUMBER
                                    ,CAST(COALESCE(UPPER(TRIM(CUR_PMDIR.PM_DIRECTOR_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_DIRECTOR_NAME
                                    ,CAST(COALESCE(CUR_PM.PM_DIR_NUMBER,-2) AS INTEGER) AS CATEGORY_DIRECTOR_NUMBER
                                    ,CAST(COALESCE(UPPER(TRIM(CUR_PMVP.PM_VP_NAME)),''UNKNOWN'') as VARCHAR(256 OCTETS)) as CATEGORY_VP_NAME
                                    ,CAST(COALESCE(CUR_PM.PM_VP_NUMBER,-2) AS INTEGER) AS CATEGORY_VP_NUMBER
                                    ,CAST(COALESCE(HUB_LOAD_DIM_DATE_INACTIVEDATE.full_date, ''1900-01-01'') AS DATE) AS INACTIVATED_DATE
                                    ,COALESCE(TRIM(CUR_PRT_CATEGORY.REVIEW), '''') AS REVIEW_CODE
                                    ,COALESCE(CUR_PRT_CATEGORY.LOADDATE, ''1900-01-01'') AS CATEGORY_TABLE_CREATE_DATE
                                    ,COALESCE(CUR_PRT_CATEGORY.LOADTIME, ''1900-01-01'') AS CATEGORY_TABLE_CREATE_TIME
                                    ,COALESCE(CUR_PRT_CATEGORY.LOADUSER, '''') AS CATEGORY_TABLE_CREATE_USER_NAME
                                    ,COALESCE(CUR_PRT_CATEGORY.UPDDATE, ''1900-01-01'') AS CATEGORY_TABLE_UPDATE_DATE
                                    ,COALESCE(CUR_PRT_CATEGORY.UPDPGM, '''') AS CATEGORY_TABLE_UPDATE_PROGRAM_NAME
                                    ,COALESCE(CUR_PRT_CATEGORY.UPDTIME, ''1900-01-01'') AS CATEGORY_TABLE_UPDATE_TIME
                                    ,COALESCE(CUR_PRT_CATEGORY.UPDUSER, '''') AS CATEGORY_TABLE_UPDATE_USER_NAME
                                    ,CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.SALES_AREA_LABEL), '''') AS VARCHAR(256 OCTETS)) AS SALES_AREA_NAME
                                    ,CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_GROUP_LABEL) , '''') AS VARCHAR(256 OCTETS)) AS TEAM_NAME
                                    ,CAST(COALESCE(TRIM(cur_CATEGORY_EXTENSION.CATEGORY_NAME) , '''') AS VARCHAR(256 OCTETS)) AS CATEGORY_NAME
                                    ,CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.SALES_ID), -1) AS INTEGER) AS SALES_AREA_NAME_SORT_NUMBER
                                    ,CAST(COALESCE(INT(cur_CATEGORY_EXTENSION.TEAM_ID), -1) AS INTEGER) AS TEAM_NAME_SORT_NUMBER
                                    FROM '||v_staging_database_name||'.'||v_source_table_name||' DIM_PROD
                                    JOIN EDW_STAGING.CUR_PRT_CATEGORY AS CUR_PRT_CATEGORY ON TRIM(CUR_PRT_CATEGORY.LINE) = DIM_PROD.LINE_CODE AND TRIM(CUR_PRT_CATEGORY.ITEM) = DIM_PROD.ITEM_CODE
                                    LEFT JOIN EDW_STAGING.CUR_PRT_CATEGORY_EXTENSION AS cur_CATEGORY_EXTENSION ON cur_CATEGORY_EXTENSION.CATEGORY_NAME = CUR_PRT_CATEGORY.CATEGORY
                                    LEFT JOIN EDW_STAGING.HUB_LOAD_DIM_DATE AS HUB_LOAD_DIM_DATE_INACTIVEDATE ON HUB_LOAD_DIM_DATE_INACTIVEDATE.FULL_DATE = CUR_PRT_CATEGORY.INACTIVEDATE
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PM CUR_PM on CUR_PRT_CATEGORY.PMNUM::INTEGER = CUR_PM.PRODUCT_MGR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMDIR CUR_PMDIR ON CUR_PM.PM_DIR_NUMBER = CUR_PMDIR.PM_DIRECTOR_NUMBER
                                    LEFT JOIN EDW_STAGING.CUR_PRT_LU_PMVP CUR_PMVP ON CUR_PM.PM_VP_NUMBER = CUR_PMVP.PM_VP_NUMBER
                                    ) AS src
                                    ON tgt.ETL_SOURCE_DATA_DELETED_FLAG = ''Y''
                                    AND tgt.LINE_CODE=src.LINE_CODE AND tgt.ITEM_CODE=src.ITEM_CODE
                                    WHEN MATCHED AND 
                                    ( tgt.SEGMENT_NUMBER <> src.SEGMENT_NUMBER
                                    OR tgt.SEGMENT_DESCRIPTION <> src.SEGMENT_DESCRIPTION
                                    OR tgt.SUB_CATEGORY_NUMBER <> src.SUB_CATEGORY_NUMBER
                                    OR tgt.SUB_CATEGORY_DESCRIPTION <> src.SUB_CATEGORY_DESCRIPTION
                                    OR tgt.CATEGORY_NUMBER <> src.CATEGORY_NUMBER
                                    OR tgt.CATEGORY_DESCRIPTION <> src.CATEGORY_DESCRIPTION
                                    OR tgt.BRAND_TYPE_NAME <> src.BRAND_TYPE_NAME
                                    OR tgt.LOCATION_TYPE_NAME <> src.LOCATION_TYPE_NAME
                                    OR tgt.MANUFACTURING_CODE_DESCRIPTION <> src.MANUFACTURING_CODE_DESCRIPTION
                                    OR tgt.QUALITY_GRADE_CODE <> src.QUALITY_GRADE_CODE
                                    OR tgt.PRIMARY_APPLICATION_NAME <> src.PRIMARY_APPLICATION_NAME
                                    --INFAETL-11515 begin change
                                    OR COALESCE(tgt.CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.CATEGORY_MANAGER_NAME,''A'')
                                    OR COALESCE(tgt.CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.CATEGORY_MANAGER_NUMBER,0)
                                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NAME,''A'') <> COALESCE(src.CATEGORY_DIRECTOR_NAME,''A'')
                                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NUMBER,0) <> COALESCE(src.CATEGORY_DIRECTOR_NUMBER,0)
                                    OR COALESCE(tgt.CATEGORY_VP_NAME,''A'') <> COALESCE(src.CATEGORY_VP_NAME,''A'')
                                    OR COALESCE(tgt.CATEGORY_VP_NUMBER,0) <> COALESCE(src.CATEGORY_VP_NUMBER,0)
                                    OR tgt.INACTIVATED_DATE <> src.INACTIVATED_DATE
                                    OR tgt.REVIEW_CODE <> src.REVIEW_CODE
                                    OR tgt.CATEGORY_TABLE_CREATE_DATE <> src.CATEGORY_TABLE_CREATE_DATE
                                    OR tgt.CATEGORY_TABLE_CREATE_TIME <> src.CATEGORY_TABLE_CREATE_TIME
                                    OR tgt.CATEGORY_TABLE_CREATE_USER_NAME <> src.CATEGORY_TABLE_CREATE_USER_NAME
                                    OR tgt.CATEGORY_TABLE_UPDATE_DATE <> src.CATEGORY_TABLE_UPDATE_DATE
                                    OR tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME <> src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME
                                    OR tgt.CATEGORY_TABLE_UPDATE_TIME <> src.CATEGORY_TABLE_UPDATE_TIME
                                    OR tgt.CATEGORY_TABLE_UPDATE_USER_NAME <> src.CATEGORY_TABLE_UPDATE_USER_NAME
                                    OR tgt.SALES_AREA_NAME <> src.SALES_AREA_NAME
                                    OR tgt.TEAM_NAME <> src.TEAM_NAME
                                    OR tgt.CATEGORY_NAME <> src.CATEGORY_NAME
                                    OR tgt.SALES_AREA_NAME_SORT_NUMBER <> src.SALES_AREA_NAME_SORT_NUMBER
                                    OR tgt.TEAM_NAME_SORT_NUMBER <> src.TEAM_NAME_SORT_NUMBER
                                    )
                                    THEN UPDATE SET
                                    tgt.SEGMENT_NUMBER = src.SEGMENT_NUMBER
                                    ,tgt.SEGMENT_DESCRIPTION = src.SEGMENT_DESCRIPTION
                                    ,tgt.SUB_CATEGORY_NUMBER = src.SUB_CATEGORY_NUMBER
                                    ,tgt.SUB_CATEGORY_DESCRIPTION = src.SUB_CATEGORY_DESCRIPTION
                                    ,tgt.CATEGORY_NUMBER = src.CATEGORY_NUMBER
                                    ,tgt.CATEGORY_DESCRIPTION = src.CATEGORY_DESCRIPTION
                                    ,tgt.BRAND_TYPE_NAME = src.BRAND_TYPE_NAME
                                    ,tgt.LOCATION_TYPE_NAME = src.LOCATION_TYPE_NAME
                                    ,tgt.MANUFACTURING_CODE_DESCRIPTION = src.MANUFACTURING_CODE_DESCRIPTION
                                    ,tgt.QUALITY_GRADE_CODE = src.QUALITY_GRADE_CODE
                                    ,tgt.PRIMARY_APPLICATION_NAME = src.PRIMARY_APPLICATION_NAME
                                    ,tgt.CATEGORY_MANAGER_NAME=src.CATEGORY_MANAGER_NAME
                                    ,tgt.CATEGORY_MANAGER_NUMBER=src.CATEGORY_MANAGER_NUMBER
                                    ,tgt.CATEGORY_DIRECTOR_NAME=src.CATEGORY_DIRECTOR_NAME
                                    ,tgt.CATEGORY_DIRECTOR_NUMBER=src.CATEGORY_DIRECTOR_NUMBER
                                    ,tgt.CATEGORY_VP_NAME=src.CATEGORY_VP_NAME
                                    ,tgt.CATEGORY_VP_NUMBER=src.CATEGORY_VP_NUMBER
                                    ,tgt.INACTIVATED_DATE = src.INACTIVATED_DATE
                                    ,tgt.REVIEW_CODE = src.REVIEW_CODE
                                    ,tgt.SALES_AREA_NAME = src.SALES_AREA_NAME
                                    ,tgt.TEAM_NAME = src.TEAM_NAME
                                    ,tgt.CATEGORY_NAME = src.CATEGORY_NAME
                                    ,tgt.SALES_AREA_NAME_SORT_NUMBER = src.SALES_AREA_NAME_SORT_NUMBER
                                    ,tgt.TEAM_NAME_SORT_NUMBER = src.TEAM_NAME_SORT_NUMBER
                                    ,tgt.CATEGORY_TABLE_CREATE_DATE = src.CATEGORY_TABLE_CREATE_DATE
                                    ,tgt.CATEGORY_TABLE_CREATE_TIME = src.CATEGORY_TABLE_CREATE_TIME
                                    ,tgt.CATEGORY_TABLE_CREATE_USER_NAME = src.CATEGORY_TABLE_CREATE_USER_NAME
                                    ,tgt.CATEGORY_TABLE_UPDATE_DATE = src.CATEGORY_TABLE_UPDATE_DATE
                                    ,tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME = src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME
                                    ,tgt.CATEGORY_TABLE_UPDATE_TIME = src.CATEGORY_TABLE_UPDATE_TIME
                                    ,tgt.CATEGORY_TABLE_UPDATE_USER_NAME = src.CATEGORY_TABLE_UPDATE_USER_NAME
                                    ,tgt.ETL_UPDATE_TIMESTAMP = CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP)
                                    ,tgt.ETL_MODIFIED_BY_JOB_ID = '|| v_stored_procedure_execution_id ||'
                                    WITH UR; ';
                        

               EXECUTE IMMEDIATE v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
                                 /* Debugging Logging */
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <CATEGORY MERGE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '22. CATEGORY MERGE', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

               
               
            END IF; -- V_SQL_OK

            --reset SQL status messages for the next calls
            SET V_SQL_CODE = 0;
            SET V_SQL_STATE = 0;
            SET V_SQL_MSG = 0;    
                        
            --Populate Hub Table DIM_PRODUCT (UPDATE STAGING TABLE - UPDATE DELETED FLAG - IMASTER)
            IF V_SQL_OK THEN
                SET v_str_sql = 'UPDATE ' || v_staging_database_name || '.' || v_staging_table_name || ' AS HUB_LOAD ' || ' 
                                SET HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''Y'',
                                      HUB_LOAD.ETL_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP - CURRENT_TIMEZONE
                                WHERE HUB_LOAD.ETL_SOURCE_TABLE_NAME = ''IMASTER'' AND
                                HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''N'' AND
                                (HUB_LOAD.LINE_CODE, HUB_LOAD.ITEM_CODE) NOT IN 
                                (
                                SELECT ILINE AS LINE_CODE, IITEM# AS ITEM_CODE
                                FROM EDW_STAGING.CUR_IVB_IMASTER
                                )                                
                                ;';
                        
               SET o_str_debug =  v_str_sql;     
               EXECUTE IMMEDIATE  v_str_sql;
               GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
           --Debugging Logging
               IF (V_SQL_CODE <> 0) THEN  --  Warning
                  SET V_SQL_OK = FALSE;
                  SET V_RETURN_STATUS = V_SQL_CODE;
               END IF;
            
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTER SOFT DELETE UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '23. IMASTER Soft Delete Update', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 


           
               
         END IF; -- V_SQL_OK

         --reset SQL status messages for the next calls
         SET V_SQL_CODE = 0;
         SET V_SQL_STATE = 0;
         SET V_SQL_MSG = 0;    
                        

        --Populate Hub Table DIM_PRODUCT (UPDATE STAGING TABLE - UPDATE DELETED FLAG - source 2 - IMASTNS)
         IF V_SQL_OK THEN
                SET v_str_sql = 'UPDATE ' || v_staging_database_name || '.' || v_staging_table_name || ' AS HUB_LOAD ' || ' 
                                SET HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''Y'',
                                    HUB_LOAD.ETL_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP - CURRENT_TIMEZONE
                                WHERE HUB_LOAD.ETL_SOURCE_TABLE_NAME = ''IMASTNS'' AND
                                HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''N'' AND
                                (HUB_LOAD.LINE_CODE, HUB_LOAD.ITEM_CODE) NOT IN 
                                (
                                SELECT LINE AS LINE_CODE, ITEM AS ITEM_CODE
                                FROM EDW_STAGING.CUR_IVB_IMASTNS
                                )                                
                                ;';
                        
                SET o_str_debug =  v_str_sql;         
                EXECUTE IMMEDIATE  v_str_sql;
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
               
                 --Debugging Logging
                IF (V_SQL_CODE < 0) THEN  --  Warning
                    SET V_SQL_OK = FALSE;
                    SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
                
              
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <IMASTNS SOFT DELETE UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '24. IMASTNS Soft Delete Update', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 


           END IF; -- V_SQL_OK
            --reset SQL status messages for the next calls
           SET V_SQL_CODE = 0;
           SET V_SQL_STATE = 0;
           SET V_SQL_MSG = 0;    


        --Populate Hub Table DIM_PRODUCT (UPDATE STAGING TABLE - UPDATE DELETED FLAG - source 3)
         IF V_SQL_OK THEN
                SET v_str_sql = 'UPDATE ' || v_staging_database_name || '.' || v_staging_table_name || ' AS HUB_LOAD ' || ' 
                                SET HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''Y'',
                                    HUB_LOAD.ETL_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP - CURRENT_TIMEZONE
                                WHERE HUB_LOAD.ETL_SOURCE_TABLE_NAME = ''ECPARTS'' AND
                                HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''N'' AND
                                (HUB_LOAD.LINE_CODE, HUB_LOAD.ITEM_CODE) NOT IN 
                                (
                                SELECT LINE AS LINE_CODE, ITEMNUMBER AS ITEM_CODE
                                FROM EDW_STAGING.CUR_IVB_ECPARTS
                                )                                
                                ;';
                        
                SET o_str_debug =  v_str_sql;         
                EXECUTE IMMEDIATE  v_str_sql;
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
               
                 --Debugging Logging
                IF (V_SQL_CODE < 0) THEN  --  Warning
                    SET V_SQL_OK = FALSE;
                    SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
                
              
                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <ECPARTS SOFT DELETE UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                   IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                      THEN SET v_sql_logging_str = v_str_sql;
                      ELSE SET v_sql_logging_str = 'no sql logged';
                   END IF;

                   CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '25. ECPARTS Soft Delete Update', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
                END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 


           END IF; -- V_SQL_OK
            --reset SQL status messages for the next calls
           SET V_SQL_CODE = 0;
           SET V_SQL_STATE = 0;
           SET V_SQL_MSG = 0;           
          
          
        IF V_SQL_OK THEN
        --Populate Hub Table DIM_PRODUCT (UPDATE STAGING TABLE - UPDATE DELETED FLAG - source 4)
              SET v_str_sql = 'UPDATE ' || v_staging_database_name || '.' || v_staging_table_name || ' AS HUB_LOAD ' || ' 
                                SET HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''Y'',
                                    HUB_LOAD.ETL_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP - CURRENT_TIMEZONE
                                WHERE HUB_LOAD.ETL_SOURCE_TABLE_NAME = ''WHNONSTK'' AND
                                HUB_LOAD.ETL_SOURCE_DATA_DELETED_FLAG = ''N'' AND
                                (HUB_LOAD.LINE_CODE, HUB_LOAD.ITEM_CODE) NOT IN 
                                (
                                SELECT WHNLINE AS LINE_CODE, WHNITEM AS ITEM_CODE
                                    FROM EDW_STAGING.CUR_IVB_WHNONSTK
                                )                                
                                ;';
                        
                SET o_str_debug =  v_str_sql;     
                EXECUTE IMMEDIATE  v_str_sql;
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

               --Debugging Logging
                IF (V_SQL_CODE < 0) THEN  --  Warning
                    SET V_SQL_OK = FALSE;
                    SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;
                
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <WHNONSTK SOFT DELETE UPDATE> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '26. WHNONSTK Soft Delete Update', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

        END IF; -- V_SQL_OK

        --reset SQL status messages for the next calls
        SET V_SQL_CODE = 0;
        SET V_SQL_STATE = 0;
        SET V_SQL_MSG = 0;    
                        

                --Populate Hub Table DIM_PRODUCT (MERGE)
        IF V_SQL_OK THEN
                SET v_str_sql =  CLOB('MERGE INTO ') || v_target_database_name || '.' || v_target_table_name || ' AS tgt ' || 
                                    ' USING ' || v_staging_database_name || '.' || v_staging_table_name || ' AS src
                                            ON (tgt.PRODUCT_ID = src.PRODUCT_ID)
                                    WHEN MATCHED AND (
                     COALESCE(tgt.LINE_DESCRIPTION,''A'') <> COALESCE(src.LINE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ITEM_DESCRIPTION,''A'') <> COALESCE(src.ITEM_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SEGMENT_NUMBER,0) <> COALESCE(src.SEGMENT_NUMBER,0)
                    OR COALESCE(tgt.SEGMENT_DESCRIPTION,''A'') <> COALESCE(src.SEGMENT_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUB_CATEGORY_NUMBER,0) <> COALESCE(src.SUB_CATEGORY_NUMBER,0)
                    OR COALESCE(tgt.SUB_CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.SUB_CATEGORY_DESCRIPTION,''A'')
                    OR COALESCE(tgt.CATEGORY_NUMBER,0) <> COALESCE(src.CATEGORY_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_DESCRIPTION,''A'') <> COALESCE(src.CATEGORY_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PRODUCT_LINE_CODE,''A'') <> COALESCE(src.PRODUCT_LINE_CODE,''A'')
                    OR COALESCE(tgt.SUB_CODE,''A'') <> COALESCE(src.SUB_CODE,''A'')
                    OR COALESCE(tgt.MANUFACTURE_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.MANUFACTURE_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPERSEDED_LINE_CODE,''A'')
                    OR COALESCE(tgt.SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SORT_CONTROL_NUMBER,0) <> COALESCE(src.SORT_CONTROL_NUMBER,0)
                    OR COALESCE(tgt.POINT_OF_SALE_DESCRIPTION,''A'') <> COALESCE(src.POINT_OF_SALE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.POPULARITY_CODE,''A'') <> COALESCE(src.POPULARITY_CODE,''A'')
                    OR COALESCE(tgt.POPULARITY_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.POPULARITY_TREND_CODE,''A'') <> COALESCE(src.POPULARITY_TREND_CODE,''A'')
                    OR COALESCE(tgt.POPULARITY_TREND_CODE_DESCRIPTION,''A'') <> COALESCE(src.POPULARITY_TREND_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.LINE_IS_MARINE_SPECIFIC_FLAG,''A'') <> COALESCE(src.LINE_IS_MARINE_SPECIFIC_FLAG,''A'')
                    OR COALESCE(tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_FLEET_SPECIFIC_CODE,''A'') <> COALESCE(src.LINE_IS_FLEET_SPECIFIC_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,''A'')
                    OR COALESCE(tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'') <> COALESCE(src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,''A'')
                    OR COALESCE(tgt.JOBBER_SUPPLIER_CODE,''A'') <> COALESCE(src.JOBBER_SUPPLIER_CODE,''A'')
                    OR COALESCE(tgt.JOBBER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.JOBBER_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.WAREHOUSE_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.WAREHOUSE_SELL_QUANTITY,0) <> COALESCE(src.WAREHOUSE_SELL_QUANTITY,0)
                    OR COALESCE(tgt.RETAIL_WEIGHT,0) <> COALESCE(src.RETAIL_WEIGHT,0)
                    OR COALESCE(tgt.QUANTITY_PER_CAR,0) <> COALESCE(src.QUANTITY_PER_CAR,0)
                    OR COALESCE(tgt.CASE_QUANTITY,0) <> COALESCE(src.CASE_QUANTITY,0)
                    OR COALESCE(tgt.STANDARD_PACKAGE,0) <> COALESCE(src.STANDARD_PACKAGE,0)
                    OR COALESCE(tgt.PAINT_BODY_AND_EQUIPMENT_PRICE,0) <> COALESCE(src.PAINT_BODY_AND_EQUIPMENT_PRICE,0)
                    OR COALESCE(tgt.WAREHOUSE_JOBBER_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_PRICE,0)
                    OR COALESCE(tgt.WAREHOUSE_COST_WUM,0) <> COALESCE(src.WAREHOUSE_COST_WUM,0)
                    OR COALESCE(tgt.WAREHOUSE_CORE_WUM,0) <> COALESCE(src.WAREHOUSE_CORE_WUM,0)
                    OR COALESCE(tgt.OREILLY_COST_PRICE,0) <> COALESCE(src.OREILLY_COST_PRICE,0)
                    OR COALESCE(tgt.JOBBER_COST,0) <> COALESCE(src.JOBBER_COST,0)
                    OR COALESCE(tgt.JOBBER_CORE_PRICE,0) <> COALESCE(src.JOBBER_CORE_PRICE,0)
                    OR COALESCE(tgt.OUT_FRONT_MERCHANDISE_FLAG,''A'') <> COALESCE(src.OUT_FRONT_MERCHANDISE_FLAG,''A'')
                    OR COALESCE(tgt.ITEM_IS_TAXED_FLAG,''A'') <> COALESCE(src.ITEM_IS_TAXED_FLAG,''A'')
                    OR COALESCE(tgt.QUANTITY_ORDER_ITEM_FLAG,''A'') <> COALESCE(src.QUANTITY_ORDER_ITEM_FLAG,''A'')
                    OR COALESCE(tgt.JOBBER_DIVIDE_QUANTITY,0) <> COALESCE(src.JOBBER_DIVIDE_QUANTITY,0)
                    OR COALESCE(tgt.ITEM_DELETE_FLAG_RECORD_CODE,''A'') <> COALESCE(src.ITEM_DELETE_FLAG_RECORD_CODE,''A'')
                    OR COALESCE(tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'') <> COALESCE(src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,''A'')
                    OR COALESCE(tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'') <> COALESCE(src.PRIMARY_UNIVERSAL_PRODUCT_CODE,''A'')
                    OR COALESCE(tgt.WARRANTY_CODE,''A'') <> COALESCE(src.WARRANTY_CODE,''A'')
                    OR COALESCE(tgt.WARRANTY_CODE_DESCRIPTION,''A'') <> COALESCE(src.WARRANTY_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.INVOICE_COST_WUM_INVOICE_COST,0) <> COALESCE(src.INVOICE_COST_WUM_INVOICE_COST,0)
                    OR COALESCE(tgt.INVOICE_CORE_WUM_CORE_COST,0) <> COALESCE(src.INVOICE_CORE_WUM_CORE_COST,0)
                    OR COALESCE(tgt.IS_CONSIGNMENT_ITEM_FLAG,''A'') <> COALESCE(src.IS_CONSIGNMENT_ITEM_FLAG,''A'')
                    OR COALESCE(tgt.WAREHOUSE_JOBBER_CORE_PRICE,0) <> COALESCE(src.WAREHOUSE_JOBBER_CORE_PRICE,0)
                    OR COALESCE(tgt.ACQUISITION_FIELD_1_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_1_CODE,''A'')
                    OR COALESCE(tgt.ACQUISITION_FIELD_2_CODE,''A'') <> COALESCE(src.ACQUISITION_FIELD_2_CODE,''A'')
                    OR COALESCE(tgt.BUY_MULTIPLE,0) <> COALESCE(src.BUY_MULTIPLE,0)
                    OR COALESCE(tgt.BUY_MULTIPLE_CODE,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE,''A'')
                    OR COALESCE(tgt.BUY_MULTIPLE_CODE_DESCRIPTION,''A'') <> COALESCE(src.BUY_MULTIPLE_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUPPLIER_CONVERSION_FACTOR_CODE,''A'') <> COALESCE(src.SUPPLIER_CONVERSION_FACTOR_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_CONVERSION_QUANTITY,0) <> COALESCE(src.SUPPLIER_CONVERSION_QUANTITY,0)
                    OR COALESCE(tgt.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'') <> COALESCE(src.SUPPLIER_UNIT_OF_MEASURE_CODE,''A'')
                    OR COALESCE(tgt.UNIT_OF_MEASURE_AMOUNT,0) <> COALESCE(src.UNIT_OF_MEASURE_AMOUNT,0)
                    OR COALESCE(tgt.UNIT_OF_MEASURE_QUANTITY,0) <> COALESCE(src.UNIT_OF_MEASURE_QUANTITY,0)
                    OR COALESCE(tgt.UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.UNIT_OF_MEASURE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'') <> COALESCE(src.TAX_CLASSIFICATION_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'') <> COALESCE(src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'') <> COALESCE(src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_LENGTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_LENGTH,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WIDTH,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WIDTH,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_HEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_HEIGHT,0)
                    OR COALESCE(tgt.DISTRIBUTION_CENTER_PICK_WEIGHT,0) <> COALESCE(src.DISTRIBUTION_CENTER_PICK_WEIGHT,0)
                    OR COALESCE(tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PICK_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.CASE_QUANTITY_CODE,''A'') <> COALESCE(src.CASE_QUANTITY_CODE,''A'')
                    OR COALESCE(tgt.CASE_LENGTH,0) <> COALESCE(src.CASE_LENGTH,0)
                    OR COALESCE(tgt.CASE_WIDTH,0) <> COALESCE(src.CASE_WIDTH,0)
                    OR COALESCE(tgt.CASE_HEIGHT,0) <> COALESCE(src.CASE_HEIGHT,0)
                    OR COALESCE(tgt.CASE_WEIGHT,0) <> COALESCE(src.CASE_WEIGHT,0)
                    OR COALESCE(tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.CASE_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.CASES_PER_PALLET,0) <> COALESCE(src.CASES_PER_PALLET,0)
                    OR COALESCE(tgt.CASES_PER_PALLET_LAYER,0) <> COALESCE(src.CASES_PER_PALLET_LAYER,0)
                    OR COALESCE(tgt.PALLET_LENGTH,0) <> COALESCE(src.PALLET_LENGTH,0)
                    OR COALESCE(tgt.PALLET_WIDTH,0) <> COALESCE(src.PALLET_WIDTH,0)
                    OR COALESCE(tgt.PALLET_HEIGHT,0) <> COALESCE(src.PALLET_HEIGHT,0)
                    OR COALESCE(tgt.PALLET_WEIGHT,0) <> COALESCE(src.PALLET_WEIGHT,0)
                    OR COALESCE(tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'') <> COALESCE(src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,''A'')
                    OR COALESCE(tgt.SHIPMENT_CLASS_CODE,''A'') <> COALESCE(src.SHIPMENT_CLASS_CODE,''A'')
                    OR COALESCE(tgt.DOT_CLASS_NUMBER,0) <> COALESCE(src.DOT_CLASS_NUMBER,0)
                    OR COALESCE(tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER,0) <> COALESCE(src.DOT_CLASS_FOR_MSDS_ID_NUMBER,0)
                    OR COALESCE(tgt.CONTAINER_DESCRIPTION,''A'') <> COALESCE(src.CONTAINER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.KEEP_FROM_FREEZING_FLAG,''A'') <> COALESCE(src.KEEP_FROM_FREEZING_FLAG,''A'')
                    OR COALESCE(tgt.FLIGHT_RESTRICTED_FLAG,''A'') <> COALESCE(src.FLIGHT_RESTRICTED_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_NEW_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_NEW_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_CORE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_CORE_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_WARRANTY_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_WARRANTY_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_RECALL_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_RECALL_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'') <> COALESCE(src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.HAZARDOUS_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PIECE_LENGTH,0) <> COALESCE(src.PIECE_LENGTH,0)
                    OR COALESCE(tgt.PIECE_WIDTH,0) <> COALESCE(src.PIECE_WIDTH,0)
                    OR COALESCE(tgt.PIECE_HEIGHT,0) <> COALESCE(src.PIECE_HEIGHT,0)
                    OR COALESCE(tgt.PIECE_WEIGHT,0) <> COALESCE(src.PIECE_WEIGHT,0)
                    OR COALESCE(tgt.PIECES_INNER_PACK,0) <> COALESCE(src.PIECES_INNER_PACK,0)
                    OR COALESCE(tgt.IN_CATALOG_CODE,''A'') <> COALESCE(src.IN_CATALOG_CODE,''A'')
                    OR COALESCE(tgt.IN_CATALOG_CODE_DESCRIPTION,''A'') <> COALESCE(src.IN_CATALOG_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ALLOW_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ALLOW_SPECIAL_ORDER_FLAG,''A'')
                    OR COALESCE(tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'') <> COALESCE(src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,''A'')
                    OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CODE,''A'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'') <> COALESCE(src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.LONG_DESCRIPTION,''A'') <> COALESCE(src.LONG_DESCRIPTION,''A'')
                    OR COALESCE(tgt.ELECTRONIC_WASTE_FLAG,''A'') <> COALESCE(src.ELECTRONIC_WASTE_FLAG,''A'')
                    OR COALESCE(tgt.STORE_MINIMUM_SALE_QUANTITY,0) <> COALESCE(src.STORE_MINIMUM_SALE_QUANTITY,0)
                    OR COALESCE(tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0) <> COALESCE(src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,0)
                    OR COALESCE(tgt.MAXIMUM_CAR_QUANTITY,0) <> COALESCE(src.MAXIMUM_CAR_QUANTITY,0)
                    OR COALESCE(tgt.MINIMUM_CAR_QUANTITY,0) <> COALESCE(src.MINIMUM_CAR_QUANTITY,0)
                    OR COALESCE(tgt.ESSENTIAL_HARD_PART_CODE,''A'') <> COALESCE(src.ESSENTIAL_HARD_PART_CODE,''A'')
                    OR COALESCE(tgt.INNER_PACK_CODE,''A'') <> COALESCE(src.INNER_PACK_CODE,''A'')
                    OR COALESCE(tgt.INNER_PACK_QUANTITY,0) <> COALESCE(src.INNER_PACK_QUANTITY,0)
                    OR COALESCE(tgt.INNER_PACK_LENGTH,0) <> COALESCE(src.INNER_PACK_LENGTH,0)
                    OR COALESCE(tgt.INNER_PACK_WIDTH,0) <> COALESCE(src.INNER_PACK_WIDTH,0)
                    OR COALESCE(tgt.INNER_PACK_HEIGHT,0) <> COALESCE(src.INNER_PACK_HEIGHT,0)
                    OR COALESCE(tgt.INNER_PACK_WEIGHT,0) <> COALESCE(src.INNER_PACK_WEIGHT,0)
                    OR COALESCE(tgt.BRAND_CODE,''A'') <> COALESCE(src.BRAND_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_CODE,''A'') <> COALESCE(src.PART_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_DISPLAY_CODE,''A'') <> COALESCE(src.PART_NUMBER_DISPLAY_CODE,''A'')
                    OR COALESCE(tgt.PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.PART_NUMBER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SPANISH_PART_NUMBER_DESCRIPTION,''A'') <> COALESCE(src.SPANISH_PART_NUMBER_DESCRIPTION,''A'')
                    OR COALESCE(tgt.SUGGESTED_ORDER_QUANTITY,0) <> COALESCE(src.SUGGESTED_ORDER_QUANTITY,0)
                    OR COALESCE(tgt.BRAND_TYPE_NAME,''A'') <> COALESCE(src.BRAND_TYPE_NAME,''A'')
                    OR COALESCE(tgt.LOCATION_TYPE_NAME,''A'') <> COALESCE(src.LOCATION_TYPE_NAME,''A'')
                    OR COALESCE(tgt.MANUFACTURING_CODE_DESCRIPTION,''A'') <> COALESCE(src.MANUFACTURING_CODE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.QUALITY_GRADE_CODE,''A'') <> COALESCE(src.QUALITY_GRADE_CODE,''A'')
                    OR COALESCE(tgt.PRIMARY_APPLICATION_NAME,''A'') <> COALESCE(src.PRIMARY_APPLICATION_NAME,''A'')
                    --INFAETL-11515 begin change
                    OR COALESCE(tgt.CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.CATEGORY_MANAGER_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.CATEGORY_MANAGER_NUMBER,0)
                    OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'') <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,''A'')
                    OR COALESCE(tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0) <> COALESCE(src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NAME,''A'') <> COALESCE(src.CATEGORY_DIRECTOR_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_DIRECTOR_NUMBER,0) <> COALESCE(src.CATEGORY_DIRECTOR_NUMBER,0)
                    OR COALESCE(tgt.CATEGORY_VP_NAME,''A'') <> COALESCE(src.CATEGORY_VP_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_VP_NUMBER,0) <> COALESCE(src.CATEGORY_VP_NUMBER,0)
                    --INFAETL-11515 end change
                    OR COALESCE(tgt.INACTIVATED_DATE,''1900-01-01'') <> COALESCE(src.INACTIVATED_DATE,''1900-01-01'')
                    OR COALESCE(tgt.REVIEW_CODE,''A'') <> COALESCE(src.REVIEW_CODE,''A'')
                    OR COALESCE(tgt.STOCKING_LINE_FLAG,''A'') <> COALESCE(src.STOCKING_LINE_FLAG,''A'')
                    OR COALESCE(tgt.OIL_LINE_FLAG,''A'') <> COALESCE(src.OIL_LINE_FLAG,''A'')
                    OR COALESCE(tgt.SPECIAL_REQUIREMENTS_LABEL,''A'') <> COALESCE(src.SPECIAL_REQUIREMENTS_LABEL,''A'')
                    OR COALESCE(tgt.SUPPLIER_ACCOUNT_NUMBER,0) <> COALESCE(src.SUPPLIER_ACCOUNT_NUMBER,0)
                    OR COALESCE(tgt.SUPPLIER_NUMBER,0) <> COALESCE(src.SUPPLIER_NUMBER,0)
                    OR COALESCE(tgt.SUPPLIER_ID,0) <> COALESCE(src.SUPPLIER_ID,0)
                    OR COALESCE(tgt.BRAND_DESCRIPTION,''A'') <> COALESCE(src.BRAND_DESCRIPTION,''A'')
                    OR COALESCE(tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0) <> COALESCE(src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,0)
                    OR COALESCE(tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0) <> COALESCE(src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,0)
                    OR COALESCE(tgt.SALES_AREA_NAME,''A'') <> COALESCE(src.SALES_AREA_NAME,''A'')
                    OR COALESCE(tgt.TEAM_NAME,''A'') <> COALESCE(src.TEAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_NAME,''A'') <> COALESCE(src.CATEGORY_NAME,''A'')
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_NAME,''A'') <> COALESCE(src.REPLENISHMENT_ANALYST_NAME,''A'')
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_NUMBER,0)
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,0)
                    OR COALESCE(tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0) <> COALESCE(src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,0)
                    OR COALESCE(tgt.SALES_AREA_NAME_SORT_NUMBER,0) <> COALESCE(src.SALES_AREA_NAME_SORT_NUMBER,0)
                    OR COALESCE(tgt.TEAM_NAME_SORT_NUMBER,0) <> COALESCE(src.TEAM_NAME_SORT_NUMBER,0)
                    OR COALESCE(tgt.BUYER_CODE,''A'') <> COALESCE(src.BUYER_CODE,''A'')
                    OR COALESCE(tgt.BUYER_NAME,''A'') <> COALESCE(src.BUYER_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'') <> COALESCE(src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'') <> COALESCE(src.BATTERY_PACKING_INSTRUCTIONS_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_CITY_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_CITY_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_STATE_NAME,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_STATE_NAME,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_ZIP_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_ZIP_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'') <> COALESCE(src.BATTERY_MANUFACTURING_COUNTRY_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_PHONE_NUMBER_CODE,''A'') <> COALESCE(src.BATTERY_PHONE_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.BATTERY_WEIGHT_IN_GRAMS,0) <> COALESCE(src.BATTERY_WEIGHT_IN_GRAMS,0)
                    OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,0)
                    OR COALESCE(tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0) <> COALESCE(src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,0)
                    OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_CELL,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_CELL,0)
                    OR COALESCE(tgt.BATTERY_WATT_HOURS_PER_BATTERY,0) <> COALESCE(src.BATTERY_WATT_HOURS_PER_BATTERY,0)
                    OR COALESCE(tgt.BATTERY_CELLS_NUMBER,0) <> COALESCE(src.BATTERY_CELLS_NUMBER,0)
                    OR COALESCE(tgt.BATTERIES_PER_PACKAGE_NUMBER,0) <> COALESCE(src.BATTERIES_PER_PACKAGE_NUMBER,0)
                    OR COALESCE(tgt.BATTERIES_IN_EQUIPMENT_NUMBER,0) <> COALESCE(src.BATTERIES_IN_EQUIPMENT_NUMBER,0)
                    OR COALESCE(tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'') <> COALESCE(src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,''A'')
                    OR COALESCE(tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'') <> COALESCE(src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,''A'')
                    OR COALESCE(tgt.COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                    OR COALESCE(tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'') <> COALESCE(src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,''A'')
                    OR COALESCE(tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'') <> COALESCE(src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,''A'')
                    OR COALESCE(tgt.SCHEDULE_B_CODE_LIST,''A'') <> COALESCE(src.SCHEDULE_B_CODE_LIST,''A'')
                    OR COALESCE(tgt.UNITED_STATES_MUNITIONS_LIST_CODE,''A'') <> COALESCE(src.UNITED_STATES_MUNITIONS_LIST_CODE,''A'')
                    OR COALESCE(tgt.PROJECT_COORDINATOR_ID_CODE,''A'') <> COALESCE(src.PROJECT_COORDINATOR_ID_CODE,''A'')
                    OR COALESCE(tgt.PROJECT_COORDINATOR_EMPLOYEE_ID,0) <> COALESCE(src.PROJECT_COORDINATOR_EMPLOYEE_ID,0)
                    OR COALESCE(tgt.STOCK_ADJUSTMENT_MONTH_NUMBER,0) <> COALESCE(src.STOCK_ADJUSTMENT_MONTH_NUMBER,0)
                    OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,''A'')
                    OR COALESCE(tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'') <> COALESCE(src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,''A'')
                    OR COALESCE(tgt.ALL_IN_COST,0) <> COALESCE(src.ALL_IN_COST,0)
                    OR COALESCE(tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'') <> COALESCE(src.CANCEL_OR_BACKORDER_REMAINDER_CODE,''A'')
                    OR COALESCE(tgt.CASE_LOT_DISCOUNT,0) <> COALESCE(src.CASE_LOT_DISCOUNT,0)
                    OR COALESCE(tgt.COMPANY_NUMBER,0) <> COALESCE(src.COMPANY_NUMBER,0)
                    OR COALESCE(tgt.CONVENIENCE_PACK_QUANTITY,0) <> COALESCE(src.CONVENIENCE_PACK_QUANTITY,0)
                    OR COALESCE(tgt.CONVENIENCE_PACK_DESCRIPTION,''A'') <> COALESCE(src.CONVENIENCE_PACK_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'') <> COALESCE(src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,''A'')
                    OR COALESCE(tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'') <> COALESCE(src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,''A'')
                    OR COALESCE(tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'') <> COALESCE(src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,''A'')
                    OR COALESCE(tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'') <> COALESCE(src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.HAZARDOUS_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.HAZARDOUS_UPDATE_USER_NAME,''A'') <> COALESCE(src.HAZARDOUS_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.LIST_PRICE,0) <> COALESCE(src.LIST_PRICE,0)
                    OR COALESCE(tgt.LOW_USER_PRICE,0) <> COALESCE(src.LOW_USER_PRICE,0)
                    OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE,0) <> COALESCE(src.MINIMUM_ADVERTISED_PRICE,0)
                    OR COALESCE(tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'') <> COALESCE(src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.MINIMUM_SELL_QUANTITY,0) <> COALESCE(src.MINIMUM_SELL_QUANTITY,0)
                    OR COALESCE(tgt.PACKAGE_SIZE_DESCRIPTION,''A'') <> COALESCE(src.PACKAGE_SIZE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.PERCENTAGE_OF_SUPPLIER_FUNDING,0) <> COALESCE(src.PERCENTAGE_OF_SUPPLIER_FUNDING,0)
                    OR COALESCE(tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'') <> COALESCE(src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,''A'')
                    OR COALESCE(tgt.PRICING_COST,0) <> COALESCE(src.PRICING_COST,0)
                    OR COALESCE(tgt.PROFESSIONAL_PRICE,0) <> COALESCE(src.PROFESSIONAL_PRICE,0)
                    OR COALESCE(tgt.RETAIL_CORE,0) <> COALESCE(src.RETAIL_CORE,0)
                    OR COALESCE(tgt.RETAIL_HEIGHT,0) <> COALESCE(src.RETAIL_HEIGHT,0)
                    OR COALESCE(tgt.RETAIL_LENGTH,0) <> COALESCE(src.RETAIL_LENGTH,0)
                    OR COALESCE(tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'') <> COALESCE(src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,''A'')
                    OR COALESCE(tgt.RETAIL_WIDTH,0) <> COALESCE(src.RETAIL_WIDTH,0)
                    OR COALESCE(tgt.SALES_PACK_CODE,''A'') <> COALESCE(src.SALES_PACK_CODE,''A'')
                    OR COALESCE(tgt.SCORE_FLAG,''A'') <> COALESCE(src.SCORE_FLAG,''A'')
                    OR COALESCE(tgt.SHIPPING_DIMENSIONS_CODE,''A'') <> COALESCE(src.SHIPPING_DIMENSIONS_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_BASE_COST,0) <> COALESCE(src.SUPPLIER_BASE_COST,0)
                    OR COALESCE(tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,''A'')
                    OR COALESCE(tgt.SUPPLIER_SUPERSEDED_LINE_CODE,''A'') <> COALESCE(src.SUPPLIER_SUPERSEDED_LINE_CODE,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_CREATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_CREATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.CATEGORY_TABLE_CREATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_CREATE_USER_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.CATEGORY_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.CATEGORY_TABLE_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,''1900-01-01'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,''A'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,''0:0:0:0'')
                    OR COALESCE(tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'') <> COALESCE(src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,''A'')
                    OR COALESCE(tgt.VIP_JOBBER,0) <> COALESCE(src.VIP_JOBBER,0)
                    OR COALESCE(tgt.WAREHOUSE_CORE,0) <> COALESCE(src.WAREHOUSE_CORE,0)
                    OR COALESCE(tgt.WAREHOUSE_COST,0) <> COALESCE(src.WAREHOUSE_COST,0)
                    --INFAETL-11815 adds the following line
                    OR COALESCE(tgt.PRODUCT_LEVEL_CODE,'''') <> COALESCE(src.PRODUCT_LEVEL_CODE,'''')
                    OR COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG,''A'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG,''A'')
                    ) ' || ' 
                    THEN 
                        UPDATE SET 
                            tgt.LINE_DESCRIPTION=src.LINE_DESCRIPTION,
                            tgt.ITEM_DESCRIPTION=src.ITEM_DESCRIPTION,
                            tgt.SEGMENT_NUMBER=src.SEGMENT_NUMBER,
                            tgt.SEGMENT_DESCRIPTION=src.SEGMENT_DESCRIPTION,
                            tgt.SUB_CATEGORY_NUMBER=src.SUB_CATEGORY_NUMBER,
                            tgt.SUB_CATEGORY_DESCRIPTION=src.SUB_CATEGORY_DESCRIPTION,
                            tgt.CATEGORY_NUMBER=src.CATEGORY_NUMBER,
                            tgt.CATEGORY_DESCRIPTION=src.CATEGORY_DESCRIPTION,
                            tgt.PRODUCT_LINE_CODE=src.PRODUCT_LINE_CODE,
                            tgt.SUB_CODE=src.SUB_CODE,
                            tgt.MANUFACTURE_ITEM_NUMBER_CODE=src.MANUFACTURE_ITEM_NUMBER_CODE,
                            tgt.SUPERSEDED_LINE_CODE=src.SUPERSEDED_LINE_CODE,
                            tgt.SUPERSEDED_ITEM_NUMBER_CODE=src.SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SORT_CONTROL_NUMBER=src.SORT_CONTROL_NUMBER,
                            tgt.POINT_OF_SALE_DESCRIPTION=src.POINT_OF_SALE_DESCRIPTION,
                            tgt.POPULARITY_CODE=src.POPULARITY_CODE,
                            tgt.POPULARITY_CODE_DESCRIPTION=src.POPULARITY_CODE_DESCRIPTION,
                            tgt.POPULARITY_TREND_CODE=src.POPULARITY_TREND_CODE,
                            tgt.POPULARITY_TREND_CODE_DESCRIPTION=src.POPULARITY_TREND_CODE_DESCRIPTION,
                            tgt.LINE_IS_MARINE_SPECIFIC_FLAG=src.LINE_IS_MARINE_SPECIFIC_FLAG,
                            tgt.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE=src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE,
                            tgt.LINE_IS_FLEET_SPECIFIC_CODE=src.LINE_IS_FLEET_SPECIFIC_CODE,
                            tgt.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE=src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE,
                            tgt.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG=src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG,
                            tgt.JOBBER_SUPPLIER_CODE=src.JOBBER_SUPPLIER_CODE,
                            tgt.JOBBER_UNIT_OF_MEASURE_CODE=src.JOBBER_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_UNIT_OF_MEASURE_CODE=src.WAREHOUSE_UNIT_OF_MEASURE_CODE,
                            tgt.WAREHOUSE_SELL_QUANTITY=src.WAREHOUSE_SELL_QUANTITY,
                            tgt.RETAIL_WEIGHT=src.RETAIL_WEIGHT,
                            tgt.QUANTITY_PER_CAR=src.QUANTITY_PER_CAR,
                            tgt.CASE_QUANTITY=src.CASE_QUANTITY,
                            tgt.STANDARD_PACKAGE=src.STANDARD_PACKAGE,
                            tgt.PAINT_BODY_AND_EQUIPMENT_PRICE=src.PAINT_BODY_AND_EQUIPMENT_PRICE,
                            tgt.WAREHOUSE_JOBBER_PRICE=src.WAREHOUSE_JOBBER_PRICE,
                            tgt.WAREHOUSE_COST_WUM=src.WAREHOUSE_COST_WUM,
                            tgt.WAREHOUSE_CORE_WUM=src.WAREHOUSE_CORE_WUM,
                            tgt.OREILLY_COST_PRICE=src.OREILLY_COST_PRICE,
                            tgt.JOBBER_COST=src.JOBBER_COST,
                            tgt.JOBBER_CORE_PRICE=src.JOBBER_CORE_PRICE,
                            tgt.OUT_FRONT_MERCHANDISE_FLAG=src.OUT_FRONT_MERCHANDISE_FLAG,
                            tgt.ITEM_IS_TAXED_FLAG=src.ITEM_IS_TAXED_FLAG,
                            tgt.QUANTITY_ORDER_ITEM_FLAG=src.QUANTITY_ORDER_ITEM_FLAG,
                            tgt.JOBBER_DIVIDE_QUANTITY=src.JOBBER_DIVIDE_QUANTITY,
                            tgt.ITEM_DELETE_FLAG_RECORD_CODE=src.ITEM_DELETE_FLAG_RECORD_CODE,
                            tgt.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE=src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE,
                            tgt.PRIMARY_UNIVERSAL_PRODUCT_CODE=src.PRIMARY_UNIVERSAL_PRODUCT_CODE,
                            tgt.WARRANTY_CODE=src.WARRANTY_CODE,
                            tgt.WARRANTY_CODE_DESCRIPTION=src.WARRANTY_CODE_DESCRIPTION,
                            tgt.INVOICE_COST_WUM_INVOICE_COST=src.INVOICE_COST_WUM_INVOICE_COST,
                            tgt.INVOICE_CORE_WUM_CORE_COST=src.INVOICE_CORE_WUM_CORE_COST,
                            tgt.IS_CONSIGNMENT_ITEM_FLAG=src.IS_CONSIGNMENT_ITEM_FLAG,
                            tgt.WAREHOUSE_JOBBER_CORE_PRICE=src.WAREHOUSE_JOBBER_CORE_PRICE,
                            tgt.ACQUISITION_FIELD_1_CODE=src.ACQUISITION_FIELD_1_CODE,
                            tgt.ACQUISITION_FIELD_2_CODE=src.ACQUISITION_FIELD_2_CODE,
                            tgt.BUY_MULTIPLE=src.BUY_MULTIPLE,
                            tgt.BUY_MULTIPLE_CODE=src.BUY_MULTIPLE_CODE,
                            tgt.BUY_MULTIPLE_CODE_DESCRIPTION=src.BUY_MULTIPLE_CODE_DESCRIPTION,
                            tgt.SUPPLIER_CONVERSION_FACTOR_CODE=src.SUPPLIER_CONVERSION_FACTOR_CODE,
                            tgt.SUPPLIER_CONVERSION_QUANTITY=src.SUPPLIER_CONVERSION_QUANTITY,
                            tgt.SUPPLIER_UNIT_OF_MEASURE_CODE=src.SUPPLIER_UNIT_OF_MEASURE_CODE,
                            tgt.UNIT_OF_MEASURE_AMOUNT=src.UNIT_OF_MEASURE_AMOUNT,
                            tgt.UNIT_OF_MEASURE_QUANTITY=src.UNIT_OF_MEASURE_QUANTITY,
                            tgt.UNIT_OF_MEASURE_DESCRIPTION=src.UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_CODE=src.TAX_CLASSIFICATION_CODE,
                            tgt.TAX_CLASSIFICATION_CODE_DESCRIPTION=src.TAX_CLASSIFICATION_CODE_DESCRIPTION,
                            tgt.TAX_CLASSIFICATION_REVIEW_STATUS_CODE=src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE,
                            tgt.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION=src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION,
                            tgt.DISTRIBUTION_CENTER_PICK_LENGTH=src.DISTRIBUTION_CENTER_PICK_LENGTH,
                            tgt.DISTRIBUTION_CENTER_PICK_WIDTH=src.DISTRIBUTION_CENTER_PICK_WIDTH,
                            tgt.DISTRIBUTION_CENTER_PICK_HEIGHT=src.DISTRIBUTION_CENTER_PICK_HEIGHT,
                            tgt.DISTRIBUTION_CENTER_PICK_WEIGHT=src.DISTRIBUTION_CENTER_PICK_WEIGHT,
                            tgt.PICK_LENGTH_WIDTH_HEIGHT_CODE=src.PICK_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASE_QUANTITY_CODE=src.CASE_QUANTITY_CODE,
                            tgt.CASE_LENGTH=src.CASE_LENGTH,
                            tgt.CASE_WIDTH=src.CASE_WIDTH,
                            tgt.CASE_HEIGHT=src.CASE_HEIGHT,
                            tgt.CASE_WEIGHT=src.CASE_WEIGHT,
                            tgt.CASE_LENGTH_WIDTH_HEIGHT_CODE=src.CASE_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.CASES_PER_PALLET=src.CASES_PER_PALLET,
                            tgt.CASES_PER_PALLET_LAYER=src.CASES_PER_PALLET_LAYER,
                            tgt.PALLET_LENGTH=src.PALLET_LENGTH,
                            tgt.PALLET_WIDTH=src.PALLET_WIDTH,
                            tgt.PALLET_HEIGHT=src.PALLET_HEIGHT,
                            tgt.PALLET_WEIGHT=src.PALLET_WEIGHT,
                            tgt.PALLET_LENGTH_WIDTH_HEIGHT_CODE=src.PALLET_LENGTH_WIDTH_HEIGHT_CODE,
                            tgt.SHIPMENT_CLASS_CODE=src.SHIPMENT_CLASS_CODE,
                            tgt.DOT_CLASS_NUMBER=src.DOT_CLASS_NUMBER,
                            tgt.DOT_CLASS_FOR_MSDS_ID_NUMBER=src.DOT_CLASS_FOR_MSDS_ID_NUMBER,
                            tgt.CONTAINER_DESCRIPTION=src.CONTAINER_DESCRIPTION,
                            tgt.KEEP_FROM_FREEZING_FLAG=src.KEEP_FROM_FREEZING_FLAG,
                            tgt.FLIGHT_RESTRICTED_FLAG=src.FLIGHT_RESTRICTED_FLAG,
                            tgt.ALLOW_NEW_RETURNS_FLAG=src.ALLOW_NEW_RETURNS_FLAG,
                            tgt.ALLOW_CORE_RETURNS_FLAG=src.ALLOW_CORE_RETURNS_FLAG,
                            tgt.ALLOW_WARRANTY_RETURNS_FLAG=src.ALLOW_WARRANTY_RETURNS_FLAG,
                            tgt.ALLOW_RECALL_RETURNS_FLAG=src.ALLOW_RECALL_RETURNS_FLAG,
                            tgt.ALLOW_MANUAL_OTHER_RETURNS_FLAG=src.ALLOW_MANUAL_OTHER_RETURNS_FLAG,
                            tgt.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG=src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG,
                            tgt.HAZARDOUS_UPDATE_DATE=src.HAZARDOUS_UPDATE_DATE,
                            tgt.PIECE_LENGTH=src.PIECE_LENGTH,
                            tgt.PIECE_WIDTH=src.PIECE_WIDTH,
                            tgt.PIECE_HEIGHT=src.PIECE_HEIGHT,
                            tgt.PIECE_WEIGHT=src.PIECE_WEIGHT,
                            tgt.PIECES_INNER_PACK=src.PIECES_INNER_PACK,
                            tgt.IN_CATALOG_CODE=src.IN_CATALOG_CODE,
                            tgt.IN_CATALOG_CODE_DESCRIPTION=src.IN_CATALOG_CODE_DESCRIPTION,
                            tgt.ALLOW_SPECIAL_ORDER_FLAG=src.ALLOW_SPECIAL_ORDER_FLAG,
                            tgt.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG=src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG,
                            tgt.SUPPLIER_LIFE_CYCLE_CODE=src.SUPPLIER_LIFE_CYCLE_CODE,
                            tgt.SUPPLIER_LIFE_CYCLE_CHANGE_DATE=src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE,
                            tgt.LONG_DESCRIPTION=src.LONG_DESCRIPTION,
                            tgt.ELECTRONIC_WASTE_FLAG=src.ELECTRONIC_WASTE_FLAG,
                            tgt.STORE_MINIMUM_SALE_QUANTITY=src.STORE_MINIMUM_SALE_QUANTITY,
                            tgt.MANUFACTURER_SUGGESTED_RETAIL_PRICE=src.MANUFACTURER_SUGGESTED_RETAIL_PRICE,
                            tgt.MAXIMUM_CAR_QUANTITY=src.MAXIMUM_CAR_QUANTITY,
                            tgt.MINIMUM_CAR_QUANTITY=src.MINIMUM_CAR_QUANTITY,
                            tgt.ESSENTIAL_HARD_PART_CODE=src.ESSENTIAL_HARD_PART_CODE,
                            tgt.INNER_PACK_CODE=src.INNER_PACK_CODE,
                            tgt.INNER_PACK_QUANTITY=src.INNER_PACK_QUANTITY,
                            tgt.INNER_PACK_LENGTH=src.INNER_PACK_LENGTH,
                            tgt.INNER_PACK_WIDTH=src.INNER_PACK_WIDTH,
                            tgt.INNER_PACK_HEIGHT=src.INNER_PACK_HEIGHT,
                            tgt.INNER_PACK_WEIGHT=src.INNER_PACK_WEIGHT,
                            tgt.BRAND_CODE=src.BRAND_CODE,
                            tgt.PART_NUMBER_CODE=src.PART_NUMBER_CODE,
                            tgt.PART_NUMBER_DISPLAY_CODE=src.PART_NUMBER_DISPLAY_CODE,
                            tgt.PART_NUMBER_DESCRIPTION=src.PART_NUMBER_DESCRIPTION,
                            tgt.SPANISH_PART_NUMBER_DESCRIPTION=src.SPANISH_PART_NUMBER_DESCRIPTION,
                            tgt.SUGGESTED_ORDER_QUANTITY=src.SUGGESTED_ORDER_QUANTITY,
                            tgt.BRAND_TYPE_NAME=src.BRAND_TYPE_NAME,
                            tgt.LOCATION_TYPE_NAME=src.LOCATION_TYPE_NAME,
                            tgt.MANUFACTURING_CODE_DESCRIPTION=src.MANUFACTURING_CODE_DESCRIPTION,
                            tgt.QUALITY_GRADE_CODE=src.QUALITY_GRADE_CODE,
                            tgt.PRIMARY_APPLICATION_NAME=src.PRIMARY_APPLICATION_NAME,
                            --INFAETL-11515 begin change
                            tgt.CATEGORY_MANAGER_NAME=src.CATEGORY_MANAGER_NAME,
                            tgt.CATEGORY_MANAGER_NUMBER=src.CATEGORY_MANAGER_NUMBER,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME,
                            tgt.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER=src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER,
                            tgt.CATEGORY_DIRECTOR_NAME=src.CATEGORY_DIRECTOR_NAME,
                            tgt.CATEGORY_DIRECTOR_NUMBER=src.CATEGORY_DIRECTOR_NUMBER,
                            tgt.CATEGORY_VP_NAME=src.CATEGORY_VP_NAME,
                            tgt.CATEGORY_VP_NUMBER=src.CATEGORY_VP_NUMBER,
                            --INFAETL-11515 end change                            tgt.INACTIVATED_DATE=src.INACTIVATED_DATE,
                            tgt.REVIEW_CODE=src.REVIEW_CODE,
                            tgt.STOCKING_LINE_FLAG=src.STOCKING_LINE_FLAG,
                            tgt.OIL_LINE_FLAG=src.OIL_LINE_FLAG,
                            tgt.SPECIAL_REQUIREMENTS_LABEL=src.SPECIAL_REQUIREMENTS_LABEL,
                            tgt.SUPPLIER_ACCOUNT_NUMBER=src.SUPPLIER_ACCOUNT_NUMBER,
                            tgt.SUPPLIER_NUMBER=src.SUPPLIER_NUMBER,
                            tgt.SUPPLIER_ID=src.SUPPLIER_ID,
                            tgt.BRAND_DESCRIPTION=src.BRAND_DESCRIPTION,
                            tgt.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER=src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER,
                            tgt.ACCOUNTS_PAYABLE_VENDOR_NUMBER=src.ACCOUNTS_PAYABLE_VENDOR_NUMBER,
                            tgt.SALES_AREA_NAME=src.SALES_AREA_NAME,
                            tgt.TEAM_NAME=src.TEAM_NAME,
                            tgt.CATEGORY_NAME=src.CATEGORY_NAME,
                            tgt.REPLENISHMENT_ANALYST_NAME=src.REPLENISHMENT_ANALYST_NAME,
                            tgt.REPLENISHMENT_ANALYST_NUMBER=src.REPLENISHMENT_ANALYST_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER=src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER,
                            tgt.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID=src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID,
                            tgt.SALES_AREA_NAME_SORT_NUMBER=src.SALES_AREA_NAME_SORT_NUMBER,
                            tgt.TEAM_NAME_SORT_NUMBER=src.TEAM_NAME_SORT_NUMBER,
                            tgt.BUYER_CODE=src.BUYER_CODE,
                            tgt.BUYER_NAME=src.BUYER_NAME,
                            tgt.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE=src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE,
                            tgt.BATTERY_PACKING_INSTRUCTIONS_CODE=src.BATTERY_PACKING_INSTRUCTIONS_CODE,
                            tgt.BATTERY_MANUFACTURING_NAME=src.BATTERY_MANUFACTURING_NAME,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_1=src.BATTERY_MANUFACTURING_ADDRESS_LINE_1,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_2=src.BATTERY_MANUFACTURING_ADDRESS_LINE_2,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_3=src.BATTERY_MANUFACTURING_ADDRESS_LINE_3,
                            tgt.BATTERY_MANUFACTURING_ADDRESS_LINE_4=src.BATTERY_MANUFACTURING_ADDRESS_LINE_4,
                            tgt.BATTERY_MANUFACTURING_CITY_NAME=src.BATTERY_MANUFACTURING_CITY_NAME,
                            tgt.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME=src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME,
                            tgt.BATTERY_MANUFACTURING_STATE_NAME=src.BATTERY_MANUFACTURING_STATE_NAME,
                            tgt.BATTERY_MANUFACTURING_ZIP_CODE=src.BATTERY_MANUFACTURING_ZIP_CODE,
                            tgt.BATTERY_MANUFACTURING_COUNTRY_CODE=src.BATTERY_MANUFACTURING_COUNTRY_CODE,
                            tgt.BATTERY_PHONE_NUMBER_CODE=src.BATTERY_PHONE_NUMBER_CODE,
                            tgt.BATTERY_WEIGHT_IN_GRAMS=src.BATTERY_WEIGHT_IN_GRAMS,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_CELL=src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL,
                            tgt.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY=src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY,
                            tgt.BATTERY_WATT_HOURS_PER_CELL=src.BATTERY_WATT_HOURS_PER_CELL,
                            tgt.BATTERY_WATT_HOURS_PER_BATTERY=src.BATTERY_WATT_HOURS_PER_BATTERY,
                            tgt.BATTERY_CELLS_NUMBER=src.BATTERY_CELLS_NUMBER,
                            tgt.BATTERIES_PER_PACKAGE_NUMBER=src.BATTERIES_PER_PACKAGE_NUMBER,
                            tgt.BATTERIES_IN_EQUIPMENT_NUMBER=src.BATTERIES_IN_EQUIPMENT_NUMBER,
                            tgt.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG=src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG,
                            tgt.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG=src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG,
                            tgt.COUNTRY_OF_ORIGIN_NAME_LIST=src.COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST=src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST,
                            tgt.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST=src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST,
                            tgt.SCHEDULE_B_CODE_LIST=src.SCHEDULE_B_CODE_LIST,
                            tgt.UNITED_STATES_MUNITIONS_LIST_CODE=src.UNITED_STATES_MUNITIONS_LIST_CODE,
                            tgt.PROJECT_COORDINATOR_ID_CODE=src.PROJECT_COORDINATOR_ID_CODE,
                            tgt.PROJECT_COORDINATOR_EMPLOYEE_ID=src.PROJECT_COORDINATOR_EMPLOYEE_ID,
                            tgt.STOCK_ADJUSTMENT_MONTH_NUMBER=src.STOCK_ADJUSTMENT_MONTH_NUMBER,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST,
                            tgt.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST=src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST,
                            tgt.ALL_IN_COST=src.ALL_IN_COST,
                            tgt.CANCEL_OR_BACKORDER_REMAINDER_CODE=src.CANCEL_OR_BACKORDER_REMAINDER_CODE,
                            tgt.CASE_LOT_DISCOUNT=src.CASE_LOT_DISCOUNT,
                            tgt.COMPANY_NUMBER=src.COMPANY_NUMBER,
                            tgt.CONVENIENCE_PACK_QUANTITY=src.CONVENIENCE_PACK_QUANTITY,
                            tgt.CONVENIENCE_PACK_DESCRIPTION=src.CONVENIENCE_PACK_DESCRIPTION,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_DATE=src.PRODUCT_SOURCE_TABLE_CREATION_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_TIME=src.PRODUCT_SOURCE_TABLE_CREATION_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME=src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE=src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME,
                            tgt.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE=src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE,
                            tgt.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE=src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE,
                            tgt.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE=src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE,
                            tgt.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG=src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG,
                            tgt.HAZARDOUS_UPDATE_PROGRAM_NAME=src.HAZARDOUS_UPDATE_PROGRAM_NAME,
                            tgt.HAZARDOUS_UPDATE_TIME=src.HAZARDOUS_UPDATE_TIME,
                            tgt.HAZARDOUS_UPDATE_USER_NAME=src.HAZARDOUS_UPDATE_USER_NAME,
                            tgt.LIST_PRICE=src.LIST_PRICE,
                            tgt.LOW_USER_PRICE=src.LOW_USER_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE=src.MINIMUM_ADVERTISED_PRICE,
                            tgt.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE=src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE,
                            tgt.MINIMUM_SELL_QUANTITY=src.MINIMUM_SELL_QUANTITY,
                            tgt.PACKAGE_SIZE_DESCRIPTION=src.PACKAGE_SIZE_DESCRIPTION,
                            tgt.PERCENTAGE_OF_SUPPLIER_FUNDING=src.PERCENTAGE_OF_SUPPLIER_FUNDING,
                            tgt.PIECE_LENGTH_WIDTH_HEIGHT_FLAG=src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG,
                            tgt.PRICING_COST=src.PRICING_COST,
                            tgt.PROFESSIONAL_PRICE=src.PROFESSIONAL_PRICE,
                            tgt.RETAIL_CORE=src.RETAIL_CORE,
                            tgt.RETAIL_HEIGHT=src.RETAIL_HEIGHT,
                            tgt.RETAIL_LENGTH=src.RETAIL_LENGTH,
                            tgt.RETAIL_UNIT_OF_MEASURE_DESCRIPTION=src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION,
                            tgt.RETAIL_WIDTH=src.RETAIL_WIDTH,
                            tgt.SALES_PACK_CODE=src.SALES_PACK_CODE,
                            tgt.SCORE_FLAG=src.SCORE_FLAG,
                            tgt.SHIPPING_DIMENSIONS_CODE=src.SHIPPING_DIMENSIONS_CODE,
                            tgt.SUPPLIER_BASE_COST=src.SUPPLIER_BASE_COST,
                            tgt.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE=src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE,
                            tgt.SUPPLIER_SUPERSEDED_LINE_CODE=src.SUPPLIER_SUPERSEDED_LINE_CODE,
                            tgt.CATEGORY_TABLE_CREATE_DATE=src.CATEGORY_TABLE_CREATE_DATE,
                            tgt.CATEGORY_TABLE_CREATE_PROGRAM_NAME=src.CATEGORY_TABLE_CREATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_CREATE_TIME=src.CATEGORY_TABLE_CREATE_TIME,
                            tgt.CATEGORY_TABLE_CREATE_USER_NAME=src.CATEGORY_TABLE_CREATE_USER_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_DATE=src.CATEGORY_TABLE_UPDATE_DATE,
                            tgt.CATEGORY_TABLE_UPDATE_PROGRAM_NAME=src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.CATEGORY_TABLE_UPDATE_TIME=src.CATEGORY_TABLE_UPDATE_TIME,
                            tgt.CATEGORY_TABLE_UPDATE_USER_NAME=src.CATEGORY_TABLE_UPDATE_USER_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_DATE=src.PRODUCT_SOURCE_TABLE_UPDATE_DATE,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_TIME=src.PRODUCT_SOURCE_TABLE_UPDATE_TIME,
                            tgt.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME=src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME,
                            tgt.VIP_JOBBER=src.VIP_JOBBER,
                            tgt.WAREHOUSE_CORE=src.WAREHOUSE_CORE,
                            tgt.WAREHOUSE_COST=src.WAREHOUSE_COST,
                            --INFAETL-11815 added the next line 
                            tgt.PRODUCT_LEVEL_CODE = src.PRODUCT_LEVEL_CODE,
                            tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
                            tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
                            --tgt.ETL_CREATE_TIMESTAMP=src.ETL_CREATE_TIMESTAMP,  -- delete per peer review
                            tgt.ETL_UPDATE_TIMESTAMP= CURRENT_TIMESTAMP-CURRENT_TIMEZONE,
                            tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
                            tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
                            ' || ' WHEN NOT MATCHED
                                THEN INSERT (PRODUCT_ID, LINE_CODE, LINE_DESCRIPTION, ITEM_CODE, ITEM_DESCRIPTION, SEGMENT_NUMBER, SEGMENT_DESCRIPTION, SUB_CATEGORY_NUMBER, SUB_CATEGORY_DESCRIPTION, CATEGORY_NUMBER, CATEGORY_DESCRIPTION, PRODUCT_LINE_CODE, SUB_CODE, MANUFACTURE_ITEM_NUMBER_CODE, SUPERSEDED_LINE_CODE, SUPERSEDED_ITEM_NUMBER_CODE, SORT_CONTROL_NUMBER, POINT_OF_SALE_DESCRIPTION, POPULARITY_CODE, POPULARITY_CODE_DESCRIPTION, POPULARITY_TREND_CODE, POPULARITY_TREND_CODE_DESCRIPTION, LINE_IS_MARINE_SPECIFIC_FLAG, LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE, LINE_IS_FLEET_SPECIFIC_CODE, LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE, LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG, JOBBER_SUPPLIER_CODE, JOBBER_UNIT_OF_MEASURE_CODE, WAREHOUSE_UNIT_OF_MEASURE_CODE, WAREHOUSE_SELL_QUANTITY, RETAIL_WEIGHT, QUANTITY_PER_CAR, CASE_QUANTITY, STANDARD_PACKAGE, PAINT_BODY_AND_EQUIPMENT_PRICE, WAREHOUSE_JOBBER_PRICE, WAREHOUSE_COST_WUM, WAREHOUSE_CORE_WUM, OREILLY_COST_PRICE, JOBBER_COST, JOBBER_CORE_PRICE, OUT_FRONT_MERCHANDISE_FLAG, ITEM_IS_TAXED_FLAG, QUANTITY_ORDER_ITEM_FLAG, JOBBER_DIVIDE_QUANTITY, ITEM_DELETE_FLAG_RECORD_CODE, SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE, PRIMARY_UNIVERSAL_PRODUCT_CODE, WARRANTY_CODE, WARRANTY_CODE_DESCRIPTION, INVOICE_COST_WUM_INVOICE_COST, INVOICE_CORE_WUM_CORE_COST, IS_CONSIGNMENT_ITEM_FLAG, WAREHOUSE_JOBBER_CORE_PRICE, ACQUISITION_FIELD_1_CODE, ACQUISITION_FIELD_2_CODE, BUY_MULTIPLE, BUY_MULTIPLE_CODE, BUY_MULTIPLE_CODE_DESCRIPTION, SUPPLIER_CONVERSION_FACTOR_CODE, SUPPLIER_CONVERSION_QUANTITY, SUPPLIER_UNIT_OF_MEASURE_CODE, UNIT_OF_MEASURE_AMOUNT, UNIT_OF_MEASURE_QUANTITY, UNIT_OF_MEASURE_DESCRIPTION, TAX_CLASSIFICATION_CODE, TAX_CLASSIFICATION_CODE_DESCRIPTION, TAX_CLASSIFICATION_REVIEW_STATUS_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE, DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION, DISTRIBUTION_CENTER_PICK_LENGTH, DISTRIBUTION_CENTER_PICK_WIDTH, DISTRIBUTION_CENTER_PICK_HEIGHT, DISTRIBUTION_CENTER_PICK_WEIGHT, PICK_LENGTH_WIDTH_HEIGHT_CODE, CASE_QUANTITY_CODE, CASE_LENGTH, CASE_WIDTH, CASE_HEIGHT, CASE_WEIGHT, CASE_LENGTH_WIDTH_HEIGHT_CODE, CASES_PER_PALLET, CASES_PER_PALLET_LAYER, PALLET_LENGTH, PALLET_WIDTH, PALLET_HEIGHT, PALLET_WEIGHT, PALLET_LENGTH_WIDTH_HEIGHT_CODE, SHIPMENT_CLASS_CODE, DOT_CLASS_NUMBER, DOT_CLASS_FOR_MSDS_ID_NUMBER, CONTAINER_DESCRIPTION, KEEP_FROM_FREEZING_FLAG, FLIGHT_RESTRICTED_FLAG, ALLOW_NEW_RETURNS_FLAG, ALLOW_CORE_RETURNS_FLAG, ALLOW_WARRANTY_RETURNS_FLAG, ALLOW_RECALL_RETURNS_FLAG, ALLOW_MANUAL_OTHER_RETURNS_FLAG, ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG, HAZARDOUS_UPDATE_DATE, PIECE_LENGTH, PIECE_WIDTH, PIECE_HEIGHT, PIECE_WEIGHT, PIECES_INNER_PACK, IN_CATALOG_CODE, IN_CATALOG_CODE_DESCRIPTION, ALLOW_SPECIAL_ORDER_FLAG, ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG, SUPPLIER_LIFE_CYCLE_CODE, SUPPLIER_LIFE_CYCLE_CHANGE_DATE, LONG_DESCRIPTION, ELECTRONIC_WASTE_FLAG, STORE_MINIMUM_SALE_QUANTITY, MANUFACTURER_SUGGESTED_RETAIL_PRICE, MAXIMUM_CAR_QUANTITY, MINIMUM_CAR_QUANTITY, ESSENTIAL_HARD_PART_CODE, INNER_PACK_CODE, INNER_PACK_QUANTITY, INNER_PACK_LENGTH, INNER_PACK_WIDTH, INNER_PACK_HEIGHT, INNER_PACK_WEIGHT, BRAND_CODE, PART_NUMBER_CODE,
                                PART_NUMBER_DISPLAY_CODE, PART_NUMBER_DESCRIPTION, SPANISH_PART_NUMBER_DESCRIPTION, SUGGESTED_ORDER_QUANTITY, BRAND_TYPE_NAME, LOCATION_TYPE_NAME, MANUFACTURING_CODE_DESCRIPTION, QUALITY_GRADE_CODE, PRIMARY_APPLICATION_NAME, 
                                --INFAETL-11515 mds renamed / added the following line
                                CATEGORY_MANAGER_NAME, CATEGORY_MANAGER_NUMBER, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME, ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER, CATEGORY_DIRECTOR_NAME, CATEGORY_DIRECTOR_NUMBER, CATEGORY_VP_NAME, CATEGORY_VP_NUMBER, 
                                INACTIVATED_DATE, REVIEW_CODE, STOCKING_LINE_FLAG, OIL_LINE_FLAG, SPECIAL_REQUIREMENTS_LABEL, SUPPLIER_ACCOUNT_NUMBER, SUPPLIER_NUMBER, SUPPLIER_ID, BRAND_DESCRIPTION, DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER, ACCOUNTS_PAYABLE_VENDOR_NUMBER, SALES_AREA_NAME, TEAM_NAME, CATEGORY_NAME, REPLENISHMENT_ANALYST_NAME, REPLENISHMENT_ANALYST_NUMBER, REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER, REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID, SALES_AREA_NAME_SORT_NUMBER, TEAM_NAME_SORT_NUMBER, BUYER_CODE, BUYER_NAME, BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE, BATTERY_PACKING_INSTRUCTIONS_CODE, BATTERY_MANUFACTURING_NAME, BATTERY_MANUFACTURING_ADDRESS_LINE_1, BATTERY_MANUFACTURING_ADDRESS_LINE_2, BATTERY_MANUFACTURING_ADDRESS_LINE_3, BATTERY_MANUFACTURING_ADDRESS_LINE_4, BATTERY_MANUFACTURING_CITY_NAME, BATTERY_MANUFACTURING_POSTAL_TOWN_NAME, BATTERY_MANUFACTURING_STATE_NAME, BATTERY_MANUFACTURING_ZIP_CODE, BATTERY_MANUFACTURING_COUNTRY_CODE, BATTERY_PHONE_NUMBER_CODE, BATTERY_WEIGHT_IN_GRAMS, BATTERY_GRAMS_OF_LITHIUM_PER_CELL, BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY, BATTERY_WATT_HOURS_PER_CELL, BATTERY_WATT_HOURS_PER_BATTERY, BATTERY_CELLS_NUMBER, BATTERIES_PER_PACKAGE_NUMBER, BATTERIES_IN_EQUIPMENT_NUMBER, BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG, BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG, COUNTRY_OF_ORIGIN_NAME_LIST, EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST, HARMONIZED_TARIFF_SCHEDULE_CODE_LIST, SCHEDULE_B_CODE_LIST, UNITED_STATES_MUNITIONS_LIST_CODE, PROJECT_COORDINATOR_ID_CODE, PROJECT_COORDINATOR_EMPLOYEE_ID, STOCK_ADJUSTMENT_MONTH_NUMBER, BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST, BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST, ALL_IN_COST, CANCEL_OR_BACKORDER_REMAINDER_CODE, CASE_LOT_DISCOUNT, COMPANY_NUMBER, CONVENIENCE_PACK_QUANTITY, CONVENIENCE_PACK_DESCRIPTION, PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_CREATION_DATE, PRODUCT_SOURCE_TABLE_CREATION_TIME, PRODUCT_SOURCE_TABLE_CREATION_USER_NAME, PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME, PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME, DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE, DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE, ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE, EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG, HAZARDOUS_UPDATE_PROGRAM_NAME, HAZARDOUS_UPDATE_TIME, HAZARDOUS_UPDATE_USER_NAME, LIST_PRICE, LOW_USER_PRICE, MINIMUM_ADVERTISED_PRICE, MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE, MINIMUM_SELL_QUANTITY, PACKAGE_SIZE_DESCRIPTION, PERCENTAGE_OF_SUPPLIER_FUNDING, PIECE_LENGTH_WIDTH_HEIGHT_FLAG, PRICING_COST, PROFESSIONAL_PRICE, RETAIL_CORE, RETAIL_HEIGHT, RETAIL_LENGTH, RETAIL_UNIT_OF_MEASURE_DESCRIPTION, RETAIL_WIDTH, SALES_PACK_CODE, SCORE_FLAG, SHIPPING_DIMENSIONS_CODE, SUPPLIER_BASE_COST, SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE, SUPPLIER_SUPERSEDED_LINE_CODE, CATEGORY_TABLE_CREATE_DATE, CATEGORY_TABLE_CREATE_PROGRAM_NAME, CATEGORY_TABLE_CREATE_TIME, CATEGORY_TABLE_CREATE_USER_NAME, CATEGORY_TABLE_UPDATE_DATE, CATEGORY_TABLE_UPDATE_PROGRAM_NAME, CATEGORY_TABLE_UPDATE_TIME, CATEGORY_TABLE_UPDATE_USER_NAME, PRODUCT_SOURCE_TABLE_UPDATE_DATE, PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME, PRODUCT_SOURCE_TABLE_UPDATE_TIME, PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME, VIP_JOBBER, WAREHOUSE_CORE, WAREHOUSE_COST, 
                                --INFAETL-11815 mds added the following line
                                PRODUCT_LEVEL_CODE, 
                                ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS
                                )     
                                VALUES ( src.PRODUCT_ID, src.LINE_CODE, src.LINE_DESCRIPTION, src.ITEM_CODE, src.ITEM_DESCRIPTION, src.SEGMENT_NUMBER, src.SEGMENT_DESCRIPTION, src.SUB_CATEGORY_NUMBER, src.SUB_CATEGORY_DESCRIPTION, src.CATEGORY_NUMBER, src.CATEGORY_DESCRIPTION, src.PRODUCT_LINE_CODE, src.SUB_CODE, src.MANUFACTURE_ITEM_NUMBER_CODE, src.SUPERSEDED_LINE_CODE, src.SUPERSEDED_ITEM_NUMBER_CODE, src.SORT_CONTROL_NUMBER, src.POINT_OF_SALE_DESCRIPTION, src.POPULARITY_CODE, src.POPULARITY_CODE_DESCRIPTION, src.POPULARITY_TREND_CODE, src.POPULARITY_TREND_CODE_DESCRIPTION, src.LINE_IS_MARINE_SPECIFIC_FLAG, src.LINE_IS_AGRICULTURAL_OR_OFFROAD_SPECIFIC_CODE, src.LINE_IS_FLEET_SPECIFIC_CODE, src.LINE_IS_SPECIFIC_TO_PAINT_STORES_CODE, src.LINE_IS_SPECIFIC_FOR_HYDRAULIC_EQUIPMENT_STORES_FLAG, src.JOBBER_SUPPLIER_CODE, src.JOBBER_UNIT_OF_MEASURE_CODE, src.WAREHOUSE_UNIT_OF_MEASURE_CODE, src.WAREHOUSE_SELL_QUANTITY, src.RETAIL_WEIGHT, src.QUANTITY_PER_CAR, src.CASE_QUANTITY, src.STANDARD_PACKAGE, src.PAINT_BODY_AND_EQUIPMENT_PRICE, src.WAREHOUSE_JOBBER_PRICE, src.WAREHOUSE_COST_WUM, src.WAREHOUSE_CORE_WUM, src.OREILLY_COST_PRICE, src.JOBBER_COST, src.JOBBER_CORE_PRICE, src.OUT_FRONT_MERCHANDISE_FLAG, src.ITEM_IS_TAXED_FLAG, src.QUANTITY_ORDER_ITEM_FLAG, src.JOBBER_DIVIDE_QUANTITY, src.ITEM_DELETE_FLAG_RECORD_CODE, src.SAFETY_DATA_SHEET_REQUIRED_FLAG_MSDS_ITEM_CODE, src.PRIMARY_UNIVERSAL_PRODUCT_CODE, src.WARRANTY_CODE, src.WARRANTY_CODE_DESCRIPTION, src.INVOICE_COST_WUM_INVOICE_COST, src.INVOICE_CORE_WUM_CORE_COST, src.IS_CONSIGNMENT_ITEM_FLAG, src.WAREHOUSE_JOBBER_CORE_PRICE, src.ACQUISITION_FIELD_1_CODE, src.ACQUISITION_FIELD_2_CODE, src.BUY_MULTIPLE, src.BUY_MULTIPLE_CODE, src.BUY_MULTIPLE_CODE_DESCRIPTION, src.SUPPLIER_CONVERSION_FACTOR_CODE, src.SUPPLIER_CONVERSION_QUANTITY, src.SUPPLIER_UNIT_OF_MEASURE_CODE, src.UNIT_OF_MEASURE_AMOUNT, src.UNIT_OF_MEASURE_QUANTITY, src.UNIT_OF_MEASURE_DESCRIPTION, src.TAX_CLASSIFICATION_CODE, src.TAX_CLASSIFICATION_CODE_DESCRIPTION, src.TAX_CLASSIFICATION_REVIEW_STATUS_CODE, src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE, src.DISTRIBUTION_CENTER_PICK_TYPE_WUM_TYPE_CODE_DESCRIPTION, src.DISTRIBUTION_CENTER_PICK_LENGTH, src.DISTRIBUTION_CENTER_PICK_WIDTH, src.DISTRIBUTION_CENTER_PICK_HEIGHT, src.DISTRIBUTION_CENTER_PICK_WEIGHT, src.PICK_LENGTH_WIDTH_HEIGHT_CODE, src.CASE_QUANTITY_CODE, src.CASE_LENGTH, src.CASE_WIDTH, src.CASE_HEIGHT, src.CASE_WEIGHT, src.CASE_LENGTH_WIDTH_HEIGHT_CODE, src.CASES_PER_PALLET, src.CASES_PER_PALLET_LAYER, src.PALLET_LENGTH, src.PALLET_WIDTH, src.PALLET_HEIGHT, src.PALLET_WEIGHT, src.PALLET_LENGTH_WIDTH_HEIGHT_CODE, src.SHIPMENT_CLASS_CODE, src.DOT_CLASS_NUMBER, src.DOT_CLASS_FOR_MSDS_ID_NUMBER, src.CONTAINER_DESCRIPTION, src.KEEP_FROM_FREEZING_FLAG, src.FLIGHT_RESTRICTED_FLAG, src.ALLOW_NEW_RETURNS_FLAG, src.ALLOW_CORE_RETURNS_FLAG, src.ALLOW_WARRANTY_RETURNS_FLAG, src.ALLOW_RECALL_RETURNS_FLAG, src.ALLOW_MANUAL_OTHER_RETURNS_FLAG, src.ALLOW_OUTSIDE_PURCHASE_RETURNS_FLAG, src.HAZARDOUS_UPDATE_DATE, src.PIECE_LENGTH, src.PIECE_WIDTH, src.PIECE_HEIGHT, src.PIECE_WEIGHT, src.PIECES_INNER_PACK, src.IN_CATALOG_CODE, src.IN_CATALOG_CODE_DESCRIPTION, src.ALLOW_SPECIAL_ORDER_FLAG, src.ITEM_ONLY_AVAILABLE_ON_SPECIAL_ORDER_FLAG, src.SUPPLIER_LIFE_CYCLE_CODE, src.SUPPLIER_LIFE_CYCLE_CHANGE_DATE, src.LONG_DESCRIPTION, src.ELECTRONIC_WASTE_FLAG, src.STORE_MINIMUM_SALE_QUANTITY, src.MANUFACTURER_SUGGESTED_RETAIL_PRICE, src.MAXIMUM_CAR_QUANTITY, src.MINIMUM_CAR_QUANTITY, src.ESSENTIAL_HARD_PART_CODE, src.INNER_PACK_CODE, src.INNER_PACK_QUANTITY, src.INNER_PACK_LENGTH, src.INNER_PACK_WIDTH, src.INNER_PACK_HEIGHT, src.INNER_PACK_WEIGHT, src.BRAND_CODE, src.PART_NUMBER_CODE,
                                         src.PART_NUMBER_DISPLAY_CODE, src.PART_NUMBER_DESCRIPTION, src.SPANISH_PART_NUMBER_DESCRIPTION, src.SUGGESTED_ORDER_QUANTITY, src.BRAND_TYPE_NAME, src.LOCATION_TYPE_NAME, src.MANUFACTURING_CODE_DESCRIPTION, src.QUALITY_GRADE_CODE, src.PRIMARY_APPLICATION_NAME, 
                                         --INFAETL-11515 mds renamed / added the following line
                                         src.CATEGORY_MANAGER_NAME, src.CATEGORY_MANAGER_NUMBER, src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NAME, src.ENTERPRISE_ALIGNMENT_CATEGORY_MANAGER_NUMBER, src.CATEGORY_DIRECTOR_NAME, src.CATEGORY_DIRECTOR_NUMBER, src.CATEGORY_VP_NAME, src.CATEGORY_VP_NUMBER, 
                                         src.INACTIVATED_DATE, src.REVIEW_CODE, src.STOCKING_LINE_FLAG, src.OIL_LINE_FLAG, src.SPECIAL_REQUIREMENTS_LABEL, src.SUPPLIER_ACCOUNT_NUMBER, src.SUPPLIER_NUMBER, src.SUPPLIER_ID, src.BRAND_DESCRIPTION, src.DATA_UNIVERSAL_NUMBERING_SYSTEM_NUMBER, src.ACCOUNTS_PAYABLE_VENDOR_NUMBER, src.SALES_AREA_NAME, src.TEAM_NAME, src.CATEGORY_NAME, src.REPLENISHMENT_ANALYST_NAME, src.REPLENISHMENT_ANALYST_NUMBER, src.REPLENISHMENT_ANALYST_TEAM_MEMBER_NUMBER, src.REPLENISHMENT_ANALYST_TEAM_EMPLOYEE_ID, src.SALES_AREA_NAME_SORT_NUMBER, src.TEAM_NAME_SORT_NUMBER, src.BUYER_CODE, src.BUYER_NAME, src.BATTERY_UNITED_NATIONS_DANGEROUS_GOODS_CODE, src.BATTERY_PACKING_INSTRUCTIONS_CODE, src.BATTERY_MANUFACTURING_NAME, src.BATTERY_MANUFACTURING_ADDRESS_LINE_1, src.BATTERY_MANUFACTURING_ADDRESS_LINE_2, src.BATTERY_MANUFACTURING_ADDRESS_LINE_3, src.BATTERY_MANUFACTURING_ADDRESS_LINE_4, src.BATTERY_MANUFACTURING_CITY_NAME, src.BATTERY_MANUFACTURING_POSTAL_TOWN_NAME, src.BATTERY_MANUFACTURING_STATE_NAME, src.BATTERY_MANUFACTURING_ZIP_CODE, src.BATTERY_MANUFACTURING_COUNTRY_CODE, src.BATTERY_PHONE_NUMBER_CODE, src.BATTERY_WEIGHT_IN_GRAMS, src.BATTERY_GRAMS_OF_LITHIUM_PER_CELL, src.BATTERY_GRAMS_OF_LITHIUM_PER_BATTERY, src.BATTERY_WATT_HOURS_PER_CELL, src.BATTERY_WATT_HOURS_PER_BATTERY, src.BATTERY_CELLS_NUMBER, src.BATTERIES_PER_PACKAGE_NUMBER, src.BATTERIES_IN_EQUIPMENT_NUMBER, src.BATTERY_LESS_THAN_30_PERCENT_STATE_OF_CHARGE_FLAG, src.BATTERY_UN_TESTED_DOCUMENTATION_PROVIDED_FLAG, src.COUNTRY_OF_ORIGIN_NAME_LIST, src.EXPORT_CONTROL_CLASSIFICIATION_NUMBER_CODE_LIST, src.HARMONIZED_TARIFF_SCHEDULE_CODE_LIST, src.SCHEDULE_B_CODE_LIST, src.UNITED_STATES_MUNITIONS_LIST_CODE, src.PROJECT_COORDINATOR_ID_CODE, src.PROJECT_COORDINATOR_EMPLOYEE_ID, src.STOCK_ADJUSTMENT_MONTH_NUMBER, src.BATTERY_COUNTRY_OF_ORIGIN_CODE_LIST, src.BATTERY_COUNTRY_OF_ORIGIN_NAME_LIST, src.ALL_IN_COST, src.CANCEL_OR_BACKORDER_REMAINDER_CODE, src.CASE_LOT_DISCOUNT, src.COMPANY_NUMBER,
                                         src.CONVENIENCE_PACK_QUANTITY, src.CONVENIENCE_PACK_DESCRIPTION, src.PRODUCT_SOURCE_TABLE_CREATION_PROGRAM_NAME, src.PRODUCT_SOURCE_TABLE_CREATION_DATE, src.PRODUCT_SOURCE_TABLE_CREATION_TIME, src.PRODUCT_SOURCE_TABLE_CREATION_USER_NAME, src.PRODUCT_SOURCE_TABLE_DATE_OF_LAST_COST_UPDATE, src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_DATE, src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_TIME, src.PRODUCT_SOURCE_TABLE_DIMENSION_UPDATE_USER_NAME, src.DO_NOT_SEND_INTO_DEMAND_OR_REPLENISHMENT_FULFILLMENT_AND_DEMAND_CODE, src.DWWMSITEM_SOURCE_TABLE_COUNTRY_OF_ORIGIN_CODE, src.ELECTRONIC_ARTICLE_SURVEILLANCE_TAG_CODE, src.EXCLUDE_FROM_FREE_SHIPPING_FOR_ONLINE_STORE_FLAG, src.HAZARDOUS_UPDATE_PROGRAM_NAME, src.HAZARDOUS_UPDATE_TIME, src.HAZARDOUS_UPDATE_USER_NAME, src.LIST_PRICE, src.LOW_USER_PRICE, src.MINIMUM_ADVERTISED_PRICE, src.MINIMUM_ADVERTISED_PRICE_EFFECTIVE_DATE, src.MINIMUM_SELL_QUANTITY, src.PACKAGE_SIZE_DESCRIPTION, src.PERCENTAGE_OF_SUPPLIER_FUNDING, src.PIECE_LENGTH_WIDTH_HEIGHT_FLAG, src.PRICING_COST, src.PROFESSIONAL_PRICE, src.RETAIL_CORE, src.RETAIL_HEIGHT, src.RETAIL_LENGTH, src.RETAIL_UNIT_OF_MEASURE_DESCRIPTION, src.RETAIL_WIDTH, src.SALES_PACK_CODE, src.SCORE_FLAG, src.SHIPPING_DIMENSIONS_CODE, src.SUPPLIER_BASE_COST, src.SUPPLIER_SUPERSEDED_ITEM_NUMBER_CODE, src.SUPPLIER_SUPERSEDED_LINE_CODE, src.CATEGORY_TABLE_CREATE_DATE, src.CATEGORY_TABLE_CREATE_PROGRAM_NAME, src.CATEGORY_TABLE_CREATE_TIME, src.CATEGORY_TABLE_CREATE_USER_NAME, src.CATEGORY_TABLE_UPDATE_DATE, src.CATEGORY_TABLE_UPDATE_PROGRAM_NAME, src.CATEGORY_TABLE_UPDATE_TIME, src.CATEGORY_TABLE_UPDATE_USER_NAME, src.PRODUCT_SOURCE_TABLE_UPDATE_DATE, src.PRODUCT_SOURCE_TABLE_UPDATE_PROGRAM_NAME, src.PRODUCT_SOURCE_TABLE_UPDATE_TIME, src.PRODUCT_SOURCE_TABLE_UPDATE_USER_NAME, src.VIP_JOBBER, src.WAREHOUSE_CORE, src.WAREHOUSE_COST, 
                                         --INFAETL-11815 mds added the following line
                                         src.PRODUCT_LEVEL_CODE,
                                         src.ETL_SOURCE_DATA_DELETED_FLAG, src.ETL_SOURCE_TABLE_NAME, CURRENT_TIMESTAMP-CURRENT_TIMEZONE, CURRENT_TIMESTAMP-CURRENT_TIMEZONE, src.ETL_MODIFIED_BY_JOB_ID, src.ETL_MODIFIED_BY_PROCESS) WITH UR
                                ;';
                               
                EXECUTE IMMEDIATE v_str_sql;
                GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
               
                                 /* Debugging Logging */
                IF (V_SQL_CODE < 0) THEN  --  Warning
                    SET V_SQL_OK = FALSE;
                    SET V_RETURN_STATUS = V_SQL_CODE;
                END IF;

              
               IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                  SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <MERGE DIM_PRODUCT> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;

                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '27. MERGE into DIM_PRODUCT', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
        END IF; -- V_SQL_OK    
 

               
/*********************************************************************/                                    
            /*README: 
            Get ROWS PROCESSED row count for Hub Load job via row count of Hub Load Table.
            */
        
        --reset SQL status messages for the next calls
        SET V_SQL_CODE = 0;
        SET V_SQL_STATE = 0;
        SET V_SQL_MSG = 0;    
                        
        
        IF V_SQL_OK THEN            
            
            --Target Table Row Count (TOTAL ROWS)
            SET v_str_sql_cursor = 'SELECT COUNT(1) FROM ' ||  v_target_database_name ||'.'|| v_target_table_name || ' 
                                    WHERE ETL_MODIFIED_BY_JOB_ID = ' || v_stored_procedure_execution_id || ';'; 
                        
            PREPARE c_table 
            FROM v_str_sql_cursor; 
            OPEN c_table_cursor; 
            FETCH c_table_cursor INTO p_processed_rows; 
            CLOSE c_table_cursor;     
        
           --Debugging Logging
            IF (V_SQL_CODE < 0) THEN  --  Warning
                SET V_SQL_OK = FALSE;
                SET V_RETURN_STATUS = V_SQL_CODE;
            END IF;
                
            GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
            IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <COUNT TOTAL ROWS> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql_cursor) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                  IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                     THEN SET v_sql_logging_str = v_str_sql_cursor;
                     ELSE SET v_sql_logging_str = 'no sql logged';
                  END IF;
                   
                  CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '28. COUNT TOTAL RECORDS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
            END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
 
           
        
            --Target Table Row Count (NEW INSERT ROWS)
            SET v_str_sql_cursor = 'SELECT COUNT(1) FROM ' ||  v_target_database_name ||'.'|| v_target_table_name || ' 
                                    WHERE ETL_MODIFIED_BY_JOB_ID = ' || v_stored_procedure_execution_id || '
                                    AND ETL_CREATE_TIMESTAMP >= ' || quote_literal(v_job_execution_starttime) || '
                                    ;'; 
                        
            PREPARE c_table 
            FROM v_str_sql_cursor; 
            OPEN c_table_cursor; 
            FETCH c_table_cursor INTO p_processed_insert_rows; 
            CLOSE c_table_cursor;     
           
            IF (V_SQL_CODE < 0) THEN  --  Warning
                SET V_SQL_OK = FALSE;
                SET V_RETURN_STATUS = V_SQL_CODE;
            END IF;
                
            GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
           --Debugging Logging    -- p_processed_insert_rows, p_processed_update_rows
            IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <COUNT INSERT ROWS> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql_cursor) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                   THEN SET v_sql_logging_str = v_str_sql_cursor;
                   ELSE SET v_sql_logging_str = 'no sql logged';
                END IF;

                CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '29. COUNT INSERT RECORDS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
            END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
      
        
            --Target Table Row Count (UPDATED ROWS)
            SET v_str_sql_cursor = 'SELECT COUNT(1) FROM ' ||  v_target_database_name ||'.'|| v_target_table_name || ' 
                                    WHERE ETL_MODIFIED_BY_JOB_ID = ' || v_stored_procedure_execution_id || '
                                    AND ETL_CREATE_TIMESTAMP <= ' || quote_literal(v_job_execution_starttime) || '
                                    AND ETL_UPDATE_TIMESTAMP > ' || quote_literal(v_job_execution_starttime) || '
                                    ;'; 
                        
            PREPARE c_table 
            FROM v_str_sql_cursor; 
            OPEN c_table_cursor; 
            FETCH c_table_cursor INTO p_processed_update_rows; 
            CLOSE c_table_cursor;     

           IF (V_SQL_CODE < 0) THEN  --  Warning
                SET V_SQL_OK = FALSE;
                SET V_RETURN_STATUS = V_SQL_CODE;
            END IF;
                
            GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;
              
           --Debugging Logging    -- p_processed_insert_rows, p_processed_update_rows
            IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <COUNT UPDATE ROWS> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql_cursor) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                   THEN SET v_sql_logging_str = v_str_sql_cursor;
                   ELSE SET v_sql_logging_str = 'no sql logged';
                END IF;

                CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '30. COUNT UPDATE RECORDS', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
            END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 

           --Debugging Logging
       --     SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql_cursor) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;

           --Debugging Logging
            --SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal('Last Step before Done') || ');';  EXECUTE IMMEDIATE v_str_sql_debug;
            
            --Procedure Completed
            SET v_error_message =  'Stored Procedure Message: COMPLETED/SUCCESS > ' || v_hub_procedure_name || ' > JOB EXECUTION ID: ' || v_stored_procedure_execution_id || ' > Total Rows Processed: ' || p_processed_rows || '.';                  
            --Populate EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION
            SET v_job_phase_id = 0;
            SET v_str_sql = 'CALL ' || v_process_database_name || '.P_LOG_JOB_H0( ' || quote_literal(v_job_execution_name) || ',' || v_stored_procedure_execution_id ||','|| quote_literal(v_error_message) || ',' || v_job_phase_id || ',' || p_parent_job_execution_id || ',' || p_source_table_id || ',' || quote_literal(v_target_table_name) || ',' || p_processed_rows || ',' || p_failed_rows || ',' || p_processed_insert_rows || ',' || p_processed_update_rows || ',' || p_processed_delete_rows || ');'; 
            SET o_str_debug =  v_str_sql;      

            --Debugging Logging
            --SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal('done') || ');';  EXECUTE IMMEDIATE v_str_sql_debug;

            EXECUTE IMMEDIATE  v_str_sql;
            GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

            IF (V_SQL_CODE < 0) THEN  --  Warning
                SET V_SQL_OK = FALSE;
                SET V_RETURN_STATUS = V_SQL_CODE;
            END IF;
          
    
            IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

                SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <P_LOG_JOB_H0> '||
                      '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                      '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                      ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                      ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                      ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                   THEN SET v_sql_logging_str = v_str_sql;
                   ELSE SET v_sql_logging_str = 'no sql logged';
                END IF;

                CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '31. call P_LOG_JOB_H0 final totals', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
            END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
        ELSE -- v_SQL_OK is FALSE - need TO CLOSE WITH failure
           SET v_error_message = 'FAILED - STORED PROCEDURE MESSAGE: PROCEDURE FAILED: '|| v_return_status;
           SET v_job_phase_id = 1; -- Error State
           SET v_str_sql = 'CALL ' || v_process_database_name || '.P_LOG_JOB_H0( ' || quote_literal(v_job_execution_name) || ',' || v_stored_procedure_execution_id ||','|| quote_literal(v_error_message) || ',' || v_job_phase_id || ',' || p_parent_job_execution_id || ',' || p_source_table_id || ',' || quote_literal(v_target_table_name) || ',' || p_processed_rows || ',' || p_failed_rows || ',' || p_processed_insert_rows || ',' || p_processed_update_rows || ',' || p_processed_delete_rows || ');'; 

          EXECUTE IMMEDIATE v_str_sql;

          GET DIAGNOSTICS V_RCOUNT = ROW_COUNT;

           IF (V_SQL_CODE < 0) THEN  --  Warning
               SET V_SQL_OK = FALSE;
               SET V_RETURN_STATUS = V_SQL_CODE;
           END IF;
          
    
           IF ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) THEN

               SET V_SQL_ERR_STR = '<P_LOAD_DIM_PRODUCT> <P_LOG_JOB_H0> '||
                     '< SQLLEN = ' || COALESCE(CAST( LENGTH(v_str_sql) AS VARCHAR),'NSQLLEN') || '> ' ||
                     '< SQLCODE = ' || COALESCE(CAST( V_SQL_CODE AS VARCHAR),'NSQLCD') || '> ' ||
                     ',< SQLSTATE = ' || COALESCE(CAST( V_SQL_STATE AS VARCHAR),'NSQLST') || '> ' ||
                     ',< SQL_MSG = ' || COALESCE(CAST( V_SQL_MSG AS VARCHAR),'NSQLMSG') || '> ' ||
                     ',< ROW_COUNT = ' || COALESCE(CAST( V_RCOUNT AS VARCHAR),'NRCNT') || '> ';

                IF ((V_SQL_CODE <> 0) OR (v_log_level >= 2)) 
                   THEN SET v_sql_logging_str = v_str_sql;
                   ELSE SET v_sql_logging_str = 'no sql logged';
                END IF;

                CALL LOCAL_LOG_STATEMENT(nvl(v_stored_procedure_execution_id,-1), '32. call P_LOG_JOB_H0 failure', V_SQL_ERR_STR, v_sql_logging_str, current_timestamp);
               
            END IF; -- ((V_SQL_CODE <> 0) OR (v_log_level >= 1)) 
        END IF; -- V_SQL_OK    
        COMMIT;
        RETURN V_RETURN_STATUS; 
       

END BASIC_START;

END --procedure

--@

--commit