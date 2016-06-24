# http://rpbouman.blogspot.com/2006/11/mysql-stored-procedure-to-create.html
# http://rpbouman.blogspot.com/2007/02/updated-procedure-for-creating-mysql.html
# Site hosting code was gone, recovered via
#    http://web.archive.org/web/20070509200118/http://forge.mysql.com/snippets/download.php?id=54
# and re-hosted here.


delimiter $$

drop  procedure if exists 
p_create_federated_table
$$
create procedure 
p_create_federated_table

(
    -- the ip address or name of the remote mysql server host (if NULL, 'localhost')
    p_remote_host     varchar(32)   
    -- the port where the remote  mysql server is listening (if NULL, 3306)
,   p_remote_port     int unsigned  
    -- the user on the remote server that accesses the table (if NULL, 'root') 
,   p_remote_user     varchar(16)   -- 
    -- the password for the remote user (if NULL, omitted)
,   p_remote_password varchar(32)
    -- the schema in which the remote table resides
,   p_remote_schema   varchar(64)
    -- the name of the remote table
,   p_remote_table    varchar(64)
    -- the local schema to create the local FEDERATED table (if NULL, p_remote_schema)
,   p_local_schema    varchar(64)
    -- the name of the local FEDERATED table (if NULL, p_remote_table)
,   p_local_table     varchar(64)
)
LANGUAGE SQL
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY INVOKER
COMMENT 'Creates a FEDERATED table.'
/*
    Changelog

    WHEN?      WHO? WHAT?
    -----------------------------------------------------------
    2007-02-20 RPB  added handler to ignore warning 1366. Not sure why this appears.
                    lowered the group_concat_max_len (bug #23856)
                    removed the ORDER BY clauses from the GROUP_CONCATs on COLUMN_TYPE (bug #23856)
                    added output so we can see what's taking so long
                    added changelog
    2006-11-20 RPB  Created Initial version  
*/ 
begin
    -- size used for the GROUP_CONCAT buffer if current is lower
    -- please see http://bugs.mysql.com/bug.php?id=23856
    declare v_group_concat_max_len smallint 
        default 16384;
    -- stores the original size of the GROUP_CONCAT buffer to restore it
    declare v_old_group_concat_max_len int unsigned
        default @@group_concat_max_len;
    -- stores the original sql_mode
    declare v_old_sql_mode varchar(255)
        default @@sql_mode;
    -- Used to drop temporary tables
    declare v_drop_table_name varchar(64);

    -- set the GROUP_CONCAT buffer sufficiently large
    set @@group_concat_max_len := greatest(
        v_group_concat_max_len
    ,   v_old_group_concat_max_len
    );
    -- set the sql_mode to default to prevent invalid column defaults
    set @@sql_mode := '';

    -- The following block contains all the meat
    -- We use a separate block to allow for proper error handling.
    -- All errors that might be expected are handled inside this block
    -- This should guarantee that the outer block is always completed.
    -- That is necessary, beause we need to do a little cleaning up 
    -- before exiting the procedure.
    begin
        -- Used for defaulting the specified host
        declare v_remote_host     varchar(32) 
            default coalesce(p_remote_host,'localhost');
        -- Used for defaulting the specified user
        declare v_remote_user     varchar(16) 
            default coalesce(p_remote_user,'root');
        -- Used for defaulting the local schema
        declare v_local_schema    varchar(64)
            default coalesce(p_local_schema,p_remote_schema);
        -- Used for defaulting the local schema
        declare v_local_table     varchar(64)
            default coalesce(p_local_table,p_remote_table);
        -- Holds the connectstring prefix for FEDERATED tables
        declare v_connectstring   varchar(255)
            default concat(
                'mysql://'
            ,   v_remote_user
            ,   if(p_remote_password is null
                ,   ''
                ,   concat(':',p_remote_password)
                )
            ,   '@'
            ,   v_remote_host
            ,   if(p_remote_port is null
                ,   ''
                ,   concat(':',p_remote_port)
                )
            ,   '/'
            );

        -- Various conditions we might encounter
        -- We rename them just for clarity
        declare TABLE_EXISTS_ERROR condition FOR 1050;
        declare UNKNOWN_COLUMN_ERROR condition FOR 1054;
        declare SYNTAX_ERROR condition FOR 1064;
        declare GROUP_CONCAT_TRUNCATION_ERROR condition FOR 1260;
        declare INCORRECT_VALUE_ERROR condition FOR 1366;
        declare TRUNCATION_ERROR condition FOR 1406;
        declare FEDERATION_ERROR condition FOR 1429;

        -- Various handlers. These will execute if one of the conditions occur.
        -- They all show a friendly error message and the exit the inner block.
        -- Execution is resumed at the clean up code, just before 
        -- the end of the proceudre
        declare exit handler for FEDERATION_ERROR 
            select  'Federation error' error_type
            ,       'Check the connectstring.' error_message
            ,       v_connectstring connectstring
            ;
        declare exit handler for GROUP_CONCAT_TRUNCATION_ERROR 
            select  'GROUP_CONCAT Truncation' error_type
            ,       'Increase GROUP_CONCAT_MAX_LEN.' error_message
            ,      @@group_concat_max_len group_concat_max_len
            ;
/*
        declare exit handler for SYNTAX_ERROR 
            select 'Syntax Error' error_type
            ,      'Check this statement.' error_message
            ,      @create_table_statment statement
            ;
*/
        declare exit handler for TABLE_EXISTS_ERROR 
            select 'Table Exists' error_type
            ,      'Drop the table first.' error_message
            ,      concat(v_local_schema,'.',v_local_table) table_identifier
            ;
        declare exit handler for UNKNOWN_COLUMN_ERROR 
            select 'Unknown Column' error_type
            ,      'Unexpected error, check the statement.' error_message
            ,      @create_table_statement table_identifier
            ;
        declare exit handler for NOT FOUND 
            select 'No such Table' error_type
            ,      'The requested table was not found on the remote host.' error_message
            ,      concat(p_remote_schema,'.',p_remote_table) table_identifier
            ;
	declare continue handler for INCORRECT_VALUE_ERROR 
            select 'We hit warning 1366. It''s probably nothing serious.'
            ;
        declare exit handler for SQLEXCEPTION
            select 'SQL Exception' error_type
            ,      'Unexpected generic error. Debug the procedure.' error_message
            ,      concat(
                           'CALL ',schema(),'.', p_create_federated_table,'(' 
                   ,       if( p_remote_host is null
                           ,   'NULL'
                           ,   concat('''',p_remote_host,'''')
                           )
                   ,',',   if( p_remote_port is null
                           ,   'NULL'
                           ,   p_remote_port
                           )
                   ,',',   if( p_remote_user is null
                           ,   'NULL'
                           ,   concat('''',p_remote_user,'''')
                           )
                   ,',',   if( p_remote_password is null
                           ,   'NULL'
                           ,   concat('''',p_remote_password,'''')
                           )
                   ,',',   if(
                               p_remote_schema is null
                           ,   'NULL'
                           ,   concat('''',p_remote_schema,'''')
                           )
                   ,',',   if( p_remote_table is null
                           ,   'NULL'
                           ,   concat('''',p_remote_table,'''')
                           )
                   ,',',   if( p_local_schema is null
                           ,   'NULL'
                           ,   concat('''',p_local_schema,'''')
                           )
                   ,',',   if( p_local_table is null
                           ,   'NULL'
                           ,   concat('''',p_local_table,'''')
                           )
                   ,   ')'
                   ) call_command
            ; 

        -- The following block creates temporary federated tables 
        -- on the remote information_schema. 
        -- These are needed to generate the structure 
        -- of the local federated table. 
	select 'Getting remote metadata...';
        begin
            -- Prefix used for the temporary tables
            declare v_temp_table_prefix char(25)
                default 'p_create_federated_table$';
            -- Used to fetch the generated DDL from the cursor
            declare v_create_table_statment text;
            -- Cursor loop control variable
            declare v_no_more_rows boolean
                default FALSE;
            -- Cursor generates DDL for createing temporary federated
            -- tables on the remote information_schema.
            -- We need this to generate the DDL to create the actual 
            -- federated table specified by the user.
            declare csr_metadata cursor for 
                select      table_name
                ,           concat(
                                'create temporary table'
                            ,'\n',schema(),'.'
                            ,v_temp_table_prefix,table_name,'('
                            ,'\n',group_concat(
                                    column_name
                                ,   ' '
                                ,   column_type
                                ,   if(
                                        character_set_name is null
                                    ,   ''
                                    ,   concat(
                                            ' character set '
                                        ,   character_set_name
                                        ,   ' collate '
                                        ,   collation_name
                                        )
                                    )
                                ,   if( is_nullable='NO'
                                    ,   ' NOT NULL'
                                    ,   ''
                                    )
                                    separator '\n,'
                                )
                            ,'\n',')'
                            ,'\n','engine = federated'
                            ,'\n','connection = '
                            ,'\n',''''
                            ,     v_connectstring
                            ,     table_schema
                            ,     '/'
                            ,     table_name
                            ,     '''' 
                            )
                from        information_schema.columns
                where       table_schema = 'information_schema'
                and         table_name IN (
                                'COLUMNS'
                            ,   'STATISTICS'
                            ,   'TABLE_CONSTRAINTS'
                            )
                group by    table_schema
                ,           table_name
                ;
            -- handler to control the cursor loop
            declare continue handler for NOT FOUND 
                set v_no_more_rows := TRUE;

            set @drop_temporary_tables_statement := null;

            -- loop through the cursor
            open csr_metadata;
            my_loop: loop
                -- get the DDL for the temporary federated 
                -- information_schema table
                fetch csr_metadata 
                into  v_drop_table_name
                ,     v_create_table_statment;
		
                -- basic cursor loop control exits if cursor is exhausted
                if v_no_more_rows then
                    close csr_metadata;
                    leave my_loop;
                end if;
                -- build a statement to drop all temporary tables
                set @drop_temporary_tables_statement := if (
                    @drop_temporary_tables_statement is null
                ,   concat(
                        'DROP TEMPORARY TABLE IF EXISTS '
                    ,   v_temp_table_prefix
                    ,   v_drop_table_name
                    )
                ,   concat(
                        @drop_temporary_tables_statement
                    ,   ','
                    ,   v_temp_table_prefix
                    ,   v_drop_table_name
                    )
                );

                -- kludge: need a user variable to execute the DDL string
                -- dynamically with the PREPARE syntax
                set @create_table_statment := v_create_table_statment;

                -- create the temporary federated information_schema table 
                prepare stmt from @create_table_statment;
                execute stmt;
                deallocate prepare stmt;

            end loop;
        end;
	-- Reset the variable. Mainly to simplify debugging
	select 'Generating CREATE TABLE statement for FEDERATED table...';
	set @create_table_statment:='...generating statement...';
        -- This creates the actual ddl for the requested local FEDERATED table.
        -- It selects the DDL directly into the user variable. 
        -- It does this by querying the remote information_schema.
        -- This DDL includes the index definitions of the remote table.
        select      concat(
                        'create table if not exists'
                    ,'\n','`',v_local_schema,'`'
                    ,'.' ,'`',v_local_table,'`'
                    ,     '('
                    ,'\n',column_definitions
                    ,     coalesce(index_definitions,'')
                    ,'\n',')'
                    ,'\n','engine = federated'
                    ,'\n','connection = '
                    ,'\n',''''
                    ,     v_connectstring
                    ,     column_definitions.table_schema
                    ,     '/'
                    ,     column_definitions.table_name
                    ,     '''' 
                    ) stmt
        into        @create_table_statement
        from        (
                    select      table_schema
                    ,           table_name 
                    ,           group_concat(
                                        '`',column_name,'` '
                                    ,   column_type
                                    ,   if(
                                            character_set_name is null
                                        ,   ''
                                        ,   concat(
                                                ' character set '
                                            ,   character_set_name
                                            ,   ' collate '
                                            ,   collation_name
                                            )
                                        )
                                    ,   if( is_nullable='NO'
                                        ,   ' not null'
                                        ,   ''
                                        )
                                    ,   if( column_default is null
                                        ,   ''
                                        ,   concat(
                                                ' default '
                                            ,   case
                                                    when data_type = 'TIMESTAMP' 
                                                    and  column_default = 'CURRENT_TIMESTAMP'
                                                        then column_default
                                                    when data_type like '%char' 
                                                    or   data_type like 'date%'
                                                    or   data_type like 'time%'
                                                    or   data_type in ('set','enum')
                                                        then concat('''',column_default,'''')
                                                    else column_default
                                                end
                                            )
                                        )
                                    ,   if(extra='','',concat(' ',extra))
                                    ,   ' comment '
                                    ,   '''',column_comment,''''                                        
                                        separator '\n,'
                                ) as column_definitions
                    from        p_create_federated_table$columns
                    where       table_schema = p_remote_schema
                    and         table_name   = p_remote_table
                    group by    table_schema
                    ,           table_name
                    ) column_definitions
        left join   (
                    select      index_definitions.table_schema
                    ,           index_definitions.table_name 
                    ,           concat(
                                    '\n,'
                                ,   group_concat(
                                        case c.constraint_type 
                                            when 'PRIMARY KEY' then 
                                                constraint_type
                                            when 'UNIQUE' then 
                                                concat(
                                                    'CONSTRAINT '
                                                ,   constraint_name
                                                ,   ' '
                                                ,   constraint_type
                                                )
                                            else
                                                concat(
                                                    if( index_type in (
                                                            'FULLTEXT'
                                                        ,   'SPATIAL'
                                                        )
                                                    ,   concat(
                                                            index_type
                                                        ,   ' '
                                                        )
                                                    ,   ''
                                                    ) 
                                                ,   if( non_unique
                                                    ,   ''
                                                    ,   'UNIQUE '
                                                    )
                                                ,   'INDEX '
                                                ,   '`',index_name,'`'
                                                )
                                        end
                                    ,   index_columns
                                    ,   if( index_type in (
                                                'BTREE'
                                            ,   'HASH'
                                            )
                                        ,   concat(
                                                ' USING '
                                            ,   index_type
                                            )
                                        ,   ''
                                        )
                                        order by c.constraint_type
                                        separator '\n,'
                                    )
                                )    as index_definitions
                    from        (
                                select      table_schema
                                ,           table_name
                                ,           index_name
                                ,           index_type
                                ,           non_unique
                                ,           concat(
                                                '('
                                            ,   group_concat(
                                                    '`',column_name,'`'
                                                ,   if( sub_part is null
                                                    ,   ''
                                                    ,   concat(
                                                            '(',sub_part,')'
                                                        )
                                                    )
                                                    order by seq_in_index
                                                )
                                            ,   ')'
                                            ) index_columns
                                from        p_create_federated_table$statistics
                                where       table_schema = p_remote_schema
                                and         table_name   = p_remote_table
                                and         index_type not in ('FULLTEXT')
                                group by    table_schema
                                ,           table_name
                                ,           index_name
                                ,           index_type
                                ,           non_unique
                                ) index_definitions
                    left join   (
                                select      table_schema
                                ,           table_name
                                ,           constraint_name
                                ,           constraint_type
                                from        p_create_federated_table$table_constraints c
                                where       table_schema    = p_remote_schema
                                and         table_name      = p_remote_table
                                and         constraint_type in (
                                                'PRIMARY KEY'
                                            ,   'UNIQUE'
                                            )
                                group by    table_schema
                                ,           table_name
                                ,           constraint_name
                                ,           constraint_type
                                ) c
                    on          index_definitions.table_schema = c.table_schema
                    and         index_definitions.table_name   = c.table_name
                    and         index_definitions.index_name   = c.constraint_name
                    group by    table_schema
                    ,           table_name
                    ) index_definitions
        on          column_definitions.table_schema = index_definitions.table_schema
        and         column_definitions.table_name = index_definitions.table_name
        ;

        -- Create the actual FEDERATED table by dynamically executing
        -- the generated DDL for the requested FFEDERATED table. 
	select 'Creating FEDERATED table...';

        prepare stmt from @create_table_statement;
        execute stmt;
        deallocate prepare stmt;

        -- Print a friendly message that we succeeded
        select   'Success' completion_type
        ,        concat(
                        'Created FEDERATED table '
                    ,   v_connectstring,'/',p_remote_schema,'/',p_remote_table
                    ) completion_message
        ,        concat(v_local_schema,'.',v_local_table) table_identifier
        ;
    end;

    -- Cleanup: restore the original sql mode 
    set @@sql_mode := v_old_sql_mode;
    -- Cleanup: restore the original GROUP_CONCAT buffer size
    set @@group_concat_max_len := v_old_group_concat_max_len;

    -- Cleanup: drop the temporary federated information_schema tables.
    prepare stmt from @drop_temporary_tables_statement;
    execute stmt;
    deallocate prepare stmt;

    -- Cleanup: reset the user defined variables.
    set @create_table_statment := null
    ,   @drop_temporary_tables_statement := null
    ;
end;
$$

delimiter ;
