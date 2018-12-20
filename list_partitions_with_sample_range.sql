
CREATE SCHEMA IF NOT EXISTS c_partition;
DROP TYPE c_partition.partition_list CASCADE;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type pt, pg_namespace pn WHERE pn.oid=pt.typnamespace AND pt.typname='partition_list' AND nspname='c_partition') THEN
        CREATE TYPE c_partition.partition_list AS (table_schema text, partition_table text, partition_column text, column_type text, partition_type text, partition_method text, start_range text, end_range text);
    END IF;
END$$;

CREATE OR REPLACE FUNCTION c_partition.list_partitions_with_range() RETURNS SETOF c_partition.partition_list AS 
$function$
DECLARE
_partition_table text;
_partition_schema text;
_child_table text;
_partition_method text;
_v_from text;
_v_to text;
p_type_char text;
p_type_c text;
is_schema text;
result_record c_partition.partition_list%rowtype;
BEGIN 
    -- loop over partitions 
    FOR _partition_schema,_partition_table IN (SELECT distinct   nmsp_parent.nspname AS parent_schema,   parent.relname  AS parent 
        FROM pg_inherits  JOIN pg_class parent  ON pg_inherits.inhparent = parent.oid 
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid   
        JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace) 
    LOOP
        result_record.table_schema = _partition_schema;
        result_record.partition_table = _partition_table;
        _partition_method = (select pc.relname from pg_partitioned_table pt , pg_class pc , pg_namespace pn where pt.partrelid=pc.oid AND pc.relnamespace=pn.oid AND pn.nspname=_partition_schema AND pc.relname=_partition_table);

        IF _partition_method IS NULL THEN
                -- Inheritance partition list code starts here
                _child_table = (SELECT i.inhrelid::regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhparent AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_partition_table ORDER BY 1 limit 1);

                is_schema = (select regexp_match(_child_table,'\.'));
                -- RAISE NOTICE '% is parent of %', _partition_table, _child_table;
 
                IF ( is_schema IS NOT NULL ) OR ( is_schema = '' ) THEN
                    _child_table = (select split_part(_child_table,'.',2));
                END IF;
                -- con_count = (SELECT count(*) FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f');
            
                p_type_char = (SELECT substring((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f') from '>=|<='));

                IF ( p_type_char IS NOT NULL ) OR ( p_type_char != '' ) THEN
                
                    p_type_c = (SELECT substring((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f') from ''''));

                    IF ( p_type_c != '' ) OR ( p_type_c IS NOT NULL ) THEN
                        result_record.partition_column = (SELECT trim(both '(> ' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '>=', 1))::VARCHAR));
                        SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 2) INTO _v_from;
                        SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 4) INTO _v_to;
                    ELSE
                        result_record.partition_column = (SELECT trim(both '(> ' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '>=', 1))::VARCHAR));
            
                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 1), '>=',2))) INTO _v_from;

                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 2), '<',2))) INTO _v_to;

                    END IF;
                ELSE

                    result_record.partition_column = (SELECT trim(both '(> ' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '=', 1))::VARCHAR));
                    SELECT trim ( both '])' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '[', 2))) INTO _v_from;

                    IF ( _v_from ='' ) OR ( _v_from IS NULL)  THEN
                        SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 2) INTO _v_from;
                    END IF;
                    -- SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 4) INTO _v_to;
                    _v_to = '';

                END IF; 
                result_record.column_type = (select data_type from information_schema.columns where table_name = _child_table and column_name=result_record.partition_column limit 1);
                result_record.partition_method = 'inheritance';
                result_record.start_range = _v_from;
                result_record.end_range = _v_to;
                IF ( _v_from != '' ) AND ( _v_to != '' ) THEN
                    result_record.partition_type = 'RANGE';
                ELSIF ( _v_from != '' ) AND ( _v_to = '' ) THEN 
                    -- RAISE NOTICE '% is parent of %', _partition_table, _child_table;
                    -- RAISE NOTICE '_v_from % and _v_to %', _v_from, _v_to;
                    result_record.partition_type = 'LIST';
                ELSE
                    result_record.partition_type = '';
                END IF;        
            
                IF result_record.partition_table IS NOT NULL THEN        
                    RETURN next result_record;
                ELSE
                    RAISE NOTICE 'partition tables are not present'; 
                END IF;    
 
        ELSE
                -- Declarative partition list code starts here
                _child_table = (SELECT i.inhrelid::regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhparent AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_partition_table ORDER BY 1 limit 1);

                is_schema = (select regexp_match(_child_table,'\.'));
                -- RAISE NOTICE '% is parent of %', _partition_table, _child_table;

                IF ( is_schema IS NOT NULL ) OR ( is_schema = '' ) THEN
                    _child_table = (select split_part(_child_table,'.',2));
                END IF;

                p_type_char = (SELECT substring( ( SELECT pg_catalog.pg_get_partition_constraintdef(inhrelid)  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhrelid AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table AND c.relispartition) from '>=|<='));
                IF ( p_type_char IS NOT NULL ) OR ( p_type_char != '' ) THEN
                    -- p_type_c = (SELECT substring((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f') from ''''));
                    p_type_c = (SELECT substring( ( SELECT pg_catalog.pg_get_partition_constraintdef(inhrelid)  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhrelid AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table AND c.relispartition) from ''''));
                    IF ( p_type_c != '' ) OR ( p_type_c IS NOT NULL ) THEN
                        result_record.partition_column = (SELECT trim(both '(> ' from (SELECT split_part((SELECT split_part((SELECT  pg_catalog.pg_get_partition_constraintdef(inhrelid)  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhrelid AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table AND c.relispartition), '>=', 1)), '(', 4)) ));

                        SELECT split_part((SELECT  pg_catalog.pg_get_partition_constraintdef(inhrelid)  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhrelid AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table AND c.relispartition), '''', 2) INTO _v_from;
                        SELECT split_part((SELECT  pg_catalog.pg_get_partition_constraintdef(inhrelid)  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_namespace pn WHERE c.oid=i.inhrelid AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table AND c.relispartition), '''', 4) INTO _v_to;
                    ELSE
                        result_record.partition_column = (SELECT trim(both '(> ' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '>=', 1))::VARCHAR));

                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 1), '>=',2))) INTO _v_from;

                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 2), '<',2))) INTO _v_to;
                    END IF;

                ELSE
                    SELECT trim ( both '])' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '[', 2))) INTO _v_from;
                    IF ( _v_from ='' ) OR ( _v_from IS NULL)  THEN
                        SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 2) INTO _v_from;
                    END IF;
                    _v_to = '';
                    result_record.partition_column = (SELECT trim(both '(> ' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_partition_schema AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '=', 1))::VARCHAR));
                END IF;
 
                IF ( _v_from != '' ) AND ( _v_to != '' ) THEN
                    result_record.partition_type = 'RANGE';
                ELSIF ( _v_from != '' ) AND ( _v_to = '' ) THEN
                    -- RAISE NOTICE '% is parent of %', _partition_table, _child_table;
                    -- RAISE NOTICE '_v_from % and _v_to %', _v_from, _v_to;
                    result_record.partition_type = 'LIST';
                ELSE
                    result_record.partition_type = '';
                END IF;
                result_record.column_type = (select data_type from information_schema.columns where table_name = _child_table and column_name=result_record.partition_column limit 1);
                result_record.partition_method = 'declarative';
                result_record.start_range = _v_from;
                result_record.end_range = _v_to;
                IF result_record.partition_table IS NOT NULL THEN
                    RETURN next result_record;
                ELSE
                    RAISE NOTICE 'partition tables are not present';
                END IF;    
        END IF;

    END LOOP; 
END;
$function$ LANGUAGE plpgsql;



