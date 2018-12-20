-- Upgrading old partitioning with ALTER TABLEs and PLPGSQL.
-- IN: _orig_table - master table which should be upgraded
-- IN: _partkey - column which used as partition key
-- IN: _seq_col - sequence column

CREATE OR REPLACE FUNCTION fn_upgrade_partitioning(_schema_name text, _orig_table text, _partkey text default '', column_type text default '', partition_type text default '') RETURNS void AS 
$function$
DECLARE 
        _new_table text = _orig_table ||'_new';         -- parent relation's name 
        _child_table text;                              -- child relation's name 
        _v_from text; 
        _v_to text; 
        _seq_name text;
        _seq_col text;
        p_type_char text;
        p_type_c text;
        _partition_method text;
        is_schema text;
        fk record;
BEGIN
    -- Checks table is already NOT converted and partition key column is present for partition
    _partition_method = (select pc.relname from pg_partitioned_table pt , pg_class pc where pt.partrelid=pc.oid AND pc.relname=_orig_table);
    IF (( _partition_method IS NULL ) OR ( _partition_method = '' )) AND (( _partkey != '' ) OR ( _partkey IS NOT NULL )) THEN 

        -- create new table and attach existing sequence

        EXECUTE format($$CREATE TABLE IF NOT EXISTS %I.%I (LIKE %I.%I including ALL excluding INDEXES) PARTITION BY %I (%I)$$, _schema_name,_new_table,_schema_name, _orig_table, partition_type,_partkey);
        -- RAISE NOTICE '%.%', _schema_name, _orig_table;

        FOR _seq_name IN (SELECT split_part((SELECT column_default FROM information_schema.columns WHERE table_schema=_schema_name AND table_name=_orig_table AND column_default LIKE '%nextval%')::VARCHAR,'''',2))
        LOOP
            IF ( _seq_name = '' ) IS FALSE THEN
                SELECT column_name FROM information_schema.columns WHERE table_schema=_schema_name AND table_name=_orig_table AND column_default LIKE '%nextval%' INTO _seq_col;
                EXECUTE format($$ALTER SEQUENCE %s OWNED BY %I.%I.%I$$, _seq_name, _schema_name,_new_table, _seq_col); 
                EXECUTE format($$ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT nextval('%s')$$, _schema_name, _new_table, _seq_col, _seq_name);
            END IF; 
        END LOOP;

        -- FOR fk IN (select conname, pg_get_constraintdef(pc.oid) as consrc from pg_constraint pc, pg_class pg , pg_namespace pn where pc.contype = 'f' and pc.conrelid = pg.oid AND pg.relnamespace=pn.oid AND pn.nspname=_schema_name AND pg.relname=_orig_table)
        -- LOOP
        --     EXECUTE format($$ALTER TABLE %I.%I ADD CONSTRAINT %I %s$$, _schema_name, _new_table,fk.conname, fk.consrc);
        --     RAISE NOTICE '% having foreign key constraint %', _orig_table, fk;
        -- END LOOP;

        -- loop over partitions 
        FOR _child_table IN (SELECT c.relname FROM pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid) JOIN pg_class AS p ON (inhparent=p.oid) JOIN pg_namespace pn ON p.relnamespace=pn.oid WHERE pn.nspname=_schema_name AND p.relname=_orig_table order by 1) 
        LOOP
            p_type_char = (SELECT regexp_match((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f'), ''''));
            
            -- calculate FROM and TO values and attach partition  

            IF ( p_type_char IS NOT NULL ) OR ( p_type_char != '' ) THEN
                p_type_c = (SELECT substring((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f') from ''''));

                IF ( p_type_c != '' ) OR ( p_type_c IS NOT NULL ) THEN
                    SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 2) INTO _v_from;
                    SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 4) INTO _v_to;
        
                    IF (( _v_to ='' ) OR ( _v_to IS NULL)) AND partition_type='RANGE' THEN
                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 1), '>=',2))) INTO _v_from;
                        SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 2) INTO _v_to;
                    END IF;

                ELSE
                    SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 1), '>=',2))) INTO _v_from;

                    SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 2), '<',2))) INTO _v_to;

                END IF;
            ELSE
                SELECT trim ( both '])' from (SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '[', 2))) INTO _v_from;

                IF ( _v_from ='' ) OR ( _v_from IS NULL)  THEN
                    p_type_c = (SELECT substring((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f') from '>='));
                    IF ( p_type_c != '' ) OR ( p_type_c IS NOT NULL ) THEN
                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 1), '>=',2))) INTO _v_from;

                        SELECT trim( both '() ' from (split_part(split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, 'AND', 2), '<',2))) INTO _v_to;
                    ELSE
                        SELECT split_part((SELECT consrc FROM pg_constraint pc , pg_class c, pg_namespace pn WHERE pc.conrelid=c.oid AND pc.contype='c' AND c.relnamespace=pn.oid AND pn.nspname=_schema_name AND c.relname=_child_table and pc.conislocal='t' and pc.connoinherit='f')::VARCHAR, '''', 2) INTO _v_from;
                        _v_to = '';
                    END IF;
                END IF;
            END IF;


            IF ( _v_from != '' ) AND ( _v_to != '' ) THEN 
                -- detach partition 
                EXECUTE format('ALTER TABLE %I.%I NO INHERIT %I.%I', _schema_name, _child_table, _schema_name,_orig_table); 
                -- RAISE NOTICE '%TO%',range_start,range_end;
                -- EXECUTE format($$ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM ('%s'::%s) TO ('%s'::%s)$$, _schema_name, _new_table, _schema_name, _child_table, _v_from, column_type, _v_to, column_type); 
                EXECUTE format($$ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM ('%s') TO ('%s')$$, _schema_name, _new_table, _schema_name, _child_table, _v_from, _v_to); 
                RAISE NOTICE '% reattached from % to %', _child_table, _orig_table, _new_table; 
            ELSE
                -- detach partition
                EXECUTE format('ALTER TABLE %I.%I NO INHERIT %I.%I', _schema_name, _child_table, _schema_name,_orig_table);
                IF ( column_type = 'text' ) OR ( column_type = 'text' ) THEN 
                    EXECUTE format($$ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN ('%s')$$, _schema_name, _new_table, _schema_name, _child_table, _v_from);
                ELSE
                    EXECUTE format($$ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%s)$$, _schema_name, _new_table, _schema_name, _child_table, _v_from);
                END IF;
                -- RAISE NOTICE '% NOT upgraded from % to %', _child_table, _orig_table, _new_table;
            END IF;
        END LOOP; 

        -- drop old parent table and rename new one 
        EXECUTE format('DROP TABLE %I.%I', _schema_name, _orig_table); 
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', _schema_name, _new_table, _orig_table); 
        RAISE NOTICE 'partitioning for % has been upgraded.', _orig_table; 
    ELSE
        RAISE NOTICE ' % Already partitioned in declarative way or partition column is not present on parent table.', _orig_table;
    END IF;
END; 
$function$ LANGUAGE plpgsql;

