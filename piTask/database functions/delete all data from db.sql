CREATE OR REPLACE FUNCTION delete_data_for_account_all_tables(account_name_param VARCHAR)
RETURNS VOID AS
$$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN
        SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE 'DELETE FROM ' || quote_ident(tbl.table_name) || ' WHERE account_name = $1' USING account_name_param;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
