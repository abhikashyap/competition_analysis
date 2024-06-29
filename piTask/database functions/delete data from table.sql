CREATE OR REPLACE FUNCTION delete_data_for_account(account_name_param VARCHAR)
RETURNS VOID AS
$$
BEGIN
    DELETE FROM your_table_name
    WHERE account_name = account_name_param;
END;
$$
LANGUAGE plpgsql;
