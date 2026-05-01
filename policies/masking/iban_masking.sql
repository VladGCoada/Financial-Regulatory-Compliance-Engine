CREATE OR REPLACE FUNCTION mask_iban(value STRING)
RETURNS STRING
RETURN concat(substr(value, 1, 4), repeat('*', greatest(length(value) - 8, 0)), substr(value, -4));
