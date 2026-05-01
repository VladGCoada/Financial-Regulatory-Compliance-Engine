CREATE OR REPLACE FUNCTION mask_name(value STRING)
RETURNS STRING
RETURN concat(substr(value, 1, 1), '***');
