CREATE OR REPLACE FUNCTION mask_email(value STRING)
RETURNS STRING
RETURN regexp_replace(value, '(^.).*(@.*$)', '$1***$2');
