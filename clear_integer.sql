-- Creacion de la función

CREATE FUNCTION keepcoding.clean_integer (pn_string STRING) RETURNS STRING
AS ((SELECT CASE WHEN (pn_string)='NULL' THEN '-999999' ELSE pn_string END));


SELECT keepcoding.clean_integer('NULL');