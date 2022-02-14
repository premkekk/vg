DROP USER vguser;
CREATE USER 'vguser' IDENTIFIED WITH mysql_native_password BY 'vgpwd';

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE , INDEX, DROP, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES , EXECUTE ON vgdb.* TO 'vguser';
GRANT FILE ON *.* TO 'vguser';
GRANT CREATE ROUTINE, ALTER ROUTINE ON vgdb.* to 'vguser';
