CREATE DATABASE vgdb;
CREATE USER 'vguser' IDENTIFIED WITH mysql_native_password BY 'vgpwd';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE , INDEX, DROP, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES , EXECUTE ON vgdb.* TO 'vguser';
GRANT FILE ON *.* TO 'vguser';
GRANT CREATE ROUTINE, ALTER ROUTINE ON vguser.* to 'vguser';
--
CREATE TABLE `symbols` (
  `SYMBOL` varchar(16) NOT NULL COMMENT 'Contains al symbols loaded from constituents file across all dates.',
  PRIMARY KEY (`SYMBOL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='CONTAINS SYMBOLS FROM CONSTITUENT PICKLE FILE';

CREATE TABLE `symhistory` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `HISTDATE` datetime DEFAULT NULL,
  `SYMBOL` varchar(16) NOT NULL COMMENT 'Foreign Key to SYMBOLS table',
  `OPENPRICE` double DEFAULT NULL,
  `HIGHPRICE` double DEFAULT NULL,
  `LOWPRICE` double DEFAULT NULL,
  `CLOSEPRICE` double DEFAULT NULL,
  `VOLUME` bigint(20) DEFAULT NULL,
  `DIVIDENDS` double DEFAULT NULL,
  `STOCKSPLITS` double DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `FK_SYMBOL_idx` (`SYMBOL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Contains symbol data from Yahoo finance';
