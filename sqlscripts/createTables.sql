USE vgdb;

CREATE TABLE IF NOT EXISTS `symbols` (
  `SYMBOL` varchar(16) NOT NULL COMMENT 'Contains al symbols loaded from constituents file across all dates.',
  `SECTOR` varchar(45) NULL,
  `NAME` varchar(100)  NULL,
  `EXCHANGE` varchar(20)  NULL,
  PRIMARY KEY (`SYMBOL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='CONTAINS SYMBOLS FROM CONSTITUENT PICKLE FILE';


CREATE TABLE  IF NOT EXISTS `symhistory` (
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


CREATE TABLE  IF NOT EXISTS `sectorweight` (
  `DATE` datetime NOT NULL,
  `SECTORNAME` varchar(100) NOT NULL,
  `TOTALCONSTITUENTS` bigint(20) DEFAULT NULL,
  `SECTORPRICE` double DEFAULT NULL,
  `SECTORWINDEX` double DEFAULT NULL COMMENT 'This will be the sector weight index for that day.\nCalculated as sectorprice divided by total constituents.\n',
  PRIMARY KEY (`DATE`,`SECTORNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Calculate the sector distribution within the index factoring their relative weights';

