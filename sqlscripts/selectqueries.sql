select 'SYMBOLS' as TABLENAME, count(*) as NbrOfRows from symbols;
select 'SYMHISTORY' as TABLENAME, count(*) as NbrOfRows  from symhistory;
select 'SECTORWEIGHT' as TABLENAME, count(*) as NbrOfRows  from sectorweight;
select 'REFDATA' as TABLENAME, count(*) from vgdb.refdata;

select * from vgdb.symbols;
select * from vgdb.symhistory;
select * from vgdb.sectorweight;
select * from vgdb.refdata;

