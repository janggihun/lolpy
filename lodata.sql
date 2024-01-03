create schema mydb;
use mydb;

CREATE USER 'icia'@'%' identified by '1234';

GRANT ALL PRIVILEGES ON *.* to 'icia'@'%';
flush privileges;
SELECT Host,User,plugin,authentication_string FROM mysql.user;


drop table lol_datas;
desc lol_datas;
commit;
select * from lol_datas;

alter table lol_datas add tier varchar(10) ;
drop table cn_kr;
create table cn_kr(

cnt int primary key,
championName varchar(10)

);

select * from cn_kr;

-- insert into cn_kr value( 910 ,'흐웨이');

select * from lol_datas order by gameId;

select *
from lol_datas l
left join cn_kr c 
on l.ban = c.championId;

drop view lol_table;
create or replace view lol_table as
select *
from lol_datas l
join cn_kr c 
on l.ban = c.championId
order by gameId;


select * from lol_table ;





select * from cn_en;


select count(*) from lol_datas;


create or replace view de as select gameId, count(gameId)  from lol_datas group by gameId having count(gameId) != 10;

select COUNT(*) from lol_datas;

select gameId,count(gameId)  from lol_datas group by gameId
having count(gameId) != 10; 