create schema mydb; -- db생성

use mydb;

CREATE USER 'icia'@'%' identified by '1234'; -- 공유 아이피로 같은 db를 사용하기 위한 권한설정

GRANT ALL PRIVILEGES ON *.* to 'icia'@'%';
flush privileges;
SELECT Host,User,plugin,authentication_string FROM mysql.user;
-- -----------------------------------------------------------------------------------------------------

-- drop table lol_datas;
desc lol_datas;
commit;
select * from lol_datas; --현재 데이터 확인

alter table lol_datas add tier varchar(10) ;
drop table cn_kr;

create table cn_kr(

cnt int primary key,
championName varchar(10)

);

select * from cn_kr; -- 추후에 밴 카드 활용시 밴번호를 한글로 변경하기 위해서 해둠

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

-- 뷰 생성 : 밴 픽 번호를 한글이름으로 확인가능



select * from cn_en;

-- 전체 데이터 확인용
select count(*) from lol_datas;

-- 데이터 얼마나 들어왔는지 확인용
select COUNT(*) from lol_datas where tier = "BRONZE";


-- 전처리용 gameId값이 10개가 맞는지 확인용
select gameId,count(gameId)  from lol_datas group by gameId
having count(gameId) != 10; 



select count(*) from lol_datas where firstpurchased = participantId; -- 가장 빨리 아이템을 구입한 사람의 총 수
select count(*) from lol_datas where firstpurchased = participantId and win = 'True'; -- 가장빨리 아이템을 구입한 사람이 이긴 확률 // 진확률 구할시에는 True >> False로 변경
