Drop table IF exists keywords;
drop table IF exists result;


Create table keywords (time string, DeviceModel string, Keyword string, Counts int) row format delimited fields terminated by ',' lines terminated by '\n';


Create external table result (number int, keyword string, counts int) row format delimited fields terminated by ','

tblproperties("skip.header.line.count" = "1");


load data inpath 'keywords.csv' overwrite into table result;

select * from result;

insert into table keywords select from_unixtime(unix_timestamp()) as date, 'Samsung Galaxy s7' as Device, keyword, counts from result;

select * from keywords;
