use venkat ;

create table departments_seq
(department_id int, department_name string) row format delimited fields terminated by ',' stored as sequencefile ;

insert into departments_seq select * from departments ;


