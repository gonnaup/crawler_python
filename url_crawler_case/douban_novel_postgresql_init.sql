drop table if exists crawl_doubannovel_progress;
create table crawl_doubannovel_progress (
	id integer not null,
	progress integer not null,
	constraint crawl_doubannovel_progress_pk primary key (id)
);
insert into crawl_doubannovel_progress values (1, 1)