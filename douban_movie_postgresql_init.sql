-- crawl progress record table
drop table if exists crawl_doubanmovie_progress;
create table crawl_doubanmovie_progress (
	id integer not null,
	progress integer not null,
	constraint crawl_doubanmovie_progress_pk primary key (id)
);
comment on column crawl_doubanmovie_progress.progress is '爬取进度';

insert into crawl_doubanmovie_progress values (1, 0);

-- crawl status table
drop table if exists crawl_doubanmovie_status;
create table crawl_doubanmovie_status(
	progress integer not null,
	status boolean default false not null,
	constraint crawl_doubanmovie_status_pk primary key (progress)
);
comment on column crawl_doubanmovie_status.progress is '爬取进度';
comment on column crawl_doubanmovie_status.status is '爬取状态，true：爬取成功，false：爬取失败';

-- movie data table
drop table if exists crawl_doubanmovie_data;
create table crawl_doubanmovie_data (
	id integer not null,
	title varchar(50) not null,
	rate decimal(2, 1) default 0.0 not null,
	directors varchar(200),
	casts varchar(500),
	url varchar(200),
	star integer,
	cover varchar(200),
	cover_x integer,
	cover_y integer,
	progress integer not null,
	constraint crawl_doubanmovie_data_pk primary key (id)
);
create index idx_crawl_doubanmovie_data_title on crawl_doubanmovie_data (title);
create index idx_crawl_doubanmovie_data_rate on crawl_doubanmovie_data (rate);
comment on column crawl_doubanmovie_data.title is '电影名称';
comment on column crawl_doubanmovie_data.rate is '豆瓣评分';
comment on column crawl_doubanmovie_data.directors is '导演';
comment on column crawl_doubanmovie_data.casts is '主要演员';
comment on column crawl_doubanmovie_data.url is '豆瓣电影详细信息页面url';
comment on column crawl_doubanmovie_data.cover is '电影封面图片url';
comment on column crawl_doubanmovie_data.progress is '所属爬取进度';