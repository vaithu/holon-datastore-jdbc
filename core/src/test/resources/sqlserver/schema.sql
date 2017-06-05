create table test1 (
	 keycode bigint primary key 
	,strv varchar(100) 
	,decv numeric(16,4) 
	,datv date 
	,datv2 date 
	,enmv int 
	,nbv int not null
	,nst1 varchar(10) 
	,nst2 numeric(16,6)
	,nss1 varchar(10) 
	,tms datetime 
	,tms2 datetime 
	,tm time
	,clb text
	,blb varbinary(100)
);

create table test2 (
	 code int NOT NULL IDENTITY (1,1) PRIMARY KEY
	,text varchar(100) not null
);

create table test3 (
	 code bigint not null primary key
	,text varchar(100) not null
);
