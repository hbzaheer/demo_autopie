------------- PREPARE DATASETS -------------------
SET client_min_messages TO WARNING;

drop schema if exists testsatmap cascade;
create schema testsatmap;

drop table if exists testsatmap.calltable;

create table testsatmap.calltable
(
	call_id varchar(16),
	call_start_epoch bigint,
	call_duration int,
	customer_id varchar(100),
	agent_id varchar(20)
);

insert into testsatmap.calltable values ('00000001', 1600000000, 45, 'c1', 'alice');
insert into testsatmap.calltable values ('00000002', 1600000100, 45, 'c2', 'alice');
insert into testsatmap.calltable values ('00000003', 1600000200, 35, 'c3', 'alice');

insert into testsatmap.calltable values ('00000004', 1600000000, 30, 'c4', 'bob');
insert into testsatmap.calltable values ('00000005', 1600000050, 90, 'c5', 'bob');
insert into testsatmap.calltable values ('00000006', 1600000250, 30, 'c6', 'bob');

insert into testsatmap.calltable values ('00000007', 1600000010, 60, 'c7', 'charlie');
insert into testsatmap.calltable values ('00000008', 1600000090, 30, 'c8', 'charlie');
insert into testsatmap.calltable values ('00000009', 1600000150, 60, 'c9', 'charlie');

insert into testsatmap.calltable values ('00000010', 1600100000, 60, 'c10', 'alice');


drop table if exists testsatmap.calldispositions;

create table testsatmap.calldispositions
(
	disposition_epoch bigint,
	customer_id varchar(100),
	agent_id varchar(20),
	disp_code varchar(10)
);

insert into testsatmap.calldispositions values (1600000040, 'c1', 'alice', 'k');
insert into testsatmap.calldispositions values (1600000150, 'c2', 'alice', 'k');
insert into testsatmap.calldispositions values (1600000210, 'c3', 'alice', 'm');

insert into testsatmap.calldispositions values (1600000020, 'c4', 'bob', 'a');
insert into testsatmap.calldispositions values (1600000040, 'c5', 'bob', 'k');
insert into testsatmap.calldispositions values (1600000270, 'c6', 'bob', 'k');

insert into testsatmap.calldispositions values (1600000040, 'c7', 'charlie', 'k');
insert into testsatmap.calldispositions values (1600000120, 'c8', 'charlie', 'm');
insert into testsatmap.calldispositions values (1600000200, 'c9', 'charlie', 'a');

insert into testsatmap.calldispositions values (1600100020, 'c10', 'alice', 'k');

drop table if exists testsatmap.outcomes;

create table testsatmap.outcomes
(
	outcome_date date,
	customer_id varchar(100),
	outcome_code varchar(10)
);

insert into testsatmap.outcomes values (to_date('2020-09-14', 'YYYY-MM-DD'), 'c1', '100');
insert into testsatmap.outcomes values (to_date('2020-09-15', 'YYYY-MM-DD'), 'c1', '10');
insert into testsatmap.outcomes values (to_date('2020-09-15', 'YYYY-MM-DD'), 'c2', '200');
insert into testsatmap.outcomes values (to_date('2020-09-15', 'YYYY-MM-DD'), 'c6', '100');
insert into testsatmap.outcomes values (to_date('2020-09-17', 'YYYY-MM-DD'), 'c7', '100');

drop table if exists testsatmap.crm;

create table testsatmap.crm
(
	data_date date,
	customer_id varchar(100),
	customer_cat_attrib varchar(10),
	customer_num_attrib numeric
);

insert into testsatmap.crm values (to_date('2020-09-10', 'YYYY-MM-DD'), 'c1', 'premium', '500');
insert into testsatmap.crm values (to_date('2020-09-10', 'YYYY-MM-DD'), 'c2', 'gold', '300');
insert into testsatmap.crm values (to_date('2020-08-10', 'YYYY-MM-DD'), 'c2', 'gold', '310');

insert into testsatmap.crm values (to_date('2020-07-10', 'YYYY-MM-DD'), 'c4', 'silver', '400');
insert into testsatmap.crm values (to_date('2020-09-10', 'YYYY-MM-DD'), 'c4', 'silver', '500');
insert into testsatmap.crm values (to_date('2020-09-10', 'YYYY-MM-DD'), 'c5', 'gold', '500');
insert into testsatmap.crm values (to_date('2020-09-10', 'YYYY-MM-DD'), 'c6', 'premium', '500');

insert into testsatmap.crm values (to_date('2020-09-01', 'YYYY-MM-DD'), 'c7', 'silver', '100');
insert into testsatmap.crm values (to_date('2020-09-01', 'YYYY-MM-DD'), 'c8', 'silver', '100');
insert into testsatmap.crm values (to_date('2020-09-01', 'YYYY-MM-DD'), 'c9', 'silver', '100');

drop table if exists testsatmap.product_ownership;

create table testsatmap.product_ownership
(
	customer_id varchar(100),
	product_id varchar(100),
	subscription_date date,
	deactivation_date date
);

insert into testsatmap.product_ownership values ('c1', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-05', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c1', 'p2', to_date('2020-09-02', 'YYYY-MM-DD'), to_date('2020-09-20', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c2', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-20', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c2', 'p2', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-30', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c2', 'p3', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-10-01', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c3', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-10', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c4', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-15', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c5', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-25', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c6', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-15', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c6', 'p2', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-25', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c7', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-02', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c7', 'p2', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-15', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c8', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-25', 'YYYY-MM-DD'));
insert into testsatmap.product_ownership values ('c9', 'p1', to_date('2020-09-01', 'YYYY-MM-DD'), to_date('2020-09-05', 'YYYY-MM-DD'));
