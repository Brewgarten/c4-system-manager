create table key_value_store
(
key		text primary key,
value	text
);

create table history
(
node		text,
name		text,
timestamp	timestamp,
status		text,

primary key (node, name, timestamp)
);

create table status
(
node		text,
name		text,
status		text,

primary key (node, name)
);

create table t_sm_latest
(
node                varchar(50),
name                varchar(50),
type                text,
details             text,

primary key (node, name)
);


create table t_sm_history
(
history_date        timestamp not null,
node                varchar(50),
name                varchar(50),
type                text,
details             text,

primary key (history_date, node, name)
);

-- Cluster configuration info like
-- node, node address, and devices are stored here
create table t_sm_configuration
(
id                  integer primary key,
parent_id           integer default null,
name                varchar(50),
state               text,
type                text,
details             varchar(1000)
);

create table t_sm_configuration_alias
(
alias				varchar(50) primary key,
node_name			varchar(50)
);

create table t_sm_policies
(
id                  integer primary key,
parent_id           integer,
name                text,
representation      text,
hash                integer,
state               text,
type                text,
properties          text
);

create table t_sm_platform
(
property			varchar(50),
value				varchar(1000)
);

create table t_sm_version
(
node                varchar(50),
name                varchar(50),
type				varchar(100),
version             varchar(50),

primary key (node, name, type)
);

CREATE TABLE t_sm_alerts
(
id                  integer primary key,
details             text
);
