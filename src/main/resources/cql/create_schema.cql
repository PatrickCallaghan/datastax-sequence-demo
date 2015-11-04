create keyspace if not exists datastax_sequence_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax_sequence_demo;

create table  if not exists sequence(
    id text PRIMARY KEY,
    sequence int
);

insert into sequence (id, sequence) values ('sequence', 0);