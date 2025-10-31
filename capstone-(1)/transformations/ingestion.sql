
create streaming table customers
as 
SELECT *,current_timestamp() as ingestion_date FROM stream read_files('/Volumes/capstone/default/raw/customers/', FORMAT => 'csv');


create streaming table transactions
as 
SELECT *,current_timestamp() as ingestion_date FROM stream read_files('/Volumes/capstone/default/raw/transactions/', FORMAT => 'csv');


create streaming table accounts 
as
select *,current_timestamp() as ingestion_date from stream read_files("/Volumes/capstone/default/raw/accounts/",
format=>"json",
multiline=>'true'
);

create streaming table branches
as
select *,current_timestamp() as ingestion_date from stream read_files("/Volumes/capstone/default/raw/branches/",
format=>"json",
multiline=>'true'
);

create streaming table loans
as
select *,current_timestamp() as ingestion_date from stream read_files("/Volumes/capstone/default/raw/loans/",
format=>"json"

);


