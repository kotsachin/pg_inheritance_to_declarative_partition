# pg_inheritance_to_declarative_partition
Convert your Inheritance based partition tables to Declarative based partition tables.


This code having small plpgsql functions to list and convert inheritance based partition to declarative syntax based partitions.

Steps to execute attached conversions functions:
1. Make sure you are PostgreSQL database is installed and running 

2. Run below command to create partition list function in your database .
$ psql -d postgres -U postgres -f ~/inheritance_to_declarative_partition/list_partitions.sql

3. Run below command to create partition conversion function in your database .
$ psql -d postgres -U postgres -f ~/inheritance_to_declarative_partition/fn_upgrade_partitioning.sql

4. Run below command to create sample partition schema in your database.
$ psql -d postgres -U postgres -f ~/inheritance_to_declarative_partition/inherit_schema.sql

5. Login to database and run below functuon call to get list of all inheritance based partitions into your database.

$ psql -d postgres -U postgres                                                               
postgres=# select * from c_partition.list_partitions();
 table_schema | partition_table | partition_column |         column_type         | partition_type | partition_method 
--------------+-----------------+------------------+-----------------------------+----------------+------------------
 public       | events1         | created_at       | timestamp without time zone | RANGE          | inheritance
 public       | events_text     | city             | text                        | LIST           | inheritance
 public       | events          | created_at       | timestamp without time zone | RANGE          | inheritance
 sales        | inventory       | invoice_date     | date                        | RANGE          | inheritance
 sales        | sales           | invoice_date     | date                        | RANGE          | inheritance
 public       | events_list     | id               | integer                     | LIST           | inheritance


6. Below query will convert all your inheritance based partitions tables to declarative based partition tables.

postgres=# select fn_upgrade_partitioning( table_schema, partition_table, partition_column, column_type, partition_type)  from c_partition.list_partitions() where partition_method='inheritance' AND partition_column!='';

7. After above query converted all paritions into declarative based , confirm same again running function call in step 6.

postgres=# select * from c_partition.list_partitions();                                                                                                  
 table_schema | partition_table | partition_column |         column_type         | partition_type | partition_method 
--------------+-----------------+------------------+-----------------------------+----------------+------------------
 public       | events          | created_at       | timestamp without time zone | RANGE          | declarative
 public       | events1         | created_at       | timestamp without time zone | RANGE          | declarative
 public       | events_list     | id               | integer                     | LIST           | declarative
 public       | events_text     | city             | text                        | LIST           | declarative
 sales        | inventory       | invoice_date     | date                        | RANGE          | declarative
 sales        | sales           | invoice_date     | date                        | RANGE          | declarative

 
Uncovered scenarios because of current limitations of declarative partition :
1. create partition table with including all
2. add/move foreign key constraint to newly created partition
3. type conversion in check constraint boundaries are not yet handled


Reference : http://blog.postgresql-consulting.com/2017/11/upgrading-inheritance-partitioning-made.html
