-- Databricks notebook source
-- MAGIC %run /Users/kuldeep.pralhadmanagoli@diggibyte.com/Copy-Datasets

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`

-- COMMAND ----------

SELECT *,
input_file_name() source_file

FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

-- COMMAND ----------

SELECT * FROM text.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

SELECT * FROM binaryFile. `dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv`

-- COMMAND ----------

 CREATE TABLE books_csv
 (book_id STRING,title STRING,author STRING, category STRING,price DOUBLE)

 USING CSV
 OPTIONS(
  header = 'true',
  delimiter = ";"
 )
 LOCATION  "dbfs:/mnt/demo-datasets/bookstore/books-csv"

-- COMMAND ----------

SELECT * from books_csv

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"dbfs:/mnt/demo-datasets/bookstore/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table("books_csv").write.mode("append").format('csv').option('header','true').option('delimiter',';').save(f"dbfs:/mnt/demo-datasets/bookstore/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"dbfs:/mnt/demo-datasets/bookstore/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT count(*) FROM books_csv

-- COMMAND ----------

 REFRESH TABLE books_csv

-- COMMAND ----------

SELECT count(*) FROM books_csv

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv`;



-- COMMAND ----------

SELECT * from books_unparsed;

-- COMMAND ----------

CREATE TEMPORARY VIEW books_tmp_vw
(book_id STRING,title STRING, author STRING, category STRING,price DOUBLE)
using CSV
options(
  path = "dbfs:/mnt/demo-datasets/bookstore/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS 
SELECT * FROM books_tmp_vw;

SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

CREATE or replace table orders as
select * from PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * from PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT into orders
SELECT * from PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

CREATE or replace temp view customers_updates AS
SELECT * from JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json-new`;

MERGE INTO customers c
using customers_updates u 
ON c.customer_id = u.customer_id
when matched and c.email is NULL AND u.email is not NULL THEN
update set email = u.email, updated = u.updated
WHEN not matched then INSERT * 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ADVANCED TRANSFORATION
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

select customer_id, profile: first_name,profile:address:country
from customers

-- COMMAND ----------

select from_json(profile) as profile_struct
from customers;

-- COMMAND ----------

select profile
from customers
LIMIT 1

-- COMMAND ----------

create or replace temp view parsed_customers as
select customer_id, from_json(profile,schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) as profile_struct from customers; 

select * from parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name,profile_struct.address.country
from parsed_customers

-- COMMAND ----------

CREATE or REplace temp view customers_final AS
select customer_id, profile_struct.*
from parsed_customers;
select * FROM customers_final

-- COMMAND ----------

select order_id, customer_id,books
from orders

-- COMMAND ----------

  select order_id, customer_id, explode(books) as book
  from orders

-- COMMAND ----------

select customer_id, collect_set(order_id) as order_set,
collect_set(books.book_id) as books_set
from orders 
group by customer_id

-- COMMAND ----------

select customer_id,
collect_set(books.book_id) as before_flatten,
array_distinct(flatten(collect_set(books.book_id))) as after_flatten
from orders
group by customer_id

-- COMMAND ----------

CREATE TEMPORARY VIEW books_tmp_vw
(book_id STRING,title STRING, author STRING, category STRING,price DOUBLE)
using CSV
options(
  path = "dbfs:/mnt/demo-datasets/bookstore/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE book AS 
SELECT * FROM books_tmp_vw;



-- COMMAND ----------

SELECT * FROM book

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC       .table("book")
-- MAGIC       .createOrReplaceTempView("books_tmp_vw")
-- MAGIC )

-- COMMAND ----------

SELECT * from books_tmp_vw

-- COMMAND ----------

select author, count(book_id) as total_books 
from books_tmp_vw
group by author

-- COMMAND ----------

create or replace temp view author_count_tmp_vw as (
  select author, count(book_id) as total_books
  from books_tmp_vw
  group by author
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  (spark.table("author_count_tmp_vw")
-- MAGIC   .writeStream
-- MAGIC   .trigger(processingTime='4 seconds')
-- MAGIC   .outputMode('complete')
-- MAGIC   .option('checkpointLocation','dbfs:/mnt/demo/author_counts_checkpoint')
-- MAGIC   .table('author_counts')
-- MAGIC   )

-- COMMAND ----------

SELECT * from author_Counts

-- COMMAND ----------

insert into book 
values('b19',"introduction to modeling and simulation","mark.m","computer science",25),
('b20',"robot modeling and control","mark w. spong","computer science",30),
("b21","turing's vision:the birth of computer science","chis","computer science",35)

-- COMMAND ----------

insert into book 
values('b16',"hands on deep learning algorithm with python","sudharsan","computer science",25),
('b17',"rneural network in natural language processing","yoav goldberg","computer science",30),
("b18","understanding digital signal processing","richard lyons","computer science",35)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("author_count_tmp_vw")                               
-- MAGIC       .writeStream           
-- MAGIC       .trigger(availableNow=True)
-- MAGIC       .outputMode("complete")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
-- MAGIC       .table("author_counts")
-- MAGIC       .awaitTermination()
-- MAGIC )

-- COMMAND ----------


SELECT *
FROM author_counts

-- COMMAND ----------


