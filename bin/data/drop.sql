if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'Account') DROP TABLE Account;
-- These are tables that may exist from 304
if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'OrderedProduct') DROP TABLE OrderedProduct;
if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'Orders') DROP TABLE Orders;
if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'Customer') DROP TABLE Customer;
