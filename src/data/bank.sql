-- Sample Bank data for SQL Server

CREATE TABLE Customer
(
    cid INTEGER NOT NULL,
	name	varchar(30),
	PRIMARY KEY (cid)
);

Insert INTO Customer VALUES (1, 'Steve Johnson');
Insert INTO Customer VALUES (2, 'Randy Johnson');
Insert INTO Customer VALUES (3, 'Karl Andreasen');
Insert INTO Customer VALUES (4, 'Craig Johnson');
Insert INTO Customer VALUES (5, 'Keri Campbell');
Insert INTO Customer VALUES (6, 'Katherine Kerr');
Insert INTO Customer VALUES (7, 'Kristen Flemal');
Insert INTO Customer VALUES (8, 'Jodi Van Vleet');
Insert INTO Customer VALUES (9, 'Karla Holtzen');
Insert INTO Customer VALUES (10, 'Lesley Nolan');

CREATE TABLE Account
(
    acctId INTEGER NOT NULL,
	cid	INTEGER,
	amount	INTEGER,
	accType INTEGER,
	PRIMARY KEY (acctId),
	FOREIGN KEY (cid) references Customer (cid)
);

-- Savings account have type =1, checking accounts have type = 2

Insert INTO Account Values (1,7,100,1);
Insert INTO Account Values (2,7,1000,1);
Insert INTO Account Values (3,7,1000,1);
Insert INTO Account Values (4,1,100,1);
Insert INTO Account Values (5,2,100,1);
Insert INTO Account Values (6,3,1000,1);
Insert INTO Account Values (7,4,1100,1);
Insert INTO Account Values (8,4,100,1);
Insert INTO Account Values (9,5,2100,1);
Insert INTO Account Values (10,6,10000,1);
Insert INTO Account Values (11,7,100,1);
Insert INTO Account Values (12,7,1000,1);
Insert INTO Account Values (13,7,10000,1);
Insert INTO Account Values (14,1,100,1);
Insert INTO Account Values (15,8,100,1);
Insert INTO Account Values (16,8,100,1);
Insert INTO Account Values (17,9,12100,1);
Insert INTO Account Values (18,9,1000,1);
Insert INTO Account Values (19,10,12100,1);
Insert INTO Account Values (20,10,1000000,1);


Insert INTO Account Values (101,7,10000,2);
Insert INTO Account Values (102,7,10100,2);
Insert INTO Account Values (103,7,10500,2);
Insert INTO Account Values (104,1,1050,2);
Insert INTO Account Values (105,2,1070,2);
Insert INTO Account Values (106,3,10400,2);
Insert INTO Account Values (107,4,1100,2);
Insert INTO Account Values (108,4,100,2);
Insert INTO Account Values (109,5,2100,2);
Insert INTO Account Values (110,6,150000,2);
Insert INTO Account Values (111,7,1700,2);
Insert INTO Account Values (112,7,10500,2);
Insert INTO Account Values (113,7,150000,2);
Insert INTO Account Values (114,1,1020,2);
Insert INTO Account Values (115,8,1020,2);
Insert INTO Account Values (116,8,100,2);
Insert INTO Account Values (117,9,142100,2);
Insert INTO Account Values (118,9,10020,2);
Insert INTO Account Values (119,10,12100,2);
Insert INTO Account Values (120,10,1000000,2);
