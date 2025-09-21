-- Switch to master so we can drop the database
USE master;
GO

-- Drop database if exists, disconnect all users
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'retail_sales')
BEGIN
    ALTER DATABASE retail_sales SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE retail_sales;
END
GO

-- Create database
CREATE DATABASE retail_sales;
GO

-- Switch to the new database
USE retail_sales;
GO

-- Create tables
CREATE TABLE orders (
    transaction_id INT IDENTITY(1,1) PRIMARY KEY,
    order_date DATE,
    quantity INT,
    price_per_unit INT,
    total_amount INT
);
GO

CREATE TABLE customer (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    transaction_id INT,
    gender VARCHAR(20),
    year_of_birth INT,
    FOREIGN KEY (transaction_id) REFERENCES orders(transaction_id)
);
GO

CREATE TABLE product_category (
    product_category_id INT IDENTITY(1,1) PRIMARY KEY,
    product_category_name VARCHAR(20)
);
GO

PRINT('Database retail_sales successfully created');
GO