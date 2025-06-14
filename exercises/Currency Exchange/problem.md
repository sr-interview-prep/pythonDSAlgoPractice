# Currency Exchange

## Description

There are two tables: `sales_amount` and `exchange_rate`.
When the exchange rate changes, a new row is inserted in the `exchange_rate` table with a new effective start date.
Write a query to get the total sales amount in USD (rounded to two decimal points) for each `sales_date`, ordered by `sales_date`.

## Tables

### sales_amount
- `sales_date` (DATE)
- `sales_amount` (DECIMAL)
- `currency` (VARCHAR)

*Data for this table is provided in `sales_amount.csv`.*

### exchange_rate
- `source_currency` (VARCHAR)
- `target_currency` (VARCHAR)
- `exchange_rate` (DECIMAL)
- `effective_start_date` (DATE)

*Data for this table is provided in `exchange_rate.csv`.*

## Task

Write a SQL query to output the `sales_date` and total `sales_amount` in USD.
The results should be ordered by `sales_date`.

The expected output format is defined in `output.csv`.
