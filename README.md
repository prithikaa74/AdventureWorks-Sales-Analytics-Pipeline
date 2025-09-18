

# ETL Pipeline: AdventureWorks Daily Sales

## Overview

A PySpark-based ETL pipeline in Databricks that processes AdventureWorks sales data, removes returns, aggregates daily sales, and stores results in a Delta table.


## Data Sources

* `adventure_works_customers` – Customer details
* `adventure_works_products` – Product details
* `adventure_works_returns` – Returned products
* `adventure_works_sales_2016`, `adventure_works_sales_2017` – Sales data



## ETL Flow

1. **Extract:** Load sales, product, customer, and return data.
2. **Transform:**

   * Union 2016 & 2017 sales
   * Join with products & customers
   * Exclude returned products
   * Aggregate sales (`TotalSales`, `NumOrders`)
   * Clean & cast columns
3. **Load:** Save results as `daily_sales` Delta table.



## Output Schema

| Column      | Type    | Description       |
| ----------- | ------- | ----------------- |
| OrderDate   | Date    | Order date        |
| ProductKey  | Integer | Product ID        |
| ProductName | String  | Product name      |
| TotalSales  | Double  | Total sales value |
| NumOrders   | Integer | Number of orders  |


## How to Run

1. Import notebook into **Databricks**.
2. Attach to a cluster.
3. Run all cells.
4. Verify the `daily_sales` table is created.

