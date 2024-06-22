# PySpark_Sales_Data_Analysis

## Overview

This project leverages PySpark for a comprehensive analysis of sales data, combining it with menu data to generate insightful reports and visualizations. The project demonstrates how to use PySpark for ETL (Extract, Transform, Load) processes and data analysis, providing valuable insights into customer spending, product popularity, and sales trends.

## Project Structure

```
.
├── Data
│   ├── menu.csv.txt
│   └── sales.csv.txt
├── Pyspark_project.html
├── Pyspark_project_notebook.html
├── Pyspark_project_notebook.ipynb
└── README.md
```

## Description

This project reads sales data and menu data from CSV files, processes the data using PySpark, and performs various analyses to derive insights. The following operations are performed:

1. **Data Loading**: Reading sales and menu data from CSV files.
2. **Data Transformation**: Extracting year, month, and quarter from the order dates.
3. **Data Aggregation and Analysis**:
   - Total amount spent by each customer.
   - Total amount spent on each product.
   - Monthly, yearly, and quarterly sales amount.
   - Product sales count and top-selling products.
   - Customer visits analysis.
   - Sales by location.
   - Sales by order source.

## Data Sources

- **Sales Data**: Contains details of each sale, including product ID, customer ID, order date, location, and source of the order.
- **Menu Data**: Contains details of each product, including product ID, product name, and price.

## Data Schema

### Sales Data Schema
- **Product_id**: Integer
- **Customer_id**: String
- **Order_date**: Date
- **Location**: String
- **Source_order**: String

### Menu Data Schema
- **Product_id**: Integer
- **Product_name**: String
- **Price**: String

## Workflow

1. **Data Loading**:
    ```python
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

    schema = StructType([
        StructField("Product_id", IntegerType(), True),
        StructField("Customer_id", StringType(), True),
        StructField("Order_date", DateType(), True),
        StructField("Location", StringType(), True),
        StructField("Source_order", StringType(), True)
    ])

    sales_df = spark.read.csv("/FileStore/tables/sales_csv.txt", schema=schema)
    ```

2. **Data Transformation**:
    ```python
    from pyspark.sql.functions import month, year, quarter

    sales_df = sales_df.withColumn("order_year", year(sales_df.Order_date))
    sales_df = sales_df.withColumn("order_month", month(sales_df.Order_date))
    sales_df = sales_df.withColumn("order_quarter", quarter(sales_df.Order_date))
    ```

3. **Data Aggregation and Analysis**:
    - Total amount spent by each customer:
        ```python
        total_amount_spent = (sales_df.join(menu_df, "Product_id")
                             .groupBy('Customer_id')
                             .agg({'Price':'sum'})
                             .orderBy('Customer_id'))
        ```

    - Total amount spent on each product:
        ```python
        total_amount_spent = (sales_df.join(menu_df, "Product_id")
                             .groupBy('Product_name')
                             .agg({'Price':'sum'})
                             .orderBy('Product_name'))
        ```

    - Monthly, yearly, and quarterly sales amount:
        ```python
        monthly_amount = (sales_df.join(menu_df, "Product_id")
                         .groupBy('Order_month')
                         .agg({'Price':'sum'})
                         .orderBy('Order_month'))
        ```

    - Product sales count and top-selling products:
        ```python
        product_sale = (sales_df.join(menu_df, "Product_id")
                       .groupBy('Product_name')
                       .agg(count('product_id').alias('product_count'))
                       .orderBy('product_count', ascending=0)).limit(5)
        ```

    - Customer visits analysis:
        ```python
        customer_visit = (sales_df.filter(sales_df.Source_order == 'Restaurant')
                         .groupBy('Customer_id')
                         .agg(countDistinct('Order_date').alias('No of Visits'))
                         .orderBy('Customer_id'))
        ```

    - Sales by location:
        ```python
        country_sales = (sales_df.join(menu_df, 'Product_id')
                        .groupBy('Location')
                        .agg({'Price':'sum'})
                        .orderBy('Location'))
        ```

    - Sales by order source:
        ```python
        mode_of_sales = (sales_df.join(menu_df, 'Product_id')
                        .groupBy('Source_order')
                        .agg({'Price':'sum'})
                        .orderBy('Source_order'))
        ```

## Tools and Technologies

- **PySpark**: For data processing and analysis.
- **Jupyter Notebook**: For interactive development and visualization.
- **Databricks**: For spark cluster.

## Conclusion

This project showcases the power of PySpark for handling large datasets and performing complex data transformations and analyses. The insights derived from this analysis can help businesses understand customer behavior, product popularity, and sales trends, enabling data-driven decision-making.
