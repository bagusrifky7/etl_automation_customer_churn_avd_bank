# Analysis Customer Churn Rate of AVD Bank With ETL Automation Process

## Repository Outline
1. dag_etl.py - Script for ETL process
2. dag_graph.jpg - Image of ETL process
3. sql_ddl.txt - Txt filled with SQL query.
4. data_raw.csv - Raw data after fetching it from SQL
5. data_clean.csv - Clean data after airflow process
6. data_validation.ipynb - File for data validation process with Great Expectations.
7. images - Folder filled with screenshot from analysis.

## Problem Background
Decreasing percentage of customer churn below 20% is important for healthier financials and improving the profit of our company. We want to identify what factor influences customers to keep using our products and not move to competitors.  A percentage of customer churn below 20% is realistic to get for this year, because it is a common target for every established bank.

## Project Output
The output of this project is to decrease customer churn to below 20%

## Data
Characteristics of my data have 17 column and 10000 rows, This dataset consist of:

- customer_id: customers unique id.
- surname: surname of customers.
- credit_score: credit scores of customers that determine is the customers good or not.
- geography: customer location.
- gender: customers gender.
- age: customer age.
- tenure: how long customers being with our bank.
- balance: customers bank balance.
- num_of_products: number of product has bought using our debit or credit card.
- has_cr_card: determine if the customers has credit card or not.
- is_active_member: determine if the customer active or not.
- estimated_salary: customers annual salaries.
- exited: whether or not the customer left the bank.
- complain: customer has complaint or not.
- satisfaction_score: score provided by customers from their complaint feedback.
- card_type: type of card hold by the customer.
- points_earned: the points earned by the customer for using credit card.

## Method
The method that we use for this analysis is the ETL process using Apache-Airflow with data that I fetch from PostgreSQL, validating data with Great Expectations, and visualisation using Kibana.

## Stacks
The packages that I use is Pandas and Great Expectations, The tools that I used is Apache-Airflow, PostGreSQL, and Kibana

## Reference
- Link to the dataset: https://www.kaggle.com/datasets/radheshyamkollipara/bank-customer-churn

---

