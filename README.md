# dbt-pintu-project
A repository to store Pintu technical test answers

## Data Modelling Architecture
<img width="441" height="41" alt="data modelling step drawio" src="https://github.com/user-attachments/assets/58828534-69a5-4d35-ba9b-f0bea939cb4a" />

## Raw Layer ERD
<img width="466" height="602" alt="raw layer erd drawio" src="https://github.com/user-attachments/assets/525be18c-b354-4ccd-bc8e-3a60784bfd69" />

From the test instruction, the data type of raw are all strings.

## Stg Layer ERD
<img width="451" height="661" alt="stg_erd drawio" src="https://github.com/user-attachments/assets/b94d0021-5dbb-4373-9516-eb38979a561f" />

Steps done:
1. Trimming and casting all dimension related attributes (Region, Status, etc.) to ensure there is no extra whitespace in the row and have the same format
2. Casting time related columns as timestamp to make sure the format is in UTC
3. Adding ingested_at column as a time identifier of the row’s ingestion time at staging layer
4. It’s generally better to generate a surrogate key instead of using the natural ID because it gives simpler key, faster performance, and smaller storage size, supports future SCD implementation, etc. But for now, I’ll keep using the natural ID as the key for simplicity.

## DWH (Warehouse) Layer ERD
<img width="491" height="741" alt="warehouse_erd drawio" src="https://github.com/user-attachments/assets/019c45b3-d2fc-4e50-854b-ce180a90671f" />

Steps done:
1. Normalized columns trade status and side to a new table **dim_trade_status** and **dim_trade_side**, and column p2p transfer status to a new column **dim_transfer_status** so that the fact only contains key and numeric metrics.
2. Using star schema to normalize the tables
3. Adding price_usd * quantity as a new column **price_amount_usd** in **fact_trades** for more convenience usage in the future.
4. Adding governance using dbt test to detect null values and duplicate rows for primary keys in DBT to ensure uniqueness and fullness.
5. Adding row number filtering for duplicate rows for failed test tables.
6. Using incremental merge strategy for fact tables because full refresh strategy is costly for a big table like facts.

## Marts Layer ERD
<img width="721" height="1007" alt="marts_erd drawio" src="https://github.com/user-attachments/assets/c89ff139-5146-4c6d-8a9b-943b88233c03" />

Here only shows ERD for main marts/fact-marts.

Steps done:
1. Joined all fact tables with their corresponding keys to create main marts/fact-marts (**mart_trade_detais** and **mart_p2p_transfers_details**), eliminating the need for repeated joins between fact and dimension tables for analysts’ convenience.
2. From that main marts, aggregated marts to answer management questions are built:
    1. **mart_region_cohort**: For visualizing cohort by region, to answer differences of retention between region and token category (Question 2B)
    2. **mart_token_category_cohort**: For visualizing cohort by token category, to answer differences of retention between region and token category (Question 2B)
    3. **mart_trade_pareto_users**: For monitoring pareto of users monthly, to know if our trades are supported by only small sets of users (Question 1B).
    4. **mart_trade_token_summary**: For monitoring token on trades, to know if the trades relies only on a few tokens (Question 1A).
    5. **mart_users_journey**: For monitoring user's starting activity and features, minimum-maximum time of each activity, to know if they continues to trade or churn (Question 2A).

## DBT Data Lineage
![WhatsApp Image 2025-10-31 at 7 22 29 PM](https://github.com/user-attachments/assets/96ac63fe-787d-4a72-9c13-06870e27d731)

## Insights to Answer Management Questions
I usually answer management question through presentation, so I made an insight presentation based on the data:
[DATA ANALYST - TEST ASSESMENT](https://docs.google.com/presentation/d/1Bt7kirHb0PlPDHLjcj0UKh24GQPHxEWzGDZsCAu0IRY/edit?slide=id.g39ed556a3b6_1_85#slide=id.g39ed556a3b6_1_85)

Presentation preview:

![WhatsApp Image 2025-10-31 at 8 58 22 PM](https://github.com/user-attachments/assets/909251ec-0155-4d45-a08a-f75d1e0a12b2)

### Suggestion
1. Due to time limitation, the reports and dashboard of management presentation data can't be fully created. But I usually prepared the insights using reports/dashboards first, then make insights out of it.
2. Data governance could be better if added with owner name, tags (scheduling), more tests cases, and more detailed documentation in the .yaml file and/or test folder.
