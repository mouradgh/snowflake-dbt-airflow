# ETL pipeline using dbt, Airflow and Snowflake

Today I will be exploring a new data stack that I have never used before.
In Data Engineering, what matters is bringing value, no matter which technology you use.
However you still need some experience if you want to land a job.
That's why it's time to expand my knowledge beyond GCP, and see what's it's like at other providers as well, like Azure or Snowflake.


#### Overview

This is an introduction to Snowflake project, so the datawarehouse will be at Snowflake.
I will use dbt for transformation and Airflow for orchestration.
I will be using the TPC-H dataset, available for free at Snowflake.

#### Snowflake trial account

Snowflake offers a 30-day free trial account with USD400 of credit, so let's go ahead and create an account [here](https://signup.snowflake.com).

![image](https://github.com/user-attachments/assets/31e4164e-f5b2-451c-8747-e9d12406ed08)

The first thing we will do is set-up our Snowflake environment, by creating a new Worksheet :

<img width="211" alt="image" src="https://github.com/user-attachments/assets/b1d42f9f-f56f-403c-88d5-b4a3a97c977b" />

In your worksheet, create a role/warehouse/database using the super user (accountadmin).
The created role will be assigned to our Snowflake user, and will be granted access to the database/warehouse :

```sql
USE ROLE accountadmin;

CREATE WAREHOUSE dbt_wh WITH warehouse_size = 'x-small';
CREATE DATABASE dbt_db;
CREATE ROLE dbt_role;

GRANT usage ON WAREHOUSE dbt_wh TO ROLE dbt_role;

GRANT ROLE dbt_role TO USER mouradgh;

GRANT all ON DATABASE dbt_db TO ROLE dbt_role;
```
You can check that dbt_role has been granted access to dbt_wh using this command :

```sql
SHOW GRANTS ON WAREHOUSE dbt_wh;
```

<img width="489" alt="image" src="https://github.com/user-attachments/assets/295f53c9-414a-41c0-b4f7-aad646682209" />

We can now switch to the newly created role :

```sql
USE ROLE dbt_role;
```

And create a schema :

```sql
CREATE SCHEMA dbt_db.dbt_schema;
```

If you refresh you Databases objects, you should be able to see your schema :

<img width="378" alt="image" src="https://github.com/user-attachments/assets/cfd10775-e258-465c-b050-aafe6babcecb" />



#### Install dbt

Create a new project in your favorite IDE then create a Python virtual environment :

```bash
python3 -m venv env
source env/bin/activate
```

Install dbt-core as well as the dbt§snowflake adapter :

```bash
pip install dbt-core dbt-snowflake
```

Now let's initialize our dbt project :

```bash
dbt init
```

When prompted, choose a name for your project, a connector, and then your Snowflake account (you should have received it in an e-mail).
You then put your username and password, and finally the role/dw/db/aschema we just created in Snowflake :

<img width="722" alt="image" src="https://github.com/user-attachments/assets/c6ffbf2b-9a56-4598-9108-aa4251573b74" />

You project should now be ready. If you're new to dbt, here are the different folders that have been created and what they're for :
- models : where we write ou SQL logic
- macros : write reusable macros
- dbt-packages : third party libraries
- seeds : static files or files that only change every few months
- snapshots : useful when creating incremental models
- tests : singular and generic tests

Now let's install a useful package called `dbt-utils`, by creating a packages.yml file :

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

And then in your terminal run `dbt deps`

You should have a `profiles.yml` file with the information you provided while initiating the project : 
For safety, you can replace your password with a variable : 

```bash
data_pipeline:
  outputs:
    dev:
      account: hcaawun-fc10922
      database: dbt_db
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: dbt_role
      schema: dbt_schema
      threads: 10
      type: snowflake
      user: mouradgh
      warehouse: dbt_wh
  target: dev

```

And in your terminal set-up a variable with your password : 

```bash
export DBT_PASSWORD='your-password'
````


#### Create source and staging tables 

Once your dbt project ready, edit your dbt_project.yml file by replacing the example model with these 2 models : 

```yaml
models:
  data_pipeline:
    # Config indicated by + and applies to all files under models/example/
    staging:
      +materialized: view
      snowflake_warehouse: dbt_wh
    marts:
      +materialized: table
      snowflake_warehouse: dbt_wh
```

In the models folder you can delete example and create 2 new folders : marts and staging.
It's a good practice to separate your staging files (source files) from marts (models that will materialize in Snowflake)

In the staging folder create a new file called `tpch_sources.yml` :

```yaml
version: 1

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey
```

And create the staging model `stg_tpch_orders.sql` :
Staging tables are one-to-one with source tables.

```sql
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}
```

To make sure everything is working fine, run `dbt run` :

![image](https://github.com/user-attachments/assets/193e79fd-79eb-47cf-a5e5-ad4c7b18628d)

If you go back to Snowflake and refresh your Databases, your view should now be there :

![image](https://github.com/user-attachments/assets/401cafcb-d08c-4966-972b-236a56967910)

Now in a new file let's create a line_items view (`stg_tpch_line_items.sql`), with a surrogate key using dbt_utils.
A natural key is a key that is derived from the data itself, such as a customer ID, a product code, or a date. A surrogate key is a key that is generated artificially, such as a sequential number, a GUID, or a hash.
In this case our key will be a hash.

```sql
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```

To run this new model only : `dbt run -s stg_tpcg_line_items`
(`-s` is a shortcut for `--select

#### Create transformed models (fact tables, data marts)

Now that we have our sources, we need to transform them to create our fact table

Quick reminder : Facts are the measurements that result from a business process event and are almost always numeric. A single fact table row has a one-to-one relationship to a measurement event as described by the fact table’s grain. Thus a fact table corresponds to a physical observable event, and not to the demands of a particular report. Within a fact table, only facts consistent with the declared grain are allowed. For example, in a retail sales transaction, the quantity of a product sold and its extended price are good facts, whereas the store manager’s salary is disallowed.

If you're not familiar with Data Modeling techniques, you can checkout [The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)

First let's create the dim tables : 

In your models > marts folder, create a new file `int_order_items.sql` :

```sql
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date
```

Notice that we used a macro in this query, called `discounted_amount`.
Macros are used to keep things D.R.Y (Don't repear yourself), we create them to re-use business logic across different models.

If you have never used dbt macros, don't worry, I have a trick to help you become an expert, you just have to click [here](https://letmegooglethat.com/?q=dbt+macros).

dbt combines SQL with Jinja, a templating language, to turn your project into a programming environment for SQL, giving you the ability to do thing that aren't normally possible in SQL alone.

Now that you're an expert, you can create a new file in your macros folder, and call it `pricing.sql`.
Inside it you can put the example from the dbt macros documentation : 

{% raw %}
```sql
{% macro cents_to_dollars(column_name, scale=2) %}
    ({{ column_name }} / 100)::numeric(16, {{ scale }})
{% endmacro %}
```


And adapt it to have a macro to calculate a discount based on the price and the discount percentage :

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{ extended_price }} * {{ discount_percentage }})::decimal(16, {{ scale }})
{% endmacro %}
```
{% endraw %}

Create another intermediate model, in a new file called ìnt_order_items_summary.sql`:

```sql
select 
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
```

And finally our fact table `fct_orders.sql`:

```sql
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
order by order_datef
```
Run `dbt run` and you're good to go : 

![image](https://github.com/user-attachments/assets/97e1cf08-ec8a-49cf-a226-7d6649c5e006)

#### Testing your data

There are two types of tests in dbt :
- generic tests : test generic properties (unique, not null...)
- singular tests : a singular SQL query that return failing rows

In the test folder, create a new `generic_tests.yml` file :

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```

This file will make sure that the field order_key from fct_orders is unique, not null, and exists in the stg_tpch_orders table.
It will also make sure that the field status_code only contains these values : 'P', 'O' or 'F'.

If you run `dbt test` you will get the results of your test :

![image](https://github.com/user-attachments/assets/e4f4dcb6-6d4f-430d-b9de-d0613cf27145)

Now let's create some singular tests. Create a new file `fct_orders_discount.sql`
This will check if the item_discount is always greater than 0, because you can't have a negative discount.

```sql
select
    *
from
    {{ref('fct_orders')}}
where
    item_discount_amount > 0
```

To understand how singular tests work, you can edit the code above by putting `item_discount_amount > 0`
In this case, the test will fail because the query has results and doesn't return null.

![image](https://github.com/user-attachments/assets/fbca382a-89e4-438d-807e-c8e1d0f14847)

Let's add another singular test, `fct_orders_date_valid.sql`, that makes sure the values are within acceptable range :

```sql
select
    *
from
    {{ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```

#### Deploy the models using Airflow

We've built some interesting things so far, now let's automate everything using Airflow.

I will be using the [Astronomor Cosmos](https://github.com/astronomer/astronomer-cosmos) library.
It's easy to install :

```bash
brew install astro
```

In the root of your project, execute these commands to initialize an Airflow project : 

```bash
mkdir dbt-dag
cd dbt-dag
astro dev init
```

Update the astro Dockerfile by adding this :

```
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```

Update the requirements.txt file :

```
astronomer-cosmos
apache-airflow-providers-snowflake
```

Now you can start your container using `astro dev start`
Once it's built, the Airflow UI will open up (if not go to [http://localhost:8080/](http://localhost:8080/)) and you can login using admin/admin.

![image](https://github.com/user-attachments/assets/670d6886-76ed-45cc-9adc-d13f308e1764)

Once logged in, go to Admin > Connections and click the "+" icon to create a new `snowflake_conn` connection : 

![image](https://github.com/user-attachments/assets/f74ba213-14c4-45fc-8fc3-c5111a6d6dbc)

```json
{
  "account": "hcaawun-fc10922",
  "warehouse": "dbt_wh",
  "database": "dbt_db",
  "role": "dbt_role",
  "insecure_mode": false
}
```

In onrder to run your dbt code in Airflow, create a dbt folder inside Airflow's dags folder, and copy your data_pipeline folder inside it. Your project should now look like this : 

![image](https://github.com/user-attachments/assets/53d41a56-c83f-4f74-b203-7e3c85a23ead)

In your dags folder, create a `dbt_dag.py` file with the following code :

```python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Use os.path.join for better path handling
project_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "dbt",
    "data_pipeline"
)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(project_path,),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)

# Add this line to make the DAG available to Airflow
dag = dbt_snowflake_dag
```

Now if you go to your DAGs tab in the Airflow UI, you should find you newly created DAG :

![image](https://github.com/user-attachments/assets/819b9688-2854-4208-b55a-fd4a86a37b04)

#### Going beyond

Snowflake is rich with features, here some of the most useful ones :

- Copilot :
  
If you go to a new worksheet, you will notice at the bottom right of your screen a button with "Ask Copilot".
You can use it to turn natural language into an SQL query :

![image](https://github.com/user-attachments/assets/539b689c-b722-42f5-b745-8241b0159926)


- Data Viz :

Snowflake has a Data Viz tool called Snowsight.
Let's run the query generated by Copilot, notice that next to results, there is a Chart tab that you can use to visualize your data :

![image](https://github.com/user-attachments/assets/f37fa5e1-12c3-4729-95fb-8c9e0e97cc6b)

You can also create dashboards on the Projects tab :

![image](https://github.com/user-attachments/assets/4b03d571-087b-436e-890c-6c5c5c3af208)

- AI & ML Studio

Snowflake offers many AI and ML tools to easily get value out of your data.

![image](https://github.com/user-attachments/assets/d26c26cb-67df-45b5-bc2c-10cdd3ab593b)

You can for example predict your sales with just a few steps using the Forecasting option.
We can select the FCT_ORDERS table is training data, TOTAL_PRICE as a target column, and ORDER_DATE as a timestamp column.
We can also use CUSTOMER_KEY as a series identifier.

Snowflake will then generate everything for you and all you have left to do is execute the code !

![image](https://github.com/user-attachments/assets/6b405cd6-a00f-47b1-96de-67b2a6726824)

The `MAX(ORDER_DATE)` of our data being 1998-08-02, the model generates predictions for the following 14 days : 

![image](https://github.com/user-attachments/assets/38a5b910-6d84-4eb4-ac21-265a0b6d4170)

#### Closing notes

I hope you enjoyed this tutorial as much as did, in which we learned :
- how to set-up a Snowflake environment (users, roles, schemas, databases, warehouses)
- how to connect dbt-core with Snowflake
- staging and source models
- fact tables and data marts
- how to use dbt macros
- generic and singular tests
- orchestrating dbt code using Airflow
- other use cases of Snowflake
  
To avoid encurring costs, you can drop everything you created in this tutorial using these commands in Snowflake :

```sql
USE ROLE accountadmin;
DROP WAREHOUSE IF EXISTS dbt_wh;
DROP DATABASE IF EXISTS dbt_db;
DROP ROLE IF EXISTS dbt_role;
```



