from airflow.decorators import task, dag
from pathlib import Path
import pandas as pd
from datetime import datetime
from airflow.providers.odbc.hooks.odbc import OdbcHook

@dag(start_date=datetime(2023, 1, 1), schedule="@daily", catchup=False)
def retail_sales_etl():

    @task    
    def extract():
        """
        Extracts data from csv into DataFrame
        
        Returns: data(DataFrame)
        """
        data_path = Path(__file__).parent.parent / "dataset" / "retail_sales_dataset.csv"
        print("Extracting data ......")
        data = pd.read_csv(data_path)
        print("Data successfully extracted")
        return data

    @task
    def transform(data):
        """
        Cleans and transforms data to match schema
        """
        print("Transforming data .....")
        data.columns = [col.lower().replace(' ', '_') for col in data.columns]
        data['customer_id'] = data['customer_id'].str[-3:]
        current_year = datetime.now().year
        data['age'] = current_year - data['age']
        data.rename(columns={'age': 'year_of_birth', 'date': 'order_date'}, inplace=True)
        
        tables = {}
        copy_data = data.copy()
        
        # Customer
        customer_df = copy_data[['customer_id', 'transaction_id', 'gender', 'year_of_birth']]
        tables['customer'] = customer_df

        # Product Category
        all_product_categories = copy_data['product_category'].unique()
        product_df = pd.DataFrame({'product_category_name': all_product_categories})
        product_df['product_category_id'] = range(1, len(product_df) + 1)
        tables['product_category'] = product_df

        # Orders
        order_df = copy_data[['transaction_id', 'order_date', 'quantity', 'price_per_unit', 'total_amount']]
        tables['orders'] = order_df
        
        print("Data successfully transformed")
        return tables

    @task
    def load(clean_data):
        """
        Loads transformed data into SQL Server via OdbcHook
        """
        print("Connecting to database via ODBC ......")
        hook = OdbcHook(odbc_conn_id="mssql_odbc_conn")
        print("Connection successfull to database")
        
        
        customer_data = clean_data.get('customer')
        product_data = clean_data.get('product_category')
        orders_data = clean_data.get('orders')

        # Convert DataFrames to list of dicts
        customer_data = customer_data.to_dict(orient='records') if customer_data is not None and not customer_data.empty else []
        product_data = product_data.to_dict(orient='records') if product_data is not None and not product_data.empty else []
        orders_data = orders_data.to_dict(orient='records') if orders_data is not None and not orders_data.empty else []

        # Insert data
        try:
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # Product category
            if product_data:
                for row in product_data:
                    cursor.execute(
                        "INSERT INTO product_category (product_category_id, product_category_name) VALUES (?, ?)",
                        row['product_category_id'], row['product_category_name']
                    )

            # Customer
            if customer_data:
                for row in customer_data:
                    cursor.execute(
                        "INSERT INTO customer (customer_id, transaction_id, gender, year_of_birth) VALUES (?, ?, ?, ?)",
                        row['customer_id'], row['transaction_id'], row['gender'], row['year_of_birth']
                    )

            # Orders
            if orders_data:
                for row in orders_data:
                    cursor.execute(
                        "INSERT INTO orders (transaction_id, order_date, quantity, price_per_unit, total_amount) VALUES (?, ?, ?, ?, ?)",
                        row['transaction_id'], row['order_date'], row['quantity'], row['price_per_unit'], row['total_amount']
                    )
            
            conn.commit()
            cursor.close()
            conn.close()
            print("Data successfully inserted into all tables")
        except Exception as e:
            print(f"Insertion failed: {e}")


    data = extract()
    clean_data = transform(data)
    load(clean_data)
    
retail_sales_etl()