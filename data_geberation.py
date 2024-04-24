import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
pd.DataFrame.iteritems = pd.DataFrame.items
spark = SparkSession.builder\
    .config("spark.executor.memory","4g")\
    .config("spark.driver.memory","4g")\
    .config("spark.executors.cores","2")\
    .getOrCreate()
# Generating random transaction data
def generate_transactions(num_transactions):
    transactions = []

    for i in range(num_transactions):
        print(i)
        transaction_date = datetime(2024, random.randint(1, 12), random.randint(1, 28))
        amount = round(random.uniform(1, 10000), 2)
        transaction_type = random.choice(["Credit", "Debit"])
        category = random.choice(["Food", "Shopping", "Transportation", "Utilities", "Entertainment", "Others"])
        transactions.append([transaction_date, amount, transaction_type, category])

    return transactions

# Creating a DataFrame from generated data
def create_dataframe(num_transactions):
    transactions_data = generate_transactions(num_transactions)
    df = spark.createDataFrame(transactions_data, schema=["Date", "Amount", "Type", "Category"])
    return df

# Generating dataset of 1GB size
def generate_dataset(file_path, num_transactions):
    df = create_dataframe(num_transactions)
    chunk_size = 100000  # Number of rows per chunk
    num_chunks = num_transactions // chunk_size
    # df = spark.createDataFrame(df)
    df.coalesce(1).write.mode('append').csv('transactions.csv', header = 'True')
    # import pdb; pdb.set_trace()
    # with pd.ExcelWriter(file_path) as writer:
    #     for i in range(num_chunks):
    #         chunk = df[i * chunk_size: (i + 1) * chunk_size]
    #         print(i)
    #         chunk.to_excel(writer, sheet_name=f"Chunk_{i}", index=False)

# Generate a dataset of 1GB size with 10 million transactions
generate_dataset("bank_transactions_1gb.xlsx", 10000000)
