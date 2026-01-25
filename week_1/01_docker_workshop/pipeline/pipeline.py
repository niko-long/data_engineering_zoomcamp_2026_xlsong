import sys
import pandas as pd

print('arguments', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({'day': [1, 2], 'num_passengers': [4, 5]})
df['month'] = month
print(df.head())

df.to_parquet(f"output_{month}.parquet")
df.to_csv(f"output_{month}.csv", index=False)

print(f'hello from pipeline! The month = {month}')