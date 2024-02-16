import numpy as np
import pandas as pd

# Sample DataFrame `df` with `response_time` in seconds
# df = pd.DataFrame({'response_time': [..response times..]})

class ValidationController:

    def __init__(self):
        pass

    def IQR_check(self, df):

        # Calculate Q1 and Q3
        Q1 = df['response_time'].quantile(0.25)
        Q3 = df['response_time'].quantile(0.75)
        IQR = Q3 - Q1

        # Define a minimum reasonable response time if known
        min_reasonable_time = 5  # assuming 5 seconds is the minimum reasonable time

        # Lower bound threshold (considering both statistical and empirical thresholds)
        lower_bound = max(Q1 - 1.5 * IQR, min_reasonable_time)

        # Flag records with response times below the lower bound
        df['flagged'] = df['response_time'] < lower_bound

        # Filter flagged records
        flagged_records = df[df['flagged']]