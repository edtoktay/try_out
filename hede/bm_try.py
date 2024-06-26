from base_moving_average import BMA
import pandas as pd

b = BMA(batch_id=1, timestamp=pd.to_datetime('2024-06-13 18:45:00.000'), boxing_size=1000)
b.process_base_moving_averages()