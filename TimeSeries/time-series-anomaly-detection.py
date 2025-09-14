# %% [markdown]
# # **Anomaly Detection in Time Series Data**
# 
# This will be a **short notebook exploring Anomaly Detection**. I will, initially, use just one algorithm (**Isolation Forest**), but with the view to expand this notebook over time.
# 
# The Isolation Forest ‘isolates’ observations by randomly selecting a feature and then randomly selecting a split value between the maximum and minimum values of the selected feature.
# 
# Since recursive partitioning can be represented by a tree structure, the number of splittings required to isolate a sample is equivalent to the path length from the root node to the terminating node.
# 
# This path length, averaged over a forest of such random trees, is a measure of normality and our decision function.
# 
# Random partitioning produces noticeably shorter paths for anomalies. Hence, when a forest of random trees collectively produce shorter path lengths for particular samples, they are highly likely to be anomalies.
# 
# ## **Different Approaches to Time Series Anomaly Detection**
# 
# Check out this notebook I put together to showcase the **STUMPY** Matrix Profiling library and how it can be used for anomaly detection:
# 
# https://www.kaggle.com/code/joshuaswords/anomaly-detection-with-stumpy-matrix-profiling

# %%
# pip install -r requirements.txt

# %%
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Extra Libs
import matplotlib.dates as mdates
import holoviews as hv
from holoviews import opts
hv.extension('bokeh')
from bokeh.models import HoverTool
from IPython.display import HTML, display

# Ignore warnings
import warnings
warnings.filterwarnings("ignore")

from sklearn.ensemble import IsolationForest

# %% [markdown]
# # **The Data**
# 
# The dataset I will use here is the New York City Taxi Demand dataset.
# 
# The raw data is from the NYC Taxi and Limousine Commission.
# The data file included here consists of aggregating the total number of
# taxi passengers into 30 minute buckets.
# 
# 
# **Some Inspiration & References for this Project**
# 
# 
# https://www.kaggle.com/victorambonati/unsupervised-anomaly-detection
# 
# https://www.youtube.com/watch?v=XCF-kqCB_vA&ab_channel=AIEngineering
# 
# https://www.kaggle.com/koheimuramatsu/industrial-machine-anomaly-detection/comments
# 
# https://holoviews.org/
# 
# http://holoviews.org/user_guide/Plotting_with_Bokeh.html
# 
# https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html

# %%
df = pd.read_csv('nyc_taxi.csv', parse_dates=['timestamp'])

(df.head(5)
 .style
 .set_caption('New York City Taxi Demand')
 .format({'value':"{:,.0f}"})
)

# %% [markdown]
# The dataset has just two columns. 
# 
# It will be good to do some **Feauture Engineering** later to extract as much information as we can from these existing features.

# %% [markdown]
# **Housekeeping**
# 
# Checking for blank values, checking Data Types etc.

# %%
def overview(df: pd.DataFrame, timestamp_col: str = None) -> None:
    print('Null Count:\n', df.isnull().sum(),'\n')
    print('Data Types:\n', df.dtypes)
    
    if timestamp_col is not None:
        print('\nDate Range:\n\nStart:\t',df[timestamp_col].min())
        print('End:\t',df[timestamp_col].max())
        print('Days:\t',(df[timestamp_col].max() - df[timestamp_col].min()))

# %%
overview(df, timestamp_col='timestamp')

# %% [markdown]
# # **Visual Overview**

# %% [markdown]
# When displayed Hourly, the dataset is hard to fully understand. I will resample this from hourly to daily to weekly, and see if we can pick out any interesting features.
# 
# **A Quick note on the visuals**
# 
# My previous notebooks all have a strong focus on data visualisation, using primarily Matplotlib & Seaborn. 
# 
# Today though, I will use **Holoviews & Bokeh**. I want to expand my Data Visualisation toolkit and this library is, to me at least, more visually pleasing than Plotly (although that too is a tool I want to begin practising with as I have seen some fantastic Plotly-based notebooks).

# %%
Hourly = hv.Curve(df.set_index('timestamp').resample('H').mean()).opts(
    opts.Curve(title="New York City Taxi Demand Hourly", xlabel="", ylabel="Demand",
               width=700, height=300,tools=['hover'],show_grid=True))

Daily = hv.Curve(df.set_index('timestamp').resample('D').mean()).opts(
    opts.Curve(title="New York City Taxi Demand Daily", xlabel="", ylabel="Demand",
               width=700, height=300,tools=['hover'],show_grid=True))

Weekly = hv.Curve(df.set_index('timestamp').resample('W').mean()).opts(
    opts.Curve(title="New York City Taxi Demand Weekly", xlabel="Date", ylabel="Demand",
               width=700, height=300,tools=['hover'],show_grid=True))


(Hourly + Daily + Weekly).opts(shared_axes=False).cols(1)

# %% [markdown]
# Seeing the data plotted in different units is helpful to see the underlying trends of the data.
# 
# Hourly data may contain a lot of information, but it is difficult to spot anomoalies at a glance. 
# In contrast, Daily & Weekly plotting is much easier to understand. We also spot clearly times of year when demand is boosted and when it lags.

# %% [markdown]
# # **Feature Engineering**

# %%
# A variety of resamples which I may or may not use
df_hourly = df.set_index('timestamp').resample('H').mean().reset_index()
df_daily = df.set_index('timestamp').resample('D').mean().reset_index()
df_weekly = df.set_index('timestamp').resample('W').mean().reset_index()

# %%
# New features 
# Loop to cycle through both DataFrames
for DataFrame in [df_hourly, df_daily]:
    DataFrame['Weekday'] = (pd.Categorical(DataFrame['timestamp'].dt.strftime('%A'),
                                           categories=['Monday', 'Tuesday', 'Wednesday', 'Thursday','Friday', 'Saturday', 'Sunday'])
                           )
    DataFrame['Hour'] = DataFrame['timestamp'].dt.hour
    DataFrame['Day'] = DataFrame['timestamp'].dt.weekday
    DataFrame['Month'] = DataFrame['timestamp'].dt.month
    DataFrame['Year'] = DataFrame['timestamp'].dt.year
    DataFrame['Month_day'] = DataFrame['timestamp'].dt.day
    DataFrame['Lag'] = DataFrame['value'].shift(1)
    DataFrame['Rolling_Mean'] = DataFrame['value'].rolling(7, min_periods=1).mean()
    DataFrame = DataFrame.dropna()
 

# %% [markdown]
# # **More Visual Exploration**

# %% [markdown]
# We are trying to detect anomales in Taxi Demand. This is the 'value' column

# %%
(hv.Distribution(df['value'])
.opts(opts.Distribution(title="Overall Value Distribution",
                        xlabel="Value",
                        ylabel="Density",
                        width=700, height=300,
                        tools=['hover'],show_grid=True)
     ))

# %% [markdown]
# We can also see how this varies by day. 
# 
# The legend acts as a filter here, so one can select/deselect certain days.

# %%
by_weekday = df_hourly.groupby(['Hour','Weekday']).mean()['value'].unstack()
plot = hv.Distribution(by_weekday['Monday'], label='Monday') * hv.Distribution(by_weekday['Tuesday'], label='Tuesday') * hv.Distribution(by_weekday['Wednesday'], label='Wednesday') * hv.Distribution(by_weekday['Thursday'], label='Thursday') * hv.Distribution(by_weekday['Friday'], label='Friday') * hv.Distribution(by_weekday['Saturday'], label='Saturday') *hv.Distribution(by_weekday['Sunday'], label='Sunday').opts(opts.Distribution(title="Demand Density by Day & Hour"))
plot.opts(opts.Distribution(width=800, height=300,tools=['hover'],show_grid=True, ylabel="Demand", xlabel="Demand"))


# %%
hv.Bars(df_hourly[['value','Weekday']].groupby('Weekday').mean()).opts(
    opts.Bars(title="New York City Taxi Demand by Day", xlabel="", ylabel="Demand",
               width=700, height=300,tools=['hover'],show_grid=True))

# %% [markdown]
# Through the plots above we learn a few interesting things.
# 
# Let's now turn to average hourly demand.

# %%
hv.Curve(df_hourly[['value','Hour']].groupby('Hour').mean()).opts(
    opts.Curve(title="New York City Taxi Demand Hourly", xlabel="Hour", ylabel="Demand",
               width=700, height=300,tools=['hover'],show_grid=True))

# %%
by_weekday = df_hourly.groupby(['Hour','Weekday']).mean()['value'].unstack()
plot = hv.Curve(by_weekday['Monday'], label='Monday') * hv.Curve(by_weekday['Tuesday'], label='Tuesday') * hv.Curve(by_weekday['Wednesday'], label='Wednesday') * hv.Curve(by_weekday['Thursday'], label='Thursday') * hv.Curve(by_weekday['Friday'], label='Friday') * hv.Curve(by_weekday['Saturday'], label='Saturday') *hv.Curve(by_weekday['Sunday'], label='Sunday').opts(opts.Curve(title="Average Demand by Day & Hour"))
plot.opts(opts.Curve(width=800, height=300,tools=['hover'],show_grid=True, ylabel="Demand"))

# in Matplotlib/Pandas
# #df_hourly.groupby(['Hour','Weekday']).mean()['value'].unstack().plot()

# %% [markdown]
# # **More Feature Engineering**

# %%
df_hourly = (df_hourly
             .join(df_hourly.groupby(['Hour','Weekday'])['value'].mean(),
                   on = ['Hour', 'Weekday'], rsuffix='_Average')
            )

df_daily = (df_daily
            .join(df_daily.groupby(['Hour','Weekday'])['value'].mean(),
                  on = ['Hour', 'Weekday'], rsuffix='_Average')
           )

df_hourly.tail()

# %% [markdown]
# Let's see how an average Saturday compares to the Saturday with the highest demand in our dataset

# %%
sat_max = (df_hourly
           .query("Day == 5")
           .set_index('timestamp')
           .loc['2015-01-31':'2015-01-31']
           .reset_index()['value']
          )


avg_sat = (df_hourly
           .groupby(['Weekday','Hour'])['value']
           .mean()
           .unstack()
           .T['Saturday']
          )

avg_max_comparison = hv.Curve(avg_sat, label='Average Saturday') * hv.Curve(sat_max, label='Busiest Saturday').opts(opts.Curve(title="Average Saturday vs Busiest Saturday"))
avg_max_comparison.opts(opts.Curve(width=800, height=300,tools=['hover'],show_grid=True, ylabel="Demand", show_legend=False))

# %% [markdown]
# # **Models**
# 
# Below is the DataFrame with the new Feautres created earlier in the notebook.

# %% [markdown]
# **Choose Features for model**

# %%
#Clear nulls
df_hourly.dropna(inplace=True)

# Daily
df_daily_model_data = df_daily[['value', 'Hour', 'Day',  'Month','Month_day','Rolling_Mean']].dropna()

# Hourly
model_data = df_hourly[['value', 'Hour', 'Day', 'Month_day', 'Month','Rolling_Mean','Lag', 'timestamp']].set_index('timestamp').dropna()
model_data.head()

# %% [markdown]
# **Fit Model & View Outliers**

# %%
def run_isolation_forest(model_data: pd.DataFrame, contamination=0.005, n_estimators=200, max_samples=0.7) -> pd.DataFrame:
    
    IF = (IsolationForest(random_state=0,
                          contamination=contamination,
                          n_estimators=n_estimators,
                          max_samples=max_samples)
         )
    
    IF.fit(model_data)
    
    output = pd.Series(IF.predict(model_data)).apply(lambda x: 1 if x == -1 else 0)
    
    score = IF.decision_function(model_data)
    
    return output, score

# %%
outliers, score = run_isolation_forest(model_data)

# %%
df_hourly = (df_hourly
             .assign(Outliers = outliers)
             .assign(Score = score)
            )

df_hourly

# %%
IF = IsolationForest(random_state=0, contamination=0.005, n_estimators=200, max_samples=0.7)
IF.fit(model_data)

# New Outliers Column
df_hourly['Outliers'] = pd.Series(IF.predict(model_data)).apply(lambda x: 1 if x == -1 else 0)

# Get Anomaly Score
score = IF.decision_function(model_data)

# New Anomaly Score column
df_hourly['Score'] = score

df_hourly.head()

# %% [markdown]
# We can now see the anomaly scores for each data point. The lower, the more abnormal. Negative scores represent outliers, positive scores represent inliers.
# 
# This offers us some flexibility in determining our cutoff points for anomalies

# %% [markdown]
# # **Viewing the Anomalies**

# %%
def outliers(thresh):
    print(f'Number of Outliers below Anomaly Score Threshold {thresh}:')
    print(len(df_hourly.query(f"Outliers == 1 & Score <= {thresh}")))

# %%
tooltips = [
    ('Weekday', '@Weekday'),
    ('Day', '@Month_day'),
    ('Month', '@Month'),
    ('Value', '@value'),
    ('Average Value', '@value_Average'),
    ('Outliers', '@Outliers')
]
hover = HoverTool(tooltips=tooltips)

hv.Points(df_hourly.query("Outliers == 1")).opts(size=10, color='#ff0000') * hv.Curve(df_hourly).opts(opts.Curve(title="New York City Taxi Demand Anomalies", xlabel="", ylabel="Demand" , height=300, responsive=True,tools=[hover,'box_select', 'lasso_select', 'tap'],show_grid=True))

# %%
len(df_hourly.query("Outliers == 1"))

# %% [markdown]
# By plotting the anomalies in our data we can begin to assess how our model performs.

# %% [markdown]
# # **Assessing Outliers**

# %%
frequencies, edges = np.histogram(score, 50)
hv.Histogram((edges, frequencies)).opts(width=800, height=300,tools=['hover'], xlabel='Score')

# %% [markdown]
# As I said above, we can now see the anomaly scores for our dataset. The lower, the more abnormal. Negative scores represent outliers, positive scores represent inliers.
# 
# This offers us some flexibility in determining our cutoff points for anomalies.

# %%
# Function to view number of outliers at a given threshold
outliers(0.05)

#for num in (np.arange(-0.08, 0.2, 0.02)):
#    print(len(df_hourly.query(f"Outliers == 1 & Score <= {num}")))
#    num_outliers = len(df_hourly.query(f"Outliers == 1 & Score <= {num}"))

# %% [markdown]
# I'll now plot only those Outliers with an anomaly score of less than 0.05 - reflecting the function above

# %%
hover = HoverTool(tooltips=tooltips)

hv.Points(df_hourly.query("Outliers == 1 & Score <= 0.05")).opts(size=10, color='#ff0000') * hv.Curve(df_hourly).opts(opts.Curve(title="New York City Taxi Demand", xlabel="", ylabel="Demand" , height=300, responsive=True,tools=[hover,'box_select', 'lasso_select', 'tap'],show_grid=True))

# %% [markdown]
# By changing the threshold for anomalies, we are effectively determining the sensitivity of our model.

# %% [markdown]
# # **Choices**
# 
# Determining the cut-off point for anomaly scores is a subjective decision. 
# 
# It will likely depends on the business, and exactly what the anomalies represent. As with many Machine Learning tasks (especially classification or anomaly detection), the balance is often between being over-cautious and highlighting too many potential anomalies, and being under-cautious and risk missing genuine anomalies.
# 
# **Next Steps**
# 
# I didn't tune the Isolation Forest model at all, so this is an obvious first step.
# 
# In addition, there are many other anomaly detection techniques that could be employed. Perhaps I'll add them to this notebook over time.
# 
# Other methods might include:
# 
# * Clustering
# * Gaussian Probability
# * One-Class SVM
# * Markov processes
# 

# %% [markdown]
# # **Conclusion**
# 
# The aim of this notebook was to add anomaly detection to my portfolio, and to utilise a new data visulisation package (Bokeh/HoloVoews). I have acheived both of those aims and am happy with the outcome.
# 
# Anomaly detection is an area I'll likely be exploring more soon! There really is a lot of value in this field.
# 
# As for the data visualisation, I enjoyed using Bokeh/HoloViews. The interactivity is a nice feature. 
# 
# However, my preferred libraries are still Maptlotlib & Seaborn due to the amount of elements & customisation a user can enjoy.
# 
# **More Time Series Anomaly Detection**
# 
# Check out this notebook I put together to showcase the **STUMPY** Matrix Profiling library and how it can be used for anomaly detection
# 
# https://www.kaggle.com/code/joshuaswords/anomaly-detection-with-stumpy-matrix-profiling

# %%



