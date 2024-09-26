#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd


# # 1. Check table trades

# In[7]:


params={
        'database': 'technical_test',
        'user': 'candidate',
        'password': 'NW337AkNQH76veGc',
        'host': 'technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com',
        'port': 5432
}
conn = psycopg2.connect(**params)
cursor = conn.cursor()
trades = pd.read_sql_query('SELECT * FROM trades', conn)
    
conn.close()
print('close connection')
trades.head()


# ## (1). Check column login_hash
# 
#     Findings: 1. All values for column login_hash have same length of 32 and there are no null values exist.
#     
#     Conclusion: No issues found for column login_hash.

# In[10]:


# Check minimum length and maximal length of value for column login_hash
min_length_login_hash = trades['login_hash'].str.len().min()
max_length_login_hash = trades['login_hash'].str.len().max()
print('Minimal length of login_hash is: ' + str(min_length_login_hash))
print('Maximal length of login_hash is: ' + str(max_length_login_hash))


# In[16]:


# Check if null value exists on column login_hash
ifNullExistsOnLogin_hash = trades['login_hash'].isnull().any();
print('If null value exists on column login_hash: ' + str(ifNullExistsOnLogin_hash))


# ## (2). Check column ticket_hash
# 
#        Findings: 1. All values for column ticket_hash have the same length of 32 and no null values exist.
#        
#        Conclusion: No issues found for column ticket_hash.

# In[17]:


# Check minimum length and maximal length of value for column ticket_hash
min_length_ticket_hash = trades['ticket_hash'].str.len().min()
max_length_ticket_hash = trades['ticket_hash'].str.len().max()
print('Minimal length of ticket_hash is: ' + str(min_length_ticket_hash))
print('Maximal length of ticket_hash is: ' + str(max_length_ticket_hash))


# In[18]:


# Check if null value exists on column ticket_hash
ifNullExistsOnTicket_hash = trades['ticket_hash'].isnull().any();
print('If null value exists on column ticket_hash: ' + str(ifNullExistsOnTicket_hash))


# ## (3). Check column server_hash
# 
#       Findings: 1. All values for column server_hash have the same length of 32 and no null values exist.
#         
#       Conclusion: No issues found for column server_hash.

# In[19]:


# Check minimum length and maximal length of value for column server_hash
min_length_server_hash = trades['server_hash'].str.len().min()
max_length_server_hash = trades['server_hash'].str.len().max()
print('Minimal length of server_hash is: ' + str(min_length_server_hash))
print('Maximal length of server_hash is: ' + str(max_length_server_hash))


# In[20]:


# Check if null value exists on column server_hash
ifNullExistsOnServer_hash = trades['server_hash'].isnull().any();
print('If null value exists on column server_hash: ' + str(ifNullExistsOnServer_hash))


# ## (4). Check column symbol
# 
#      Findings: 1. No null values found for column symbol
#                2. Value 'USD,CHF' which appears two times looks strange, I presume the value should be 'USDCHF'
#                3. Value 'COFFEE' appears in this column, it looks strange but I am not sure if it is a valid   
#                   value here as I am lack of business knowledge. But I will list it here.
#                     
#      Conclusion: Value 'USD,CHF' is not correct and the comma needs to be removed. For value 'COFFEE', it looks 
#                  a bit strange but as I am lack of the business knwoledge, so just list it here for further 
#                  investigation.

# In[21]:


# Check if null values exist on column symbol
ifNullExistsOnSymbol = trades['symbol'].isnull().any();
print('If null value exists on column symbol: ' + str(ifNullExistsOnSymbol))


# In[24]:


# Check strange values for column symbol, check distinct values for column symbol
distinctValues = trades['symbol'].unique()
distinctValues


# ## (5). Check column digits
# 
#        Findings: 1. No null values exit for column digits and max value and min value are reasonable.
#        
#        Conclusion: No issues found for column digits.

# In[25]:


# Check if null values exist on column digits
ifNullExistsOnDigits = trades['digits'].isnull().any();
print('If null value exists on column digits: ' + str(ifNullExistsOnDigits))


# In[26]:


# Check minimal and maximal value on column digits
minValueDigits = trades['digits'].min()
maxValueDigits = trades['digits'].max()
print('Minimal value of digits is: ' + str(minValueDigits))
print('Maximal value of digits is: ' + str(maxValueDigits))


# ## (6). Check column cmd
# 
#        Findings: 1. No null values exist on column cmd. Distinct values 0 and 1 which are reasonable.
#        
#        Conclusion: No issues found on column cmd.

# In[27]:


# Check if null values exist on column cmd
ifNullExistsOnCmd = trades['cmd'].isnull().any();
print('If null value exists on column cmd: ' + str(ifNullExistsOnCmd))


# In[28]:


# Check strange values for column cmd, check distinct values for column cmd
distinctValues = trades['cmd'].unique()
distinctValues


# ## (7). Check column volume
# 
#        Findings: 1. No null values found on column volume.
#                  2. Minimum value of column volume is 0 that appears one time. It looks strange to me.
#                  
#        Conclusion: Value 0 happens one time on column volume which seems strange. Need further investigation
#                    with business knowledge.

# In[29]:


# Check if null values exist on column volume
ifNullExistsOnVolume = trades['volume'].isnull().any();
print('If null value exists on column volume: ' + str(ifNullExistsOnVolume))


# In[30]:


# Check minimal and maximal value on column volume
minValueVolume = trades['volume'].min()
maxValueVolume = trades['volume'].max()
print('Minimal value of volume is: ' + str(minValueVolume))
print('Maximal value of volume is: ' + str(maxValueVolume))


# ## (8). Check column open_time
# 
#        Findings: 1. No null values exist on column open_time.
#                  2. Minimal and maximal value are 2020-08-03 and 2020-08-31 which are reasonable.
#                
#        Conclusion: No issues found on column open_time.

# In[31]:


# Check minimal and maximal value on column open_time
minValueOpenTime = trades['open_time'].dt.date.min()
maxValueOpenTime = trades['open_time'].dt.date.max()
print('Minimal value of open_time is: ' + str(minValueOpenTime))
print('Maximal value of open_time is: ' + str(maxValueOpenTime))


# In[32]:


# Check if null values exist on column open_time
ifNullExistsOnOpenTime = trades['open_time'].isnull().any();
print('If null value exists on column open_time: ' + str(ifNullExistsOnOpenTime))


# ## (9). Check column open_price
# 
#       Findings: 1. No null values found on column open_price.
#                 2. No strange values found on column open_price (I presume values less than 1 is reasonable).
#             
#       Conclusion: No issues found on column open_price.

# In[33]:


# Check if null values exist on column open_price
ifNullExistsOnOpenPrice = trades['open_price'].isnull().any();
print('If null value exists on column open_price: ' + str(ifNullExistsOnOpenPrice))


# In[34]:


# Check minimal and maximal value on column open_price
minValueOpenPrice = trades['open_price'].min()
maxValueOpenPrice = trades['open_price'].max()
print('Minimal value of open_price is: ' + str(minValueOpenPrice))
print('Maximal value of open_price is: ' + str(maxValueOpenPrice))


# ## (10). Check column close_time
# 
#         Findings: 1. No null values exist on column close_time.
#                   2. There are 36 trades where close time equals open time. I presume it is a bit strange.
#                   3. Maximal close time is 2022-08-18, while maximal open time is 2020-08-31. I found 4780 
#                      trades are closed in 2022 which take too long. I presume this might be interesting.
#                  
#         Conclusion: Found 36 trades of which the close time equals the open time. 4780 trades that are opened in 
#                     2020 are closed in 2022. These long-duration trades might need to be investigated.

# In[36]:


# Check if null values exist on column close_time
ifNullExistsOnCloseTime = trades['close_time'].isnull().any();
print('If null value exists on column close_time: ' + str(ifNullExistsOnCloseTime))


# In[39]:


# Check if close time equals open time
sub_df = trades[trades['close_time'] == trades['open_time']]
sub_df.shape


# In[40]:


# Check minimal and maximal value on column close_time
minValueCloseTime = trades['close_time'].dt.date.min()
maxValueCloseTime = trades['close_time'].dt.date.max()
print('Minimal value of close_time is: ' + str(minValueCloseTime))
print('Maximal value of close_time is: ' + str(maxValueCloseTime))


# ## (11). Check column contractsize
# 
#         Findings: 1. Null values appear 7 times on column contractsize. 
#         
#         Conclusion: I presume null value shouldn't appear on column contractsize, this needs to be investigated.

# In[41]:


# Check if null values exist on column contractsize
ifNullExistsOnContractSize = trades['contractsize'].isnull().any();
print('If null value exists on column contractsize: ' + str(ifNullExistsOnContractSize))


# In[42]:


# Check minimal and maximal value on column contractsize
minValueContractSize = trades['contractsize'].min()
maxValueContractSize = trades['contractsize'].max()
print('Minimal value of contractsize is: ' + str(minValueContractSize))
print('Maximal value of contractsize is: ' + str(maxValueContractSize))


# # 2. Check table users

# In[43]:


params={
        'database': 'technical_test',
        'user': 'candidate',
        'password': 'NW337AkNQH76veGc',
        'host': 'technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com',
        'port': 5432
}
conn = psycopg2.connect(**params)
cursor = conn.cursor()
users = pd.read_sql_query('SELECT * FROM users', conn)
    
conn.close()
print('close connection')
users.head()


# ## (1). For column login_hash
# 
#         Findings: 1. No null values found on column login_hash. All values have the same length of 32.
#         
#         Conclusion: No issues found on this column.

# In[44]:


# Check minimum length and maximal length of value for column login_hash
min_length_login_hash = users['login_hash'].str.len().min()
max_length_login_hash = users['login_hash'].str.len().max()
print('Minimal length of login_hash is: ' + str(min_length_login_hash))
print('Maximal length of login_hash is: ' + str(max_length_login_hash))


# In[45]:


# Check if null value exists on column login_hash
ifNullExistsOnLogin_hash = users['login_hash'].isnull().any();
print('If null value exists on column login_hash: ' + str(ifNullExistsOnLogin_hash))


# ## (2). For column server_hash
# 
#       Findings: 1. No null values found on column server_hash. All values have the same length of 32.
#       
#       Conclusion: No issues found on this column

# In[46]:


# Check minimum length and maximal length of value for column server_hash
min_length_server_hash = users['server_hash'].str.len().min()
max_length_server_hash = users['server_hash'].str.len().max()
print('Minimal length of server_hash is: ' + str(min_length_server_hash))
print('Maximal length of server_hash is: ' + str(max_length_server_hash))


# In[47]:


# Check if null value exists on column server_hash
ifNullExistsOnServer_hash = users['server_hash'].isnull().any();
print('If null value exists on column server_hash: ' + str(ifNullExistsOnServer_hash))


# ## (3). For column country_hash
# 
#        Findings: 1. No null values found on column country_hash. All values have the same length of 32.
#        
#        Conclusion: No issues found on this column.

# In[48]:


# Check minimum length and maximal length of value for column country_hash
min_length_country_hash = users['country_hash'].str.len().min()
max_length_country_hash = users['country_hash'].str.len().max()
print('Minimal length of country_hash is: ' + str(min_length_country_hash))
print('Maximal length of country_hash is: ' + str(max_length_country_hash))


# In[49]:


# Check if null value exists on column country_hash
ifNullExistsOnCountry_hash = users['country_hash'].isnull().any();
print('If null value exists on column country_hash: ' + str(ifNullExistsOnCountry_hash))


# ## (4). For column currency
# 
#       Findings: 1. No null value exists on column currency. No strange values found on this column.
#       
#       Conclusion: No issues found on this column.

# In[50]:


# Check strange values for column currency, check distinct values for column currency
distinctValues = users['currency'].unique()
distinctValues


# In[51]:


# Check if null value exists on column currency
ifNullExistsOnCurrency = users['currency'].isnull().any();
print('If null value exists on column currency: ' + str(ifNullExistsOnCurrency))


# ## (5). For column enable
# 
#        Findings: 1. No null values exist on column enable. No strange values found on this column.
#        
#        Conclusion: No issues found on this column.

# In[52]:


# Check strange values for column enable, check distinct values for column enable
distinctValues = users['enable'].unique()
distinctValues


# In[53]:


# Check if null value exists on column enable
ifNullExistsOnEnable = users['enable'].isnull().any();
print('If null value exists on column enable: ' + str(ifNullExistsOnEnable))


# In[ ]:




