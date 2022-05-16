import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas.io.sql as sqlio

def data_fetch():

    #Snowflake
    url= URL(
    user='hunain.mohiuddin@airlifttech.com',
    account='ul14727.ap-southeast-1',
    warehouse= 'METABASE_WAREHOUSE',
    database = 'DATA_WAREHOUSE',
    schema = 'BUSINESS_VAULT',
    authenticator='externalbrowser',)

    engine = create_engine(url)
    connection = engine.connect()

    query= f"""
    select 
    date_trunc('day',  dispatchtime + interval '5hours')::date as "Date", 
    extract(hour from date_trunc('hour',dispatchtime + interval '5hours' )) as "Hour",

    od.city as "City",
    od.warehouse as "Warehouse",

    count(od.id) as "forecasted_orders"

    from order_details od
    join orders o on od.id= o.id

    where 
    od.country = 'Pakistan'
    and od.status in ( 'delivered', 'returned','return_flow_complete' )
    and od.warehouse not in ('Karachi Electronics Fulfillment Center', 'Lahore Electronics Fulfillment Center', 'Islamabad Electronics Fulfillment Center')
    and od.warehouse not like ('%Pharma%')
    and ( date_trunc('day',  dispatchtime + interval '5hours' ) in (
    '2022-05-07','2022-05-09'
    ) )
    group by 1,2,3,4
    order by 1,4,2
    """
    print("Data Fetched-")

    return sqlio.read_sql_query(query, connection)

#Add remapping of orders-


def aggregation(df):
    #Hours & Base
    hour=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
    Hour= pd.DataFrame({'Hour':hour})
    base= pd.DataFrame(columns=['Date','Hour','City','Warehouse','OrderCount'])

    basedfs=[]
    for date in df.Date.unique():
        ews_= df[df.Date==date]
        for warehouse in df.Warehouse.unique():
            ew= ews_[ews_.Warehouse==warehouse]
            
            ew= pd.merge(Hour,ew, how='left', on = 'Hour')
            
            ew.Warehouse.fillna(warehouse, inplace= True)
            ew.Date.fillna(date, inplace= True)    
            ew.forecasted_orders.fillna(0, inplace = True)
            
            base= pd.concat([base,ew],  ignore_index=True)
            basedfs.append(pd.concat([base,ew],  ignore_index=True))
    avg= base.groupby(['Hour','Warehouse']).mean().reset_index()
    print("Data grouped")

    df.to_excel('Hourly_data.xlsx')
    avg.to_excel('Grouped_data.xlsx')
    print("Data saved at root")

    return df,avg

