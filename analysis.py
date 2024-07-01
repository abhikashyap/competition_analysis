import streamlit as st 
from scrapper import fk_scrapper_functions as sc
from scrapper import flipkart_json_scrapper_with_all_specifications as fk_scrapper
import time
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
st.set_page_config(layout="wide")

competitor_data=pd.read_pickle('competitor_data.pkl')
competitor_data['brand']=competitor_data['title'].str.split(" ").str[0]
brand_level_data=competitor_data.copy()

for index, row in brand_level_data.iterrows():
    specs = row['all_specs']
    for key, value in specs.items():
        if key not in brand_level_data.columns:
            brand_level_data[key] = None
        brand_level_data.at[index, key] = value
most_important_feature={}
feature_name=[]
feature_count=[]
for column in brand_level_data.columns:
    feature_name.append(column)
    feature_count.append(brand_level_data[column].count())
most_important_feature['feature_name']=feature_name
most_important_feature['feature_count']=feature_count
important_features=pd.DataFrame(most_important_feature)
important_features['feautre_present_in_%_of_products']=important_features['feature_count']*100/important_features['feature_count'].max()
filtered_important_features=important_features[important_features['feautre_present_in_%_of_products'] >=10]['feature_name'].to_list()
final_scrapped_data=brand_level_data[filtered_important_features]
final_scrapped_data['final_selling_price']=final_scrapped_data['final_selling_price'].astype(float)
min_price=final_scrapped_data['final_selling_price'].min()
max_price=final_scrapped_data['final_selling_price'].max()
# price_filter = st.slider(
#     "Select Price to filter",
#     min_price, max_price, (min_price, max_price))
# st.write("Values:", price_filter)
price_filter_col1,price_filter_col2=st.columns(2)
with price_filter_col1:
    lower_price=st.number_input("Insert the min price",value=min_price,step=50.0)
with price_filter_col2:
    higher_price=st.number_input("Insert the max price",value=max_price,step=50.0)

final_scrapped_data=final_scrapped_data[(final_scrapped_data['final_selling_price']>lower_price)&(final_scrapped_data['final_selling_price']<higher_price)]
st.dataframe(final_scrapped_data)
st.write(f"Total Product count : {len(final_scrapped_data)}")



filter_out_less_product_count=st.number_input("Product count is greater than the inserted number is considered as others",value=3,step=1)
Brand_dominance=pd.DataFrame(final_scrapped_data['brand'].value_counts())
Brand_dominance.reset_index(inplace=True)
# Brand_dominance['Market_share']=Brand_dominance['count']*100/Brand_dominance['count'].sum()
Brand_dominance.loc[Brand_dominance['count']<int(filter_out_less_product_count),'Brand_type'] = 'others'
Brand_dominance['Brand_type'].fillna(Brand_dominance['brand'],inplace=True)
Brand_dominance.groupby('Brand_type')['count'].sum()
Brand_dominance_fig=px.pie(Brand_dominance, values='count', names='Brand_type', title=f'''Brand Dominance in price range {lower_price} to {higher_price} with count of product > 5''',height=800,width=1200)
st.plotly_chart(Brand_dominance_fig)
st.dataframe(Brand_dominance)


#option
options_to_select = []
for column in filtered_important_features:
    if column not in competitor_data.columns.to_list():
        options_to_select.append(column)
option = st.selectbox(
    "Select Feature",
    tuple(options_to_select))

feature=final_scrapped_data.copy()
col1,col2=st.columns(2)
color=feature.groupby(['brand',option]).agg({"fsn":'count'}).rename({'fsn':"count_of_products"},axis=1)
color.reset_index(inplace=True)
color=color.sort_values(by='count_of_products',ascending=False)
top_to_color=color.head(10)

fig = px.sunburst(top_to_color, values='count_of_products', path=['brand', option], title=f'Feature Importance Of {option}',height=800,width=1200)
st.dataframe(color)
with col1:
    st.plotly_chart(fig, use_container_width=False)
with col2:
    color_overall=feature.groupby([option]).agg({"fsn":'count'}).rename({'fsn':"count_of_products"},axis=1)
    color_overall.reset_index(inplace=True)
    color_overall=color_overall.sort_values(by='count_of_products',ascending=False)
    top_to_color_overall=color_overall.head(10)
    pie= px.pie(top_to_color_overall, values='count_of_products', names=option, title=f'Feature Importance Of {option} in market',height=800,width=1200)
    st.plotly_chart(pie)
    

