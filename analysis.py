import streamlit as st 
from scrapper import fk_scrapper_functions as sc
from scrapper import flipkart_json_scrapper_with_all_specifications as fk_scrapper
import time
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import analysis_function as f
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

df_col1,df_col2,df_col3,df_col4=st.columns(4)
with df_col1:
    lower_price=st.number_input("Insert the min price",value=min_price,step=50.0)
with df_col2:
    higher_price=st.number_input("Insert the max price",value=max_price,step=50.0)
with df_col3:
    rating_count=st.number_input("Insert ratings count",value=0.0,step=1.0)
with df_col4:
    review_count=st.number_input("Insert review counts",value=0.0,step=1.0)

final_scrapped_data=final_scrapped_data[(final_scrapped_data['final_selling_price']>lower_price)&(final_scrapped_data['final_selling_price']<higher_price)]
final_scrapped_data['ratings_count']=final_scrapped_data['ratings_count'].apply(lambda x: x if f.is_integer(x) else None)
final_scrapped_data['reviews_count']=final_scrapped_data['reviews_count'].apply(lambda x: x if f.is_integer(x) else None)
final_scrapped_data=final_scrapped_data[(final_scrapped_data['ratings_count']>=int(rating_count))]
final_scrapped_data=final_scrapped_data[(final_scrapped_data['reviews_count']>=int(review_count))]


metrics=st.multiselect("enter metrics to select",filtered_important_features)
metric_to_filter={}
for metric in metrics:
    option_values=final_scrapped_data[metric].unique().tolist()
    metric_to_filter[metric]=option_values

for key,value in metric_to_filter.items():

    selected_metric_col1,selected_metric_col2=st.columns(2)
    with selected_metric_col1:
        selected_field_value = st.multiselect(f"Enter selections for {key}", value)
    with selected_metric_col2:
        typed_value = st.text_input(f"search all for {key}")

    if typed_value:
        typed_value = str(typed_value).lower()
        preselected_values = [str(v) for v in value if typed_value in str(v).lower()]
        selected_field_value.extend(preselected_values)
        selected_field_value = list(set(selected_field_value))

    selected_field_value = [str(i).lower() for i in selected_field_value]

    final_scrapped_data = final_scrapped_data[final_scrapped_data[key].astype(str).str.lower().isin(selected_field_value)]

st.dataframe(final_scrapped_data)
st.markdown("# Product distributions")
st.write(f"Total Product count : {len(final_scrapped_data)}")

dominance_col1,dominance_col2=st.columns(2)
with dominance_col1:
    filter_out_less_product_count=st.number_input("Product count is greater than the inserted number is considered as others",value=3,step=1)
    Brand_dominance=pd.DataFrame(final_scrapped_data['brand'].value_counts())
    Brand_dominance.reset_index(inplace=True)
    # Brand_dominance['Market_share']=Brand_dominance['count']*100/Brand_dominance['count'].sum()
    Brand_dominance.loc[Brand_dominance['count']<int(filter_out_less_product_count),'Brand_type'] = 'others'
    Brand_dominance['Brand_type'].fillna(Brand_dominance['brand'],inplace=True)
    Brand_dominance.groupby('Brand_type')['count'].sum()
    Brand_dominance_fig=px.pie(Brand_dominance, values='count', names='Brand_type', title=f'''Brand Dominance in price range {lower_price} to {higher_price} with count of product > {filter_out_less_product_count}''',height=800,width=1200)
    st.plotly_chart(Brand_dominance_fig)
with dominance_col2:
    with st.expander("See details of count of product"):
        st.dataframe(Brand_dominance)
    other=Brand_dominance[Brand_dominance['count']<filter_out_less_product_count]
    other_brands=other.groupby('count').agg({'brand': lambda x: list(x)})
    other_brands.reset_index(inplace=True)
    other_brands=other_brands.rename({'count':'No of products'},axis=1)
    st.write(other_brands)
    rating_df=final_scrapped_data.groupby('brand').agg({"ratings_count":"sum"})
    rating_df.reset_index(inplace=True)
    rating_df=rating_df.sort_values(by='ratings_count',ascending=False).head(20)
    # rating_pie=px.bar(rating_df, y='ratings_count', x='brand',height=800,width=1200)
    rating_pie=px.pie(rating_df, values='ratings_count', names='brand',height=600,width=1000)
    st.plotly_chart(rating_pie)
with st.container():
    price_col1,price_col2=st.columns(2)
    with price_col1:
        price_hist=px.histogram(final_scrapped_data, x='final_selling_price', nbins=10,height=1000,width=1000, title='Distribution of Final Selling Prices')
        st.plotly_chart(price_hist)

    price_vs_ratings=final_scrapped_data.groupby('final_selling_price').agg({'ratings_count':'sum'})
    price_vs_ratings.reset_index(inplace=True)
    with price_col2:
        rating_hist=px.scatter(price_vs_ratings, x='final_selling_price', y='ratings_count', 
                        title='Scatter Plot of Final Selling Price vs Ratings Count',
                        labels={'final_selling_price': 'Final Selling Price', 'ratings_count': 'Ratings Count'},
                        trendline="ols")

        st.plotly_chart(rating_hist)
        list_of_price=st.text_input("Enter list of price ",value=int(price_vs_ratings['final_selling_price'].values[0]))
        list_of_price=[int(i) for i in list_of_price.split(',')]
        brand_info=final_scrapped_data[final_scrapped_data['final_selling_price'].isin(list_of_price)]
        brand_info=brand_info.groupby('brand').agg({"brand":"count"}).rename({"brand":"count_of_products"},axis=1)
        brand_info.reset_index(inplace=True)
        brand_presence_with_count_of_fsn=px.pie(brand_info, values='count_of_products', names='brand')
        st.plotly_chart(brand_presence_with_count_of_fsn)


rating_df=final_scrapped_data.groupby('brand').agg({"ratings_count":"sum"})
rating_df.reset_index(inplace=True)
all_discription_filter=list(set(filtered_important_features).difference(set(competitor_data.columns.tolist())))
list_of_brand=st.text_input("Enter list of brands ",value='boAt')
list_of_brand=[str(i) for i in list_of_brand.split(',')]
features_to_analyze=final_scrapped_data[final_scrapped_data['brand'].isin(list_of_brand) & final_scrapped_data['final_selling_price'].isin(list_of_price)]
all_discription_filter.extend(['brand','fsn'])

feature_df=features_to_analyze[all_discription_filter]

features_after_not_unique=[description for description in feature_df if feature_df[description].nunique()>1]
feature_df
for fe in features_after_not_unique:
    if (fe not in ['brand', 'fsn']) and ('warranty' not in fe.lower()):
        feature_col1,feature_col2=st.columns(2)
        
        with feature_col1:
            st.markdown(f"##### Comparison for {fe}")
        
            feature_df_temp=feature_df[features_after_not_unique]
            feature_df_temp_grouped=feature_df_temp.groupby(['brand',fe]).agg({"fsn":["count",list]})
            feature_df_temp_grouped.reset_index(inplace=True)
            feature_df_temp_grouped.columns=[" ".join(i) for i in feature_df_temp_grouped.columns]
                
            st.dataframe(feature_df_temp_grouped)
        with feature_col2:
        
            fe_distribution=pd.DataFrame(final_scrapped_data[fe].value_counts())
            fe_distribution.reset_index(inplace=True)
            fe_distribution_pie=fe_distribution.sort_values(by='count',ascending=False).head(10)
            fe_fig=px.pie(fe_distribution_pie,values='count',names=fe,title=f'Feature Importance Of {fe} in market')
            st.plotly_chart(fe_fig)
    # st.write(feature_df_temp_grouped)
# # #option
# options_to_select = []
# for column in filtered_important_features:
#     if column not in competitor_data.columns.to_list():
#         options_to_select.append(column)
# option = st.selectbox(
#     "Select Feature",
#     tuple(options_to_select))

# feature=final_scrapped_data.copy()
# col1,col2=st.columns(2)
# color=feature.groupby(['brand',option]).agg({"fsn":'count'}).rename({'fsn':"count_of_products"},axis=1)
# color.reset_index(inplace=True)
# color=color.sort_values(by='count_of_products',ascending=False)
# top_to_color=color.head(10)

# fig = px.sunburst(top_to_color, values='count_of_products', path=['brand', option], title=f'Feature Importance Of {option}',height=800,width=1200)
# st.dataframe(color)
# with col1:
#     st.plotly_chart(fig, use_container_width=False)
# with col2:
#     color_overall=feature.groupby([option]).agg({"fsn":'count'}).rename({'fsn':"count_of_products"},axis=1)
#     color_overall.reset_index(inplace=True)
#     color_overall=color_overall.sort_values(by='count_of_products',ascending=False)
#     top_to_color_overall=color_overall.head(10)
#     pie= px.pie(top_to_color_overall, values='count_of_products', names=option, title=f'Feature Importance Of {option} in market',height=800,width=1200)
#     st.plotly_chart(pie)
    

