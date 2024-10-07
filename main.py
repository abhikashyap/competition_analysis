import streamlit as st
import pandas as pd
import plotly.express as px
import analysis_function as f
from scrapper import fk_scrapper_functions as sc
from scrapper import flipkart_json_scrapper_with_all_specifications as fk_scrapper
from scrapper.all_fsn_list import collect_all_data_ids

st.set_page_config(layout="wide")

def list_to_string(x):
    if isinstance(x, list):
        return ', '.join(map(str, x))
    elif pd.isnull(x):  # Handle NaN values if any
        return ''
    else:
        return str(x)

st.markdown("# Competition Analysis")

# Initialize session state variables
if 'all_fsns' not in st.session_state:
    st.session_state['all_fsns'] = []
if 'competitor_data' not in st.session_state:
    st.session_state['competitor_data'] = pd.DataFrame()

with st.container():
    keywords = st.text_input("Enter keywords to do competitive analysis (separated by commas)")

    if st.button('Submit'):
        if keywords:
            st.write(f"Submitted keywords are: {keywords}")
            list_of_keywords = [kw.strip() for kw in keywords.split(",")]

            progress_text = "Collecting FSNs for the given keywords..."
            overall_progress = st.progress(0, text=progress_text)

            for i, keyword in enumerate(list_of_keywords):
                search_queries = [keyword]
                start_page = 1
                end_page = 5  # Adjust the number of pages as needed

                # Collect FSNs
                all_data = collect_all_data_ids(search_queries, start_page, end_page)
                st.write(f"Total data-id attributes found for '{keyword}': {len(all_data)}")

                # Create DataFrame
                df = pd.DataFrame(all_data, columns=['search_query', 'position', 'page_no', 'data_id'])
                st.write(df)

                # Get unique FSNs and update session state
                new_fsns = list(set(df['data_id'].tolist()))
                st.session_state['all_fsns'].extend(new_fsns)
                # Remove duplicates
                st.session_state['all_fsns'] = list(set(st.session_state['all_fsns']))

                # Update progress
                overall_progress.progress((i + 1) / len(list_of_keywords), text=f"{progress_text} ({i + 1}/{len(list_of_keywords)})")

            overall_progress.progress(1.0, text="FSN collection complete!")
        else:
            st.warning("Please enter at least one keyword.")
    else:
        st.info("Enter keywords and click 'Submit' to begin.")

# Use session state variable
all_fsns = st.session_state['all_fsns']
st.write(f"Total number of product IDs collected: {len(all_fsns)}")

if len(all_fsns) > 0:
    final_fsn_list = all_fsns  # Already unique

    if st.session_state['competitor_data'].empty:
        # Scrape data and store in session state
        with st.spinner("Scraping competitor data..."):
            st.session_state['competitor_data'] = fk_scrapper.scrape_all_fsns(final_fsn_list)
        st.success("Data scraping complete!")

    # Use a copy to avoid modifying session state directly
    competitor_data = st.session_state['competitor_data'].copy()

    if not competitor_data.empty:
        # Data processing
        competitor_data['brand'] = competitor_data['title'].str.split(" ").str[0]
        brand_level_data = competitor_data.copy()
        brand_level_data['highlights'] = brand_level_data['highlights'].apply(list_to_string)

        # Ensure 'fsn' and 'brand' are included
        required_columns = ['fsn', 'brand']
        for col in required_columns:
            if col in brand_level_data.columns and col not in brand_level_data.columns:
                brand_level_data[col] = competitor_data[col]

        # Display DataFrame
        st.dataframe(brand_level_data)

        # Expand 'all_specs' into separate columns
        for index, row in brand_level_data.iterrows():
            specs = row['all_specs']
            if isinstance(specs, dict):
                for key, value in specs.items():
                    if key not in brand_level_data.columns:
                        brand_level_data[key] = None
                    brand_level_data.at[index, key] = value

        # Calculate feature importance
        most_important_feature = {}
        feature_name = []
        feature_count = []
        for column in brand_level_data.columns:
            feature_name.append(column)
            feature_count.append(brand_level_data[column].count())
        most_important_feature['feature_name'] = feature_name
        most_important_feature['feature_count'] = feature_count
        important_features = pd.DataFrame(most_important_feature)
        important_features['feature_present_in_%_of_products'] = important_features['feature_count'] * 100 / important_features['feature_count'].max()
        filtered_important_features = important_features[important_features['feature_present_in_%_of_products'] >= 0]['feature_name'].tolist()

        # Ensure 'fsn' is included in filtered features
        if 'fsn' not in filtered_important_features:
            filtered_important_features.append('fsn')

        final_scrapped_data = brand_level_data[filtered_important_features]
        final_scrapped_data = final_scrapped_data[final_scrapped_data['final_selling_price'] != ""]
        final_scrapped_data['final_selling_price'] = final_scrapped_data['final_selling_price'].astype(float)

        min_price = final_scrapped_data['final_selling_price'].min()
        max_price = final_scrapped_data['final_selling_price'].max()

        # Price and rating filters
        df_col1, df_col2, df_col3, df_col4 = st.columns(4)
        with df_col1:
            lower_price = st.number_input("Insert the min price", value=float(min_price), step=50.0)
        with df_col2:
            higher_price = st.number_input("Insert the max price", value=float(max_price), step=50.0)
        with df_col3:
            rating_count = st.number_input("Insert ratings count", value=0, step=1)
        with df_col4:
            review_count = st.number_input("Insert review counts", value=0, step=1)

        # Apply filters
        final_scrapped_data = final_scrapped_data[
            (final_scrapped_data['final_selling_price'] >= lower_price) &
            (final_scrapped_data['final_selling_price'] <= higher_price)
        ]
        final_scrapped_data['ratings_count'] = pd.to_numeric(final_scrapped_data['ratings_count'], errors='coerce').fillna(0).astype(int)
        final_scrapped_data['reviews_count'] = pd.to_numeric(final_scrapped_data['reviews_count'], errors='coerce').fillna(0).astype(int)
        final_scrapped_data = final_scrapped_data[final_scrapped_data['ratings_count'] >= int(rating_count)]
        final_scrapped_data = final_scrapped_data[final_scrapped_data['reviews_count'] >= int(review_count)]

        # Metrics selection
        metrics = st.multiselect("Select metrics to filter", filtered_important_features)
        metric_to_filter = {}
        for metric in metrics:
            option_values = final_scrapped_data[metric].dropna().unique().tolist()
            metric_to_filter[metric] = option_values

        for key, value in metric_to_filter.items():
            selected_metric_col1, selected_metric_col2 = st.columns(2)
            with selected_metric_col1:
                selected_field_value = st.multiselect(f"Select values for {key}", value)
            with selected_metric_col2:
                typed_value = st.text_input(f"Search for {key}")

            if typed_value:
                typed_value = str(typed_value).lower()
                preselected_values = [str(v) for v in value if typed_value in str(v).lower()]
                selected_field_value.extend(preselected_values)
                selected_field_value = list(set(selected_field_value))

            selected_field_value = [str(i).lower() for i in selected_field_value]

            final_scrapped_data = final_scrapped_data[final_scrapped_data[key].astype(str).str.lower().isin(selected_field_value)]

        st.dataframe(final_scrapped_data)
        st.markdown("# Product Distributions")
        st.write(f"Total Product count: {len(final_scrapped_data)}")

        # Brand dominance analysis
        dominance_col1, dominance_col2 = st.columns(2)
        with dominance_col1:
            filter_out_less_product_count = st.number_input("Minimum product count to display (others will be grouped)", value=3, step=1)
            Brand_dominance = final_scrapped_data['brand'].value_counts().reset_index()
            Brand_dominance.columns = ['brand', 'count']
            Brand_dominance['Brand_type'] = Brand_dominance.apply(lambda row: row['brand'] if row['count'] >= filter_out_less_product_count else 'Others', axis=1)
            Brand_dominance_grouped = Brand_dominance.groupby('Brand_type')['count'].sum().reset_index()
            Brand_dominance_fig = px.pie(Brand_dominance_grouped, values='count', names='Brand_type',
                                         title=f"Brand Dominance in price range {lower_price} to {higher_price} with product count > {filter_out_less_product_count}",
                                         height=800, width=1200, template='presentation')
            st.plotly_chart(Brand_dominance_fig)
        with dominance_col2:
            with st.expander("See details of product counts"):
                st.dataframe(Brand_dominance)
            other_brands = Brand_dominance[Brand_dominance['Brand_type'] == 'Others']
            other_brands_list = other_brands['brand'].tolist()
            st.write(f"Brands grouped as 'Others': {', '.join(other_brands_list)}")
            rating_df = final_scrapped_data.groupby('brand').agg({"ratings_count": "sum", "fsn": "count"}).reset_index()
            rating_df = rating_df.rename(columns={'fsn': 'product_count'})
            rating_df = rating_df.sort_values(by='ratings_count', ascending=False).head(20)
            rating_pie = px.pie(rating_df, values='ratings_count', names='brand', custom_data=['product_count'],
                                height=600, width=1000, template='presentation')
            rating_pie.update_traces(hovertemplate='<b>%{label}</b><br>Ratings Count: %{value}<br>Product Count: %{customdata[0]}<extra></extra>')
            st.plotly_chart(rating_pie)

        # Price distribution and scatter plot
        with st.container():
            price_col1, price_col2 = st.columns(2)
            with price_col1:
                price_hist = px.histogram(final_scrapped_data, x='final_selling_price', nbins=10, height=800, width=1000,
                                          title='Distribution of Final Selling Prices', color='brand',
                                          template='presentation')
                st.plotly_chart(price_hist)

            price_vs_ratings = final_scrapped_data.groupby('final_selling_price').agg({'ratings_count': 'sum', 'brand': lambda x: list(set(x)), 'fsn': 'count'}).reset_index()
            price_vs_ratings['brand'] = price_vs_ratings['brand'].apply(lambda x: ", ".join(x))
            with price_col2:
                rating_scatter = px.scatter(price_vs_ratings, x='final_selling_price', y='ratings_count',
                                            title='Final Selling Price vs Ratings Count', labels={'final_selling_price': 'Final Selling Price', 'ratings_count': 'Ratings Count'},
                                            hover_data={'brand': True, 'fsn': True}, trendline="ols", size='fsn',
                                            width=800, height=600)
                rating_scatter.update_traces(
                    hovertemplate="<span style='font-size:16px'><b>Price: %{x}</b><br>"
                                  "<b>Ratings Count: %{y}</b><br>"
                                  "<b>Brands: %{customdata[0]}</b><br>"
                                  "<b>Product Count: %{customdata[1]}</b><br></span>",
                    customdata=price_vs_ratings[['brand', 'fsn']]
                )
                st.plotly_chart(rating_scatter)

        # Additional price range filter and brand distribution
        st.markdown("## Brand Distribution within Specific Price Range")
        pricing_col1, pricing_col2 = st.columns(2)
        with pricing_col1:
            lower_range = st.number_input(label="Enter lower price range for brand analysis", value=float(min_price), step=50.0)
        with pricing_col2:
            higher_range = st.number_input(label="Enter upper price range for brand analysis", value=float(max_price), step=50.0)
        brand_info = final_scrapped_data[
            (final_scrapped_data['final_selling_price'] >= lower_range) &
            (final_scrapped_data['final_selling_price'] <= higher_range)
        ]
        brand_info = brand_info.groupby('brand').agg({"fsn": "count"}).rename(columns={"fsn": "count_of_products"}).reset_index()
        brand_presence_with_count_of_fsn = px.pie(brand_info, values='count_of_products', names='brand',
                                                  title=f"Brand Presence with Product Count in Price Range {lower_range} to {higher_range}",
                                                  template='presentation')
        st.plotly_chart(brand_presence_with_count_of_fsn)

        # Brand and feature analysis
        rating_df = final_scrapped_data.groupby('brand').agg({"ratings_count": "sum"}).reset_index()
        brand_filtered = rating_df['brand'].tolist()
        all_description_filter = list(set(filtered_important_features).difference(set(competitor_data.columns.tolist())))
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            # **Updated Code to Select All Brands by Default**
            selected_brand = st.multiselect("Select brand names", brand_filtered, default=brand_filtered)
        features_to_analyze = final_scrapped_data[
            (final_scrapped_data['brand'].isin(selected_brand)) &
            (final_scrapped_data['final_selling_price'] >= lower_price) &
            (final_scrapped_data['final_selling_price'] <= higher_price)
        ]
        all_description_filter.extend(['brand', 'fsn', 'image_link'])

        feature_df = features_to_analyze[all_description_filter]

        features_after_not_unique = [description for description in feature_df if feature_df[description].nunique() > 0]
        with filter_col2:
            features_after_not_unique_filtered = st.multiselect("Select Features for comparison", features_after_not_unique)

        for fe in features_after_not_unique_filtered:
            if (fe not in ['brand', 'fsn']) and ('warranty' not in fe.lower()):
                feature_col1, feature_col2 = st.columns(2)
                with feature_col1:
                    st.markdown(f"##### Comparison for {fe}")
                    feature_df_temp = feature_df[features_after_not_unique]
                    feature_df_temp_grouped = feature_df_temp.groupby(['brand', fe]).agg({"fsn": ["count", list], "image_link": list}).reset_index()
                    feature_df_temp_grouped.columns = ['brand', fe, 'product_count', 'fsn_list', 'image_links']
                    feature_df_temp_grouped['image_link'] = feature_df_temp_grouped['image_links'].apply(lambda x: x[0] if x else None)
                    st.dataframe(feature_df_temp_grouped, column_config={
                        "image_link": st.column_config.ImageColumn("Image")
                    })

                with feature_col2:
                    fe_distribution = final_scrapped_data[fe].value_counts().reset_index()
                    fe_distribution.columns = [fe, 'count']
                    fe_distribution_pie = fe_distribution.sort_values(by='count', ascending=False).head(10)
                    fe_fig = px.pie(fe_distribution_pie, values='count', names=fe, title=f'Feature Importance of {fe} in Market',
                                    template='presentation')
                    st.plotly_chart(fe_fig)
                with st.expander(f"Click for all feature comparison for {fe}"):
                    final_fsn_list = sum(feature_df_temp_grouped['fsn_list'], [])
                    final_fsn = final_scrapped_data[final_scrapped_data['fsn'].isin(final_fsn_list)]
                    final_fsn1 = final_fsn.dropna(how='all', axis=1)
                    final_fsn1 = final_fsn1.set_index('fsn')
                    final_fsn2 = final_fsn1.transpose()
                    if 'image_link' in final_fsn2.index:
                        img_row = final_fsn2.loc['image_link']
                        final_fsn2 = final_fsn2.drop('image_link')
                        final_fsn2 = pd.concat([img_row.to_frame().T, final_fsn2])

                    # Render image links correctly in the DataFrame
                    def render_cell(x):
                        if isinstance(x, str) and x.startswith('http'):
                            return f'<a href="{x}" target="_blank"><img src="{x}" width="100"></a>'
                        else:
                            return x

                    final_fsn2_html = final_fsn2.applymap(render_cell)
                    final_fsn2_html_str = final_fsn2_html.to_html(escape=False)
                    st.markdown(final_fsn2_html_str, unsafe_allow_html=True)

else:
    st.info("No product IDs collected yet. Please enter keywords and click Submit.")
