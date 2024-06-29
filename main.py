import streamlit as st 
import requests

st.set_page_config(layout="wide")

st.markdown("""
            # Competition analysis
            # """)
with st.container():
    # Text input for keywords
    keywords = st.text_input("Enter keywords to do competitive analysis")

    # Button to submit keywords
    if st.button('Submit'):
        st.write(f"Submitted keywords are  : {keywords}")
        list_of_keywords = keywords.split(",")  # Split keywords by comma
        for keyword in list_of_keywords:
            st.write(f'https://www.flipkart.com/search?q={keyword.strip().replace(" ", "%20")}')
    else:
        st.write("Enter Keywords")

data=requests.get(f'https://www.flipkart.com/search?q={keyword.strip().replace(" ", "%20")}')

st.write(data)