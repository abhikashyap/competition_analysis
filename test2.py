import test
from scrapper import flipkart_json_scrapper_with_all_specifications as scrap
import streamlit as st
# a=test.title_scrap('ACCGZRZ2SSCF6Y4Y')
# st.write(a)
# st.write(response_code)
# st.write(response_header)
title=scrap.scrape_all_fsns(['ACCGZRZ2SSCF6Y4Y'])
# print(title)
st.write(title)