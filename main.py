import streamlit as st 
from scrapper import fk_scrapper_functions as sc
from scrapper import flipkart_json_scrapper_with_all_specifications as fk_scrapper
import time
import pandas as pd
st.set_page_config(layout="wide")


st.markdown("""
            # Competition analysis
            # """)

all_fsns=[]
with st.container():

    keywords = st.text_input("Enter keywords to do competitive analysis")
    
    if st.button('Submit'):
        st.write(f"Submitted keywords are  : {keywords}")
        list_of_keywords = keywords.split(",") 
        progress_text=f"collecting all fsns in 5 pages of the given kewords  "
        i=0
        overall_progress = st.progress(round(i+1/len(list_of_keywords)+1), text=progress_text)
        for i in range(len(list_of_keywords)):
            url=f'https://www.flipkart.com/search?q={list_of_keywords[i].strip().replace(" ", "%20")}'

            overall_progress.progress((i + 1) / len(list_of_keywords), text=f"{progress_text} ({i + 1}/{len(list_of_keywords)})")
            fsns=sc.fns_scrap(sc.open_chrome_headless(),url,3)
            all_fsns.extend(fsns)
        overall_progress.progress(1.0, text="Scraping complete!")
    else:
        st.write("Enter Keywords")
    

# all_link_buttons=[]
# for fsn in all_fsns:
#     all_link_buttons.extend(st.link_button(f"product id {fsn}",f"https://www.flipkart.com/frony-uit-474x-p47-wireless-bluetooth-headphones-hd-sound-bass-mic-sd-card-slot-headset/p/itma26df4ab6c240?pid={fsn}"))
# df= pd.DataFrame(all_link_buttons,columns="products")
# st.dataframe(df)
st.write(f"Total no of product id collected is {len(all_fsns)}")
final_fsn_list=list(set(all_fsns))
competitor_data=fk_scrapper.scrape_all_fsns(all_fsns)
st.dataframe(competitor_data)