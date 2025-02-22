# Import necessary libraries
import streamlit as st
import pandas as pd
import time
import plotly.express as px


st.set_page_config(
    page_title="Stock Price Average",
    page_icon="📈",
    layout="wide",
)
st.title("Visualization of Apple stock data")


def load_data():
    return pd.read_parquet("data").set_index("time").sort_index()


placeholder = st.empty()
time.sleep(5)
with placeholder.container():
        st.markdown("### The line chart shows the difference between the closing price and the opening price")
        chart = st.line_chart()

        st.markdown("### Table View")
        table = st.dataframe()


while True:
    data = load_data() 

    chart.add_rows(data)
    table.dataframe(data.sort_index(ascending=False))

    time.sleep(1)

