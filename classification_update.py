
import pandas as pd
import json
import streamlit as st
import time
from snowflake.snowpark import Session


connexion_params = {
    "account": st.secrets["account"],
    "user": st.secrets["user"],
    "password": st.secrets["password"],
    "warehouse": st.secrets["warehouse"],
    "database": "mehdi_test_share",
    "schema": "public",
    "role": st.secrets["role"]
  }

if 'snowflake_connection' not in st.session_state:
    # connect to Snowflake
    connection_parameters = connexion_params
    st.session_state.snowflake_connection = Session.builder.configs(connection_parameters).create()
    session = st.session_state.snowflake_connection
else:
    session = st.session_state.snowflake_connection

st.set_page_config(layout="centered", page_title="Data Editor", page_icon="🧮")
st.title("Snowflake Table Editor ❄️")
st.caption("This is a demo of the `st.experimental_data_editor`.")

def get_dataset():
    # load messages df
    df = session.table("customers")

    return df

dataset = get_dataset()

with st.form("data_editor_form"):
    st.caption("Edit the dataframe below")
    edited = st.experimental_data_editor(dataset, use_container_width=True, num_rows="dynamic")
    submit_button = st.form_submit_button("Submit")

if submit_button:
    try:
        session.write_pandas(edited, "customers", overwrite=True)
        st.success(edited.loc[0, "CUSTOMER_ID"])
        time.sleep(5)
    except:
        st.warning("Error updating table")
    #display success message for 5 seconds and update the table to reflect what is in Snowflake
    st.experimental_rerun()
