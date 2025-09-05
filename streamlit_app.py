import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import pandas as pd
import requests

# --- Snowflake Connection ---
cnx = st.connection("snowflake")
session = cnx.session()

# --- TITLE ---
st.title('My Parents New Healthy Diner')

# --- Smoothie Order Section ---
st.header("🧾 Place a New Smoothie Order")

# Get fruit options from Snowflake
fruit_rows = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME")).collect()
fruit_options = [row.FRUIT_NAME for row in fruit_rows]

# Name input
name_on_order = st.text_input("Name on Smoothie:")

# Ingredient selection
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)

# Order submission
if name_on_order and ingredients_list:
    ingredients_string = ', '.join(ingredients_list)
    insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """
    if st.button("📤 Submit Order"):
        session.sql(insert_stmt).collect()
        st.success("✅ Your Smoothie has been ordered!")

# --- Nutrition Info Section ---
if ingredients_list:
    st.subheader("🍓 Nutrition Info for Selected Fruits")

    for fruit in ingredients_list:
        # Make API call for each fruit
        try:
            response = requests.get(f"https://my.smoothiefroot.com/api/fruit/{fruit.lower()}")
            if response.status_code == 200:
                fruit_data = response.json()
                st.dataframe(data=fruit_data, use_container_width=True)
            else:
                st.warning(f"No data found for {fruit}")
        except Exception as e:
            st.error(f"Error fetching data for {fruit}: {e}")

# --- Pending Orders Section ---
st.header("📋 View Pending Orders")

# Get all unfilled orders
pending_orders_df = session.table("smoothies.public.orders")\
    .filter(col("ORDER_FILLED") == False)\
    .select("ORDER_UID", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_FILLED")\
    .to_pandas()

if not pending_orders_df.empty:
    st.dataframe(pending_orders_df)
else:
    st.info("📭 No pending smoothie orders!")

# --- Update Orders Section ---
st.header("✅ Mark Orders as Filled")

# Reuse dataframe to make it editable
if not pending_orders_df.empty:
    editable_df = st.data_editor(pending_orders_df, key="editable_orders")

    if st.button("✔️ Submit Updates"):
        # Convert edited data back to Snowpark DataFrame
        edited_dataset = session.create_dataframe(editable_df)
        original_dataset = session.table("smoothies.public.orders")

        try:
            original_dataset.merge(
                edited_dataset,
                original_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"],
                [when_matched().update({"ORDER_FILLED": edited_dataset["ORDER_FILLED"]})]
            )
            st.success("Orders updated successfully!", icon="👍")
        except Exception as e:
            st.error(f"Something went wrong: {e}")
