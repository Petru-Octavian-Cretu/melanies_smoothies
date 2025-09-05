import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import pandas as pd
import requests

# --- Snowflake Connection ---
cnx = st.connection("snowflake")
session = cnx.session()

# --- Title ---
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

# --- Nutrition Information Section ---
if ingredients_list:
    ingredients_string = ''

    # Get SEARCH_ON values for each selected fruit
    search_values = session.table("smoothies.public.fruit_options")\
        .filter(col("FRUIT_NAME").isin(ingredients_list))\
        .select("FRUIT_NAME", "SEARCH_ON")\
        .to_pandas()

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ', '

        # Look up SEARCH_ON value for the fruit
        search_term = search_values.loc[
            search_values['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'
        ].values[0]

        # Display subheader and API data
        st.subheader(f"{fruit_chosen} Nutrition Information")
        smoothiefroot_response = requests.get(
            f"https://my.smoothiefroot.com/api/fruit/{search_term}"
        )
        st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    ingredients_string = ingredients_string.rstrip(', ')

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

if not pending_orders_df.empty:
    editable_df = st.data_editor(pending_orders_df, key="editable_orders")

    if st.button("✔️ Submit Updates"):
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
