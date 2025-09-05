import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import pandas as pd
import requests

# --- Connect to Snowflake ---
cnx = st.connection("snowflake")
session = cnx.session()

# --- Page Title ---
st.title("🥤 Customize Your Smoothie!")

# --- Load Fruit Options (with SEARCH_ON column) ---
my_dataframe = session.table("smoothies.public.fruit_options").select(
    col("FRUIT_NAME"), col("SEARCH_ON")
)
pd_df = my_dataframe.to_pandas()  # Convert to Pandas to use .loc

# --- Ingredient Multiselect ---
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    pd_df["FRUIT_NAME"],
    max_selections=5
)

# --- Show Nutrition Info from Fruityvice API ---
if ingredients_list:
    for fruit_chosen in ingredients_list:
        # Get the corresponding search term
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]

        # Show subheader
        st.subheader(f"{fruit_chosen} Nutrition Information")

        # API call
        fruityvice_response = requests.get(f"https://fruityvice.com/api/fruit/{search_on}")

        # Show response in table format
        st.dataframe(data=fruityvice_response.json(), use_container_width=True)

# --- Smoothie Order Submission Section ---
st.header("🧾 Place a New Smoothie Order")

name_on_order = st.text_input("Name on Smoothie:")

if name_on_order and ingredients_list:
    ingredients_string = ', '.join(ingredients_list)
    insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """
    if st.button("📤 Submit Order"):
        session.sql(insert_stmt).collect()
        st.success("✅ Your Smoothie has been ordered!")

# --- Pending Orders Section ---
st.header("📋 View Pending Orders")

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
            ).execute()
            st.success("Orders updated successfully!", icon="👍")
        except Exception as e:
            st.error(f"Something went wrong: {e}")

