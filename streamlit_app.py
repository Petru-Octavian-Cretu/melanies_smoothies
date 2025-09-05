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

# Debug: Show fruit names and corresponding search terms
st.write("Available fruits and their API search keys:")
st.dataframe(pd_df)

# --- Ingredient Multiselect ---
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    pd_df["FRUIT_NAME"],
    max_selections=5
)

# --- Helper function to get API search term handling plurals and case ---
def get_search_on_for_fruit(fruit_chosen, df):
    fruit_lower = fruit_chosen.lower().strip()
    
    # Try exact match (case insensitive)
    exact_match = df[df['FRUIT_NAME'].str.lower() == fruit_lower]
    if not exact_match.empty:
        return exact_match['SEARCH_ON'].iloc[0].strip().lower()
    
    # Try removing trailing 's' (simple plural handling)
    if fruit_lower.endswith('s'):
        singular = fruit_lower[:-1]
        singular_match = df[df['FRUIT_NAME'].str.lower() == singular]
        if not singular_match.empty:
            return singular_match['SEARCH_ON'].iloc[0].strip().lower()
    
    # Try adding trailing 's' (in case API uses plural)
    if not fruit_lower.endswith('s'):
        plural = fruit_lower + 's'
        plural_match = df[df['FRUIT_NAME'].str.lower() == plural]
        if not plural_match.empty:
            return plural_match['SEARCH_ON'].iloc[0].strip().lower()

    # No match found
    return None

# --- Show Nutrition Info from Fruityvice API ---
if ingredients_list:
    for fruit_chosen in ingredients_list:
        search_on = get_search_on_for_fruit(fruit_chosen, pd_df)
        if not search_on:
            st.warning(f"⚠️ Could not find API search term for '{fruit_chosen}'. Skipping.")
            continue
        
        st.subheader(f"{fruit_chosen} Nutrition Information")

        url = f"https://fruityvice.com/api/fruit/{search_on}"
        fruityvice_response = requests.get(url)

        if fruityvice_response.status_code == 200:
            st.dataframe(data=fruityvice_response.json(), use_container_width=True)
        elif fruityvice_response.status_code == 404:
            st.warning(f"⚠️ '{fruit_chosen}' not found in Fruityvice API.")
        else:
            st.error(f"❌ Error fetching data for '{fruit_chosen}' (HTTP {fruityvice_response.status_code})")

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
