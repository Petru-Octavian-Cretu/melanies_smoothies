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

# --- Plural to Singular mapping for tricky fruits ---
plural_to_singular = {
    "blueberries": "blueberry",
    "cherries": "cherry",
    "strawberries": "strawberry",
    "raspberries": "raspberry",
    "blackberries": "blackberry",
    # add more irregular plurals here if needed
}

def singularize(word):
    word = word.lower().strip()
    if word in plural_to_singular:
        return plural_to_singular[word]
    elif word.endswith('s'):
        return word[:-1]  # naive fallback
    else:
        return word

# --- Helper function to get API search term forcing singular ---
def get_search_on_for_fruit(fruit_chosen, df):
    fruit_lower = fruit_chosen.lower().strip()
    
    # Try exact match (case insensitive)
    exact_match = df[df['FRUIT_NAME'].str.lower() == fruit_lower]
    if not exact_match.empty:
        search_on = exact_match['SEARCH_ON'].iloc[0].strip().lower()
    else:
        # Could add more fuzzy logic here if needed
        return None
    
    # Singularize with mapping
    search_on_singular = singularize(search_on)
    return search_on_singular

# --- Try Fruityvice API with multiple variants ---
def try_fruityvice_api(search_on):
    variants = [
        search_on,
        search_on.replace(' ', '-'),
        search_on.replace(' ', '_')
    ]
    for variant in variants:
        url = f"https://fruityvice.com/api/fruit/{variant}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    return None  # None if no variant worked

# --- Ingredient Multiselect ---
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    pd_df["FRUIT_NAME"],
    max_selections=5
)

# --- Show Nutrition Info from Fruityvice API ---
if ingredients_list:
    for fruit_chosen in ingredients_list:
        search_on = get_search_on_for_fruit(fruit_chosen, pd_df)
        if not search_on:
            st.warning(f"⚠️ Could not find API search term for '{fruit_chosen}'. Skipping.")
            continue
        
        st.subheader(f"{fruit_chosen} Nutrition Information")

        result = try_fruityvice_api(search_on)
        if result:
            st.dataframe(data=result, use_container_width=True)
        else:
            st.warning(f"⚠️ Nutrition data not available for '{fruit_chosen}'.")

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
