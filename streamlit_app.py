import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import pandas as pd
import requests

# --- Connect to Snowflake ---
cnx = st.connection("snowflake")
session = cnx.session()

# --- STEP 0: Verify table exists ---
tables = [row['name'] for row in session.sql("SHOW TABLES IN SCHEMA smoothies.public").collect()]
if 'FRUIT_OPTIONS' not in tables:
    st.error("❌ Table 'fruit_options' does not exist in smoothies.public schema.")
    st.stop()  # oprește scriptul dacă tabelul nu există

# --- STEP 1: Add SEARCH_ON column if not exists ---
columns = [row['column_name'].upper() for row in session.sql("SHOW COLUMNS IN TABLE smoothies.public.fruit_options").collect()]
if 'SEARCH_ON' not in columns:
    session.sql("ALTER TABLE smoothies.public.fruit_options ADD COLUMN SEARCH_ON STRING").collect()

# --- STEP 2: Copy FRUIT_NAME to SEARCH_ON where SEARCH_ON is NULL ---
if 'FRUIT_NAME' in columns:
    session.sql("""
        UPDATE smoothies.public.fruit_options
        SET SEARCH_ON = FRUIT_NAME
        WHERE SEARCH_ON IS NULL
    """).collect()
else:
    st.error("❌ Column 'FRUIT_NAME' does not exist in fruit_options table.")
    st.stop()

# --- STEP 3: Fix specific plural cases ---
plural_fixes = {
    'Apples': 'Apple',
    'Blueberries': 'Blueberry',
    'Cherries': 'Cherry',
    'Strawberries': 'Strawberry',
    'Raspberries': 'Raspberry',
    'Blackberries': 'Blackberry'
}

for plural, singular in plural_fixes.items():
    session.sql(f"""
        UPDATE smoothies.public.fruit_options
        SET SEARCH_ON = '{singular}'
        WHERE FRUIT_NAME = '{plural}'
    """).collect()

# --- Load Fruit Options (with SEARCH_ON column) ---
my_dataframe = session.table("smoothies.public.fruit_options").select(
    col("FRUIT_NAME"), col("SEARCH_ON")
)
pd_df = my_dataframe.to_pandas()

# --- Ingredient Multiselect ---
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    pd_df["FRUIT_NAME"],
    max_selections=5
)

# --- Show Nutrition Info from Fruityvice API ---
if ingredients_list:
    for fruit_chosen in ingredients_list:
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.subheader(f"{fruit_chosen} Nutrition Information")

        variants = [
            search_on,
            search_on.replace(' ', '-'),
            search_on.replace(' ', '_')
        ]
        response = None
        for variant in variants:
            url = f"https://fruityvice.com/api/fruit/{variant.lower()}"
            res = requests.get(url)
            if res.status_code == 200:
                response = res.json()
                break

        if response:
            st.dataframe(data=response, use_container_width=True)
        else:
            st.warning(f"⚠️ Nutrition data not available for '{fruit_chosen}'.")

# --- Smoothie Order Submission Section ---
st.header("🧾 Place a New Smoothie Order")

name_on_order = st.text_input("Name on Smoothie:")

if name_on_order and ingredients_list:
    ingredients_string = ''
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
    ingredients_string = ingredients_string.strip()  # elimină spațiul extra la final

    # Escape apostrofuri pentru SQL
    ingredients_string_safe = ingredients_string.replace("'", "''")
    name_on_order_safe = name_on_order.replace("'", "''")

    insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_ts)
        VALUES ('{ingredients_string_safe}', '{name_on_order_safe}', CURRENT_TIMESTAMP)
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
            )
            st.success("Orders updated successfully!", icon="👍")
        except Exception as e:
            st.error(f"Something went wrong: {e}")
