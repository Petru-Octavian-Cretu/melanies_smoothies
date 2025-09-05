import streamlit as st
from snowflake.snowpark.functions import col, when_matched
import requests
import pandas as pd

st.title("🥤 My Parents New Healthier Diner")
st.write("Omega 3 & Blueberry Oatmeal")

# --- Input nume ---
name_on_order = st.text_input("Name on smoothie")
st.write("The name on your smoothie will be: ", name_on_order)

# --- Conexiune Snowflake ---
cnx = st.connection("snowflake")
session = cnx.session()

# --- Obține fructe ---
my_dataframe = session.table("smoothies.public.fruit_options").select(
    col('FRUIT_NAME'), col('SEARCH_ON')
)
pd_df = my_dataframe.to_pandas()

# --- Select ingrediente ---
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    pd_df['FRUIT_NAME'],
    max_selections=5
)

# --- Dacă avem ingrediente selectate ---
if ingredients_list:
    ingredients_string = ''
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '

        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        
        st.subheader(f"{fruit_chosen} Nutrition Information")
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + fruit_chosen)
        if smoothiefroot_response.status_code == 200:
            st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)
        else:
            st.warning(f"No nutrition data found for {fruit_chosen}")

    # --- Insert Order ---
    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_ts)
        VALUES ('{ingredients_string}', '{name_on_order}', CURRENT_TIMESTAMP)
    """

    if st.button('📤 Submit Order'):
        session.sql(my_insert_stmt).collect()
        st.success('✅ Your Smoothie is ordered!')


st.header("📋 View Pending Orders")

pending_orders_df = session.table("smoothies.public.orders") \
    .filter(col("ORDER_FILLED") == False) \
    .select("ORDER_UID", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_FILLED") \
    .to_pandas()

if not pending_orders_df.empty:
    st.dataframe(pending_orders_df)
else:
    st.info("📭 No pending smoothie orders!")


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
