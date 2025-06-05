import streamlit as st
from sqlalchemy import create_engine, text
import os
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
import pydeck as pdk
import plotly.express as px

st.title("Fraud Detection Project")

# Define connection URL (you can also read these from st.secrets)
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASS")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")



try: 
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_url)
except Exception as e:
    st.error(f"Error connecting to the database: {e}")
    st.stop()


# --- Multipage sidebar ---
page = st.sidebar.selectbox(
    "Choose a page",
    (
        "Home 🏠", 
        "Transactions Table 📔", 
        "Demographic Analysis (Age, Gender) 👤",
        "Geographic analytics 🌎",
        "Behavioral & Merchant Analysis ⏱️🏪",
    )
)


def show_home():
    st.subheader("👋 Welcome to the Fraud Detection Project")
    st.write("""
    🔍 This project aims to **analyze and visualize** transaction data to detect **fraudulent activities**.

    🧭 Use the **sidebar** to navigate through different sections of the application:
    - 📊 View analytics
    - 🗺️ Explore a map of transactions
    - 📋 Inspect the processed data in table format

    ✅ Make sure your **database is connected** properly to see the data.
    """)
   

def show_analytics():
    st.subheader("Analytics")
    
    # Total transactions
    total_transactions = pd.read_sql("SELECT COUNT(*) FROM processed_transactions", engine).iloc[0, 0]
    st.metric("Total Transactions", total_transactions)
    
    # Total frauds
    total_frauds = pd.read_sql("SELECT COUNT(*) FROM processed_transactions WHERE is_fraud = TRUE", engine).iloc[0, 0]
    st.metric("Total Frauds", total_frauds)
    
    # Fraud rate
    fraud_rate = (total_frauds / total_transactions) * 100 if total_transactions > 0 else 0
    st.metric("Fraud Rate (%)", f"{fraud_rate:.2f}")



def show_map():
    st.subheader("🌍 Geographic Fraud Analysis")

    # --- State filter ---
    states = pd.read_sql("SELECT DISTINCT state FROM processed_transactions WHERE is_fraud = TRUE AND state IS NOT NULL", engine)
    state_options = sorted([s for s in states["state"] if pd.notna(s)])
    selected_states = st.multiselect("Filter by State", options=state_options, default=state_options)

    # Build WHERE clause for state filter
    where_clause = "is_fraud = TRUE AND lat IS NOT NULL AND long IS NOT NULL"
    params = {}
    if selected_states and len(selected_states) < len(state_options):
        where_clause += " AND state IN :states"
        params["states"] = tuple(selected_states)

    # Load map data
    df = pd.read_sql(
        text(f"""
        SELECT city, state, lat AS latitude, long AS longitude, amt
        FROM processed_transactions
        WHERE {where_clause}
        """),
        engine,
        params=params
    )

    if df.empty:
        st.warning("No fraud data for the selected state(s).")
        return

    # Group by location
    df_grouped = df.groupby(
        ['city', 'state', 'latitude', 'longitude'], as_index=False
    ).agg(
        total_amt=('amt', 'sum'),
        fraud_count=('amt', 'count')
    )

    total_amount = df_grouped["total_amt"].sum()
    total_frauds = df_grouped["fraud_count"].sum()

    col1, col2 = st.columns(2)
    col1.metric("💰 Total Fraud Amount", f"${total_amount:,.2f}")
    col2.metric("⚠️ Total Fraud Count", f"{total_frauds:,}")

    st.pydeck_chart(pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=df_grouped["latitude"].mean(),
            longitude=df_grouped["longitude"].mean(),
            zoom=4,
            pitch=0,
        ),
        layers=[
            pdk.Layer(
                "HeatmapLayer",
                data=df_grouped,
                get_position='[longitude, latitude]',
                get_weight="fraud_count",
                radiusPixels=60,
            ),
            pdk.Layer(
                "ScatterplotLayer",
                data=df_grouped,
                get_position='[longitude, latitude]',
                get_color='[200, 30, 0, 160]',
                get_radius="amt * 0.1",  # Adjust radius based on amount
                pickable=True,
                radius_min_pixels=5,
                radius_max_pixels=100,
            ),  
        ],
        tooltip={"text": "{city}, {state}\nFrauds: {fraud_count}\nTotal: ${total_amt}"}
    ))

    # ---- 1. Top 10 cities by number of frauds ----
    st.markdown("### 🏙️ Top 10 Cities by Number of Frauds")
    city_counts = df.groupby('city').size().reset_index(name='fraud_count').sort_values(by='fraud_count', ascending=False).head(10)
    fig1 = px.bar(city_counts, x='city', y='fraud_count',
                  labels={'city': 'City', 'fraud_count': 'Number of Frauds'},
                  color='fraud_count', color_continuous_scale='Reds')
    st.plotly_chart(fig1, use_container_width=True)

    # ---- 2. Top 10 states by total fraud amount ----
    st.markdown("### 🗺️ Top 10 States by Total Fraud Amount")
    state_amt = df.groupby('state')['amt'].sum().reset_index().sort_values(by='amt', ascending=False).head(10)
    fig2 = px.bar(state_amt, x='state', y='amt',
                  labels={'state': 'State', 'amt': 'Total Fraud Amount ($)'},
                  color='amt', color_continuous_scale='OrRd')
    st.plotly_chart(fig2, use_container_width=True)




def data_frame2():
    # Sidebar filters
    st.sidebar.header("Filters")
    min_amount = st.sidebar.number_input("Min amount", value=0)
    max_amount = st.sidebar.number_input("Max amount", value=1000000)

    categories = pd.read_sql("SELECT DISTINCT category FROM processed_transactions", engine)
    selected_category = st.sidebar.multiselect("Transaction category", options=list(categories["category"]))

    # Date range filter
    date_minmax = pd.read_sql("SELECT MIN(transaction_time) as min_date, MAX(transaction_time) as max_date FROM processed_transactions", engine)
    date_min = pd.to_datetime(date_minmax["min_date"][0])
    date_max = pd.to_datetime(date_minmax["max_date"][0])
    selected_date = st.sidebar.date_input("Transaction date range", value=(date_min, date_max), min_value=date_min, max_value=date_max)
    if not isinstance(selected_date, tuple) or len(selected_date) != 2:
        selected_date = (date_min, date_max)


    # --- Gender filter (simple select) ---
    genders = pd.read_sql("SELECT DISTINCT gender FROM processed_transactions", engine)
    gender_options = ["All"] + [g for g in genders["gender"] if pd.notna(g)]
    selected_gender = st.sidebar.selectbox("Gender", options=gender_options)

    age_minmax = pd.read_sql("SELECT MIN(age_at_transaction) as min_age, MAX(age_at_transaction) as max_age FROM processed_transactions", engine)
    age_min = int(age_minmax["min_age"][0])
    age_max = int(age_minmax["max_age"][0])
    selected_age = st.sidebar.slider("Age range", min_value=age_min, max_value=age_max, value=(age_min, age_max))

    job_categories = pd.read_sql("SELECT DISTINCT job_category FROM processed_transactions", engine)
    selected_job = st.sidebar.multiselect("Job category", options=list(job_categories["job_category"]))

    fraud_option = st.sidebar.selectbox("Is Fraud?", options=["All", "Yes", "No"])

    cities = pd.read_sql("SELECT DISTINCT city FROM processed_transactions", engine)
    states = pd.read_sql("SELECT DISTINCT state FROM processed_transactions", engine)
    selected_cities = st.sidebar.multiselect("City", options=list(cities["city"]))
    selected_states = st.sidebar.multiselect("State", options=list(states["state"]))

    # --- Pagination controls under the table ---
    if "page_size" not in st.session_state:
        st.session_state.page_size = 100
    if "page_num" not in st.session_state:
        st.session_state.page_num = 1

    # Build WHERE clause
    where_clauses = [
        "amt >= :min_amount", "amt <= :max_amount",
        "transaction_time >= :date_start", "transaction_time <= :date_end",
        "age_at_transaction >= :age_min", "age_at_transaction <= :age_max"
    ]
    params = {
        "min_amount": min_amount,
        "max_amount": max_amount,
        "date_start": selected_date[0],
        "date_end": selected_date[1],
        "age_min": selected_age[0],
        "age_max": selected_age[1],
    }
    if selected_category:
        where_clauses.append(f"category IN :categories")
        params["categories"] = tuple(selected_category)
    if selected_job:
        where_clauses.append(f"job_category IN :job_categories")
        params["job_categories"] = tuple(selected_job)
    if selected_gender != "All":
        where_clauses.append("gender = :gender")
        params["gender"] = selected_gender
    if fraud_option == "Yes":
        where_clauses.append("is_fraud = TRUE")
    elif fraud_option == "No":
        where_clauses.append("is_fraud = FALSE")
    if selected_cities:
        where_clauses.append("city IN :cities")
        params["cities"] = tuple(selected_cities)
    if selected_states:
        where_clauses.append("state IN :states")
        params["states"] = tuple(selected_states)
    where_sql = " AND ".join(where_clauses)

    # Get total records for pagination
    total_query = text(f"""
        SELECT COUNT(*) FROM processed_transactions
        WHERE {where_sql}
    """)
    total_records = pd.read_sql(total_query, engine, params=params).iloc[0, 0]
    total_pages = max(1, (total_records + st.session_state.page_size - 1) // st.session_state.page_size)

    # Clamp page_num if out of range
    if st.session_state.page_num > total_pages:
        st.session_state.page_num = total_pages
    if st.session_state.page_num < 1:
        st.session_state.page_num = 1

    # Query data for current page
    params.update({
        "limit": st.session_state.page_size,
        "offset": (st.session_state.page_num - 1) * st.session_state.page_size
    })
    query = text(f"""
        SELECT * FROM processed_transactions
        WHERE {where_sql}
        ORDER BY transaction_id
        LIMIT :limit OFFSET :offset
    """)
    df = pd.read_sql(query, engine, params=params)

    # AgGrid with fraud row highlighting
    cell_style_jscode = JsCode("""
    function(params) {
        if (params.data.is_fraud === true || params.data.is_fraud === 1) {
            return {'backgroundColor': '#ffcccc'};
        }
        return {};
    }
    """)
    gb = GridOptionsBuilder.from_dataframe(df)

    # Set pretty column labels
    gb.configure_column("transaction_id", header_name="Transaction ID")
    gb.configure_column("transaction_time", header_name="Transaction Time")
    gb.configure_column("amt", header_name="Amount ($)")
    gb.configure_column("category", header_name="Category")
    gb.configure_column("job_category", header_name="Job Category")
    gb.configure_column("age_at_transaction", header_name="Age")
    gb.configure_column("gender", header_name="Gender")
    gb.configure_column("is_fraud", header_name="Is Fraud")
    gb.configure_column("city", header_name="City")
    gb.configure_column("state", header_name="State")
    gb.configure_column("lat", header_name="Latitude")
    gb.configure_column("long", header_name="Longitude")
    gb.configure_column("merchant", header_name="Merchant")
    gb.configure_column("hour", header_name="Hour")
    gb.configure_column("day_of_week", header_name="Day of Week")
    gb.configure_column("year", header_name="Year")
    gb.configure_column("hour", header_name="Hour of Day")
    gb.configure_column("is_weekend", header_name="Is Weekend")
    gb.configure_column("processed_at", header_name="Processed At")

    
    gb.configure_grid_options(getRowStyle=cell_style_jscode)


    grid_options = gb.build()
    AgGrid(
        df,
        gridOptions=grid_options,
        allow_unsafe_jscode=True,
        theme="streamlit"
    )

    # --- Pagination info and input fields under the table ---
    col_left, col_right = st.columns([2, 3])
    with col_left:
        st.markdown(
            f"<div style='font-size:14px; font-weight:bold;'>Page {st.session_state.page_num} of {total_pages}</div>",
            unsafe_allow_html=True
        )
    with col_right:
        new_page_size = st.number_input(
            "Rows per page",
            min_value=10,
            max_value=1000,
            value=st.session_state.page_size,
            step=10,
            key="page_size_bottom"
        )
        new_page_num = st.number_input(
            "Page number",
            min_value=1,
            max_value=total_pages,
            value=st.session_state.page_num,
            step=1,
            key="page_num_bottom"
        )
        # Update session state if changed
        if new_page_size != st.session_state.page_size:
            st.session_state.page_size = new_page_size
            st.session_state.page_num = 1  # Reset to first page if page size changes
            st.rerun()
        elif new_page_num != st.session_state.page_num:
            st.session_state.page_num = new_page_num
            st.rerun()

    

def behavior_merchant_analysis():
    st.subheader("⏱️🏪 Behavioral & Merchant Analysis")

    df = pd.read_sql("SELECT * FROM processed_transactions", engine)
    df['is_fraud'] = df['is_fraud'].map({False: 'Legit', True: 'Fraud'})


    # ---- 1. Fraud by Hour ----
    st.markdown("### 🕒 Fraud by Hour of Day")
    hour_fraud = df.groupby('hour')['is_fraud'].value_counts().unstack().fillna(0)
    fig1 = px.bar(hour_fraud, barmode='group',
                  labels={'value': 'Count', 'hour': 'Hour of Day'},
                  title='Fraud vs Legit by Hour')
    st.plotly_chart(fig1, use_container_width=True)


    # ---- 2. Fraud by Day of Week ----
    st.markdown("### 📅 Fraud by Day of Week")
    dow_fraud = df.groupby('day_of_week')['is_fraud'].value_counts().unstack().fillna(0)
    fig2 = px.bar(dow_fraud, barmode='group',
                  labels={'value': 'Count', 'day_of_week': 'Day of Week'},
                  title='Fraud vs Legit by Day')
    st.plotly_chart(fig2, use_container_width=True)


    # ---- 3. Top Merchants with Most Frauds ----
    st.markdown("### 🏪 Top 10 Merchants with Most Fraud Transactions")
    top_merchants = df[df['is_fraud'] == 'Fraud'].groupby('merchant').size().reset_index(name='fraud_count')
    top_merchants = top_merchants.sort_values(by='fraud_count', ascending=False).head(10)
    fig3 = px.bar(top_merchants, x='merchant', y='fraud_count',
                  color='fraud_count', color_continuous_scale='Reds',
                  labels={'merchant': 'Merchant', 'fraud_count': 'Fraud Count'})
    st.plotly_chart(fig3, use_container_width=True)



def show_demographic_analysis():
    # Load data
    df = pd.read_sql("SELECT gender, age_at_transaction, is_fraud FROM processed_transactions", engine)


    df['is_fraud'] = df['is_fraud'].map({False: 'Legit', True: 'Fraud'})

    # ---- 1. Fraud by gender
    st.markdown("### 🚻 Fraud by Gender")
    gender_fraud = df.groupby(['gender', 'is_fraud']).size().reset_index(name='count')
    fig1 = px.bar(gender_fraud, x='gender', y='count', color='is_fraud', barmode='group',
                  labels={'count': 'Number of Transactions'}, color_discrete_map={'Fraud': 'red', 'Legit': 'green'})
    st.plotly_chart(fig1, use_container_width=True)

    # ---- 2. Age distribution plot
    st.markdown("### 🎂 Age Distribution by Fraud Type")
    fig2 = px.histogram(df, x='age_at_transaction', color='is_fraud', nbins=30,
                        labels={'age_at_transaction': 'Age'}, barmode='overlay', opacity=0.6,
                        color_discrete_map={'Fraud': 'red', 'Legit': 'green'})
    st.plotly_chart(fig2, use_container_width=True)

    # ---- 3. Boxplot age
    st.markdown("### 📦 Age Distribution (Box Plot)")
    fig3 = px.box(df, x='is_fraud', y='age_at_transaction', color='is_fraud',
                  labels={'age_at_transaction': 'Age', 'is_fraud': 'Transaction Type'},
                  color_discrete_map={'Fraud': 'red', 'Legit': 'green'})
    st.plotly_chart(fig3, use_container_width=True)

    # ---- 4. Fraud rate by gender
    st.markdown("### 📈 Fraud Rate by Gender")
    # Calculate fraud rate per gender
    fraud_counts = df[df['is_fraud'] == 'Fraud'].groupby('gender').size()
    total_counts = df.groupby('gender').size()
    gender_rate = (fraud_counts / total_counts * 100).fillna(0).reset_index()
    gender_rate.columns = ['gender', 'Fraud Rate (%)']
    fig4 = px.pie(
        gender_rate,
        names='gender',
        values='Fraud Rate (%)',
        labels={'Fraud Rate (%)': 'Fraud Rate (%)'},
        color='gender',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig4, use_container_width=True)
    



#Page routing
if page == "Analytics 📊":
    show_analytics()
elif page == "Geographic analytics 🌎":
    show_map()
elif page == "Transactions Table 📔":
    data_frame2()
elif page == "Home 🏠":
    show_home()
elif page == "Demographic Analysis (Age, Gender) 👤":
    show_demographic_analysis()
elif page == "Behavioral & Merchant Analysis ⏱️🏪":
    behavior_merchant_analysis()
else:
    st.error("Page not found. Please select a valid page from the sidebar.")
