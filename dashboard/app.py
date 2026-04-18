import streamlit as st
import pandas as pd
import plotly.express as px
from databricks import sql
import os
from dotenv import load_dotenv

# ============================================
# Databricks Connection
# ============================================

load_dotenv()

conn = sql.connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)

@st.cache_data
def load_data():
    query = """
        SELECT
            beneficiary_id,
            gender,
            race,
            state_code,
            age,
            risk_score,
            risk_tier,
            has_esrd,
            part_a_months,
            part_b_months
        FROM workspace.default.fct_member_risk
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        return pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )

# ============================================
# Dashboard Layout
# ============================================
st.set_page_config(
    page_title="Member Health Risk Dashboard",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Member Health Risk Analytics")
st.markdown("Built with Databricks + dbt + Streamlit")

# Load data
df = load_data()

# ============================================
# KPI Metrics Row
# ============================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Members", f"{len(df):,}")
with col2:
    high_risk = len(df[df['risk_tier'] == 'High Risk'])
    st.metric("High Risk Members", f"{high_risk:,}")
with col3:
    esrd_count = len(df[df['has_esrd'] == True])
    st.metric("Members with ESRD", f"{esrd_count:,}")
with col4:
    avg_risk = round(df['risk_score'].mean(), 2)
    st.metric("Avg Risk Score", avg_risk)

st.divider()

# ============================================
# Charts Row 1
# ============================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("Risk Tier Distribution")
    risk_counts = df['risk_tier'].value_counts().reset_index()
    risk_counts.columns = ['Risk Tier', 'Count']
    fig = px.pie(
        risk_counts,
        values='Count',
        names='Risk Tier',
        color='Risk Tier',
        color_discrete_map={
            'High Risk': '#FF4B4B',
            'Medium Risk': '#FFA500',
            'Low Risk': '#00CC96'
        }
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Risk Score by Age Group")
    df['age_group'] = pd.cut(
        df['age'],
        bins=[0, 45, 55, 65, 75, 150],
        labels=['<45', '45-54', '55-64', '65-74', '75+']
    )
    age_risk = df.groupby('age_group')['risk_score'].mean().reset_index()
    fig2 = px.bar(
        age_risk,
        x='age_group',
        y='risk_score',
        color='risk_score',
        color_continuous_scale='Reds',
        labels={'age_group': 'Age Group', 'risk_score': 'Avg Risk Score'}
    )
    st.plotly_chart(fig2, use_container_width=True)

# ============================================
# Charts Row 2
# ============================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("Risk Distribution by Gender")
    gender_risk = df.groupby(['gender', 'risk_tier']).size().reset_index(name='count')
    fig3 = px.bar(
        gender_risk,
        x='gender',
        y='count',
        color='risk_tier',
        color_discrete_map={
            'High Risk': '#FF4B4B',
            'Medium Risk': '#FFA500',
            'Low Risk': '#00CC96'
        },
        barmode='group'
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.subheader("Risk Distribution by Race")
    race_risk = df.groupby(['race', 'risk_tier']).size().reset_index(name='count')
    fig4 = px.bar(
        race_risk,
        x='race',
        y='count',
        color='risk_tier',
        color_discrete_map={
            'High Risk': '#FF4B4B',
            'Medium Risk': '#FFA500',
            'Low Risk': '#00CC96'
        },
        barmode='group'
    )
    st.plotly_chart(fig4, use_container_width=True)

# ============================================
# Raw Data Table
# ============================================
st.divider()
st.subheader("📋 Member Risk Data")
st.dataframe(
    df[['beneficiary_id', 'gender', 'race', 'age',
        'risk_score', 'risk_tier', 'has_esrd',
        'part_a_months', 'part_b_months']],
    use_container_width=True
)