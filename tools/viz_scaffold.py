import os
import pandas as pd
import duckdb
from typing import Optional


def make_dashboard(root: str, slug: str, title: str) -> str:
    """
    Create a Streamlit dashboard file for a given metric slug.
    
    Args:
        root: Directory to create the dashboard in (e.g., "dashboards")
        slug: Metric slug/identifier
        title: Dashboard title
    
    Returns:
        Path to the created dashboard file
    """
    # Ensure directory exists
    os.makedirs(root, exist_ok=True)
    
    # Create dashboard file path
    dashboard_path = os.path.join(root, f"{slug}.py")
    
    # Dashboard template
    dashboard_content = f'''import os
import pandas as pd
import duckdb
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

st.set_page_config(page_title="{title}", layout="wide")

st.title("📊 {title}")
st.caption(f"Dashboard for metric: {slug}")

# Load data from dbt target (DuckDB)
@st.cache_data
def load_data():
    """Load data from the dbt-generated metric table."""
    try:
        # Try to find the dbt target database
        dbt_paths = [
            "dbt_project/target/dev.duckdb",
            "dbt_project/target/prod.duckdb",
            "dbt_project/target/{slug}.duckdb",
            "target/dev.duckdb",
            "target/prod.duckdb"
        ]
        
        db_path = None
        for path in dbt_paths:
            if os.path.exists(path):
                db_path = path
                break
        
        if not db_path:
            st.error("❌ Could not find dbt target database. Please run `dbt run` first.")
            return pd.DataFrame()
        
        # Connect to DuckDB and load the metric data
        con = duckdb.connect(db_path)
        
        # Try to query the metric table
        try:
            df = con.execute(f"SELECT * FROM {slug} LIMIT 500").df()
        except Exception as e:
            st.warning(f"Could not query table '{slug}': {{e}}")
            # Try to find available tables
            tables = con.execute("SHOW TABLES").df()
            if not tables.empty:
                st.info("Available tables in the database:")
                st.write(tables['name'].tolist())
                # Use the first available table as fallback
                first_table = tables['name'].iloc[0]
                df = con.execute(f"SELECT * FROM {{first_table}} LIMIT 500").df()
            else:
                st.error("No tables found in the database.")
                return pd.DataFrame()
        
        con.close()
        return df
        
    except Exception as e:
        st.error(f"❌ Error loading data: {{e}}")
        return pd.DataFrame()

# Main dashboard content
df = load_data()

if df.empty:
    st.warning("No data available. Please ensure the dbt model has been generated.")
    st.info("Try running: `dbt run --select {slug}`")
else:
    # Data overview
    st.subheader("📋 Data Overview")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Rows", len(df))
    
    with col2:
        st.metric("Total Columns", len(df.columns))
    
    with col3:
        numeric_cols = df.select_dtypes(include=['number']).columns
        st.metric("Numeric Columns", len(numeric_cols))
    
    # Display the data
    st.subheader("📄 Data Table")
    st.dataframe(df, use_container_width=True)
    
    # Column information
    st.subheader("📊 Column Information")
    col_info = pd.DataFrame({{
        'Column Name': df.columns,
        'Data Type': df.dtypes.astype(str),
        'Non-Null Count': df.count(),
        'Unique Values': [df[col].nunique() for col in df.columns]
    }})
    st.dataframe(col_info, use_container_width=True)
    
    # Auto-generate charts for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    if numeric_cols:
        st.subheader("📈 Auto-Generated Charts")
        
        # If we have at least 2 numeric columns, create scatter plots
        if len(numeric_cols) >= 2:
            st.write("**Scatter Plot:**")
            x_col = st.selectbox("Select X-axis:", numeric_cols, key="x_axis")
            y_col = st.selectbox("Select Y-axis:", numeric_cols, key="y_axis", index=min(1, len(numeric_cols)-1))
            
            if x_col and y_col and x_col != y_col:
                st.scatter_chart(df[[x_col, y_col]], use_container_width=True)
        
        # Create bar/line charts for the first numeric column
        first_numeric = numeric_cols[0]
        
        # Look for potential categorical columns for grouping
        categorical_cols = []
        for col in df.columns:
            if df[col].dtype == 'object' and df[col].nunique() < 20:  # Reasonably small cardinality
                categorical_cols.append(col)
        
        if categorical_cols:
            st.write("**Aggregation by Category:**")
            cat_col = st.selectbox("Group by:", categorical_cols, key="group_by")
            
            if cat_col:
                agg_type = st.radio("Aggregation:", ["Sum", "Mean", "Count"], key="agg_type")
                
                if agg_type == "Sum":
                    grouped_df = df.groupby(cat_col)[first_numeric].sum().sort_values(ascending=False)
                elif agg_type == "Mean":
                    grouped_df = df.groupby(cat_col)[first_numeric].mean().sort_values(ascending=False)
                else:
                    grouped_df = df.groupby(cat_col)[first_numeric].count().sort_values(ascending=False)
                
                st.bar_chart(grouped_df, use_container_width=True)
                
                # Show the actual numbers
                st.write("**Aggregated Data:**")
                st.dataframe(grouped_df.reset_index(), use_container_width=True)
        
        # Line chart if we have date/time columns
        date_cols = []
        for col in df.columns:
            if df[col].dtype in ['datetime64[ns]', 'datetime64', 'object']:
                try:
                    pd.to_datetime(df[col], errors='raise')
                    date_cols.append(col)
                except:
                    pass
        
        if date_cols and numeric_cols:
            st.write("**Time Series:**")
            date_col = st.selectbox("Date column:", date_cols, key="date_col")
            num_col = st.selectbox("Numeric column for time series:", numeric_cols, key="num_col")
            
            if date_col and num_col:
                # Convert date column and create time series
                df_copy = df.copy()
                df_copy[date_col] = pd.to_datetime(df_copy[date_col])
                time_series = df_copy.groupby(date_col)[num_col].sum().sort_index()
                
                st.line_chart(time_series, use_container_width=True)
    
    # Filter functionality
    st.subheader("🔍 Data Filters")
    
    # Create filters for each column
    filtered_df = df.copy()
    
    for col in df.columns:
        if df[col].dtype == 'object':
            # Categorical filter
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) <= 20:  # Only show filter for reasonably small cardinality
                selected_vals = st.multiselect(
                    f"Filter by {{col}}:",
                    options=unique_vals,
                    default=unique_vals,
                    key=f"filter_{{col}}"
                )
                if selected_vals:
                    filtered_df = filtered_df[filtered_df[col].isin(selected_vals)]
        elif df[col].dtype in ['int64', 'float64']:
            # Numeric range filter
            col_min, col_max = float(df[col].min()), float(df[col].max())
            selected_range = st.slider(
                f"Filter by {{col}}:",
                min_value=col_min,
                max_value=col_max,
                value=(col_min, col_max),
                key=f"filter_{{col}}"
            )
            filtered_df = filtered_df[
                (filtered_df[col] >= selected_range[0]) & 
                (filtered_df[col] <= selected_range[1])
            ]
    
    # Show filtered data
    if len(filtered_df) != len(df):
        st.write(f"**Filtered Results:** {{len(filtered_df)}} rows (from {{len(df)}} total)")
        st.dataframe(filtered_df, use_container_width=True)

# Footer
st.markdown("---")
st.caption("Dashboard auto-generated by Marketplace Intelligence")
st.caption(f"Metric slug: {slug} | Database: dbt target")
'''
    
    # Write the dashboard file
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(dashboard_content)
    
    return dashboard_path
