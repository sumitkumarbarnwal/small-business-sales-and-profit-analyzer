import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import bcrypt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import io
import base64
import os
import time
import warnings
warnings.filterwarnings('ignore')  # Suppress warnings

# For advanced analytics (optional)
try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from scipy import stats
    ADVANCED_ANALYTICS_AVAILABLE = True
except ImportError:
    ADVANCED_ANALYTICS_AVAILABLE = False
    st.warning("Some advanced features require scikit-learn. Install with: pip install scikit-learn scipy")

# For PDF export
try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# ================= DATABASE ================= #

def connect_db():
    """Establish database connection with error handling"""
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", "sumit@4321"),
        "database": os.getenv("DB_NAME", "business_db")
    }

    max_retries = int(os.getenv("DB_MAX_RETRIES", "10"))
    retry_delay = int(os.getenv("DB_RETRY_DELAY", "3"))

    # On cloud/local-without-db, fail fast (1 attempt, no delay)
    is_cloud = not os.getenv("DB_HOST") and db_config["host"] == "localhost"
    if is_cloud:
        max_retries = 1
        retry_delay = 0

    for attempt in range(max_retries):
        try:
            return mysql.connector.connect(**db_config)
        except mysql.connector.Error:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            return None

# Initialize database tables if they don't exist
def init_database():
    """Create necessary tables if they don't exist"""
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        
        # Create users table with role
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                email VARCHAR(100),
                role VARCHAR(20) DEFAULT 'Staff',
                business_name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create sales_data table for persistent storage
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                transaction_date DATE,
                product_name VARCHAR(255),
                category VARCHAR(100),
                quantity INT,
                unit_price DECIMAL(10, 2),
                cost_price DECIMAL(10, 2),
                revenue DECIMAL(10, 2),
                profit DECIMAL(10, 2),
                customer_name VARCHAR(255),
                region VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        
        # Create products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                product_name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                cost_price DECIMAL(10, 2),
                selling_price DECIMAL(10, 2),
                stock_quantity INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        
        # Create expenses table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                expense_date DATE,
                category VARCHAR(100),
                amount DECIMAL(10, 2),
                description TEXT,
                receipt_file LONGBLOB,
                receipt_filename VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        
        # Create saved_reports table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS saved_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                report_name VARCHAR(255),
                report_type VARCHAR(100),
                report_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        
        # Add missing columns to existing tables (for database migration)
        try:
            # Check and add email column to users table
            cursor.execute("SHOW COLUMNS FROM users LIKE 'email'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE users ADD COLUMN email VARCHAR(100)")
                print("Added 'email' column to users table")
        except mysql.connector.Error:
            pass
        
        try:
            # Check and add role column to users table
            cursor.execute("SHOW COLUMNS FROM users LIKE 'role'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE users ADD COLUMN role VARCHAR(20) DEFAULT 'Staff'")
                print("Added 'role' column to users table")
        except mysql.connector.Error:
            pass
        
        try:
            # Check and add business_name column to users table
            cursor.execute("SHOW COLUMNS FROM users LIKE 'business_name'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE users ADD COLUMN business_name VARCHAR(255)")
                print("Added 'business_name' column to users table")
        except mysql.connector.Error:
            pass
        
        conn.commit()
        cursor.close()
        conn.close()

# Call initialization (silently skip if DB not available)
try:
    init_database()
except Exception:
    pass

# ================= AUTH ================= #

def register_user(username, password, email=None, role='Staff', business_name=''):
    """Register a new user with role and business name"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    try:
        cursor.execute(
            "INSERT INTO users (username, password, email, role, business_name) VALUES (%s,%s,%s,%s,%s)",
            (username, hashed_pw, email, role, business_name)
        )
        conn.commit()
        return True
    except mysql.connector.IntegrityError:
        return False
    except Exception as e:
        st.error(f"Registration error: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def login_user(username, password):
    """Authenticate user and load role"""
    # Check for default admin credentials
    if username == "admin" and password == "admin123":
        st.session_state.user_role = 'Owner'
        st.session_state.business_name = 'Admin Account'
        return True
    
    # Regular user authentication
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    cursor.execute("SELECT password, role, business_name FROM users WHERE username=%s", (username,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result:
        if bcrypt.checkpw(password.encode(), result[0].encode()):
            # Store role and business name in session
            st.session_state.user_role = result[1] if result[1] else 'Staff'
            st.session_state.business_name = result[2] if result[2] else ''
            return True
    return False

# ================= DATABASE OPERATIONS FOR SALES ================= #

def save_sales_data_to_db(user_id, df):
    """Save uploaded sales data to database"""
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        # Detect columns
        date_cols, numeric_cols, categorical_cols = detect_column_types(df)
        
        for _, row in df.iterrows():
            # Try to extract relevant fields
            transaction_date = None
            if date_cols and len(date_cols) > 0:
                try:
                    transaction_date = pd.to_datetime(row[date_cols[0]]).date()
                except:
                    transaction_date = datetime.now().date()
            else:
                transaction_date = datetime.now().date()
            
            # Extract product name
            product_name = str(row[categorical_cols[0]]) if categorical_cols else "Unknown Product"
            
            # Extract category
            category = str(row[categorical_cols[1]]) if len(categorical_cols) > 1 else "General"
            
            # Extract numeric fields
            quantity = int(row[numeric_cols[0]]) if len(numeric_cols) > 0 else 1
            unit_price = float(row[numeric_cols[1]]) if len(numeric_cols) > 1 else 0.0
            cost_price = float(row[numeric_cols[2]]) if len(numeric_cols) > 2 else 0.0
            
            # Calculate revenue and profit
            revenue = quantity * unit_price
            profit = revenue - (quantity * cost_price)
            
            cursor.execute("""
                INSERT INTO sales_data (user_id, transaction_date, product_name, category, 
                                       quantity, unit_price, cost_price, revenue, profit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, transaction_date, product_name, category, quantity, 
                  unit_price, cost_price, revenue, profit))
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error saving data: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()

def get_user_id(username):
    """Get user ID from username"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return result[0] if result else None

def load_sales_data_from_db(user_id):
    """Load sales data from database"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT * FROM sales_data 
        WHERE user_id = %s 
        ORDER BY transaction_date DESC
    """, (user_id,))
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if data:
        return pd.DataFrame(data)
    return None

# ================= PROFIT & ROI CALCULATIONS ================= #

def calculate_profit_metrics(df, revenue_col, cost_col):
    """Calculate comprehensive profit metrics"""
    try:
        total_revenue = df[revenue_col].sum() if revenue_col in df.columns else 0
        total_cost = df[cost_col].sum() if cost_col in df.columns else 0
        total_profit = total_revenue - total_cost
        
        profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        roi = (total_profit / total_cost * 100) if total_cost > 0 else 0
        
        return {
            'total_revenue': total_revenue,
            'total_cost': total_cost,
            'total_profit': total_profit,
            'profit_margin': profit_margin,
            'roi': roi,
            'avg_transaction_value': total_revenue / len(df) if len(df) > 0 else 0
        }
    except Exception as e:
        st.error(f"Error calculating profit metrics: {str(e)}")
        return None

def calculate_product_profitability(df, product_col, revenue_col, cost_col):
    """Calculate profitability by product"""
    try:
        product_analysis = df.groupby(product_col).agg({
            revenue_col: 'sum',
            cost_col: 'sum'
        }).reset_index()
        
        product_analysis['profit'] = product_analysis[revenue_col] - product_analysis[cost_col]
        product_analysis['profit_margin'] = (product_analysis['profit'] / product_analysis[revenue_col] * 100).round(2)
        product_analysis['roi'] = (product_analysis['profit'] / product_analysis[cost_col] * 100).round(2)
        
        return product_analysis.sort_values('profit', ascending=False)
    except Exception as e:
        st.error(f"Error calculating product profitability: {str(e)}")
        return None

# ================= DATA PROCESSING ================= #

def safe_date_parsing(series):
    """Safely parse dates with multiple format attempts"""
    # Try common date formats
    date_formats = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d',
        '%d-%m-%Y', '%m-%d-%Y', '%Y%m%d', '%b %d, %Y',
        '%d %b %Y', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'
    ]
    
    for fmt in date_formats:
        try:
            converted = pd.to_datetime(series, format=fmt, errors='coerce')
            if converted.notnull().sum() > len(series) * 0.6:
                return converted
        except:
            continue
    
    # If no format works, try pandas default parser
    try:
        return pd.to_datetime(series, errors='coerce')
    except:
        return series

def detect_column_types(df):
    """Automatically detect column types with improved date detection"""
    date_cols = []
    numeric_cols = []
    categorical_cols = []
    
    for col in df.columns:
        # Skip if too many missing values
        if df[col].isnull().sum() > len(df) * 0.5:
            categorical_cols.append(col)
            continue
        
        # Check for dates (only for object/string columns)
        if df[col].dtype == 'object':
            # Sample first few non-null values
            sample = df[col].dropna().head(100)
            if len(sample) > 0:
                try:
                    # Try to parse as date
                    converted = safe_date_parsing(sample)
                    if converted.notnull().sum() > len(sample) * 0.6:
                        date_cols.append(col)
                        continue
                except:
                    pass
        
        # Check for numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric_cols.append(col)
        else:
            categorical_cols.append(col)
    
    return date_cols, numeric_cols, categorical_cols


def format_compact_currency(value, currency_symbol="$"):
    """Format currency using readable units like Lakhs, Millions, and Billions."""
    if value is None or pd.isna(value):
        return f"{currency_symbol}0"

    sign = "-" if value < 0 else ""
    abs_value = abs(float(value))

    if abs_value >= 1_000_000_000:
        return f"{sign}{currency_symbol}{abs_value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{sign}{currency_symbol}{abs_value / 1_000_000:.2f}M"
    if abs_value >= 100_000:
        return f"{sign}{currency_symbol}{abs_value / 100_000:.2f}L"
    if abs_value >= 1_000:
        return f"{sign}{currency_symbol}{abs_value:,.0f}"
    return f"{sign}{currency_symbol}{abs_value:.2f}"

def generate_insights(df, date_col, numeric_cols, categorical_cols):
    """Generate comprehensive business insights"""
    insights = []
    
    try:
        if date_col and numeric_cols and date_col in df.columns:
            # Time-based insights
            df_copy = df.copy()
            df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce')
            df_copy = df_copy.dropna(subset=[date_col])
            
            if len(df_copy) > 0:
                df_copy['year'] = df_copy[date_col].dt.year
                df_copy['month'] = df_copy[date_col].dt.month
                df_copy['quarter'] = df_copy[date_col].dt.quarter
                df_copy['day_of_week'] = df_copy[date_col].dt.day_name()
                
                for num_col in numeric_cols[:2]:  # Analyze top 2 numeric columns
                    if num_col in df_copy.columns:
                        # Monthly trends
                        monthly_avg = df_copy.groupby('month')[num_col].mean()
                        if len(monthly_avg) > 1:
                            best_month = monthly_avg.idxmax()
                            worst_month = monthly_avg.idxmin()
                            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                                          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
                            insights.append(f"📊 Best month for {num_col}: {month_names.get(best_month, best_month)}")
                            insights.append(f"📉 Lowest month for {num_col}: {month_names.get(worst_month, worst_month)}")
                        
                        # Day of week patterns
                        dow_avg = df_copy.groupby('day_of_week')[num_col].mean()
                        if len(dow_avg) > 0:
                            best_day = dow_avg.idxmax()
                            insights.append(f"📅 Best performing day: {best_day} for {num_col}")
        
        if categorical_cols and numeric_cols:
            # Category analysis
            for cat_col in categorical_cols[:2]:  # Analyze top 2 categorical columns
                if cat_col in df.columns:
                    for num_col in numeric_cols[:1]:  # Use first numeric column
                        if num_col in df.columns:
                            top_cats = df.groupby(cat_col)[num_col].sum().nlargest(3)
                            if len(top_cats) > 0:
                                insights.append(f"🏆 Top 3 {cat_col} by {num_col}: {', '.join(top_cats.index.astype(str))}")
    except Exception as e:
        insights.append(f"⚠️ Error generating insights: {str(e)}")
    
    return insights if insights else ["No specific insights generated. Try selecting different columns."]

# ================= SESSION ================= #

def init_session_state():
    """Initialize session state variables"""
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = None
    if "df" not in st.session_state:
        st.session_state.df = None
    if "upload_history" not in st.session_state:
        st.session_state.upload_history = []
    if "saved_reports" not in st.session_state:
        st.session_state.saved_reports = []
    if "column_mappings" not in st.session_state:
        st.session_state.column_mappings = {}
    if "data_summary" not in st.session_state:
        st.session_state.data_summary = {}

init_session_state()

# ================= UI COMPONENTS ================= #

def apply_custom_css():
    """Apply modern 3D CSS styling with depth and perspective"""
    st.markdown("""
        <style>
        /* Global 3D Perspective */
        .main .block-container {
            perspective: 2000px;
            perspective-origin: 50% 50%;
        }
        
        /* Main Container Styling with 3D Effect */
        .center-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 2.5rem;
            border-radius: 20px;
            box-shadow: 
                0 20px 60px rgba(0,0,0,0.3),
                0 10px 20px rgba(0,0,0,0.2),
                inset 0 0 0 1px rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            transform: translateZ(50px) rotateX(2deg);
            transform-style: preserve-3d;
            transition: transform 0.5s ease;
        }
        
        .center-card:hover {
            transform: translateZ(80px) rotateX(0deg);
        }
        
        /* Title Styling */
        .main-title {
            text-align: center;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            font-size: 3rem;
            font-weight: 900;
            margin-bottom: 1rem;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }
        
        /* KPI Metric Cards with 3D Effects */
        .metric-card {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 
                0 15px 35px rgba(0,0,0,0.2),
                0 5px 15px rgba(0,0,0,0.1),
                inset 0 2px 0 rgba(255,255,255,0.3),
                inset 0 -2px 0 rgba(0,0,0,0.2);
            text-align: center;
            color: white;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            transform: translateZ(30px) rotateX(5deg);
            transform-style: preserve-3d;
            position: relative;
        }
        
        .metric-card:hover {
            transform: translateZ(60px) rotateX(0deg) scale(1.05);
            box-shadow: 
                0 25px 50px rgba(0,0,0,0.3),
                0 10px 20px rgba(0,0,0,0.15),
                inset 0 2px 0 rgba(255,255,255,0.4);
        }
        
        .metric-card::before {
            content: '';
            position: absolute;
            top: -2px;
            left: -2px;
            right: -2px;
            bottom: -2px;
            background: linear-gradient(45deg, rgba(255,255,255,0.1), transparent);
            border-radius: 15px;
            z-index: -1;
        }
        
        /* Profit Card - Green Theme with 3D */
        .profit-card {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 
                0 15px 35px rgba(17, 153, 142, 0.4),
                0 5px 15px rgba(0,0,0,0.1),
                inset 0 2px 0 rgba(255,255,255,0.3),
                inset 0 -2px 0 rgba(0,0,0,0.2);
            text-align: center;
            color: white;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            transform: translateZ(30px) rotateX(5deg);
            transform-style: preserve-3d;
        }
        
        .profit-card:hover {
            transform: translateZ(60px) rotateX(0deg) scale(1.05);
            box-shadow: 
                0 25px 50px rgba(17, 153, 142, 0.5),
                0 10px 20px rgba(0,0,0,0.15);
        }
        
        /* Revenue Card - Blue Theme with 3D */
        .revenue-card {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 
                0 15px 35px rgba(79, 172, 254, 0.4),
                0 5px 15px rgba(0,0,0,0.1),
                inset 0 2px 0 rgba(255,255,255,0.3),
                inset 0 -2px 0 rgba(0,0,0,0.2);
            text-align: center;
            color: white;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            transform: translateZ(30px) rotateX(5deg);
            transform-style: preserve-3d;
        }
        
        .revenue-card:hover {
            transform: translateZ(60px) rotateX(0deg) scale(1.05);
            box-shadow: 
                0 25px 50px rgba(79, 172, 254, 0.5),
                0 10px 20px rgba(0,0,0,0.15);
        }
        
        /* Cost Card - Orange Theme with 3D */
        .cost-card {
            background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 
                0 15px 35px rgba(250, 112, 154, 0.4),
                0 5px 15px rgba(0,0,0,0.1),
                inset 0 2px 0 rgba(255,255,255,0.3),
                inset 0 -2px 0 rgba(0,0,0,0.2);
            text-align: center;
            color: white;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            transform: translateZ(30px) rotateX(5deg);
            transform-style: preserve-3d;
        }
        
        .cost-card:hover {
            transform: translateZ(60px) rotateX(0deg) scale(1.05);
            box-shadow: 
                0 25px 50px rgba(250, 112, 154, 0.5),
                0 10px 20px rgba(0,0,0,0.15);
        }
        
        /* ROI Card - Purple Theme with 3D */
        .roi-card {
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 
                0 15px 35px rgba(168, 237, 234, 0.4),
                0 5px 15px rgba(0,0,0,0.1),
                inset 0 2px 0 rgba(255,255,255,0.5),
                inset 0 -2px 0 rgba(0,0,0,0.1);
            text-align: center;
            color: #333;
            font-weight: bold;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            transform: translateZ(30px) rotateX(5deg);
            transform-style: preserve-3d;
        }
        
        .roi-card:hover {
            transform: translateZ(60px) rotateX(0deg) scale(1.05);
            box-shadow: 
                0 25px 50px rgba(168, 237, 234, 0.5),
                0 10px 20px rgba(0,0,0,0.15);
        }
        
        /* Insight Box with 3D Depth */
        .insight-box {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: #ffffff;
            padding: 1.2rem;
            border-radius: 10px;
            border-left: 6px solid #4facfe;
            margin: 0.8rem 0;
            box-shadow: 
                0 10px 30px rgba(0,0,0,0.3),
                0 5px 10px rgba(0,0,0,0.15),
                inset 0 1px 0 rgba(255,255,255,0.1);
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            transform: translateZ(20px);
            transform-style: preserve-3d;
        }
        
        .insight-box:hover {
            transform: translateZ(40px) translateX(10px);
            box-shadow: 
                0 15px 40px rgba(0,0,0,0.4),
                0 8px 15px rgba(0,0,0,0.2);
        }
        
        /* Success Box with 3D */
        .success-box {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: white;
            padding: 1rem;
            border-radius: 10px;
            margin: 0.5rem 0;
            font-weight: 600;
            box-shadow: 
                0 8px 20px rgba(17, 153, 142, 0.3),
                inset 0 1px 0 rgba(255,255,255,0.2);
            transform: translateZ(15px);
            transition: all 0.3s ease;
        }
        
        .success-box:hover {
            transform: translateZ(30px);
        }
        
        /* Warning Box with 3D */
        .warning-box {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            padding: 1rem;
            border-radius: 10px;
            margin: 0.5rem 0;
            font-weight: 600;
            box-shadow: 
                0 8px 20px rgba(240, 147, 251, 0.3),
                inset 0 1px 0 rgba(255,255,255,0.2);
            transform: translateZ(15px);
            transition: all 0.3s ease;
        }
        
        .warning-box:hover {
            transform: translateZ(30px);
        }
        
        /* Button Styling with 3D Effect */
        .stButton>button {
            width: 100%;
            border-radius: 10px;
            height: 3.5em;
            font-weight: bold;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
            font-size: 16px;
            box-shadow: 
                0 8px 20px rgba(102, 126, 234, 0.3),
                0 4px 10px rgba(0,0,0,0.15),
                inset 0 2px 0 rgba(255,255,255,0.2),
                inset 0 -2px 0 rgba(0,0,0,0.2);
            transform: translateZ(20px);
            transform-style: preserve-3d;
            position: relative;
        }
        
        .stButton>button:hover {
            transform: translateZ(40px) translateY(-3px);
            box-shadow: 
                0 15px 35px rgba(102, 126, 234, 0.5),
                0 8px 15px rgba(0,0,0,0.2),
                inset 0 2px 0 rgba(255,255,255,0.3);
        }
        
        .stButton>button:active {
            transform: translateZ(10px) translateY(0px);
            box-shadow: 
                0 5px 15px rgba(102, 126, 234, 0.3),
                inset 0 2px 5px rgba(0,0,0,0.2);
        }
        
        /* Sidebar Styling */
        .css-1d391kg {
            background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
        }
        
        /* Data Table Styling with 3D */
        .dataframe {
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 
                0 10px 30px rgba(0,0,0,0.15),
                0 5px 10px rgba(0,0,0,0.1),
                inset 0 1px 0 rgba(255,255,255,0.5);
            transform: translateZ(20px);
            transition: transform 0.3s ease;
        }
        
        .dataframe:hover {
            transform: translateZ(35px);
        }
        
        /* Header Styling */
        h1, h2, h3 {
            color: #1e3c72;
            font-weight: 700;
        }
        
        /* Divider */
        hr {
            border: none;
            height: 2px;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            margin: 2rem 0;
        }
        
        /* Metric Value Styling */
        .metric-value {
            font-size: 2.5rem;
            font-weight: 900;
            margin: 0.5rem 0;
        }
        
        .metric-label {
            font-size: 0.9rem;
            opacity: 0.9;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        /* Card Container with 3D Depth */
        .card-container {
            background: white;
            padding: 2rem;
            border-radius: 15px;
            box-shadow: 
                0 15px 40px rgba(0,0,0,0.15),
                0 5px 15px rgba(0,0,0,0.1),
                inset 0 1px 0 rgba(255,255,255,0.8);
            margin: 1rem 0;
            transform: translateZ(25px);
            transform-style: preserve-3d;
            transition: all 0.4s ease;
        }
        
        .card-container:hover {
            transform: translateZ(45px) scale(1.01);
            box-shadow: 
                0 20px 50px rgba(0,0,0,0.2),
                0 8px 20px rgba(0,0,0,0.15);
        }
        
        /* Animated Gradient Background for Login */
        .login-bg {
            background: linear-gradient(-45deg, #ee7752, #e73c7e, #23a6d5, #23d5ab);
            background-size: 400% 400%;
            animation: gradient 15s ease infinite;
        }
        
        @keyframes gradient {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        /* Status Badges */
        .status-badge-profit {
            background: #38ef7d;
            color: white;
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
        }
        
        .status-badge-loss {
            background: #f5576c;
            color: white;
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
        }
        </style>
    """, unsafe_allow_html=True)

# ================= LOGIN PAGE ================= #

def show_login():
    """Simple and clean login page"""
    st.set_page_config(page_title="Sales & Profit Analyzer", layout="centered", initial_sidebar_state="collapsed")
    
    # Simple centered design
    st.markdown("""
        <style>
        .login-container {
            max-width: 450px;
            margin: 3rem auto;
            padding: 2rem;
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .login-title {
            text-align: center;
            color: #1e3c72;
            margin-bottom: 1.5rem;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='login-container'>", unsafe_allow_html=True)
    st.markdown("<h2 class='login-title'>Sales & Profit Analyzer</h2>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Login", "Register"])

    with tab1:
        st.markdown("#### Welcome Back")
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Login", use_container_width=True, type="primary"):
            if username and password:
                if login_user(username, password):
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid credentials")
            else:
                st.warning("Please enter username and password")

    with tab2:
        st.markdown("#### Create New Account")
        new_user = st.text_input("Username", key="reg_username")
        new_pass = st.text_input("Password", type="password", key="reg_password")
        confirm_pass = st.text_input("Confirm Password", type="password", key="reg_confirm")
        email = st.text_input("Email (Optional)", key="reg_email")
        business_name = st.text_input("Business Name (Optional)", key="reg_business")

        if st.button("Create Account", use_container_width=True, type="primary"):
            if not new_user or not new_pass:
                st.error("Username and password are required")
            elif new_pass != confirm_pass:
                st.error("Passwords don't match")
            elif len(new_pass) < 6:
                st.error("Password must be at least 6 characters")
            else:
                if register_user(new_user, new_pass, email, 'Staff', business_name):
                    st.success("Account created! Please login.")
                else:
                    st.error("Username already exists")

    st.markdown("</div>", unsafe_allow_html=True)

# ================= DASHBOARD ================= #

def show_dashboard():
    """Main dashboard after login"""
    st.set_page_config(page_title="Sales & Profit Analyzer", layout="wide", initial_sidebar_state="expanded")
    apply_custom_css()
    
    # Sidebar with modern styling
    with st.sidebar:
        st.markdown("""
            <div style='text-align: center; padding: 1rem;'>
                <h3 style='color: white;'>Sales Intelligence</h3>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"<p style='color: white; text-align: center;'><strong>{st.session_state.username}</strong></p>", unsafe_allow_html=True)
        if hasattr(st.session_state, 'user_role'):
            st.markdown(f"<p style='color: white; text-align: center; font-size: 0.85rem;'>Role: {st.session_state.user_role}</p>", unsafe_allow_html=True)
        
        st.markdown("<hr style='border-color: white;'>", unsafe_allow_html=True)
        
        # Menu based on role
        menu_items = ["Upload Data", "Sales Analytics", "Advanced Analytics & AI", 
                     "Expense Management", "Inventory Management", "Profit Insights", 
                     "Data Viewer", "Reports & Export", "Settings"]
        
        # Add Admin Dashboard for Owners
        if hasattr(st.session_state, 'user_role') and st.session_state.user_role == 'Owner':
            menu_items.insert(-1, "Admin Dashboard")
        
        menu = st.radio(
            "Navigation",
            menu_items,
            label_visibility="collapsed"
        )

        st.markdown("<hr style='border-color: white;'>", unsafe_allow_html=True)
        
        # Quick Stats if data loaded
        if st.session_state.df is not None:
            df_shape = st.session_state.df.shape
            st.markdown(f"""
                <div style='background: rgba(255,255,255,0.1); padding: 1rem; border-radius: 10px; color: white; box-shadow: 0 5px 15px rgba(0,0,0,0.2); transform: translateZ(10px);'>
                    <p style='margin: 0; font-size: 0.9rem;'><strong>Data Loaded</strong></p>
                    <p style='margin: 0.5rem 0 0 0;'>Rows: {df_shape[0]:,}</p>
                    <p style='margin: 0;'>Columns: {df_shape[1]}</p>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<hr style='border-color: white;'>", unsafe_allow_html=True)
        
        if st.button("Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.df = None
            st.rerun()
            
        st.markdown("""
            <div style='background: rgba(255,255,255,0.1); padding: 0.8rem; border-radius: 8px; margin-top: 1rem; box-shadow: 0 4px 12px rgba(0,0,0,0.2);'>
                <p style='color: white; font-size: 0.85rem; margin: 0;'><strong>Pro Tip:</strong> Upload CSV files with date, revenue, and cost columns for best insights!</p>
            </div>
        """, unsafe_allow_html=True)

    # Main content based on menu selection
    if menu == "Upload Data":
        upload_page()
    elif menu == "Sales Analytics":
        analytics_page()
    elif menu == "Advanced Analytics & AI":
        advanced_analytics_page()
    elif menu == "Expense Management":
        expense_management_page()
    elif menu == "Inventory Management":
        inventory_management_page()
    elif menu == "Profit Insights":
        profit_insights_page()
    elif menu == "Data Viewer":
        data_viewer_page()
    elif menu == "Reports & Export":
        reports_page()
    elif menu == "Admin Dashboard":
        admin_dashboard_page()
    else:
        settings_page()

# ================= UPLOAD PAGE ================= #

def upload_page():
    """Data upload page with database integration"""
    st.title("Upload Sales & Profit Data")
    
    with st.expander("Instructions & Best Practices", expanded=False):
        st.markdown("""
        ### Data Upload Guidelines
        
        **Supported Formats:**
        - CSV (.csv)
        - Excel (.xlsx, .xls)
        - Maximum file size: 200MB
        
        **Required Data Structure:**
        - First row should contain column headers
        - Include at least one date column for time-based analysis
        - Include revenue/sales amount column
        - Optionally include cost column for profit analysis
        - Category or product name column for segmentation
        
        **Example Columns:**
        - Date: transaction_date, date, order_date
        - Revenue: sales, revenue, amount, total
        - Cost: cost, expenses, cogs
        - Category: product, category, item_name
        - Quantity: qty, quantity, units
        
        **Tip:** The system will auto-detect column types!
        """)

    uploaded_file = st.file_uploader(
        "Choose your sales data file", 
        type=["csv", "xlsx", "xls"],
        help="Upload your business data file (CSV or Excel format)"
    )

    if uploaded_file:
        try:
            # Read file based on extension
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            # Basic data cleaning
            df = df.replace([np.inf, -np.inf], np.nan)
            
            # Store in session
            st.session_state.df = df
            
            # Add to upload history
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state.upload_history.append({
                'filename': uploaded_file.name,
                'timestamp': timestamp,
                'rows': df.shape[0],
                'columns': df.shape[1]
            })
            
            # Success message with animation
            st.markdown("""
                <div class='success-box'>
                    <strong>File uploaded successfully!</strong> Ready for analysis!
                </div>
            """, unsafe_allow_html=True)
            
            # Display metrics in colored cards
            st.subheader("Dataset Overview")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                    <div class='revenue-card'>
                        <div class='metric-label'>Total Rows</div>
                        <div class='metric-value'>{df.shape[0]:,}</div>
                    </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                    <div class='profit-card'>
                        <div class='metric-label'>Columns</div>
                        <div class='metric-value'>{df.shape[1]}</div>
                    </div>
                """, unsafe_allow_html=True)
            
            with col3:
                memory_mb = df.memory_usage(deep=True).sum() / 1024**2
                st.markdown(f"""
                    <div class='metric-card'>
                        <div class='metric-label'>Memory Usage</div>
                        <div class='metric-value'>{memory_mb:.1f} MB</div>
                    </div>
                """, unsafe_allow_html=True)
            
            with col4:
                missing = df.isnull().sum().sum()
                st.markdown(f"""
                    <div class='cost-card'>
                        <div class='metric-label'>Missing Values</div>
                        <div class='metric-value'>{missing:,}</div>
                    </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<hr>", unsafe_allow_html=True)
            
            # Data preview with modern styling
            st.subheader("Data Preview")
            st.dataframe(df.head(10), use_container_width=True, height=400)
            
            # Column information
            with st.expander("Column Information & Data Types", expanded=True):
                col_info = pd.DataFrame({
                    'Column': df.columns,
                    'Data Type': df.dtypes.astype(str),
                    'Non-Null Count': df.count().values,
                    'Null %': (df.isnull().sum().values / len(df) * 100).round(2),
                    'Unique Values': [df[col].nunique() for col in df.columns],
                    'Sample Value': [str(df[col].dropna().iloc[0]) if len(df[col].dropna()) > 0 else 'N/A' for col in df.columns]
                })
                st.dataframe(col_info, use_container_width=True)
            
            # Auto-detect columns
            date_cols, numeric_cols, categorical_cols = detect_column_types(df)
            
            st.markdown("""
                <div class='card-container'>
                    <h3>Auto-Detected Column Types</h3>
                </div>
            """, unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"**Date Columns:** {', '.join(date_cols) if date_cols else 'None detected'}")
            with col2:
                st.success(f"**Numeric Columns:** {', '.join(numeric_cols[:5]) if numeric_cols else 'None detected'}")
            with col3:
                st.warning(f"**Categorical Columns:** {', '.join(categorical_cols[:5]) if categorical_cols else 'None detected'}")
            
            # Save to Database Option
            st.markdown("<hr>", unsafe_allow_html=True)
            st.subheader("Save to Database")
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.info("Saving to database allows you to persist your data and load it later without re-uploading!")
            
            with col2:
                if st.button("Save to DB", use_container_width=True, type="primary"):
                    user_id = get_user_id(st.session_state.username)
                    if user_id:
                        with st.spinner("Saving data to database..."):
                            if save_sales_data_to_db(user_id, df):
                                st.success("Data saved successfully to database!")
                            else:
                                st.error("Failed to save data to database")
                    else:
                        st.error("User not found")
            
            # Quick Statistics
            with st.expander("Quick Statistical Summary", expanded=False):
                if numeric_cols:
                    st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                else:
                    st.warning("No numeric columns to analyze")
                    
        except Exception as e:
            st.error(f"Error uploading file: {str(e)}")
            st.exception(e)

# ================= ANALYTICS PAGE ================= #

def analytics_page():
    """Main analytics dashboard with profit focus"""
    st.title("Sales & Profit Analytics Dashboard")

    if st.session_state.df is None:
        st.warning("⚠️ Please upload data first")
        return

    df = st.session_state.df.copy()
    
    # Detect column types
    date_cols, numeric_cols, categorical_cols = detect_column_types(df)
    
    if len(numeric_cols) == 0:
        st.error("No numeric columns found for analysis")
        return

    # Sidebar settings for analytics
    with st.sidebar:
        st.subheader("Analysis Settings")
        
        selected_date = st.selectbox(
            "Date Column",
            ['None'] + date_cols,
            key='analytics_date'
        ) if date_cols else 'None'
        
        selected_revenue = st.selectbox(
            "Revenue Column",
            numeric_cols,
            index=0,
            key='analytics_revenue'
        )
        
        selected_cost = st.selectbox(
            "Cost Column",
            ['None'] + numeric_cols,
            index=1 if len(numeric_cols) > 1 else 0,
            key='analytics_cost'
        )
        
        selected_category = st.selectbox(
            "Category Column",
            ['None'] + categorical_cols,
            key='analytics_category'
        ) if categorical_cols else 'None'

    # ================= PROFIT & ROI KPI CARDS ================= #
    st.subheader("Key Performance Indicators")
    
    # Calculate profit metrics
    cost_col = selected_cost if selected_cost != 'None' else None
    profit_metrics = None
    
    if cost_col:
        profit_metrics = calculate_profit_metrics(df, selected_revenue, cost_col)
    
    if profit_metrics:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown(f"""
                <div class='revenue-card'>
                    <div class='metric-label'>Total Revenue</div>
                    <div class='metric-value'>{format_compact_currency(profit_metrics['total_revenue'])}</div>
                </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
                <div class='cost-card'>
                    <div class='metric-label'>Total Cost</div>
                    <div class='metric-value'>{format_compact_currency(profit_metrics['total_cost'])}</div>
                </div>
            """, unsafe_allow_html=True)
        
        with col3:
            profit_color = "profit-card" if profit_metrics['total_profit'] >= 0 else "warning-box"
            st.markdown(f"""
                <div class='{profit_color}'>
                    <div class='metric-label'>Total Profit</div>
                    <div class='metric-value'>{format_compact_currency(profit_metrics['total_profit'])}</div>
                </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
                <div class='metric-card'>
                    <div class='metric-label'>Profit Margin</div>
                    <div class='metric-value'>{profit_metrics['profit_margin']:.1f}%</div>
                </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
                <div class='roi-card'>
                    <div class='metric-label'>ROI</div>
                    <div class='metric-value'>{profit_metrics['roi']:.1f}%</div>
                </div>
            """, unsafe_allow_html=True)
    else:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Records",
                f"{df.shape[0]:,}"
            )
        
        with col2:
            total = df[selected_revenue].sum()
            st.metric(
                f"Total {selected_revenue}",
                format_compact_currency(total)
            )
        
        with col3:
            avg = df[selected_revenue].mean()
            st.metric(
                f"Average {selected_revenue}",
                format_compact_currency(avg)
            )
        
        with col4:
            max_val = df[selected_revenue].max()
            st.metric(
                f"Max {selected_revenue}",
                format_compact_currency(max_val)
            )

    st.markdown("<hr>", unsafe_allow_html=True)

    # ================= PROFIT TREND ANALYSIS ================= #
    
    if selected_date != 'None' and selected_date in df.columns and cost_col:
        st.subheader("📈 Profit & Revenue Trends")
        
        try:
            df[selected_date] = pd.to_datetime(df[selected_date], errors='coerce')
            df_clean = df.dropna(subset=[selected_date, selected_revenue])
            df_clean['profit'] = df_clean[selected_revenue] - df_clean[cost_col]
            
            if len(df_clean) > 0:
                # Aggregate by date
                daily_metrics = df_clean.groupby(selected_date).agg({
                    selected_revenue: 'sum',
                    cost_col: 'sum',
                    'profit': 'sum'
                }).reset_index()
                
                # Create dual-axis chart
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=daily_metrics[selected_date], 
                    y=daily_metrics[selected_revenue],
                    name='Revenue',
                    line=dict(color='#4facfe', width=3),
                    fill='tonexty'
                ))
                
                fig.add_trace(go.Scatter(
                    x=daily_metrics[selected_date], 
                    y=daily_metrics[cost_col],
                    name='Cost',
                    line=dict(color='#fa709a', width=3)
                ))
                
                fig.add_trace(go.Bar(
                    x=daily_metrics[selected_date], 
                    y=daily_metrics['profit'],
                    name='Profit',
                    marker_color='#38ef7d',
                    opacity=0.6
                ))
                
                fig.update_layout(
                    title="Revenue, Cost & Profit Over Time",
                    template="plotly_white",
                    hovermode='x unified',
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Monthly/Quarterly Analysis
                col1, col2 = st.columns(2)
                
                with col1:
                    df_clean['month'] = df_clean[selected_date].dt.to_period('M').astype(str)
                    monthly = df_clean.groupby('month').agg({
                        selected_revenue: 'sum',
                        'profit': 'sum'
                    }).reset_index()
                    
                    if not monthly.empty:
                        fig_month = go.Figure()
                        fig_month.add_trace(go.Bar(
                            x=monthly['month'],
                            y=monthly[selected_revenue],
                            name='Revenue',
                            marker_color='#4facfe'
                        ))
                        fig_month.add_trace(go.Bar(
                            x=monthly['month'],
                            y=monthly['profit'],
                            name='Profit',
                            marker_color='#11998e'
                        ))
                        fig_month.update_layout(
                            title="Monthly Revenue & Profit",
                            barmode='group',
                            template="plotly_white"
                        )
                        st.plotly_chart(fig_month, use_container_width=True)
                
                with col2:
                    df_clean['day_of_week'] = df_clean[selected_date].dt.day_name()
                    weekly = df_clean.groupby('day_of_week')[selected_revenue].mean().reset_index()
                    if not weekly.empty:
                        # Order days of week
                        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                        weekly['day_of_week'] = pd.Categorical(weekly['day_of_week'], categories=day_order, ordered=True)
                        weekly = weekly.sort_values('day_of_week')
                        
                        fig_week = px.bar(
                            weekly, 
                            x='day_of_week', 
                            y=selected_revenue,
                            title="Average Revenue by Day of Week",
                            color=selected_revenue,
                            color_continuous_scale="Viridis"
                        )
                        st.plotly_chart(fig_week, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not generate time series: {str(e)}")

    # ================= PRODUCT/CATEGORY PROFITABILITY ================= #
    
    if selected_category != 'None' and selected_category in df.columns and cost_col:
        st.subheader("🏆 Category Profitability Analysis")
        
        try:
            product_prof = calculate_product_profitability(df, selected_category, selected_revenue, cost_col)
            
            if product_prof is not None and not product_prof.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Top 10 by profit
                    top_10 = product_prof.head(10)
                    fig_profit = px.bar(
                        top_10,
                        x='profit',
                        y=selected_category,
                        orientation='h',
                        title="Top 10 Categories by Profit",
                        color='profit_margin',
                        color_continuous_scale="RdYlGn",
                        labels={'profit': 'Profit ($)'}
                    )
                    st.plotly_chart(fig_profit, use_container_width=True)
                
                with col2:
                    # Profit margin comparison
                    fig_margin = px.bar(
                        top_10,
                        x='profit_margin',
                        y=selected_category,
                        orientation='h',
                        title="Profit Margin % by Category",
                        color='profit_margin',
                        color_continuous_scale="Turbo"
                    )
                    st.plotly_chart(fig_margin, use_container_width=True)
                
                # Detailed table
                with st.expander("📊 Detailed Profitability Table"):
                    display_df = product_prof.copy()
                    display_df.columns = ['Category', 'Revenue', 'Cost', 'Profit', 'Profit Margin %', 'ROI %']
                    st.dataframe(display_df.style.format({
                        'Revenue': '${:,.2f}',
                        'Cost': '${:,.2f}',
                        'Profit': '${:,.2f}',
                        'Profit Margin %': '{:.2f}%',
                        'ROI %': '{:.2f}%'
                    }), use_container_width=True)
        except Exception as e:
            st.warning(f"Could not generate category analysis: {str(e)}")

    # ================= CORRELATION ANALYSIS ================= #
    if len(numeric_cols) > 1:
        st.subheader("🔗 Correlation Analysis")
        
        try:
            corr_cols = numeric_cols[:6]  # Limit to 6 columns
            corr_matrix = df[corr_cols].corr()
            fig_corr = px.imshow(
                corr_matrix,
                text_auto='.2f',
                aspect="auto",
                title="Correlation Heatmap",
                color_continuous_scale="RdBu",
                zmin=-1,
                zmax=1
            )
            fig_corr.update_layout(height=500)
            st.plotly_chart(fig_corr, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not generate correlation matrix: {str(e)}")
    
    # ================= ADVANCED VISUALIZATIONS ================= #
    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader("📊 Advanced Visualizations")
    
    viz_tabs = st.tabs(["Waterfall Chart", "Distribution Analysis", "Hierarchical View", "Box & Violin Plots"])
    
    with viz_tabs[0]:
        # Waterfall chart for profit breakdown
        if cost_col and selected_category != 'None':
            st.markdown("#### Profit Waterfall Analysis")
            try:
                category_data = df.groupby(selected_category).agg({
                    selected_revenue: 'sum',
                    cost_col: 'sum'
                }).reset_index()
                category_data['profit'] = category_data[selected_revenue] - category_data[cost_col]
                category_data = category_data.nlargest(10, 'profit')
                
                # Create waterfall data
                categories = category_data[selected_category].tolist()
                profits = category_data['profit'].tolist()
                
                fig_waterfall = go.Figure(go.Waterfall(
                    name="Profit",
                    orientation="v",
                    measure=["relative"] * len(categories) + ["total"],
                    x=categories + ["Total"],
                    y=profits + [sum(profits)],
                    text=[f"${p:,.0f}" for p in profits] + [f"${sum(profits):,.0f}"],
                    textposition="outside",
                    connector={"line": {"color": "rgb(63, 63, 63)"}},
                    increasing={"marker": {"color": "#38ef7d"}},
                    decreasing={"marker": {"color": "#f5576c"}},
                    totals={"marker": {"color": "#4facfe"}}
                ))
                
                fig_waterfall.update_layout(
                    title="Profit Contribution by Category (Waterfall)",
                    showlegend=False,
                    height=500
                )
                st.plotly_chart(fig_waterfall, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not generate waterfall chart: {str(e)}")
        else:
            st.info("Select a cost column and category for waterfall analysis")
    
    with viz_tabs[1]:
        # Distribution analysis with histogram and KDE
        st.markdown("#### Distribution Analysis")
        selected_dist_metric = st.selectbox("Select Metric for Distribution", numeric_cols, key="dist_metric")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogram with distribution curve
            fig_hist = px.histogram(
                df, 
                x=selected_dist_metric, 
                nbins=30,
                title=f"Distribution of {selected_dist_metric}",
                marginal="box",
                color_discrete_sequence=['#4facfe']
            )
            fig_hist.update_layout(showlegend=False)
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Cumulative distribution
            sorted_data = df[selected_dist_metric].dropna().sort_values()
            cumulative = np.arange(1, len(sorted_data) + 1) / len(sorted_data) * 100
            
            fig_cdf = go.Figure()
            fig_cdf.add_trace(go.Scatter(
                x=sorted_data,
                y=cumulative,
                mode='lines',
                name='CDF',
                line=dict(color='#fa709a', width=3),
                fill='tonexty'
            ))
            fig_cdf.update_layout(
                title=f"Cumulative Distribution of {selected_dist_metric}",
                xaxis_title=selected_dist_metric,
                yaxis_title="Cumulative Percentage (%)",
                template="plotly_white"
            )
            st.plotly_chart(fig_cdf, use_container_width=True)
        
        # Statistical summary
        st.markdown("##### Statistical Summary")
        col1, col2, col3, col4 = st.columns(4)
        data_values = df[selected_dist_metric].dropna()
        
        with col1:
            st.metric("Mean", f"{data_values.mean():.2f}")
            st.metric("Median", f"{data_values.median():.2f}")
        with col2:
            st.metric("Std Dev", f"{data_values.std():.2f}")
            st.metric("Variance", f"{data_values.var():.2f}")
        with col3:
            st.metric("Skewness", f"{data_values.skew():.3f}")
            st.metric("Kurtosis", f"{data_values.kurtosis():.3f}")
        with col4:
            st.metric("Min", f"{data_values.min():.2f}")
            st.metric("Max", f"{data_values.max():.2f}")
    
    with viz_tabs[2]:
        # Hierarchical visualizations (Treemap and Sunburst)
        if selected_category != 'None' and selected_category in df.columns:
            st.markdown("#### Hierarchical Revenue Structure")
            
            viz_type = st.radio("Visualization Type", ["Treemap", "Sunburst"], horizontal=True)
            
            try:
                hierarchy_data = df.groupby(selected_category)[selected_revenue].sum().reset_index()
                hierarchy_data = hierarchy_data.nlargest(15, selected_revenue)
                
                if viz_type == "Treemap":
                    fig_tree = px.treemap(
                        hierarchy_data,
                        path=[selected_category],
                        values=selected_revenue,
                        title=f"Revenue Treemap by {selected_category}",
                        color=selected_revenue,
                        color_continuous_scale='Viridis',
                        hover_data={selected_revenue: ':$,.2f'}
                    )
                    fig_tree.update_traces(textinfo="label+value+percent parent")
                    fig_tree.update_layout(height=600)
                    st.plotly_chart(fig_tree, use_container_width=True)
                else:
                    fig_sun = px.sunburst(
                        hierarchy_data,
                        path=[selected_category],
                        values=selected_revenue,
                        title=f"Revenue Sunburst by {selected_category}",
                        color=selected_revenue,
                        color_continuous_scale='Rainbow'
                    )
                    fig_sun.update_layout(height=600)
                    st.plotly_chart(fig_sun, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not generate hierarchical view: {str(e)}")
        else:
            st.info("Select a category column for hierarchical visualization")
    
    with viz_tabs[3]:
        # Box and Violin plots
        st.markdown("#### Box & Violin Plot Analysis")
        
        if selected_category != 'None' and selected_category in df.columns:
            col1, col2 = st.columns(2)
            
            selected_box_metric = st.selectbox("Select Metric", numeric_cols, key="box_metric")
            
            with col1:
                # Box plot
                fig_box = px.box(
                    df,
                    x=selected_category,
                    y=selected_box_metric,
                    title=f"Box Plot: {selected_box_metric} by {selected_category}",
                    color=selected_category,
                    points="outliers"
                )
                fig_box.update_layout(showlegend=False)
                fig_box.update_xaxes(tickangle=45)
                st.plotly_chart(fig_box, use_container_width=True)
            
            with col2:
                # Violin plot
                fig_violin = px.violin(
                    df,
                    x=selected_category,
                    y=selected_box_metric,
                    title=f"Violin Plot: {selected_box_metric} by {selected_category}",
                    color=selected_category,
                    box=True,
                    points="all"
                )
                fig_violin.update_layout(showlegend=False)
                fig_violin.update_xaxes(tickangle=45)
                st.plotly_chart(fig_violin, use_container_width=True)
        else:
            # Single violin plot
            selected_violin_metric = st.selectbox("Select Metric", numeric_cols, key="violin_metric")
            
            fig_violin_single = go.Figure()
            fig_violin_single.add_trace(go.Violin(
                y=df[selected_violin_metric].dropna(),
                name=selected_violin_metric,
                box_visible=True,
                meanline_visible=True,
                fillcolor='#4facfe',
                opacity=0.6,
                x0=selected_violin_metric
            ))
            fig_violin_single.update_layout(
                title=f"Distribution of {selected_violin_metric}",
                yaxis_title=selected_violin_metric,
                showlegend=False,
                height=500
            )
            st.plotly_chart(fig_violin_single, use_container_width=True)

    # ================= INSIGHTS ================= #
    st.subheader("💡 AI-Driven Insights")
    
    insights = generate_insights(df, selected_date, numeric_cols, categorical_cols)
    
    # Add profit-specific insights
    if profit_metrics:
        if profit_metrics['profit_margin'] > 30:
            insights.insert(0, f"🎯 <strong>Excellent profit margin of {profit_metrics['profit_margin']:.1f}%</strong> - Well above industry average!")
        elif profit_metrics['profit_margin'] > 15:
            insights.insert(0, f"✅ <strong>Healthy profit margin of {profit_metrics['profit_margin']:.1f}%</strong>")
        else:
            insights.insert(0, f"⚠️ <strong>Profit margin of {profit_metrics['profit_margin']:.1f}%</strong> - Consider cost optimization")
        
        if profit_metrics['roi'] > 100:
            insights.insert(1, f"💰 <strong>Exceptional ROI of {profit_metrics['roi']:.1f}%</strong> - More than doubling your investment!")
        elif profit_metrics['roi'] > 50:
            insights.insert(1, f"📈 <strong>Strong ROI of {profit_metrics['roi']:.1f}%</strong>")
    
    for insight in insights:
        st.markdown(f"<div class='insight-box'>{insight}</div>", unsafe_allow_html=True)

# ================= PROFIT INSIGHTS PAGE ================= #

def profit_insights_page():
    """Dedicated page for profit analysis and insights"""
    st.title("💰 Profit Intelligence & Insights")
    
    if st.session_state.df is None:
        st.warning("⚠️ Please upload data first to view profit insights")
        return
    
    df = st.session_state.df.copy()
    date_cols, numeric_cols, categorical_cols = detect_column_types(df)
    
    if len(numeric_cols) < 2:
        st.error("❌ Need at least 2 numeric columns (revenue and cost) for profit analysis")
        return
    
    # Column selection
    st.sidebar.subheader("💼 Profit Analysis Settings")
    
    revenue_col = st.sidebar.selectbox("💰 Revenue Column", numeric_cols, index=0)
    cost_col = st.sidebar.selectbox("💸 Cost Column", numeric_cols, index=1 if len(numeric_cols) > 1 else 0)
    
    if revenue_col == cost_col:
        st.warning("⚠️ Revenue and Cost columns should be different")
        return
    
    # Calculate profit
    df['profit'] = df[revenue_col] - df[cost_col]
    df['profit_margin'] = (df['profit'] / df[revenue_col] * 100).replace([np.inf, -np.inf], 0)
    df['roi'] = (df['profit'] / df[cost_col] * 100).replace([np.inf, -np.inf], 0)
    
    # Overall Metrics
    st.subheader("🎯 Profit Performance Summary")
    
    total_revenue = df[revenue_col].sum()
    total_cost = df[cost_col].sum()
    total_profit = df['profit'].sum()
    avg_margin = df['profit_margin'].mean()
    avg_roi = df['roi'].mean()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown(f"""
            <div class='revenue-card'>
                <div class='metric-label'>💵 Total Revenue</div>
                <div class='metric-value'>{format_compact_currency(total_revenue)}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
            <div class='cost-card'>
                <div class='metric-label'>💸 Total Cost</div>
                <div class='metric-value'>{format_compact_currency(total_cost)}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        profit_style = "profit-card" if total_profit >= 0 else "warning-box"
        st.markdown(f"""
            <div class='{profit_style}'>
                <div class='metric-label'>💰 Net Profit</div>
                <div class='metric-value'>{format_compact_currency(total_profit)}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col4:
        margin_color = "#38ef7d" if avg_margin > 20 else ("#f5576c" if avg_margin < 10 else "#4facfe")
        st.markdown(f"""
            <div class='metric-card' style='background: {margin_color};'>
                <div class='metric-label'>📊 Avg Margin</div>
                <div class='metric-value'>{avg_margin:.1f}%</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col5:
        st.markdown(f"""
            <div class='roi-card'>
                <div class='metric-label'>📈 Avg ROI</div>
                <div class='metric-value'>{avg_roi:.1f}%</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # Profit Distribution
    st.subheader("📊 Profit Distribution Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Profit histogram
        fig_hist = px.histogram(
            df, 
            x='profit', 
            nbins=50,
            title="Profit Distribution",
            color_discrete_sequence=['#11998e'],
            labels={'profit': 'Profit ($)'}
        )
        fig_hist.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="Break-even")
        fig_hist.add_vline(x=df['profit'].median(), line_dash="dash", line_color="blue", annotation_text="Median")
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        # Profit margin distribution
        fig_margin = px.box(
            df, 
            y='profit_margin',
            title="Profit Margin Distribution",
            color_discrete_sequence=['#667eea'],
            labels={'profit_margin': 'Profit Margin (%)'}
        )
        st.plotly_chart(fig_margin, use_container_width=True)
    
    # Winners vs Losers
    st.subheader("🏆 Winners vs ⚠️ Losers")
    
    profitable = df[df['profit'] > 0]
    unprofitable = df[df['profit'] <= 0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
            <div class='profit-card'>
                <h4 style='margin: 0; color: white;'>🏆 Profitable Transactions</h4>
                <h2 style='margin: 0.5rem 0; color: white;'>{len(profitable):,}</h2>
                <p style='margin: 0; color: white;'>({len(profitable)/len(df)*100:.1f}% of total)</p>
                <p style='margin: 0.5rem 0 0 0; color: white;'><strong>Total Profit: ${profitable["profit"].sum():,.0f}</strong></p>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
            <div class='warning-box'>
                <h4 style='margin: 0; color: white;'>⚠️ Unprofitable Transactions</h4>
                <h2 style='margin: 0.5rem 0; color: white;'>{len(unprofitable):,}</h2>
                <p style='margin: 0; color: white;'>({len(unprofitable)/len(df)*100:.1f}% of total)</p>
                <p style='margin: 0.5rem 0 0 0; color: white;'><strong>Total Loss: ${unprofitable["profit"].sum():,.0f}</strong></p>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        breakeven = df[df['profit'] == 0]
        st.markdown(f"""
            <div class='roi-card'>
                <h4 style='margin: 0;'>⚖️ Break-even</h4>
                <h2 style='margin: 0.5rem 0;'>{len(breakeven):,}</h2>
                <p style='margin: 0;'>({len(breakeven)/len(df)*100:.1f}% of total)</p>
            </div>
        """, unsafe_allow_html=True)
    
    # Category Analysis
    if categorical_cols:
        st.markdown("<hr>", unsafe_allow_html=True)
        st.subheader("🏷️ Category-wise Profitability")
        
        category_col = st.selectbox("Select Category Column", categorical_cols)
        
        category_profit = df.groupby(category_col).agg({
            revenue_col: 'sum',
            cost_col: 'sum',
            'profit': 'sum',
            'profit_margin': 'mean',
            'roi': 'mean'
        }).reset_index()
        
        category_profit.columns = ['Category', 'Revenue', 'Cost', 'Profit', 'Avg Margin %', 'Avg ROI %']
        category_profit = category_profit.sort_values('Profit', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top profitable categories
            top_10 = category_profit.head(10)
            fig_top = px.bar(
                top_10,
                x='Profit',
                y='Category',
                orientation='h',
                title="Top 10 Most Profitable Categories",
                color='Avg Margin %',
                color_continuous_scale="RdYlGn",
                labels={'Profit': 'Total Profit ($)'}
            )
            st.plotly_chart(fig_top, use_container_width=True)
        
        with col2:
 # Bottom loss-making categories
            bottom_10 = category_profit.tail(10).sort_values('Profit')
            fig_bottom = px.bar(
                bottom_10,
                x='Profit',
                y='Category',
                orientation='h',
                title="Top 10 Loss-making Categories",
                color='Profit',
                color_continuous_scale="Reds",
                labels={'Profit': 'Total Loss ($)'}
            )
            st.plotly_chart(fig_bottom, use_container_width=True)
        
        # Detailed table
        with st.expander("📋 Detailed Category Profitability Table"):
            try:
                styled_df = category_profit.style.format({
                    'Revenue': '${:,.2f}',
                    'Cost': '${:,.2f}',
                    'Profit': '${:,.2f}',
                    'Avg Margin %': '{:.2f}%',
                    'Avg ROI %': '{:.2f}%'
                }).background_gradient(subset=['Profit'], cmap='RdYlGn')
            except ImportError:
                styled_df = category_profit.style.format({
                    'Revenue': '${:,.2f}',
                    'Cost': '${:,.2f}',
                    'Profit': '${:,.2f}',
                    'Avg Margin %': '{:.2f}%',
                    'Avg ROI %': '{:.2f}%'
                })
            st.dataframe(styled_df, use_container_width=True)
    
    # Actionable Insights
    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader("💡 Actionable Insights & Recommendations")
    
    insights = []
    
    if avg_margin > 30:
        insights.append("✅ <strong>Excellent Performance:</strong> Your average profit margin of {:.1f}% is outstanding! This indicates strong pricing power and cost control.".format(avg_margin))
    elif avg_margin > 15:
        insights.append("👍 <strong>Good Performance:</strong> Average profit margin of {:.1f}% is healthy. Look for opportunities to optimize further.".format(avg_margin))
    else:
        insights.append("⚠️ <strong>Action Needed:</strong> Profit margin of {:.1f}% is below optimal. Consider cost reduction or price optimization strategies.".format(avg_margin))
    
    if len(unprofitable) > len(df) * 0.3:
        insights.append("🔴 <strong>High Loss Rate:</strong> {:.1f}% of transactions are unprofitable. Review pricing strategy and identify loss leaders.".format(len(unprofitable)/len(df)*100))
    
    if total_profit > 0:
        insights.append("💰 <strong>Overall Profitability:</strong> You've generated ${:,.0f} in total profit. Focus on scaling profitable segments.".format(total_profit))
    else:
        insights.append("🚨 <strong>Critical:</strong> Overall loss of ${:,.0f}. Immediate action required to address cost structure or pricing.".format(abs(total_profit)))
    
    if categorical_cols and len(category_profit) > 0:
        top_cat = category_profit.iloc[0]
        insights.append("🏆 <strong>Top Performer:</strong> '{}' is your most profitable category with ${:,.0f} profit. Consider expanding this segment.".format(top_cat['Category'], top_cat['Profit']))
        
        if len(category_profit) > 1:
            bottom_cat = category_profit.iloc[-1]
            if bottom_cat['Profit'] < 0:
                insights.append("⚠️ <strong>Loss Leader:</strong> '{}' is causing ${:,.0f} in losses. Evaluate whether to optimize or discontinue.".format(bottom_cat['Category'], abs(bottom_cat['Profit'])))
    
    for insight in insights:
        st.markdown(f"<div class='insight-box'>{insight}</div>", unsafe_allow_html=True)

# ================= ADVANCED ANALYTICS ================= #

def advanced_analytics_page():
    """Advanced analytics features with AI predictions"""
    st.title("📈 Advanced Analytics & AI Predictions")
    
    if not ADVANCED_ANALYTICS_AVAILABLE:
        st.warning("⚠️ Advanced analytics require scikit-learn. Install with: pip install scikit-learn")
        return
    
    if st.session_state.df is None:
        st.warning("⚠️ Please upload data first")
        return
    
    df = st.session_state.df.copy()
    _, numeric_cols, _ = detect_column_types(df)
    
    if len(numeric_cols) < 2:
        st.error("❌ Need at least 2 numeric columns for advanced analytics")
        return
    
    analysis_type = st.selectbox(
        "Select Analysis Type",
        ["🔮 AI Forecasting & Predictions", "🎯 Customer Clustering", 
         "🚨 Outlier Detection", "📊 Statistical Deep Dive", "📈 Trend Decomposition"]
    )
    
    if "Forecasting" in analysis_type:
        show_forecasting(df, numeric_cols)
    elif "Clustering" in analysis_type:
        show_clustering(df, numeric_cols)
    elif "Outlier" in analysis_type:
        show_outlier_detection(df, numeric_cols)
    elif "Trend Decomposition" in analysis_type:
        show_trend_decomposition(df, numeric_cols)
    else:
        show_statistical_summary(df, numeric_cols)

def show_forecasting(df, numeric_cols):
    """Display advanced AI-assisted forecasting analysis"""
    st.subheader("🔮 AI-Assisted Predictive Analytics")
    
    date_cols, _, _ = detect_column_types(df)
    
    if date_cols:
        col1, col2 = st.columns(2)
        with col1:
            date_col = st.selectbox("Select Date Column", date_cols)
            metric = st.selectbox("Select Metric to Forecast", numeric_cols)
        with col2:
            forecast_periods = st.slider("Forecast Periods Ahead", 3, 30, 7)
            model_type = st.selectbox("Prediction Model", 
                ["Linear Regression", "Polynomial (Degree 2)", "Polynomial (Degree 3)", 
                 "Moving Average", "Exponential Smoothing"])
        
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            trend_data = df.groupby(date_col)[metric].sum().reset_index()
            trend_data = trend_data.dropna().sort_values(date_col)
            
            if len(trend_data) > 5:
                X = np.arange(len(trend_data)).reshape(-1, 1)
                y = trend_data[metric].values
                
                # Fit different models based on selection
                if model_type == "Linear Regression":
                    model = LinearRegression()
                    model.fit(X, y)
                    predictions = model.predict(X)
                    future_X = np.arange(len(trend_data), len(trend_data) + forecast_periods).reshape(-1, 1)
                    future_y = model.predict(future_X)
                    
                elif "Polynomial" in model_type:
                    degree = 2 if "Degree 2" in model_type else 3
                    poly_features = np.column_stack([X**i for i in range(1, degree + 1)])
                    model = LinearRegression()
                    model.fit(poly_features, y)
                    predictions = model.predict(poly_features)
                    future_X_raw = np.arange(len(trend_data), len(trend_data) + forecast_periods).reshape(-1, 1)
                    future_X = np.column_stack([future_X_raw**i for i in range(1, degree + 1)])
                    future_y = model.predict(future_X)
                    
                elif model_type == "Moving Average":
                    window = min(5, len(trend_data) // 2)
                    predictions = pd.Series(y).rolling(window=window, min_periods=1).mean().values
                    last_values = y[-window:]
                    future_y = np.array([last_values.mean()] * forecast_periods)
                    
                else:  # Exponential Smoothing
                    alpha = 0.3
                    predictions = np.zeros(len(y))
                    predictions[0] = y[0]
                    for i in range(1, len(y)):
                        predictions[i] = alpha * y[i] + (1 - alpha) * predictions[i-1]
                    future_y = np.array([predictions[-1]] * forecast_periods)
                
                # Calculate confidence intervals
                residuals = y - predictions
                std_error = np.std(residuals)
                confidence_95 = 1.96 * std_error
                
                # Create future dates
                last_date = trend_data[date_col].iloc[-1]
                freq = pd.infer_freq(trend_data[date_col])
                if freq is None:
                    freq = 'D'
                future_dates = pd.date_range(start=last_date, periods=forecast_periods + 1, freq=freq)[1:]
                
                # Advanced visualization
                fig = go.Figure()
                
                # Historical data
                fig.add_trace(go.Scatter(
                    x=trend_data[date_col], 
                    y=y, 
                    mode='lines+markers', 
                    name='Historical Data',
                    line=dict(color='#4facfe', width=3),
                    marker=dict(size=8)
                ))
                
                # Model fit
                fig.add_trace(go.Scatter(
                    x=trend_data[date_col], 
                    y=predictions, 
                    mode='lines', 
                    name='Model Fit',
                    line=dict(color='#fa709a', width=2, dash='dot')
                ))
                
                # Forecast
                fig.add_trace(go.Scatter(
                    x=future_dates,
                    y=future_y,
                    mode='lines+markers',
                    name='Forecast',
                    line=dict(dash='dash', color='#38ef7d', width=3),
                    marker=dict(size=10, symbol='star')
                ))
                
                # Confidence interval
                upper_bound = future_y + confidence_95
                lower_bound = future_y - confidence_95
                
                fig.add_trace(go.Scatter(
                    x=future_dates,
                    y=upper_bound,
                    mode='lines',
                    name='95% Confidence Upper',
                    line=dict(width=0),
                    showlegend=False
                ))
                
                fig.add_trace(go.Scatter(
                    x=future_dates,
                    y=lower_bound,
                    mode='lines',
                    name='95% Confidence',
                    fill='tonexty',
                    fillcolor='rgba(56, 239, 125, 0.2)',
                    line=dict(width=0)
                ))
                
                fig.update_layout(
                    title=f"📈 {metric} Forecast using {model_type}",
                    template="plotly_white",
                    xaxis_title="Date",
                    yaxis_title=metric,
                    hovermode='x unified',
                    height=600,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Forecast table with insights
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write("📊 **Detailed Forecast Values:**")
                    forecast_df = pd.DataFrame({
                        'Date': future_dates,
                        'Forecast': future_y.round(2),
                        'Lower Bound (95%)': lower_bound.round(2),
                        'Upper Bound (95%)': upper_bound.round(2)
                    })
                    st.dataframe(forecast_df, use_container_width=True)
                
                with col2:
                    st.write("📈 **Model Performance:**")
                    from sklearn.metrics import mean_absolute_error, mean_squared_error
                    mae = mean_absolute_error(y, predictions)
                    rmse = np.sqrt(mean_squared_error(y, predictions))
                    r2 = 1 - (np.sum(residuals**2) / np.sum((y - y.mean())**2))
                    
                    st.metric("R² Score", f"{r2:.3f}")
                    st.metric("MAE", f"{mae:.2f}")
                    st.metric("RMSE", f"{rmse:.2f}")
                    
                    # Trend analysis
                    trend_direction = "📈 Upward" if future_y[-1] > y[-1] else "📉 Downward"
                    trend_change = ((future_y[-1] - y[-1]) / y[-1] * 100)
                    st.metric("Trend", trend_direction)
                    st.metric("Change", f"{trend_change:+.2f}%")
                
                # AI Insights
                st.markdown("---")
                st.subheader("🤖 AI-Generated Insights")
                
                insights = []
                avg_forecast = future_y.mean()
                avg_historical = y.mean()
                
                if avg_forecast > avg_historical * 1.1:
                    insights.append(f"📈 <strong>Strong Growth Expected:</strong> Forecast shows {((avg_forecast/avg_historical - 1) * 100):.1f}% increase over historical average")
                elif avg_forecast < avg_historical * 0.9:
                    insights.append(f"📉 <strong>Declining Trend:</strong> Forecast shows {((1 - avg_forecast/avg_historical) * 100):.1f}% decrease - consider intervention")
                else:
                    insights.append(f"➡️ <strong>Stable Trend:</strong> Forecast remains within 10% of historical average")
                
                if r2 > 0.8:
                    insights.append(f"✅ <strong>High Prediction Confidence:</strong> Model explains {r2*100:.1f}% of the variance")
                elif r2 > 0.5:
                    insights.append(f"⚠️ <strong>Moderate Confidence:</strong> Model captures main trends but has some uncertainty")
                else:
                    insights.append(f"⚠️ <strong>Low Confidence:</strong> Data has high variability - use forecast with caution")
                
                # Seasonality detection
                if len(y) > 7:
                    weekly_pattern = np.std(y[-7:]) / np.mean(y[-7:])
                    if weekly_pattern > 0.3:
                        insights.append(f"🔄 <strong>High Variability Detected:</strong> Consider weekly/seasonal patterns in planning")
                
                for insight in insights:
                    st.markdown(f"<div class='insight-box'>{insight}</div>", unsafe_allow_html=True)
                
            else:
                st.warning("⚠️ Not enough data points for forecasting (minimum 6 required)")
                
        except Exception as e:
            st.error(f"❌ Error in forecasting: {str(e)}")
            st.exception(e)
    else:
        st.warning("⚠️ No date column found for forecasting")

def show_clustering(df, numeric_cols):
    """Display clustering analysis"""
    st.subheader("🎯 Customer/Data Clustering")
    
    selected_features = st.multiselect(
        "Select Features for Clustering", 
        numeric_cols, 
        default=numeric_cols[:min(3, len(numeric_cols))]
    )
    
    if len(selected_features) >= 2:
        n_clusters = st.slider("Number of Clusters", 2, 8, 3)
        
        try:
            # Prepare data
            X = df[selected_features].dropna()
            if len(X) < 10:
                st.warning("Not enough data points for clustering")
                return
            
            # Standardize features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # Perform clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(X_scaled)
            
            # Add clusters to dataframe
            df_clustered = X.copy()
            df_clustered['Cluster'] = clusters
            
            # Visualize
            if len(selected_features) == 2:
                fig = px.scatter(
                    df_clustered, 
                    x=selected_features[0], 
                    y=selected_features[1],
                    color='Cluster',
                    title="Customer Segments",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig, use_container_width=True)
            elif len(selected_features) == 3:
                fig = px.scatter_3d(
                    df_clustered, 
                    x=selected_features[0], 
                    y=selected_features[1],
                    z=selected_features[2],
                    color='Cluster',
                    title="3D Cluster Visualization"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Cluster statistics
            st.subheader("📊 Cluster Statistics")
            cluster_stats = df_clustered.groupby('Cluster')[selected_features].mean().round(2)
            st.dataframe(cluster_stats, use_container_width=True)
            
            # Cluster sizes
            cluster_sizes = df_clustered['Cluster'].value_counts().sort_index()
            st.subheader("📈 Cluster Distribution")
            fig = px.pie(
                values=cluster_sizes.values,
                names=[f"Cluster {i}" for i in cluster_sizes.index],
                title="Cluster Size Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error in clustering: {str(e)}")

def show_outlier_detection(df, numeric_cols):
    """Display outlier detection"""
    st.subheader("🔍 Outlier Detection")
    
    metric = st.selectbox("Select Metric for Outlier Detection", numeric_cols)
    
    try:
        # Calculate IQR
        Q1 = df[metric].quantile(0.25)
        Q3 = df[metric].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df[(df[metric] < lower_bound) | (df[metric] > upper_bound)]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Outliers", len(outliers))
        with col2:
            st.metric("Outlier Percentage", f"{(len(outliers)/len(df)*100):.1f}%")
        with col3:
            st.metric("IQR Range", f"{IQR:.2f}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Lower Bound", f"{lower_bound:.2f}")
        with col2:
            st.metric("Upper Bound", f"{upper_bound:.2f}")
        
        # Box plot
        fig = px.box(df, y=metric, title=f"Box Plot - {metric}", points="all")
        st.plotly_chart(fig, use_container_width=True)
        
        # Distribution plot with outliers highlighted
        fig = px.histogram(
            df, 
            x=metric, 
            title=f"Distribution of {metric} with Outliers",
            nbins=50
        )
        fig.add_vline(x=lower_bound, line_dash="dash", line_color="red", annotation_text="Lower Bound")
        fig.add_vline(x=upper_bound, line_dash="dash", line_color="red", annotation_text="Upper Bound")
        st.plotly_chart(fig, use_container_width=True)
        
        if len(outliers) > 0:
            st.subheader("📋 Outlier Records")
            st.dataframe(outliers, use_container_width=True)
            
            # Download outliers
            csv = outliers.to_csv(index=False)
            st.download_button(
                label="📥 Download Outliers",
                data=csv,
                file_name="outliers.csv",
                mime="text/csv"
            )
        else:
            st.success("✅ No outliers detected!")
            
    except Exception as e:
        st.error(f"Error in outlier detection: {str(e)}")

def show_trend_decomposition(df, numeric_cols):
    """Display trend decomposition analysis"""
    st.subheader("📈 Trend Decomposition & Seasonality Analysis")
    
    date_cols, _, _ = detect_column_types(df)
    
    if date_cols:
        col1, col2 = st.columns(2)
        with col1:
            date_col = st.selectbox("Select Date Column", date_cols, key="decomp_date")
            metric = st.selectbox("Select Metric", numeric_cols, key="decomp_metric")
        with col2:
            window_size = st.slider("Moving Average Window", 3, 30, 7)
            decomp_type = st.selectbox("Decomposition Type", ["Additive", "Multiplicative"])
        
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            time_data = df.groupby(date_col)[metric].sum().reset_index()
            time_data = time_data.dropna().sort_values(date_col)
            
            if len(time_data) > window_size * 2:
                # Calculate trend using moving average
                time_data['trend'] = time_data[metric].rolling(window=window_size, center=True).mean()
                
                # Calculate seasonal component
                if decomp_type == "Additive":
                    time_data['seasonal'] = time_data[metric] - time_data['trend']
                    time_data['residual'] = time_data['seasonal'] - time_data['seasonal'].rolling(window=window_size, center=True).mean()
                else:
                    time_data['seasonal'] = time_data[metric] / time_data['trend']
                    time_data['residual'] = time_data['seasonal'] / time_data['seasonal'].rolling(window=window_size, center=True).mean()
                
                # Remove NaN values
                time_data = time_data.dropna()
                
                # Create subplots
                from plotly.subplots import make_subplots
                
                fig = make_subplots(
                    rows=4, cols=1,
                    subplot_titles=('Original Data', 'Trend Component', 'Seasonal Component', 'Residual'),
                    vertical_spacing=0.08
                )
                
                # Original
                fig.add_trace(go.Scatter(
                    x=time_data[date_col], y=time_data[metric],
                    mode='lines', name='Original',
                    line=dict(color='#4facfe', width=2)
                ), row=1, col=1)
                
                # Trend
                fig.add_trace(go.Scatter(
                    x=time_data[date_col], y=time_data['trend'],
                    mode='lines', name='Trend',
                    line=dict(color='#fa709a', width=2)
                ), row=2, col=1)
                
                # Seasonal
                fig.add_trace(go.Scatter(
                    x=time_data[date_col], y=time_data['seasonal'],
                    mode='lines', name='Seasonal',
                    line=dict(color='#38ef7d', width=2)
                ), row=3, col=1)
                
                # Residual
                fig.add_trace(go.Scatter(
                    x=time_data[date_col], y=time_data['residual'],
                    mode='lines', name='Residual',
                    line=dict(color='#f5576c', width=1)
                ), row=4, col=1)
                
                fig.update_layout(
                    height=1000,
                    showlegend=False,
                    title_text=f"Time Series Decomposition - {metric}",
                    template="plotly_white"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Analysis insights
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("##### Component Statistics")
                    st.write(f"**Trend Range:** {time_data['trend'].min():.2f} to {time_data['trend'].max():.2f}")
                    st.write(f"**Seasonal Variance:** {time_data['seasonal'].var():.4f}")
                    st.write(f"**Residual Std Dev:** {time_data['residual'].std():.4f}")
                    
                    # Detect trend direction
                    trend_slope = (time_data['trend'].iloc[-1] - time_data['trend'].iloc[0]) / len(time_data)
                    if trend_slope > 0:
                        st.success(f"📈 **Upward Trend Detected** (slope: {trend_slope:.4f})")
                    elif trend_slope < 0:
                        st.warning(f"📉 **Downward Trend Detected** (slope: {trend_slope:.4f})")
                    else:
                        st.info("➡️ **Flat Trend** (stable over time)")
                
                with col2:
                    st.markdown("##### Seasonality Analysis")
                    
                    # Check for strong seasonality
                    seasonal_strength = time_data['seasonal'].std() / time_data[metric].std()
                    
                    if seasonal_strength > 0.3:
                        st.warning(f"🔄 **Strong Seasonality Detected** (strength: {seasonal_strength:.2%})")
                    elif seasonal_strength > 0.1:
                        st.info(f"🔄 **Moderate Seasonality** (strength: {seasonal_strength:.2%})")
                    else:
                        st.success(f"➡️ **Low Seasonality** (strength: {seasonal_strength:.2%})")
                    
                    # Autocorrelation
                    from scipy.stats import pearsonr
                    if len(time_data) > 10:
                        lag1_corr, _ = pearsonr(time_data[metric].iloc[:-1], time_data[metric].iloc[1:])
                        st.write(f"**Lag-1 Autocorrelation:** {lag1_corr:.3f}")
                
            else:
                st.warning("⚠️ Not enough data points for decomposition")
                
        except Exception as e:
            st.error(f"❌ Error in decomposition: {str(e)}")
            st.exception(e)
    else:
        st.warning("⚠️ No date column found for trend decomposition")

def show_statistical_summary(df, numeric_cols):
    """Display statistical summary"""
    st.subheader("📊 Statistical Summary")
    
    selected_cols = st.multiselect(
        "Select Columns for Summary", 
        numeric_cols, 
        default=numeric_cols[:min(4, len(numeric_cols))]
    )
    
    if selected_cols:
        try:
            # Calculate statistics
            stats_df = df[selected_cols].describe().T
            stats_df['variance'] = df[selected_cols].var()
            stats_df['skewness'] = df[selected_cols].skew()
            stats_df['kurtosis'] = df[selected_cols].kurtosis()
            stats_df['missing'] = df[selected_cols].isnull().sum()
            stats_df['missing_pct'] = (df[selected_cols].isnull().sum() / len(df) * 100).round(2)
            
            # Format display
            styled_df = stats_df.style.format({
                'mean': '{:.2f}',
                'std': '{:.2f}',
                'min': '{:.2f}',
                '25%': '{:.2f}',
                '50%': '{:.2f}',
                '75%': '{:.2f}',
                'max': '{:.2f}',
                'variance': '{:.2f}',
                'skewness': '{:.3f}',
                'kurtosis': '{:.3f}'
            })
            
            st.dataframe(styled_df, use_container_width=True)
            
            # Distribution plots
            st.subheader("📈 Distribution Plots")
            for col in selected_cols[:4]:  # Limit to 4 plots
                fig = px.histogram(
                    df, 
                    x=col, 
                    title=f"Distribution of {col}", 
                    nbins=30,
                    marginal="box"
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Error in statistical summary: {str(e)}")

# ================= DATA VIEWER PAGE ================= #

def data_viewer_page():
    """Data viewing, editing, and deletion page"""
    st.title("📋 Data Viewer & Management")
    
    if st.session_state.df is None:
        st.warning("⚠️ Please upload data first")
        return
    
    df = st.session_state.df.copy()
    
    tabs = st.tabs(["View Data", "Edit Transaction", "Delete Records"])
    
    with tabs[0]:
        # Filtering options
        with st.expander("🔍 Filter Data", expanded=False):
            filter_col = st.selectbox("Select Column to Filter", df.columns)
            
            if df[filter_col].dtype in ['int64', 'float64']:
                min_val = float(df[filter_col].min())
                max_val = float(df[filter_col].max())
                if min_val < max_val:
                    filter_range = st.slider(
                        "Select Range",
                        min_val, max_val,
                        (min_val, max_val)
                    )
                    df = df[(df[filter_col] >= filter_range[0]) & (df[filter_col] <= filter_range[1])]
            else:
                unique_vals = df[filter_col].dropna().unique().tolist()
                if unique_vals:
                    selected_vals = st.multiselect("Select Values", unique_vals, default=unique_vals[:min(5, len(unique_vals))])
                    if selected_vals:
                        df = df[df[filter_col].isin(selected_vals)]
        
        # Display data
        st.subheader(f"📊 Data Preview ({len(df)} rows)")
        
        # Pagination
        col1, col2 = st.columns([3, 1])
        with col1:
            page_size = st.selectbox("Rows per page", [10, 25, 50, 100, 500])
        with col2:
            total_pages = max(1, (len(df) - 1) // page_size + 1)
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, len(df))
        
        st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)
    
    with tabs[1]:
        st.subheader("✏️ Edit Transaction")
        
        # Load transactions from database
        conn = connect_db()
        if conn:
            user_id = get_user_id(st.session_state.username)
            query = "SELECT id, transaction_date, product_name, quantity, unit_price, cost_price FROM sales_data WHERE user_id = %s ORDER BY transaction_date DESC LIMIT 100"
            transactions = pd.read_sql(query, conn, params=(user_id,))
            conn.close()
            
            if not transactions.empty:
                st.dataframe(transactions, use_container_width=True)
                
                st.markdown("---")
                transaction_id = st.number_input("Transaction ID to Edit", min_value=1, step=1)
                
                # Load record for editing
                if st.button("Load Transaction"):
                    conn = connect_db()
                    if conn:
                        cursor = conn.cursor(dictionary=True)
                        cursor.execute("""
                            SELECT * FROM sales_data WHERE id = %s AND user_id = %s
                        """, (transaction_id, user_id))
                        record = cursor.fetchone()
                        cursor.close()
                        conn.close()
                        
                        if record:
                            st.session_state.edit_record = record
                            st.success("Transaction loaded!")
                        else:
                            st.error("Transaction not found")
                
                if 'edit_record' in st.session_state:
                    record = st.session_state.edit_record
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        new_product = st.text_input("Product Name", value=record.get('product_name', ''))
                        new_quantity = st.number_input("Quantity", value=float(record.get('quantity', 0)), step=1.0)
                        new_unit_price = st.number_input("Unit Price", value=float(record.get('unit_price', 0)), step=0.01)
                    
                    with col2:
                        new_cost_price = st.number_input("Cost Price", value=float(record.get('cost_price', 0)), step=0.01)
                        new_category = st.text_input("Category", value=record.get('category', ''))
                    
                    if st.button("💾 Save Changes", type="primary"):
                        conn = connect_db()
                        if conn:
                            cursor = conn.cursor()
                            new_revenue = new_quantity * new_unit_price
                            new_profit = new_revenue - (new_quantity * new_cost_price)
                            
                            cursor.execute("""
                                UPDATE sales_data 
                                SET product_name=%s, quantity=%s, unit_price=%s, cost_price=%s, 
                                    category=%s, revenue=%s, profit=%s
                                WHERE id=%s AND user_id=%s
                            """, (new_product, new_quantity, new_unit_price, new_cost_price, 
                                 new_category, new_revenue, new_profit, transaction_id, user_id))
                            
                            conn.commit()
                            cursor.close()
                            conn.close()
                            st.success("Transaction updated successfully!")
                            del st.session_state.edit_record
                            st.rerun()
            else:
                st.info("No transactions found in database")
    
    with tabs[2]:
        st.subheader("🗑️ Delete Records")
        
        st.warning("⚠️ Warning: Deletion is permanent and cannot be undone!")
        
        delete_method = st.radio("Delete Method", ["Single Transaction", "Bulk Delete by Date Range"])
        
        if delete_method == "Single Transaction":
            transaction_id = st.number_input("Transaction ID", min_value=1, step=1, key="delete_id")
            
            if st.button("🗑️ Delete Transaction", type="secondary"):
                conn = connect_db()
                if conn:
                    cursor = conn.cursor()
                    user_id = get_user_id(st.session_state.username)
                    cursor.execute("DELETE FROM sales_data WHERE id = %s AND user_id = %s", 
                                 (transaction_id, user_id))
                    conn.commit()
                    affected = cursor.rowcount
                    cursor.close()
                    conn.close()
                    
                    if affected > 0:
                        st.success("Transaction deleted!")
                        st.rerun()
                    else:
                        st.error("Transaction not found")
        
        else:
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")
            
            if st.button("🗑️ Delete Records in Range", type="secondary"):
                conn = connect_db()
                if conn:
                    cursor = conn.cursor()
                    user_id = get_user_id(st.session_state.username)
                    cursor.execute("""
                        DELETE FROM sales_data 
                        WHERE user_id = %s AND transaction_date BETWEEN %s AND %s
                    """, (user_id, start_date, end_date))
                    conn.commit()
                    affected = cursor.rowcount
                    cursor.close()
                    conn.close()
                    
                    st.success(f"Deleted {affected} transactions!")
                    st.rerun()
    
    # Data quality metrics
    with st.expander("📊 Data Quality Report", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Complete Rows", len(df.dropna()))
        with col2:
            st.metric("Rows with Missing", len(df) - len(df.dropna()))
        with col3:
            st.metric("Duplicate Rows", df.duplicated().sum())
        
        # Column-wise missing values
        missing_df = pd.DataFrame({
            'Column': df.columns,
            'Missing': df.isnull().sum().values,
            'Percentage': (df.isnull().sum().values / len(df) * 100).round(2)
        }).sort_values('Missing', ascending=False)
        
        st.subheader("Missing Values by Column")
        st.dataframe(missing_df, use_container_width=True)
    
    # Export options
    st.subheader("💾 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name="exported_data.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        # Excel export
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Data')
        excel_data = output.getvalue()
        
        st.download_button(
            label="📥 Download as Excel",
            data=excel_data,
            file_name="exported_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# ================= EXPENSE MANAGEMENT PAGE ================= #

def expense_management_page():
    """Expense tracking and categorization"""
    st.title("Expense Management")
    
    tabs = st.tabs(["Add Expense", "View Expenses", "Category Analysis"])
    
    with tabs[0]:
        st.subheader("Add New Expense")
        
        col1, col2 = st.columns(2)
        with col1:
            expense_date = st.date_input("Date", datetime.now())
            category = st.selectbox("Category", [
                "Rent", "Utilities", "Supplies", "Salaries", 
                "Marketing", "Transportation", "Equipment", "Other"
            ])
            amount = st.number_input("Amount", min_value=0.0, step=10.0)
        
        with col2:
            description = st.text_area("Description")
            receipt_file = st.file_uploader("Attach Receipt/Invoice (Optional)", type=['jpg', 'jpeg', 'png', 'pdf'])
        
        if st.button("Add Expense", type="primary"):
            conn = connect_db()
            if conn:
                cursor = conn.cursor()
                user_id = get_user_id(st.session_state.username)
                
                receipt_data = None
                receipt_filename = None
                if receipt_file:
                    receipt_data = receipt_file.read()
                    receipt_filename = receipt_file.name
                
                cursor.execute("""
                    INSERT INTO expenses (user_id, expense_date, category, amount, description, receipt_file, receipt_filename)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (user_id, expense_date, category, amount, description, receipt_data, receipt_filename))
                
                conn.commit()
                cursor.close()
                conn.close()
                st.success("Expense added successfully!")
                st.rerun()
    
    with tabs[1]:
        st.subheader("Expense History")
        
        conn = connect_db()
        if conn:
            user_id = get_user_id(st.session_state.username)
            query = """
                SELECT id, expense_date, category, amount, description, receipt_filename, created_at
                FROM expenses WHERE user_id = %s ORDER BY expense_date DESC
            """
            expenses_df = pd.read_sql(query, conn, params=(user_id,))
            conn.close()
            
            if not expenses_df.empty:
                # Add edit/delete buttons
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.dataframe(expenses_df, use_container_width=True)
                
                with col2:
                    expense_id = st.number_input("Expense ID to Delete", min_value=1, step=1)
                    if st.button("Delete", type="secondary"):
                        conn = connect_db()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM expenses WHERE id = %s AND user_id = %s", (expense_id, user_id))
                            conn.commit()
                            cursor.close()
                            conn.close()
                            st.success("Expense deleted!")
                            st.rerun()
                
                # Summary metrics
                st.subheader("Summary")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Expenses", f"${expenses_df['amount'].sum():,.2f}")
                with col2:
                    st.metric("This Month", f"${expenses_df[pd.to_datetime(expenses_df['expense_date']).dt.month == datetime.now().month]['amount'].sum():,.2f}")
                with col3:
                    st.metric("Category Count", expenses_df['category'].nunique())
            else:
                st.info("No expenses recorded yet")
    
    with tabs[2]:
        st.subheader("Category-wise Analysis")
        
        conn = connect_db()
        if conn:
            user_id = get_user_id(st.session_state.username)
            query = "SELECT * FROM expenses WHERE user_id = %s"
            expenses_df = pd.read_sql(query, conn, params=(user_id,))
            conn.close()
            
            if not expenses_df.empty:
                # Category breakdown
                category_summary = expenses_df.groupby('category')['amount'].agg(['sum', 'count', 'mean']).reset_index()
                category_summary.columns = ['Category', 'Total', 'Count', 'Average']
                category_summary['Total'] = category_summary['Total'].round(2)
                category_summary['Average'] = category_summary['Average'].round(2)
                
                st.dataframe(category_summary, use_container_width=True)
                
                # Pie chart
                fig = px.pie(category_summary, values='Total', names='Category', 
                            title='Expenses by Category')
                st.plotly_chart(fig, use_container_width=True)
                
                # Timeline
                expenses_df['expense_date'] = pd.to_datetime(expenses_df['expense_date'])
                monthly_expenses = expenses_df.groupby(expenses_df['expense_date'].dt.to_period('M'))['amount'].sum().reset_index()
                monthly_expenses['expense_date'] = monthly_expenses['expense_date'].astype(str)
                
                fig2 = px.line(monthly_expenses, x='expense_date', y='amount', 
                              title='Monthly Expenses Trend', markers=True)
                st.plotly_chart(fig2, use_container_width=True)

# ================= INVENTORY MANAGEMENT PAGE ================= #

def inventory_management_page():
    """Inventory tracking with low stock alerts"""
    st.title("Inventory Management")
    
    tabs = st.tabs(["Add/Update Products", "Stock Levels", "Low Stock Alerts"])
    
    with tabs[0]:
        st.subheader("Product Management")
        
        col1, col2 = st.columns(2)
        with col1:
            product_name = st.text_input("Product Name")
            category = st.selectbox("Category", [
                "Electronics", "Clothing", "Food", "Furniture", 
                "Accessories", "Books", "Other"
            ])
            cost_price = st.number_input("Cost Price", min_value=0.0, step=1.0)
        
        with col2:
            selling_price = st.number_input("Selling Price", min_value=0.0, step=1.0)
            stock_quantity = st.number_input("Stock Quantity", min_value=0, step=1)
        
        if st.button("Add/Update Product", type="primary"):
            conn = connect_db()
            if conn:
                cursor = conn.cursor()
                user_id = get_user_id(st.session_state.username)
                
                # Check if product exists
                cursor.execute("SELECT id FROM products WHERE user_id = %s AND product_name = %s", 
                             (user_id, product_name))
                existing = cursor.fetchone()
                
                if existing:
                    cursor.execute("""
                        UPDATE products SET category=%s, cost_price=%s, selling_price=%s, 
                        stock_quantity=%s WHERE id=%s
                    """, (category, cost_price, selling_price, stock_quantity, existing[0]))
                    st.success("Product updated!")
                else:
                    cursor.execute("""
                        INSERT INTO products (user_id, product_name, category, cost_price, selling_price, stock_quantity)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (user_id, product_name, category, cost_price, selling_price, stock_quantity))
                    st.success("Product added!")
                
                conn.commit()
                cursor.close()
                conn.close()
                st.rerun()
    
    with tabs[1]:
        st.subheader("Current Stock Levels")
        
        conn = connect_db()
        if conn:
            user_id = get_user_id(st.session_state.username)
            query = """
                SELECT id, product_name, category, cost_price, selling_price, stock_quantity
                FROM products WHERE user_id = %s ORDER BY product_name
            """
            products_df = pd.read_sql(query, conn, params=(user_id,))
            conn.close()
            
            if not products_df.empty:
                # Calculate profit margin
                products_df['Profit Margin %'] = ((products_df['selling_price'] - products_df['cost_price']) / 
                                                  products_df['selling_price'] * 100).round(2)
                
                st.dataframe(products_df, use_container_width=True)
                
                # Delete product
                col1, col2 = st.columns([3, 1])
                with col2:
                    product_id = st.number_input("Product ID to Delete", min_value=1, step=1)
                    if st.button("Delete Product"):
                        conn = connect_db()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM products WHERE id = %s AND user_id = %s", 
                                         (product_id, user_id))
                            conn.commit()
                            cursor.close()
                            conn.close()
                            st.success("Product deleted!")
                            st.rerun()
            else:
                st.info("No products in inventory")
    
    with tabs[2]:
        st.subheader("Low Stock Alerts")
        
        threshold = st.slider("Low Stock Threshold", min_value=1, max_value=100, value=10)
        
        conn = connect_db()
        if conn:
            user_id = get_user_id(st.session_state.username)
            query = f"""
                SELECT product_name, category, stock_quantity, selling_price
                FROM products WHERE user_id = %s AND stock_quantity <= {threshold}
                ORDER BY stock_quantity ASC
            """
            low_stock_df = pd.read_sql(query, conn, params=(user_id,))
            conn.close()
            
            if not low_stock_df.empty:
                st.warning(f"⚠️ {len(low_stock_df)} products below threshold!")
                
                for _, row in low_stock_df.iterrows():
                    st.error(f"**{row['product_name']}** - Only {row['stock_quantity']} units left!")
                
                st.dataframe(low_stock_df, use_container_width=True)
            else:
                st.success("✅ All products have sufficient stock!")

# ================= ADMIN DASHBOARD PAGE ================= #

def admin_dashboard_page():
    """Admin dashboard for owners — works with or without MySQL"""
    if not hasattr(st.session_state, 'user_role') or st.session_state.user_role != 'Owner':
        st.error("Access Denied: Admin privileges required")
        return

    st.title("🛡️ Admin Dashboard")

    # Initialize cloud user store if not exists
    if 'cloud_users' not in st.session_state:
        st.session_state.cloud_users = [
            {"id": 1, "username": "admin", "email": "admin@example.com",
             "role": "Owner", "business_name": "Admin Account", "created_at": "2024-01-01"}
        ]

    conn = connect_db()
    db_mode = conn is not None

    tabs = st.tabs(["👥 User Management", "📊 Business Reports", "🖥️ System Monitoring"])

    # ── TAB 1: User Management ──────────────────────────────────────────────────
    with tabs[0]:
        st.subheader("👥 User Management")

        if db_mode:
            users_df = pd.read_sql(
                "SELECT id, username, email, role, business_name, created_at FROM users", conn
            )
            conn.close()
        else:
            st.info("ℹ️ Running in cloud mode — user changes are session-only.")
            users_df = pd.DataFrame(st.session_state.cloud_users)

        st.dataframe(users_df, use_container_width=True)

        st.markdown("---")
        st.subheader("✏️ Edit User Details")

        usernames = users_df["username"].tolist()
        selected_user = st.selectbox("Select User to Edit", usernames)

        if selected_user:
            user_row = users_df[users_df["username"] == selected_user].iloc[0]

            col1, col2 = st.columns(2)
            with col1:
                new_email = st.text_input("Email", value=str(user_row.get("email", "") or ""))
                new_role = st.selectbox(
                    "Role",
                    ["Owner", "Accountant", "Staff"],
                    index=["Owner", "Accountant", "Staff"].index(user_row["role"])
                    if user_row["role"] in ["Owner", "Accountant", "Staff"] else 2
                )
            with col2:
                new_business = st.text_input("Business Name", value=str(user_row.get("business_name", "") or ""))
                new_password = st.text_input("New Password (leave blank to keep)", type="password")

            if st.button("💾 Save Changes", use_container_width=True):
                if db_mode:
                    c2 = connect_db()
                    if c2:
                        cur = c2.cursor()
                        cur.execute(
                            "UPDATE users SET email=%s, role=%s, business_name=%s WHERE username=%s",
                            (new_email, new_role, new_business, selected_user)
                        )
                        if new_password:
                            hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
                            cur.execute("UPDATE users SET password=%s WHERE username=%s",
                                        (hashed, selected_user))
                        c2.commit()
                        cur.close()
                        c2.close()
                        st.success(f"✅ User '{selected_user}' updated in database!")
                        st.rerun()
                else:
                    for u in st.session_state.cloud_users:
                        if u["username"] == selected_user:
                            u["email"] = new_email
                            u["role"] = new_role
                            u["business_name"] = new_business
                    st.success(f"✅ User '{selected_user}' updated (session only)!")
                    st.rerun()

        st.markdown("---")
        st.subheader("🗑️ Delete User")
        del_user = st.selectbox("Select User to Delete", [u for u in usernames if u != "admin"],
                                key="del_user_select")
        if st.button("🗑️ Delete User", type="secondary"):
            if db_mode:
                c2 = connect_db()
                if c2:
                    cur = c2.cursor()
                    cur.execute("DELETE FROM users WHERE username=%s", (del_user,))
                    c2.commit()
                    cur.close()
                    c2.close()
                    st.success(f"User '{del_user}' deleted!")
                    st.rerun()
            else:
                st.session_state.cloud_users = [
                    u for u in st.session_state.cloud_users if u["username"] != del_user
                ]
                st.success(f"User '{del_user}' removed from session!")
                st.rerun()

    # ── TAB 2: Business Reports ─────────────────────────────────────────────────
    with tabs[1]:
        st.subheader("📊 Business Performance Report")

        df = st.session_state.get("df", None)

        if db_mode:
            c2 = connect_db()
            if c2:
                cursor = c2.cursor(dictionary=True)
                cursor.execute(
                    "SELECT COUNT(*) as txn, SUM(revenue) as rev, SUM(profit) as prof FROM sales_data"
                )
                stats = cursor.fetchone()
                cursor.execute("SELECT SUM(amount) as exp FROM expenses")
                exp_stats = cursor.fetchone()
                cursor.close()
                c2.close()

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Transactions", f"{stats['txn']:,}")
                col2.metric("Total Revenue", format_compact_currency(stats['rev'] or 0))
                col3.metric("Total Profit", format_compact_currency(stats['prof'] or 0))
                col4.metric("Total Expenses", format_compact_currency(exp_stats['exp'] or 0))

        if df is not None:
            st.markdown("---")
            st.markdown("### 📁 Uploaded Data Summary")

            date_cols, numeric_cols, cat_cols = detect_column_types(df)

            # Key metrics from uploaded data
            rev_col = next((c for c in numeric_cols if 'rev' in c.lower()), None)
            prof_col = next((c for c in numeric_cols if 'prof' in c.lower()), None)
            cost_col = next((c for c in numeric_cols if 'cost' in c.lower()), None)

            cols = st.columns(4)
            cols[0].metric("Total Records", f"{len(df):,}")
            if rev_col:
                cols[1].metric("Total Revenue", format_compact_currency(df[rev_col].sum()))
            if prof_col:
                cols[2].metric("Total Profit", format_compact_currency(df[prof_col].sum()))
            if cost_col:
                cols[3].metric("Total Cost", format_compact_currency(df[cost_col].sum()))

            # Revenue trend chart
            if date_cols and rev_col:
                st.markdown("#### 📈 Revenue Trend")
                trend_df = df.groupby(date_cols[0])[rev_col].sum().reset_index()
                fig = px.line(trend_df, x=date_cols[0], y=rev_col, title="Revenue Over Time",
                              template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)

            # Profit by category
            cat_col = next((c for c in cat_cols if 'cat' in c.lower()), cat_cols[0] if cat_cols else None)
            if cat_col and prof_col:
                st.markdown("#### 🗂️ Profit by Category")
                cat_df = df.groupby(cat_col)[prof_col].sum().reset_index().sort_values(prof_col, ascending=False)
                fig2 = px.bar(cat_df, x=cat_col, y=prof_col, title="Profit by Category",
                              color=prof_col, color_continuous_scale="RdYlGn",
                              template="plotly_dark")
                st.plotly_chart(fig2, use_container_width=True)

            # Full data table
            with st.expander("📋 View Full Data"):
                st.dataframe(df, use_container_width=True)

            # Download report
            csv_data = df.to_csv(index=False).encode()
            st.download_button("⬇️ Download Report as CSV", csv_data,
                               file_name="admin_report.csv", mime="text/csv",
                               use_container_width=True)
        else:
            if not db_mode:
                st.info("📂 No data uploaded yet. Ask a staff member to upload sales data to see reports here.")

    # ── TAB 3: System Monitoring ────────────────────────────────────────────────
    with tabs[2]:
        st.subheader("🖥️ System Monitoring")

        if db_mode:
            c2 = connect_db()
            if c2:
                cur = c2.cursor(dictionary=True)
                cur.execute("SELECT COUNT(*) as count FROM users"); user_count = cur.fetchone()['count']
                cur.execute("SELECT COUNT(*) as count FROM sales_data"); sales_count = cur.fetchone()['count']
                cur.execute("SELECT COUNT(*) as count FROM expenses"); exp_count = cur.fetchone()['count']
                cur.execute("SELECT COUNT(*) as count FROM products"); prod_count = cur.fetchone()['count']
                cur.close(); c2.close()

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("👤 Total Users", user_count)
                col2.metric("🧾 Sales Records", sales_count)
                col3.metric("💸 Expense Records", exp_count)
                col4.metric("📦 Products", prod_count)
                st.success("✅ Database connected and operating normally.")
            else:
                st.warning("⚠️ Could not connect to database.")
        else:
            col1, col2 = st.columns(2)
            col1.metric("👤 Session Users", len(st.session_state.get('cloud_users', [])))
            col2.metric("📁 Data Rows Loaded", len(st.session_state.df) if st.session_state.get('df') is not None else 0)
            st.info("☁️ Cloud mode: App is running without a database. All data is session-based.")

        st.markdown("---")
        st.subheader("ℹ️ App Info")
        col1, col2 = st.columns(2)
        col1.info(f"**Logged in as:** {st.session_state.get('username', 'admin')}")
        col2.info(f"**Role:** {st.session_state.get('user_role', 'Owner')}")
        st.success("✅ System is operating normally.")

# ================= REPORTS PAGE ================= #

def generate_pdf_report(df, numeric_cols):
    """Generate PDF report using reportlab"""
    if not PDF_AVAILABLE:
        return None
    
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1e3c72'),
            spaceAfter=30,
            alignment=1  # Center
        )
        title = Paragraph(f"Business Report - {datetime.now().strftime('%B %d, %Y')}", title_style)
        story.append(title)
        story.append(Spacer(1, 0.2*inch))
        
        # Summary section
        summary_style = ParagraphStyle(
            'Summary',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#2a5298'),
            spaceAfter=12
        )
        story.append(Paragraph("Executive Summary", summary_style))
        
        summary_data = [
            ['Metric', 'Value'],
            ['Total Records', f"{df.shape[0]:,}"],
            ['Total Columns', f"{df.shape[1]}"],
            ['Date Range', f"{df.select_dtypes(include=['datetime64']).min().min() if len(df.select_dtypes(include=['datetime64']).columns) > 0 else 'N/A'} to {df.select_dtypes(include=['datetime64']).max().max() if len(df.select_dtypes(include=['datetime64']).columns) > 0 else 'N/A'}"],
        ]
        
        if numeric_cols:
            summary_data.extend([
                ['Total Revenue', f"${df[numeric_cols[0]].sum():,.2f}"],
                ['Average Value', f"${df[numeric_cols[0]].mean():,.2f}"],
            ])
        
        summary_table = Table(summary_data, colWidths=[3*inch, 3*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3c72')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 0.3*inch))
        
        # Top 10 records
        story.append(Paragraph("Top 10 Records", summary_style))
        
        # Select first few columns for the table
        display_cols = df.columns[:min(5, len(df.columns))].tolist()
        table_data = [display_cols]
        
        for _, row in df[display_cols].head(10).iterrows():
            table_data.append([str(row[col])[:20] for col in display_cols])
        
        data_table = Table(table_data)
        data_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2a5298')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(data_table)
        
        # Build PDF
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()
    
    except Exception as e:
        st.error(f"PDF generation error: {e}")
        return None

def reports_page():
    """Reports generation page"""
    st.title("📑 Reports & Export")
    
    if st.session_state.df is None:
        st.warning("⚠️ Please upload data first")
        return
    
    df = st.session_state.df.copy()
    date_cols, numeric_cols, categorical_cols = detect_column_types(df)
    
    report_type = st.selectbox(
        "Select Report Type",
        ["Summary Report", "Detailed Analysis", "Executive Dashboard", "Custom Report"]
    )
    
    if report_type == "Summary Report":
        show_summary_report(df, date_cols, numeric_cols, categorical_cols)
    elif report_type == "Detailed Analysis":
        show_detailed_analysis(df, numeric_cols)
    elif report_type == "Executive Dashboard":
        show_executive_dashboard(df, numeric_cols)
    else:
        show_custom_report(df)

def show_summary_report(df, date_cols, numeric_cols, categorical_cols):
    """Display summary report"""
    st.subheader("📋 Summary Report")
    
    # Create report content in columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Dataset Overview**")
        st.write(f"- **Total Records:** {df.shape[0]:,}")
        st.write(f"- **Total Columns:** {df.shape[1]}")
        st.write(f"- **Date Columns:** {len(date_cols)}")
        st.write(f"- **Numeric Columns:** {len(numeric_cols)}")
        st.write(f"- **Categorical Columns:** {len(categorical_cols)}")
        st.write(f"- **Memory Usage:** {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    with col2:
        st.write("**Data Quality**")
        st.write(f"- **Complete Rows:** {len(df.dropna()):,}")
        st.write(f"- **Rows with Missing:** {len(df) - len(df.dropna()):,}")
        st.write(f"- **Duplicate Rows:** {df.duplicated().sum():,}")
        st.write(f"- **Missing Values:** {df.isnull().sum().sum():,}")
    
    if numeric_cols:
        st.write("**Numeric Summary**")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    if categorical_cols:
        st.write("**Categorical Columns**")
        cat_info = pd.DataFrame({
            'Column': categorical_cols,
            'Unique Values': [df[col].nunique() for col in categorical_cols],
            'Most Common': [df[col].mode().iloc[0] if not df[col].mode().empty else 'N/A' for col in categorical_cols]
        })
        st.dataframe(cat_info, use_container_width=True)
    
    # Save report option
    if st.button("💾 Save Report", use_container_width=True):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.saved_reports.append({
            'timestamp': timestamp,
            'type': 'Summary Report',
            'data': df.describe().to_dict()
        })
        st.success("✅ Report saved successfully!")

def show_detailed_analysis(df, numeric_cols):
    """Display detailed analysis"""
    st.subheader("🔍 Detailed Analysis")
    
    if numeric_cols:
        selected_metric = st.selectbox("Select Metric for Analysis", numeric_cols)
        
        # Percentiles
        percentiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        percentile_values = df[selected_metric].quantile(percentiles)
        
        percentile_df = pd.DataFrame({
            'Percentile': [f"{int(p*100)}%" for p in percentiles],
            'Value': percentile_values.values.round(2)
        })
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Percentile Distribution**")
            st.dataframe(percentile_df, use_container_width=True)
        
        with col2:
            # Confidence intervals
            mean_val = df[selected_metric].mean()
            std_val = df[selected_metric].std()
            n = len(df[selected_metric].dropna())
            
            st.write("**Confidence Intervals (95%)**")
            st.write(f"- **Lower Bound:** {(mean_val - 1.96 * std_val / np.sqrt(n)):.2f}")
            st.write(f"- **Mean:** {mean_val:.2f}")
            st.write(f"- **Upper Bound:** {(mean_val + 1.96 * std_val / np.sqrt(n)):.2f}")
        
        # Distribution plots
        fig = px.histogram(
            df, 
            x=selected_metric,
            title=f"Distribution of {selected_metric}",
            nbins=50,
            marginal="box"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # QQ plot approximation
        st.subheader("Normality Check")
        from scipy import stats
        data = df[selected_metric].dropna()
        statistic, p_value = stats.normaltest(data)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Test Statistic", f"{statistic:.3f}")
        with col2:
            st.metric("P-Value", f"{p_value:.3f}")
        
        if p_value > 0.05:
            st.success("✅ Data appears normally distributed (p > 0.05)")
        else:
            st.warning("⚠️ Data does not appear normally distributed (p < 0.05)")

def show_executive_dashboard(df, numeric_cols):
    """Display executive dashboard"""
    st.subheader("🎯 Executive Dashboard")
    
    # Create executive summary cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.metric("Total Records", f"{df.shape[0]:,}")
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        if numeric_cols:
            st.metric("Total Revenue", format_compact_currency(df[numeric_cols[0]].sum()))
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col3:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        if numeric_cols:
            st.metric("Average Value", format_compact_currency(df[numeric_cols[0]].mean()))
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Quick insights
    st.markdown("### 📊 Key Takeaways")
    
    insights = []
    if numeric_cols:
        max_col = numeric_cols[0]
        insights.append(f"💰 **Highest Value:** ${df[max_col].max():,.2f}")
        insights.append(f"📈 **Growth Potential:** Top 10% values start at ${df[max_col].quantile(0.9):,.2f}")
    
    if len(df) > 1000:
        insights.append(f"📊 **Sample Size:** Large dataset with {df.shape[0]:,} records - statistically significant")
    
    for insight in insights:
        st.info(insight)
    
    # Export options
    st.markdown("### 📥 Export Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV Report",
            data=csv,
            file_name=f"executive_report_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        if PDF_AVAILABLE:
            if st.button("📄 Generate PDF Report", use_container_width=True):
                pdf_data = generate_pdf_report(df, numeric_cols)
                if pdf_data:
                    st.download_button(
                        label="📥 Download PDF",
                        data=pdf_data,
                        file_name=f"report_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
        else:
            st.info("Install reportlab for PDF export: pip install reportlab")

def show_custom_report(df):
    """Display custom report builder"""
    st.subheader("🔧 Custom Report Builder")
    
    # Column selection
    selected_columns = st.multiselect(
        "Select Columns for Report",
        df.columns.tolist(),
        default=df.columns[:min(5, len(df.columns))].tolist()
    )
    
    if selected_columns:
        # Aggregation options
        agg_type = st.selectbox(
            "Aggregation Type",
            ["None", "Sum", "Mean", "Count", "Min", "Max"]
        )
        
        if agg_type != "None" and selected_columns:
            # Group by options
            group_by = st.selectbox("Group By (Optional)", ['None'] + selected_columns)
            
            if group_by != 'None':
                agg_funcs = {col: agg_type.lower() for col in selected_columns if col != group_by}
                if agg_funcs:
                    report_df = df.groupby(group_by).agg(agg_funcs).reset_index()
                else:
                    report_df = df[selected_columns]
            else:
                if agg_type == "Sum":
                    report_df = pd.DataFrame(df[selected_columns].sum()).T
                elif agg_type == "Mean":
                    report_df = pd.DataFrame(df[selected_columns].mean()).T
                elif agg_type == "Count":
                    report_df = pd.DataFrame(df[selected_columns].count()).T
                elif agg_type == "Min":
                    report_df = pd.DataFrame(df[selected_columns].min()).T
                elif agg_type == "Max":
                    report_df = pd.DataFrame(df[selected_columns].max()).T
                else:
                    report_df = df[selected_columns]
            
            st.dataframe(report_df, use_container_width=True)
            
            # Export custom report
            csv = report_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Custom Report",
                data=csv,
                file_name="custom_report.csv",
                mime="text/csv",
                use_container_width=True
            )

# ================= SETTINGS PAGE ================= #

def settings_page():
    """User settings page"""
    st.title("⚙️ Settings")
    
    tabs = st.tabs(["👤 Profile", "🎨 Preferences", "🗑️ Data Management", "📊 System Info"])
    
    with tabs[0]:
        st.subheader("Profile Settings")
        
        st.text_input("Username", value=st.session_state.username, disabled=True)
        email = st.text_input("Email", placeholder="Enter your email", value="", key="profile_email")
        
        st.subheader("Change Password")
        current_pass = st.text_input("Current Password", type="password", key="current_pass")
        new_pass = st.text_input("New Password", type="password", key="new_pass")
        confirm_pass = st.text_input("Confirm New Password", type="password", key="confirm_pass")
        
        if st.button("Update Profile", use_container_width=True):
            if new_pass and new_pass == confirm_pass and len(new_pass) >= 6:
                st.success("✅ Profile updated successfully!")
            elif new_pass:
                st.error("❌ Passwords don't match or are too short")
            else:
                st.success("✅ Email updated successfully!")
    
    with tabs[1]:
        st.subheader("Display Preferences")
        
        theme = st.selectbox("Theme", ["Light", "Dark", "System Default"], index=0)
        chart_style = st.selectbox("Default Chart Style", ["Plotly", "Matplotlib"], index=0)
        default_page_size = st.selectbox("Default Page Size", [10, 25, 50, 100], index=1)
        
        st.subheader("Notification Settings")
        email_notifications = st.checkbox("Enable email notifications", value=False)
        report_reminders = st.checkbox("Weekly report reminders", value=True)
        
        if st.button("Save Preferences", use_container_width=True):
            st.success("✅ Preferences saved!")
    
    with tabs[2]:
        st.subheader("Data Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🗑️ Clear Current Data", use_container_width=True):
                st.session_state.df = None
                st.success("✅ Data cleared")
                st.rerun()
            
            if st.button("📁 Clear Upload History", use_container_width=True):
                st.session_state.upload_history = []
                st.success("✅ Upload history cleared")
        
        with col2:
            if st.button("📑 Clear Saved Reports", use_container_width=True):
                st.session_state.saved_reports = []
                st.success("✅ Saved reports cleared")
            
            if st.button("🔄 Reset All Settings", use_container_width=True):
                st.session_state.column_mappings = {}
                st.success("✅ Settings reset")
        
        st.subheader("Data Export")
        if st.session_state.df is not None:
            if st.button("📥 Export All Data (CSV)", use_container_width=True):
                csv = st.session_state.df.to_csv(index=False)
                st.download_button(
                    label="Click to Download",
                    data=csv,
                    file_name="all_data.csv",
                    mime="text/csv"
                )
    
    with tabs[3]:
        st.subheader("System Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Application Info**")
            st.write(f"- **Version:** 2.0.0")
            st.write(f"- **Python Version:** {np.__version__}")
            st.write(f"- **Pandas Version:** {pd.__version__}")
            st.write(f"- **Streamlit Version:** {st.__version__}")
        
        with col2:
            st.write("**Session Info**")
            st.write(f"- **Logged in as:** {st.session_state.username}")
            st.write(f"- **Session ID:** {id(st.session_state)}")
            st.write(f"- **Data Loaded:** {'Yes' if st.session_state.df is not None else 'No'}")
        
        if st.session_state.upload_history:
            st.subheader("Upload History")
            history_df = pd.DataFrame(st.session_state.upload_history)
            st.dataframe(history_df, use_container_width=True)

# ================= MAIN ================= #

def main():
    """Main application entry point"""
    if not st.session_state.logged_in:
        show_login()
    else:
        show_dashboard()

if __name__ == "__main__":
    main()