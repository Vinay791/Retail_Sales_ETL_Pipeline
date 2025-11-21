# Retail Sales ETL Pipeline (Airflow + Polars)

This project is a complete ETL (Extract–Transform–Load) pipeline built using **Python**, **Polars**, **Airflow**, and **Parquet** on a Linux environment (Ubuntu / WSL / VMware).

The pipeline:
- Extracts raw CSV sales files  
- Transforms & cleans them using Polars  
- Loads the cleaned parquet data into a target folder  
- Generates summary analytics  

---

## 📂 Project Structure

retail_sales_etl/
│
├── data/
│ ├── raw/ # Raw CSV files
│ └── processed/ # Parquet & analytics output
│
├── scripts/
│ ├── extract.py # Extract step
│ ├── transform.py # Clean & analytics
│ └── load.py # Load step
│
├── airflow/
│ └── dags/
│ └── retail_sales_etl.py
│
└── README.md


---

## 🚀 How the ETL Pipeline Works

### **1. Extract (`extract.py`)**
- Reads CSV files such as `sales_2024_01.csv`
- Merges all CSVs into a Polars DataFrame  
- Outputs:  
  - `data/processed/extracted.parquet`

### **2. Transform (`transform.py`)**
Cleans:
- Removes null order IDs  
- Strips whitespace  
- Replaces empty strings with `None`  

Generates:
- `clean_sales.parquet`  
- Analytics:
  - Top 5 customers  
  - Daily totals  
  - Monthly totals  

### **3. Load (`load.py`)**
- Reads `clean_sales.parquet`
- Saves a copy as `clean_sales_copy.parquet`  
  _(Represents loading to a target system)_

### **4. Airflow DAG (`retail_sales_etl.py`)**
Workflow:

extract → transform → load

Airflow schedules and runs the pipeline automatically (daily).

---

## 🧑‍💻 Setup Instructions (Linux / WSL / Ubuntu)

### **1. Install dependencies**
```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip -y
```
##2. Create Airflow environment
```
mkdir airflow
cd airflow
python3 -m venv venv
source venv/bin/activate
````
##3. Install Airflow
```
pip install apache-airflow
```
##4. Install project dependencies
```
pip install polars
```

##5. Initialize Airflow
```
airflow db init
```

##6. Start Airflow services

###Terminal 1:
```
airflow webserver
```
###Terminal 2:
```
airflow scheduler
```
###Open UI:
```
http://localhost:8080
```

#📦 Running the ETL

##Place your DAG here:
```
~/airflow/dags/retail_sales_etl.py
```

##Place the project folder (scripts, data) here:
```
~/airflow/retail_sales_project/
```

##Trigger the DAG:
```
airflow dags trigger retail_sales_etl
```
##📊 Outputs Generated

All outputs appear in data/processed/:

extracted.parquet

- clean_sales.parquet

- clean_sales_copy.parquet

- analytics_top_customers.csv

- analytics_daily_sales.csv

- analytics_monthly_sales.csv

| Component       | Technology                    |
| --------------- | ----------------------------- |
| Data Processing | Polars                        |
| Scheduling      | Airflow                       |
| File Format     | Parquet                       |
| Environment     | Linux / Ubuntu / WSL / VMware |
| Language        | Python                        |


