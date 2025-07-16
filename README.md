# Real-Time Credit Card Transaction Processing System

## 🧠 Project Overview

This project simulates a **real-time data processing system** for a financial company that monitors **credit card transactions** from various POS terminals. The primary goal is to **detect frauds**, **transform and store valid transactions**, and **visualize insightful statistics** via Power BI.

---

## 🔧 Technologies Used

- **Apache Kafka**: Real-time event streaming for simulating POS transactions.
- **Apache Spark Structured Streaming**: Real-time data processing from Kafka.
- **Hadoop HDFS**: Distributed storage for processed data.
- **Power BI**: Data visualization and business intelligence.
- **Apache Airflow**: Workflow scheduling for automating daily Power BI data refresh.

---

## 📌 Project Objectives

- **Simulate real-time credit card transactions** (from CSV via Kafka).
- **Filter and transform data**:
  - Remove invalid/fraudulent transactions (`Is Fraud? = Yes`).
  - Convert `Amount` to VND based on daily FX rates.
  - Format time fields (`dd/mm/yyyy`, `hh:mm:ss`).
- **Store valid transactions** in Hadoop.
- **Daily aggregation and statistics** by:
  - Merchant
  - City
  - Time (day, month, year)
- **Visualize results** in Power BI and keep it updated daily via Airflow.

---

## 🗂️ Project Structure

