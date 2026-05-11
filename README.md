# NBA Player Data Statistics and Predictions Based on Spark 🏀

## Overview
This repository contains a comprehensive big data processing and machine learning pipeline designed to analyze and predict NBA player statistics. Built with **Apache Spark (Scala)** for high-performance distributed data processing and **Jupyter Notebook (Python)** for data visualization, this project uncovers valuable insights into player performance, career trends, and team dynamics.

## Key Features & Analytical Modules
The core data processing logic is implemented in Scala and categorized into several analytical modules:

* **Data Preprocessing (`Player_Stats_Preprocess.scala`):** Cleans and standardizes raw NBA datasets for downstream analysis.
* **Exploratory Data Analysis (EDA):**
    * `Basic_Stats_Analyze.scala`: Extracts fundamental statistical distributions of player metrics.
    * `Three_Point_Analyze.scala`: Analyzes the evolution and impact of 3-point shooting in the modern era.
    * `Age_Exp_Thrend_Analyze.scala`: Tracks performance trajectories based on player age and league experience.
    * `Team_Change_Analyze.scala`: Evaluates how trades and team changes affect player performance.
* **Advanced Metrics (`Zscore_Calculator.scala` & `Zscore_task2.scala`):** Computes normalized Z-scores across different eras/seasons to fairly evaluate player dominance regardless of league-wide pace changes.
* **Machine Learning (`RandomForest1.scala`):** Utilizes Spark MLlib to train a Random Forest model, predicting future player performance or categorical outcomes based on historical features.
* **Data Visualization (`main.ipynb`):** A frontend Jupyter Notebook that ingests Spark outputs to generate intuitive graphs, charts, and interactive visual reports.

## Tech Stack
* **Distributed Computing:** Apache Spark, Scala
* **Build Tool:** Maven (`pom.xml`)
* **Data Visualization:** Python, Jupyter Notebook, Pandas, Matplotlib/Seaborn
* **Machine Learning:** Spark MLlib (Random Forest)

## Getting Started

### Prerequisites
* Java Development Kit (JDK 8 or 11 recommended)
* Apache Spark installed and configured
* Maven
* Python 3.x with Jupyter Notebook installed

### How to Run
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/yeyuchen223/NBA-Player-Data-Statistics-and-Predictions-Based-on-Spark.git
    cd NBA-Player-Data-Statistics-and-Predictions-Based-on-Spark/NBA1
    ```

2.  **Build the Spark project:**
    Compile the Scala code and package it using Maven:
    ```bash
    mvn clean package
    ```

3.  **Execute Spark Jobs:**
    Submit the packaged JAR to your local Spark cluster. For example, to run the preprocessor:
    ```bash
    spark-submit --class com.demo1.Player_Stats_Preprocess target/nba-analysis-1.0-SNAPSHOT.jar
    ```

4.  **Visualize Results:**
    Once the Spark jobs have generated the output data, open the Jupyter Notebook to render the visualizations:
    ```bash
    jupyter notebook main.ipynb
    ```

## Repository Structure
```text
.
├── NBA1/
│   ├── src/main/java/com/demo1/   # Core Scala scripts for Spark jobs
│   ├── data/                      # Dataset directory (ensure raw CSVs are placed here)
│   └── pom.xml                    # Maven configuration and dependencies
└── main.ipynb                     # Python visualization script
