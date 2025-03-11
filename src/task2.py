from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

def initialize_spark(app_name="Task2_Valued_No_Suggestions"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_valued_no_suggestions(df):
    """
    Find employees who feel valued but have not provided suggestions and calculate their proportion.
    """
    valued_no_suggestions_df = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))
    number = valued_no_suggestions_df.count()
    total_employees = df.count()
    proportion = round((number / total_employees) * 100, 2) if total_employees > 0 else 0
    return number, proportion

def write_output(number, proportion, output_path):
    """
    Write the results to a text file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(f"Number of Employees Feeling Valued without Suggestions: {number}\n")
        f.write(f"Proportion: {proportion}%\n")

def main():
    """
    Main function to execute Task 2.
    """
    spark = initialize_spark()
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-ramala-prasanth/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-ramala-prasanth/outputs/task2/valued_no_suggestions.txt"
    
    df = load_data(spark, input_file)
    number, proportion = identify_valued_no_suggestions(df)
    write_output(number, proportion, output_file)
    
    spark.stop()

if __name__ == "__main__":
    main()