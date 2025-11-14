from hdfs import InsecureCLient

try:    
    client = InsecureCLient('http://localhost:50070', user='kupriyanovvn')
    print("Connected to HDFS successfully.")
except Exception as e:
    print(f"Error connecting to HDFS: {e}")
    exit(1)


