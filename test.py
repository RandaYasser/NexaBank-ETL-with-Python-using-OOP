from src.extractors.csv_extractor import CSVExtractor
from src.extractors.json_extractor import JSONExtractor
from src.extractors.txt_extractor import TXTExtractor

# csv_extractor = CSVExtractor('D:\\ITI\\26 Python\\Final Project\\incoming_data\\2025-04-18\\14\\customer_profiles.csv')

# # Extract the data
# df, partition_info = csv_extractor.extract()

# # Print the DataFrame
# print("\n=== DataFrame Contents ===")
# print(df.head(2))  # Basic print

# print("\n=== DataFrame Info ===")
# print(df.info())  # Detailed information about the DataFrame

# print("\n=== Formatted Partition Info ===")
# for key, value in partition_info.items():
#     print(f"{key}: {value}")

# json_extractor = JSONExtractor('D:\\ITI\\26 Python\\Final Project\\incoming_data\\2025-04-18\\14\\transactions.json')

# # Extract the data
# df, partition_info = json_extractor.extract()

# # Print the DataFrame
# print("\n=== DataFrame Contents ===")
# print(df.head(2))  # Basic print

# print("\n=== DataFrame Info ===")
# print(df.info())  # Detailed information about the DataFrame

# print("\n=== Formatted Partition Info ===")
# for key, value in partition_info.items():
#     print(f"{key}: {value}")


txt_extractor = TXTExtractor('D:\\ITI\\26 Python\\Final Project\\incoming_data\\2025-04-18\\14\\loans.txt')

# Extract the data
df, partition_info = txt_extractor.extract()

# Print the DataFrame
print("\n=== DataFrame Contents ===")
print(df.head(2))  # Basic print

print("\n=== DataFrame Info ===")
print(df.info())  # Detailed information about the DataFrame

print("\n=== Formatted Partition Info ===")
for key, value in partition_info.items():
    print(f"{key}: {value}")
