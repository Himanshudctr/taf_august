import csv

# File name
filename = "customers.csv"

# Sample customer data
customer_data = [
    ["CustomerID", "Name", "Email", "Phone", "City"],
    [1, "John Doe", "john@example.com", "9876543210", "Bangalore"],
    [2, "Mary Smith", "mary@example.com", "9123456780", "Hyderabad"],
    [3, "David Johnson", "david@example.com", "9988776655", "Mumbai"],
    [4, "Priya Sharma", "priya@example.com", "9909090909", "Mumbai"],
    [5, "Rahul Verma", "rahul@example.com", "8088088080", "Delhi"],
]

# Write CSV
with open(filename, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerows(customer_data)

print(f"CSV file '{filename}' created successfully!")
