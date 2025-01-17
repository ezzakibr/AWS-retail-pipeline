import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)

def generate_customers(num_customers=1000):
    first_names = ['John', 'Emma', 'Michael', 'Sarah', 'David', 'Lisa', 'James', 'Mary']
    last_names = ['Smith', 'Johnson', 'Brown', 'Davis', 'Wilson', 'Moore', 'Taylor']
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
    
    customers = []
    for i in range(num_customers):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,999)}@{random.choice(domains)}"
        
        customers.append({
            'customer_id': i + 1,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'registration_date': datetime.now() - timedelta(days=random.randint(1, 1000))
        })
    
    return pd.DataFrame(customers)

def generate_products(num_products=100):
    categories = ['Electronics', 'Books', 'Clothing', 'Home & Kitchen', 'Sports']
    adjectives = ['Premium', 'Basic', 'Elite', 'Essential', 'Deluxe']
    products = []
    
    for i in range(num_products):
        category = random.choice(categories)
        adj = random.choice(adjectives)
        products.append({
            'product_id': i + 1,
            'product_name': f"{adj} {category} Item {i+1}",
            'category': category,
            'price': round(random.uniform(10, 1000), 2),
            'stock_quantity': random.randint(0, 1000)
        })
    
    return pd.DataFrame(products)

def generate_orders(customers_df, products_df, num_orders=5000):
    orders = []
    
    for i in range(num_orders):
        customer_id = random.choice(customers_df['customer_id'])
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        
        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.choice(products_df['product_id'])
            product_price = float(products_df[products_df['product_id'] == product_id]['price'])
            quantity = random.randint(1, 5)
            
            orders.append({
                'order_id': i + 1,
                'customer_id': customer_id,
                'product_id': product_id,
                'order_date': order_date,
                'quantity': quantity,
                'unit_price': product_price,
                'total_amount': quantity * product_price
            })
    
    return pd.DataFrame(orders)

# Generate the data
customers_df = generate_customers()
products_df = generate_products()
orders_df = generate_orders(customers_df, products_df)

# Save to CSV files
customers_df.to_csv('customers.csv', index=False)
products_df.to_csv('products.csv', index=False)
orders_df.to_csv('orders.csv', index=False)