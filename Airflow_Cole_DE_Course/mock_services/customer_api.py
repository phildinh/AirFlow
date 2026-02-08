
from flask import Flask, jsonify, request
import random
from datetime import datetime

app = Flask(__name__)

# Sample customer data
CUSTOMERS = [
    {"id": 1, "name": "John Doe", "email": "john@example.com", "segment": "Premium", "region": "North"},
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "segment": "Standard", "region": "South"},
    {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "segment": "Premium", "region": "East"},
    {"id": 4, "name": "Alice Brown", "email": "alice@example.com", "segment": "Basic", "region": "West"},
    {"id": 5, "name": "Charlie Wilson", "email": "charlie@example.com", "segment": "Standard", "region": "North"},
]

@app.route('/customers', methods=['GET'])
def get_customers():
    """Return customer data"""
    date_param = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    # Simulate some variation in customer data based on date
    customers = []
    for customer in CUSTOMERS:
        # Add some random variation
        customer_copy = customer.copy()
        customer_copy['last_updated'] = date_param
        customer_copy['status'] = random.choice(['active', 'inactive', 'pending'])
        customers.append(customer_copy)
    
    return jsonify(customers)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)