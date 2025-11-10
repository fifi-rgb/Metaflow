-- Create sample e-commerce schema for testing

-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO customers (email, first_name, last_name, phone) VALUES
    ('john.doe@email.com', 'John', 'Doe', '555-0101'),
    ('jane.smith@email.com', 'Jane', 'Smith', '555-0102'),
    ('bob.johnson@email.com', 'Bob', 'Johnson', '555-0103'),
    ('alice.williams@email.com', 'Alice', 'Williams', '555-0104'),
    ('charlie.brown@email.com', 'Charlie', 'Brown', '555-0105');

INSERT INTO products (product_name, category, price, stock_quantity) VALUES
    ('Laptop Pro 15', 'Electronics', 1299.99, 50),
    ('Wireless Mouse', 'Electronics', 29.99, 200),
    ('USB-C Cable', 'Accessories', 12.99, 500),
    ('Desk Chair', 'Furniture', 299.99, 30),
    ('Standing Desk', 'Furniture', 599.99, 15);

INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES
    (1, '2025-01-01 10:30:00', 1329.98, 'completed'),
    (2, '2025-01-02 14:15:00', 329.98, 'completed'),
    (3, '2025-01-03 09:45:00', 612.98, 'shipped'),
    (1, '2025-01-04 16:20:00', 42.98, 'processing'),
    (4, '2025-01-05 11:00:00', 899.98, 'completed');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1299.99),
    (1, 2, 1, 29.99),
    (2, 4, 1, 299.99),
    (2, 2, 1, 29.99),
    (3, 5, 1, 599.99),
    (3, 3, 1, 12.99),
    (4, 2, 1, 29.99),
    (4, 3, 1, 12.99),
    (5, 1, 1, 1299.99),
    (5, 5, 1, 599.99);

-- Create indexes
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);

-- Create view for reporting
CREATE VIEW customer_order_summary AS
SELECT 
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.email, c.first_name, c.last_name;