CREATE TABLE IF NOT EXISTS products (
  id UUID PRIMARY KEY,
  name VARCHAR,
  categories VARCHAR,
  price REAL,
  weight REAL,
  modification_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
