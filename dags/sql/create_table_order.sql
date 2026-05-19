CREATE TABLE IF NOT EXISTS orders (
  id UUID,
  date_order TIMESTAMP,
  date_shipping TIMESTAMP,
  quantity SMALLINT,
  price REAL,
  customer_id UUID REFERENCES customers(id),
  product_id UUID REFERENCES products(id)
);
