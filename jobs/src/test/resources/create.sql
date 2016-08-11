CREATE TABLE IF NOT EXISTS PUBLIC.customer (
  customer_id BIGINT AUTO_INCREMENT,
  customer_name VARCHAR(255) NOT NULL,
  customer_age INT NOT NULL,
  PRIMARY KEY(customer_id)
);
