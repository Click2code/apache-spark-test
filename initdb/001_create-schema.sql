CREATE TABLE IF NOT EXISTS PUBLIC.customer (
  "customer_id" BIGSERIAL PRIMARY KEY,
  "customer_name" VARCHAR(255) NOT NULL,
  "customer_age" INT NOT NULL
);

CREATE TABLE IF NOT EXISTS PUBLIC.order_customer (
  "order_id" BIGINT NOT NULL,
  "customer_name" VARCHAR(255) NOT NULL,
  "customer_age" INT NOT NULL
);

INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (1, 'Billie Dale', 26);
INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (2, 'Ollie Olson', 34);
INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (3, 'Craig Hahn', 21);
INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (4, 'Arlene Holbrook', 64);
INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (5, 'Joyce Mcclure', 52);
INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (6, 'Brad Franks', 47);
INSERT INTO PUBLIC.customer ("customer_id", "customer_name", "customer_age") VALUES (7, 'Louis Hatch', 18);