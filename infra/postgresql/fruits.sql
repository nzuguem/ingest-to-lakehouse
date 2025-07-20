DROP TABLE IF EXISTS Fruits CASCADE;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE Fruits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255),
    price NUMERIC(10, 2),
    quantity INTEGER
);

-- publish the previous state of a record
-- https://stackoverflow.com/questions/59799503/postgres-debezium-does-not-publish-the-previous-state-of-a-record
ALTER TABLE Fruits REPLICA IDENTITY FULL;

INSERT INTO Fruits (name, price, quantity) VALUES ('Pomme', 1.20, 100);
INSERT INTO Fruits (name, price, quantity) VALUES ('Banane', 0.80, 150);
INSERT INTO Fruits (name, price, quantity) VALUES ('Mangue', 2.50, 60);
INSERT INTO Fruits (name, price, quantity) VALUES ('Orange', 1.00, 200);
INSERT INTO Fruits (name, price, quantity) VALUES ('Ananas', 3.00, 40);
INSERT INTO Fruits (name, price, quantity) VALUES ('Fraise', 2.20, 90);
INSERT INTO Fruits (name, price, quantity) VALUES ('Pastèque', 4.50, 30);
INSERT INTO Fruits (name, price, quantity) VALUES ('Kiwi', 1.40, 75);
INSERT INTO Fruits (name, price, quantity) VALUES ('Papaye', 3.20, 50);
INSERT INTO Fruits (name, price, quantity) VALUES ('Raisin', 2.80, 120);