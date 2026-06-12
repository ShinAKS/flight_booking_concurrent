-- Drop and recreate for clean runs
DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS seats;
DROP TABLE IF EXISTS customers;

CREATE TABLE seats (
    seat_id    INTEGER PRIMARY KEY,
    status     TEXT NOT NULL DEFAULT 'available',  -- 'available' | 'booked'
    customer_id INTEGER
);

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name        TEXT NOT NULL
);

-- Seed 100 seats
INSERT INTO seats (seat_id, status)
SELECT generate_series(1, 100), 'available';

-- Seed 100 customers
INSERT INTO customers (customer_id, name)
SELECT gs, 'Customer ' || gs FROM generate_series(1, 100) AS gs;
