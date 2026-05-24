-- Initial Schema for Financial Ledger

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    balance DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID UNIQUE NOT NULL, -- Distributed ID from Ingestion Service
    from_account VARCHAR(50) REFERENCES accounts(account_id),
    to_account VARCHAR(50) REFERENCES accounts(account_id),
    amount DECIMAL(18, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed some test accounts
INSERT INTO accounts (account_id, balance, currency) VALUES 
('acc_789', 10000.00, 'USD'),
('acc_ledger', 0.00, 'USD')
ON CONFLICT DO NOTHING;
