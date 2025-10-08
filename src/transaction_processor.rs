use std::{
    collections::{HashMap, hash_map::Entry},
    error::Error,
};

use tokio::sync::mpsc;

use crate::{
    account::Account,
    transaction_types::{ClientId, Transaction, TransactionType},
};

// Processor that handles transactions for a set of clients.
// Each client has only one associated account.
pub(crate) struct TransactionProcessor {
    accounts: HashMap<ClientId, Account>,
}

// The message type used to control the processing.
pub(crate) enum ProcessorMessage {
    // Transaction processing request.
    ProcessTransaction(Transaction),
    // A shutdown request for the processor. A shutdown message should be issued only after all transactions have been pushed to the queue.
    Shutdown,
}

impl ProcessorMessage {
    pub(crate) fn process_transaction(transaction: Transaction) -> Self {
        Self::ProcessTransaction(transaction)
    }

    pub(crate) fn shutdown() -> Self {
        Self::Shutdown
    }
}

impl TransactionProcessor {
    pub(crate) fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    // Process a single transaction. This would be called by the prcessing task when a transaction processing message is received.
    // This function will propagate the error up the call stack.
    fn process_transaction(&mut self, transaction: &Transaction) -> Result<(), Box<dyn Error>> {
        let client = transaction.client();
        let transaction_id = transaction.id();

        let account = match self.accounts.entry(client) {
            Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
            Entry::Vacant(vacant_entry) => vacant_entry.insert(Account::new(client)?),
        };

        match transaction.transaction_type() {
            TransactionType::Deposit => {
                let amount = transaction.amount().unwrap();
                account.deposit(amount, transaction_id)?;
            }
            TransactionType::Withdrawal => {
                let amount = transaction.amount().unwrap();
                account.withdraw(amount, transaction_id)?;
            }
            TransactionType::Dispute => {
                account.dispute(transaction_id)?;
            }
            TransactionType::Resolve => {
                account.resolve_dispute(transaction_id)?;
            }
            TransactionType::Chargeback => {
                account.chargeback(transaction_id)?;
            }
        }
        Ok(())
    }

    // Run the processing task.
    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<ProcessorMessage>) -> Self {
        while let Some(message) = rx.recv().await {
            match message {
                ProcessorMessage::ProcessTransaction(transaction) => {
                    if let Err(err) = self.process_transaction(&transaction) {
                        // We just print out the error on stderr. We don't stop processing on any error.
                        eprintln!("Error processing transaction: {}", err);
                    }
                }
                ProcessorMessage::Shutdown => {
                    break;
                }
            }
        }

        self
    }

    // Write out the account records to the csv writer.
    pub(crate) fn write_csv_records<W: std::io::Write>(&self, writer: &mut csv::Writer<W>) {
        for account in self.accounts.values() {
            if let Err(err) = writer.serialize(account) {
                eprintln!(
                    "Cannot serialize account with client_id: {}; {}",
                    account.client(),
                    err
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_process_multiple_deposits_and_withdrawals() {
        let transactions = vec![
            Transaction::new(
                TransactionType::Deposit,
                1.into(),
                1.into(),
                Some(100.0.into()),
            ),
            // 1 deposit 100
            Transaction::new(
                TransactionType::Deposit,
                2.into(),
                2.into(),
                Some(200.0.into()),
            ),
            // 2 deposit 200
            Transaction::new(
                TransactionType::Deposit,
                1.into(),
                3.into(),
                Some(200.0.into()),
            ),
            // 1 deposit 200, total 300
            Transaction::new(
                TransactionType::Withdrawal,
                2.into(),
                4.into(),
                Some(150.0.into()),
            ), // 2 withdrawal 150, total 50
            Transaction::new(
                TransactionType::Withdrawal,
                1.into(),
                5.into(),
                Some(300.0.into()),
            ), // 1 withdraw 300, total 0
        ];

        let mut processor = TransactionProcessor::new();

        for transaction in transactions.iter() {
            assert!(processor.process_transaction(transaction).is_ok());
        }

        assert_eq!(
            processor.accounts.get(&1.into()).unwrap().available(),
            0.0.into()
        );
        assert_eq!(
            processor.accounts.get(&2.into()).unwrap().available(),
            50.0.into()
        );
    }
}
