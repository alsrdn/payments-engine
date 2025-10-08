use std::{error::Error, fs::File, path::Path};

use crate::transaction_types::Transaction;
use csv::{Reader, StringRecord};

/// A parser for the input CSV files.
pub(crate) struct CsvFileReader {
    reader: Reader<File>,
}

impl CsvFileReader {
    /// Initialize the parser from a specified file.
    pub(crate) fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All) // Remove all whitespace.
            .has_headers(false) // So that we can support both headerless and inputs with headers
            .from_path(path)?;

        Ok(CsvFileReader { reader })
    }

    /// Returns an iterator over the deserialized records.
    pub(crate) fn records(&mut self) -> impl Iterator<Item = Result<Transaction, csv::Error>> {
        // Chech if the first record is either a header or input data.
        let mut record = StringRecord::new();
        let pos = self.reader.position().clone();
        if self.reader.read_record(&mut record).is_ok()
            && record != vec!["type", "client", "tx", "amount"]
        {
            // If the record is a header, seek back to the beginning and start deserializing.
            let _ = self.reader.seek(pos);
        }
        self.reader.deserialize::<Transaction>()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        csv_reader::CsvFileReader,
        transaction_types::{Transaction, TransactionType},
    };
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn should_parse_file() {
        let mut transactions_csv = NamedTempFile::new().unwrap();

        let data = "type, client, tx, amount
                                  deposit, 1, 1, 1.0
                                  deposit, 2, 2, 2.0
                                  deposit, 1, 3, 2.0
                                  withdrawal, 1, 4, 1.5
                                  withdrawal, 2, 5, 3.0";

        transactions_csv.write_all(data.as_bytes()).unwrap();
        transactions_csv.flush().unwrap();

        let mut reader = CsvFileReader::from_path(transactions_csv.path()).unwrap();

        let transactions: Vec<Transaction> = reader
            .records()
            .map(|res| res.expect("Expected a valid transaction."))
            .collect();

        assert_eq!(transactions[3].amount(), Some(1.5.into()));
        assert_eq!(
            transactions[3].transaction_type(),
            TransactionType::Withdrawal
        );
        assert_eq!(transactions[3].client(), 1.into());
        assert_eq!(transactions[3].id(), 4.into());
    }

    #[test]
    fn should_round_values_with_more_decimal_places() {
        let mut transactions_csv = NamedTempFile::new().unwrap();

        let data = "type, client, tx, amount
                                  deposit, 1, 1, 1.999999
                                  deposit, 1, 2, 1.499999
                                  withdrawal, 2, 5, 3.0";

        transactions_csv.write_all(data.as_bytes()).unwrap();
        transactions_csv.flush().unwrap();

        let mut reader = CsvFileReader::from_path(transactions_csv.path()).unwrap();

        let transactions: Vec<Transaction> = reader
            .records()
            .map(|res| res.expect("Expected a valid transaction."))
            .collect();

        assert_eq!(transactions[0].amount(), Some(1.9999.into()));
        assert_eq!(transactions[0].transaction_type(), TransactionType::Deposit);
        assert_eq!(transactions[0].client(), 1.into());
        assert_eq!(transactions[0].id(), 1.into());

        assert_eq!(transactions[1].amount(), Some(1.4999.into()));
        assert_eq!(transactions[1].transaction_type(), TransactionType::Deposit);
        assert_eq!(transactions[1].client(), 1.into());
        assert_eq!(transactions[1].id(), 2.into());
    }

    #[test]
    fn should_reject_negative_amounts() {
        let mut transactions_csv = NamedTempFile::new().unwrap();

        let data = "type, client, tx, amount
                                  deposit, 1, 1, -1.999999
                                  deposit, 1, 2, -1.499999
                                  withdrawal, 2, 5, 3.0";

        transactions_csv.write_all(data.as_bytes()).unwrap();
        transactions_csv.flush().unwrap();

        let mut reader = CsvFileReader::from_path(transactions_csv.path()).unwrap();

        let transactions: Vec<_> = reader.records().collect();
        assert!(transactions[0].is_err());
        assert!(transactions[1].is_err());
        assert!(transactions[2].is_ok());
    }

    #[test]
    fn should_parse_input_without_header() {
        let mut transactions_csv = NamedTempFile::new().unwrap();

        let data = "deposit, 1, 1, 200
                                  deposit, 1, 2, 100
                                  withdrawal, 2, 5, 3.0";

        transactions_csv.write_all(data.as_bytes()).unwrap();
        transactions_csv.flush().unwrap();

        let mut reader = CsvFileReader::from_path(transactions_csv.path()).unwrap();

        let transactions: Vec<Transaction> = reader
            .records()
            .map(|res| res.expect("Expected a valid transaction."))
            .collect();

        assert_eq!(transactions[0].transaction_type(), TransactionType::Deposit);
        assert_eq!(transactions[1].transaction_type(), TransactionType::Deposit);
        assert_eq!(
            transactions[2].transaction_type(),
            TransactionType::Withdrawal
        );
    }

    #[test]
    fn should_not_panic_on_empty_file() {
        let mut transactions_csv = NamedTempFile::new().unwrap();

        let data = "";

        transactions_csv.write_all(data.as_bytes()).unwrap();
        transactions_csv.flush().unwrap();

        let mut reader = CsvFileReader::from_path(transactions_csv.path()).unwrap();

        let transactions: Vec<Transaction> = reader
            .records()
            .map(|res| res.expect("Expected a valid transaction."))
            .collect();

        assert_eq!(transactions.len(), 0);
    }

    #[test]
    fn should_parse_input_with_uneven_whitespaces() {
        let mut transactions_csv = NamedTempFile::new().unwrap();

        let data = "    deposit,     1,    1, 200   
                                    deposit , 1,2 , 100  
                                  withdrawal,2,5,   3.0";

        transactions_csv.write_all(data.as_bytes()).unwrap();
        transactions_csv.flush().unwrap();

        let mut reader = CsvFileReader::from_path(transactions_csv.path()).unwrap();

        let transactions: Vec<Transaction> = reader
            .records()
            .map(|res| res.expect("Expected a valid transaction."))
            .collect();

        assert_eq!(transactions[0].transaction_type(), TransactionType::Deposit);
        assert_eq!(transactions[1].transaction_type(), TransactionType::Deposit);
        assert_eq!(
            transactions[2].transaction_type(),
            TransactionType::Withdrawal
        );
    }
}
