mod account;
mod csv_reader;
mod transaction_processor;
mod transaction_types;

use std::hash::Hasher;
use std::{
    env,
    error::Error,
    hash::{DefaultHasher, Hash},
};

use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};

use crate::{
    transaction_processor::{ProcessorMessage, TransactionProcessor},
    transaction_types::ClientId,
};

// Number of workers to use for processing transactions.
static NUM_WORKERS: usize = 4;

// Assign a client to a worker based on the client ID. All transactions that have the same client ID are processed by the same worker.
fn assign_client_to_worker(client: ClientId) -> usize {
    let mut hasher = DefaultHasher::new();
    client.hash(&mut hasher);
    (hasher.finish() as usize) % NUM_WORKERS
}

// A task that processes transactions. A worker can handle transactions from multiple clients.
struct Worker {
    handle: JoinHandle<TransactionProcessor>,
    tx: Sender<ProcessorMessage>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <transactions.csv>", args[0]);
        std::process::exit(1);
    }

    let transactions_file = &args[1];

    // We create a task for each worker.
    let mut workers = Vec::new();
    for _ in 0..NUM_WORKERS {
        let (tx, rx) = mpsc::channel(1024); //TODO: fine-tune the size of the channel
        let payment_worker = TransactionProcessor::new();
        let worker = Worker {
            handle: tokio::spawn(payment_worker.run(rx)),
            tx,
        };
        workers.push(worker);
    }

    // Start parsing the CSV file and feed each transaction record to the correct processor by client id.
    let mut file_parser = csv_reader::CsvFileReader::from_path(transactions_file)?;
    for record in file_parser.records() {
        match record {
            Ok(transaction) => {
                let transaction_id = transaction.id();
                let client = transaction.client();
                let worker_id = assign_client_to_worker(client);
                let worker = &mut workers[worker_id];
                if let Err(e) = worker
                    .tx
                    .send(ProcessorMessage::process_transaction(transaction))
                    .await
                {
                    eprintln!(
                        "Could not process transaction {} for client {}: worker error {}",
                        transaction_id, client, e
                    );
                }
            }
            Err(e) => {
                eprintln!("Error reading CSV record: {:?}", e);
            }
        }
    }

    // Finished reading all the transactions. Signal all workers to stop gracefully.
    for worker in workers.iter() {
        if let Err(e) = worker.tx.send(ProcessorMessage::shutdown()).await {
            eprintln!("Could not stop worker: error {}", e);
        }
    }

    // Wait for workers to finish and write out the results to stdout.
    let mut csv_writer = csv::Writer::from_writer(std::io::stdout());
    for worker in workers {
        match worker.handle.await {
            Ok(payment_worker) => {
                payment_worker.write_csv_records(&mut csv_writer);
            }
            Err(e) => eprintln!("Payment worker encountered an error: {}", e),
        }
    }

    Ok(())
}
