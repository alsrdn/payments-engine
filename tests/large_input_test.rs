use std::fs::File;
use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};
use tempfile::tempdir;

// AI generated: "generate an integration test that generates a csv with tens of thousands of deposit transactions for 2 clients. each deposits has the amount of 1. at the end I want to check if the balance is the number of transactions"
#[test]
fn large_scale_deposits_should_sum_correctly() {
    // ---- Config ----
    let deposits_per_client = 50_000u32;
    let tmp_dir = tempdir().unwrap();
    let input_path = tmp_dir.path().join("large_input.csv");

    // ---- Generate test input ----
    {
        let mut f = BufWriter::new(File::create(&input_path).unwrap());
        writeln!(f, "type,client,tx,amount").unwrap();
        for i in 0..deposits_per_client {
            writeln!(f, "deposit,1,{},1.0", i + 1).unwrap();
            writeln!(f, "deposit,2,{},1.0", i + deposits_per_client + 1).unwrap();
        }
    }

    // ---- Run your compiled binary ----
    // NOTE: During `cargo test`, the binary name defaults to your package name (Cargo.toml: [package].name)
    let output = Command::new(env!("CARGO_BIN_EXE_payments-engine")) // magic env var provided by Cargo test harness
        .arg(&input_path)
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute process");

    assert!(
        output.status.success(),
        "processor exited with non-zero status"
    );

    // ---- Check results ----
    let stdout = String::from_utf8(output.stdout).unwrap();

    // Find lines for each client
    let mut found_client1 = false;
    let mut found_client2 = false;
    for line in stdout.lines() {
        if line.starts_with("1,") {
            found_client1 = true;
            assert!(
                line.contains("50000.0"),
                "Client 1 balance mismatch: {line}"
            );
        }
        if line.starts_with("2,") {
            found_client2 = true;
            assert!(
                line.contains("50000.0"),
                "Client 2 balance mismatch: {line}"
            );
        }
    }

    assert!(found_client1 && found_client2, "Did not find both clients");
}

// AI generated: generate the same test but now also add withdrawals of 1 after the deposits. The final result should be that balance is 0 for both clients.
#[test]
fn large_deposit_then_withdraw_should_end_at_zero() {
    // ---- Config ----
    let n = 50_000u32; // deposits per client
    let tmp_dir = tempdir().unwrap();
    let input_path = tmp_dir.path().join("large_in.csv");

    // ---- Generate CSV ----
    {
        let mut f = BufWriter::new(File::create(&input_path).unwrap());
        writeln!(f, "type,client,tx,amount").unwrap();

        // Deposits
        for i in 0..n {
            writeln!(f, "deposit,1,{},1.0", i + 1).unwrap();
            writeln!(f, "deposit,2,{},1.0", i + n + 1).unwrap();
        }

        // Withdrawals
        for i in 0..n {
            writeln!(f, "withdrawal,1,{},1.0", i + 1 + n * 2).unwrap();
            writeln!(f, "withdrawal,2,{},1.0", i + 1 + n * 3).unwrap();
        }
    }

    // ---- Run payment processor binary ----
    let output = Command::new(env!("CARGO_BIN_EXE_payments-engine"))
        .arg(&input_path)
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute processor");

    assert!(output.status.success(), "processor exited with error");

    let stdout = String::from_utf8(output.stdout).unwrap();

    // ---- Check both clients have zero available/total ----
    for line in stdout.lines() {
        if line.starts_with("1,") {
            assert!(
                line.contains("0.0"),
                "Client 1 should have 0 balance: {line}"
            );
        }
        if line.starts_with("2,") {
            assert!(
                line.contains("0.0"),
                "Client 2 should have 0 balance: {line}"
            );
        }
    }
}
