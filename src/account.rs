use serde::{Deserialize, Serialize, ser::SerializeStruct};

use payments_engine::transactions_cache::{self, SqliteKvStore, TransactionCache};

use crate::transaction_types::{Amount, ClientId, TransactionId};
use thiserror::Error;

// A error describing why the account operation failed.
#[derive(Error, Debug)]
pub(crate) enum AccountError {
    #[error("Account is locked. No transaction can be performed.")]
    AccountLocked,
    #[error("Account has insufficient funds to satisfy this transaction.")]
    InsufficientFunds,
    #[error("Cannot deposit because the limit was reached.")]
    DepositLimitReached,
    #[error("There is no transaction matching this id.")]
    TransactionMissing,
    #[error("This transaction can no longer be disputed.")]
    TransactionCannotBeDisputed,
    #[error("Withdrawal dispute is not implemented yet.")]
    WithdrawalDisputeNotSupported,
    #[error("Transaction is not disputed.")]
    TransactionNotDisputed,
    #[error("Dispute was already resolved.")]
    DisputeAlreadyResolved,
    #[error("Dispute was already resolved through chargeback.")]
    TransactionWasChargedBack,
    #[error("This transaction already exists.")]
    DuplicateTransaction,
    #[error("Specified ammount is invalid.")]
    InvalidAmount,
    #[error("Transaction cache error: {0}")]
    TransactionCache(#[from] transactions_cache::CacheError),
}

// Transaction dispute state.
#[derive(Debug, Serialize, Deserialize)]
enum DisputeState {
    // This transaction was never disputed.
    None,
    // There was a dispute initiated for this transaction.
    DisputeInitiated,
    // The dispute was resolved in favor of the merchant.
    DisputeResolved,
    // The dispute was resolved through a charge-back.
    ChargedBack,
}

// The type of processed transaction.
#[derive(Debug, Serialize, Deserialize)]
enum FundingType {
    Deposit,
    Withdrawal,
}

// An already processed transaction.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FundingLogEntry {
    funding_type: FundingType,
    amount: Amount,
    state: DisputeState,
}

impl FundingLogEntry {
    pub(crate) fn new_deposit(amount: Amount) -> Self {
        Self {
            funding_type: FundingType::Deposit,
            amount,
            state: DisputeState::None,
        }
    }

    fn new_withdrawal(amount: Amount) -> Self {
        Self {
            funding_type: FundingType::Withdrawal,
            amount,
            state: DisputeState::None,
        }
    }

    pub(crate) fn amount(&self) -> Amount {
        self.amount
    }

    // A transaction can be disputed only if it was not already disputed before.
    fn can_be_disputed(&self) -> bool {
        match self.state {
            DisputeState::None => true,
            DisputeState::DisputeResolved
            | DisputeState::DisputeInitiated
            | DisputeState::ChargedBack => false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Account {
    client_id: ClientId,
    /// The total funds that are held for dispute. This should be equal to total - available amounts
    held: Amount,
    /// The total funds that are available or held. This should be equal to available + held
    total: Amount,
    /// Whether the account is locked. An account is locked if a charge back occurs
    locked: bool,
    /// A log of transactions that were processed for this account.
    transactions: TransactionCache<SqliteKvStore, TransactionId, FundingLogEntry, 128>, //HashMap<TransactionId, FundingLogEntry>,
}

// Custom serializer for the Account structure to be written to CSV.
// Mainly needed because we don't store the available field which is calculated on the fly.
// We also skip serializing the transaction log.
impl Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut account = serializer.serialize_struct("Account", 5)?;
        account.serialize_field("client", &self.client_id)?;
        account.serialize_field("available", &self.available())?;
        account.serialize_field("held", &self.held)?;
        account.serialize_field("total", &self.total)?;
        account.serialize_field("locked", &self.locked)?;
        account.end()
    }
}

impl Account {
    pub(crate) fn new(client_id: ClientId) -> Result<Self, AccountError> {
        Ok(Self {
            client_id,
            held: Amount::zero(),
            total: Amount::zero(),
            locked: false,
            transactions: TransactionCache::new()?,
        })
    }

    pub(crate) fn client(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn lock(&mut self) {
        self.locked = true;
    }

    /// The total funds that are available for trading, staking, withdrawal, etc.
    /// This should be equal to the total - held amounts
    pub(crate) fn available(&self) -> Amount {
        self.total
            .checked_sub(self.held)
            .expect("Programmer error.")
    }

    /// Deposit funds to the account.
    pub(crate) fn deposit(
        &mut self,
        amount: Amount,
        transaction_id: TransactionId,
    ) -> Result<(), AccountError> {
        // Don't allow deposits to locked accounts.
        if self.locked {
            return Err(AccountError::AccountLocked);
        }

        // Don't re-play the same transaction twice.
        if self.transactions.contains_key(&transaction_id)? {
            return Err(AccountError::DuplicateTransaction);
        }

        // Zero amount deposits are just spam. Don't allow them.
        if amount == Amount::zero() {
            return Err(AccountError::InvalidAmount);
        }

        // Increase the total ammount and store the tx.
        self.total = self
            .total
            .checked_add(amount)
            .ok_or(AccountError::DepositLimitReached)?;
        self.transactions
            .put(transaction_id, FundingLogEntry::new_deposit(amount))?;

        Ok(())
    }

    /// Withdraw funds from the account.
    pub(crate) fn withdraw(
        &mut self,
        amount: Amount,
        transaction_id: TransactionId,
    ) -> Result<(), AccountError> {
        if self.locked {
            return Err(AccountError::AccountLocked);
        }

        if self.transactions.contains_key(&transaction_id)? {
            return Err(AccountError::DuplicateTransaction);
        }

        // Check that there's enough balance for a withdrawal to take place.
        if self.available() < amount {
            return Err(AccountError::InsufficientFunds);
        }

        if amount == Amount::zero() {
            return Err(AccountError::InvalidAmount);
        }

        self.total = self
            .total
            .checked_sub(amount)
            .ok_or(AccountError::InsufficientFunds)?;
        self.transactions
            .put(transaction_id, FundingLogEntry::new_withdrawal(amount))?;

        Ok(())
    }

    /// Dispute a previous deposit.
    pub(crate) fn dispute(&mut self, transaction_id: TransactionId) -> Result<(), AccountError> {
        if self.locked {
            return Err(AccountError::AccountLocked);
        }

        // Check if the referenced transaction exists.
        let transaction = self
            .transactions
            .get_mut(&transaction_id)?
            .ok_or(AccountError::TransactionMissing)?;
        let amount = transaction.amount();

        // Only dispute if it was not disputed before.
        if transaction.can_be_disputed() {
            match transaction.funding_type {
                FundingType::Deposit => {
                    self.held = self
                        .held
                        .checked_add(amount)
                        .expect("Programmer error. Held amount should not exceed total, and there is a deposit limit on total.");
                    transaction.state = DisputeState::DisputeInitiated;
                }
                // We don't allow disputes for withdrawals. From what I can reasearch it's in line with what other processors like Stripe or Paypal do.
                // There may be situations where it makes sense to dispute a withdrawal but not supporting in for now.
                FundingType::Withdrawal => return Err(AccountError::WithdrawalDisputeNotSupported),
            }
            Ok(())
        } else {
            Err(AccountError::TransactionCannotBeDisputed)
        }
    }

    // A dispute resolution in favor of the merchant.
    pub(crate) fn resolve_dispute(
        &mut self,
        transaction_id: TransactionId,
    ) -> Result<(), AccountError> {
        if self.locked {
            return Err(AccountError::AccountLocked);
        }

        // Check if the referenced transaction exists.
        let transaction = self
            .transactions
            .get_mut(&transaction_id)?
            .ok_or(AccountError::TransactionMissing)?;

        // Check the correct state transition. Only allow resolution if dispute was started.
        match transaction.state {
            DisputeState::None => Err(AccountError::TransactionNotDisputed),
            DisputeState::DisputeInitiated => {
                self.held = self
                    .held
                    .checked_sub(transaction.amount())
                    .expect("Programmer error.");
                transaction.state = DisputeState::DisputeResolved;
                Ok(())
            }
            DisputeState::DisputeResolved => Err(AccountError::DisputeAlreadyResolved),
            DisputeState::ChargedBack => Err(AccountError::TransactionWasChargedBack),
        }
    }

    // A dispute resolution in favor of the client.
    pub(crate) fn chargeback(&mut self, transaction_id: TransactionId) -> Result<(), AccountError> {
        if self.locked {
            return Err(AccountError::AccountLocked);
        }

        let transaction = self
            .transactions
            .get_mut(&transaction_id)?
            .ok_or(AccountError::TransactionMissing)?;
        let amount = transaction.amount();

        match transaction.state {
            DisputeState::None => Err(AccountError::TransactionNotDisputed),
            DisputeState::DisputeInitiated => {
                self.held = self.held.checked_sub(amount).unwrap();
                self.total = self.total.checked_sub(amount).unwrap();
                transaction.state = DisputeState::ChargedBack;
                self.lock();
                Ok(())
            }
            DisputeState::DisputeResolved => Err(AccountError::DisputeAlreadyResolved),
            DisputeState::ChargedBack => Err(AccountError::TransactionWasChargedBack),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Account {
        fn new_with_funds(client_id: ClientId, initial_amount: Amount) -> Self {
            let mut account = Self::new(client_id).unwrap();
            account.total = initial_amount;

            account
        }
    }

    #[test]
    fn should_deposit_successfully() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(1.0.into(), 1.into()).is_ok());
        assert_eq!(account.total, 1.0.into());
        assert_eq!(account.available(), 1.0.into());
        assert_eq!(account.held, 0.0.into());
    }

    #[test]
    fn should_deposit_multiple_times_successfully() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(1.0.into(), 1.into()).is_ok());
        assert!(account.deposit(2.0.into(), 3.into()).is_ok());

        assert_eq!(account.total, 3.0.into());
        assert_eq!(account.available(), 3.0.into());
        assert_eq!(account.held, 0.0.into());
    }

    #[test]
    fn should_not_allow_deposit_with_duplicate_ids() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(matches!(
            account.deposit(200.0.into(), 1.into()),
            Err(AccountError::DuplicateTransaction)
        ));

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, 0.0.into());
    }

    #[test]
    fn should_withdraw_successfully() {
        let mut account = Account::new_with_funds(1u16.into(), 10.55.into());

        assert!(account.withdraw(5.0.into(), 1.into()).is_ok());
        assert_eq!(account.available(), 5.55.into());
    }

    #[test]
    fn should_withdraw_multiple_times_successfully() {
        let mut account = Account::new_with_funds(1u16.into(), 10.55.into());

        assert!(account.withdraw(5.0.into(), 1.into()).is_ok());
        assert_eq!(account.available(), 5.55.into());
        assert!(account.withdraw(3.55.into(), 2.into()).is_ok());
        assert_eq!(account.available(), 2.0.into());
        assert_eq!(account.total, 2.0.into());
    }

    #[test]
    fn should_not_deposit_when_locked() {
        let mut account = Account::new(1u16.into()).unwrap();
        account.lock();

        assert!(matches!(
            account.deposit(1.0.into(), 1.into()),
            Err(AccountError::AccountLocked)
        ));

        assert_eq!(account.available(), 0.0.into());
        assert_eq!(account.total, 0.0.into());
    }

    #[test]
    fn should_not_deposit_zero_amount() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(matches!(
            account.deposit(Amount::zero(), 1.into()),
            Err(AccountError::InvalidAmount)
        ));
    }

    #[test]
    fn should_not_allow_deposits_beyond_limits() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(Amount::max(), 1.into()).is_ok());

        assert!(matches!(
            account.deposit(1.0.into(), 2.into()),
            Err(AccountError::DepositLimitReached)
        ));
    }

    #[test]
    fn should_not_withdraw_when_locked() {
        let mut account = Account::new(1u16.into()).unwrap();
        account.lock();

        assert!(matches!(
            account.withdraw(1.0.into(), 1.into()),
            Err(AccountError::AccountLocked)
        ));

        assert_eq!(account.available(), 0.0.into());
        assert_eq!(account.total, 0.0.into());
    }

    #[test]
    fn should_not_allow_withdrawal_with_duplicate_ids() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());

        assert!(matches!(
            account.withdraw(100.0.into(), 1.into()),
            Err(AccountError::DuplicateTransaction)
        ));

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, 0.0.into());
    }

    #[test]
    fn should_not_withdraw_because_insufficient_funds() {
        let mut account = Account::new_with_funds(1u16.into(), 10.55.into());

        assert!(matches!(
            account.withdraw(20.0.into(), 1.into()),
            Err(AccountError::InsufficientFunds)
        ));
        assert_eq!(account.available(), 10.55.into());
        assert_eq!(account.total, 10.55.into());
    }

    #[test]
    fn should_not_withdraw_zero_amounts() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(matches!(
            account.withdraw(Amount::zero(), 1.into()),
            Err(AccountError::InvalidAmount)
        ));
    }

    #[test]
    fn should_deposit_and_withdraw() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(1.0.into(), 1.into()).is_ok());
        assert!(account.deposit(2.0.into(), 3.into()).is_ok());

        assert!(account.withdraw(1.5.into(), 4.into()).is_ok());

        assert_eq!(account.total, 1.5.into());
        assert_eq!(account.available(), 1.5.into());
        assert_eq!(account.held, 0.0.into());
    }

    #[test]
    fn should_hold_disputed_amount() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.dispute(1.into()).is_ok());

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), Amount::zero());
        assert_eq!(account.held, 100.0.into());
        assert!(!account.locked)
    }

    #[test]
    fn should_increase_hold_on_multiple_disputes() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());
        assert!(account.deposit(300.0.into(), 3.into()).is_ok());

        assert!(account.dispute(1.into()).is_ok());
        assert!(account.dispute(3.into()).is_ok());

        assert_eq!(account.total, 600.0.into());
        assert_eq!(account.available(), 200.0.into());
        assert_eq!(account.held, 400.0.into());
        assert!(!account.locked)
    }

    #[test]
    fn should_not_dispute_missing_transaction() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(matches!(
            account.dispute(2.into()),
            Err(AccountError::TransactionMissing)
        ));

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, Amount::zero());
        assert!(!account.locked)
    }

    #[test]
    fn should_release_hold_on_resolve_dispute() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.dispute(1.into()).is_ok());
        assert!(account.resolve_dispute(1.into()).is_ok());

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, Amount::zero());
        assert!(!account.locked)
    }

    #[test]
    fn should_support_partial_resolutions() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());
        assert!(account.deposit(300.0.into(), 3.into()).is_ok());

        assert!(account.dispute(1.into()).is_ok());
        assert!(account.dispute(3.into()).is_ok());

        assert!(account.resolve_dispute(1.into()).is_ok());

        assert_eq!(account.total, 600.0.into());
        assert_eq!(account.available(), 300.0.into());
        assert_eq!(account.held, 300.0.into());
        assert!(!account.locked)
    }

    #[test]
    fn should_not_resolve_dispute_without_prior_dispute() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(matches!(
            account.resolve_dispute(1.into()),
            Err(AccountError::TransactionNotDisputed)
        ));

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, Amount::zero());
        assert!(!account.locked)
    }

    #[test]
    fn should_create_negative_available_balance_on_dispute_with_insufficient_funds() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());
        assert!(account.withdraw(300.0.into(), 4.into()).is_ok());

        assert!(account.dispute(1.into()).is_ok());
        assert!(account.dispute(2.into()).is_ok());

        assert_eq!(account.total, Amount::zero());
        assert_eq!(account.available(), (-300.0).into());
        assert_eq!(account.held, 300.0.into());
    }

    #[test]
    fn should_settle_insufficient_funds_balance_on_dispute_resolution() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());
        assert!(account.withdraw(300.0.into(), 4.into()).is_ok());

        assert!(account.dispute(1.into()).is_ok());
        assert!(account.dispute(2.into()).is_ok());

        assert!(account.resolve_dispute(1.into()).is_ok());
        assert!(account.resolve_dispute(2.into()).is_ok());

        assert_eq!(account.total, Amount::zero());
        assert_eq!(account.available(), Amount::zero());
        assert_eq!(account.held, Amount::zero());
        assert!(!account.locked)
    }

    #[test]
    fn should_have_nagative_total_balance_on_chargeback_with_insufficient_funds() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());
        assert!(account.withdraw(300.0.into(), 4.into()).is_ok());

        assert!(account.dispute(1.into()).is_ok());
        assert!(account.dispute(2.into()).is_ok());

        assert!(account.resolve_dispute(1.into()).is_ok());
        assert!(account.chargeback(2.into()).is_ok());

        assert_eq!(account.total, (-200.0).into());
        assert_eq!(account.available(), (-200.0).into());
        assert_eq!(account.held, Amount::zero());
        assert!(account.locked)
    }

    #[test]
    fn should_decrease_amounts_on_chargeback() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.dispute(1.into()).is_ok());
        assert!(account.chargeback(1.into()).is_ok());

        assert_eq!(account.total, Amount::zero());
        assert_eq!(account.available(), Amount::zero());
        assert_eq!(account.held, Amount::zero());
        assert!(account.locked)
    }

    #[test]
    fn should_not_charge_back_without_prior_dispute() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(matches!(
            account.chargeback(1.into()),
            Err(AccountError::TransactionNotDisputed)
        ));

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, Amount::zero());
        assert!(!account.locked)
    }

    #[test]
    fn should_not_allow_withdrawal_of_held_funds() {
        let mut account = Account::new(1u16.into()).unwrap();

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());

        assert!(account.dispute(2.into()).is_ok());

        assert!(matches!(
            account.withdraw(200.0.into(), 3.into()),
            Err(AccountError::InsufficientFunds)
        ));

        assert_eq!(account.total, 300.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, 200.0.into());
        assert!(!account.locked)
    }

    /*
    #[test]
    fn should_create_negative_balance_on_withdrawal_disputes() {
        let mut account = Account::new(1u16.into());

        assert!(account.deposit(100.0.into(), 1.into()).is_ok());
        assert!(account.deposit(200.0.into(), 2.into()).is_ok());

        assert!(account.withdraw(200.0.into(), 3.into()).is_ok());

        assert_eq!(account.total, 100.0.into());
        assert_eq!(account.available(), 100.0.into());
        assert_eq!(account.held, Amount::zero());

        assert!(account.dispute(3.into()).is_ok());
    }
    */
}
