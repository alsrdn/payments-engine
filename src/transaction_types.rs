use std::fmt::Display;

use rust_decimal::{Decimal, prelude::Zero};
use serde::{Deserialize, Serialize, de::Error};

/// Transaction definition as specified in the CSV file.
#[derive(Debug, Deserialize)]
pub(crate) struct Transaction {
    /// Transaction type.
    #[serde(rename = "type")]
    transaction_type: TransactionType,
    /// Client Id.
    client: ClientId,
    /// Transaction id.
    tx: TransactionId,
    /// Amount which is only specified for deposits and withdrawals.
    amount: Option<Amount>,
}

impl Transaction {
    pub(crate) fn amount(&self) -> Option<Amount> {
        self.amount
    }

    pub(crate) fn client(&self) -> ClientId {
        self.client
    }

    pub(crate) fn transaction_type(&self) -> TransactionType {
        self.transaction_type
    }

    pub(crate) fn id(&self) -> TransactionId {
        self.tx
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Newtype that wraps a u16 for client id safety.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) struct ClientId(u16);

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for ClientId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

/// Newtype that wraps a u32 for transaction id safety.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct TransactionId(u32);

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for TransactionId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

/// Newtype to handle decimal ammounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Amount(Decimal);

/// Custom deserializer for Amount. Ensures that the amount is non-negative and rounded to 4 decimal places.
/// This ensures that all inputs to the system ar normalized so all values are correct by construction.
impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let decimal = rust_decimal::serde::str::deserialize(deserializer)?;
        if decimal.is_sign_negative() {
            return Err(D::Error::custom("amount cannot be negative"));
        }

        // round up to 4 decimal points.
        Ok(decimal
            .round_dp_with_strategy(4, rust_decimal::RoundingStrategy::ToZero)
            .into())
    }
}

/// Custom serializer so that the amount is normalized when outputed as a string. (e.g. 1.0 is displayed as 1)
impl Serialize for Amount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let normalized = self.0.normalize();

        rust_decimal::serde::str::serialize(&normalized, serializer)
    }
}

impl Amount {
    pub(crate) fn zero() -> Self {
        Self(Decimal::zero())
    }

    /// Add with overflow check.
    pub(crate) fn checked_add(self, other: Amount) -> Option<Amount> {
        self.0.checked_add(other.0).map(Amount)
    }

    /// Subtract with overflow check .We allow for negative amounts.
    pub(crate) fn checked_sub(self, other: Amount) -> Option<Amount> {
        //        if other.0 > self.0 {
        //            None
        //        } else {
        self.0.checked_sub(other.0).map(Amount)
        //        }
    }
}

impl From<Decimal> for Amount {
    fn from(value: Decimal) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Transaction {
        pub(crate) fn new(
            transaction_type: TransactionType,
            client: ClientId,
            tx: TransactionId,
            amount: Option<Amount>,
        ) -> Self {
            Transaction {
                transaction_type,
                client,
                tx,
                amount,
            }
        }
    }

    impl Amount {
        pub(crate) fn max() -> Self {
            Self(Decimal::MAX)
        }
    }

    impl From<f64> for Amount {
        fn from(value: f64) -> Self {
            let number = Decimal::from_f64_retain(value).unwrap();
            Self(number.round_dp(4))
        }
    }

    #[test]
    fn amount_add_overflow_not_allowed() {
        let a: Amount = Decimal::MAX.into();
        let b: Amount = 2.0.into();

        assert_eq!(a.checked_add(b), None);
    }

    #[test]
    fn amount_sub_allows_negative_values() {
        let zero = Amount::zero();
        let b: Amount = 2.5.into();

        assert_eq!(zero.checked_sub(b), Some((-2.5).into()));
    }

    #[test]
    fn amount_sub() {
        let a: Amount = 10.8.into();
        let b: Amount = 2.3.into();

        assert_eq!(a.checked_sub(b), Some(8.5.into()))
    }
}
