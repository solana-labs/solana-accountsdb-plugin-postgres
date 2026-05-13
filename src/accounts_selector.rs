use {log::*, solana_sdk::pubkey::Pubkey, std::collections::HashSet, std::str::FromStr};

#[derive(Debug)]
pub(crate) struct AccountsSelector {
    pub accounts: HashSet<Pubkey>,
    pub owners: HashSet<Pubkey>,
    pub select_all_accounts: bool,
}

impl Default for AccountsSelector {
    fn default() -> Self {
        AccountsSelector {
            accounts: HashSet::default(),
            owners: HashSet::default(),
            select_all_accounts: true,
        }
    }
}

impl AccountsSelector {
    pub fn new(accounts: &[String], owners: &[String]) -> Self {
        info!(
            "Creating AccountsSelector from accounts: {:?}, owners: {:?}",
            accounts, owners
        );

        let select_all_accounts = accounts.iter().any(|key| key == "*");
        if select_all_accounts {
            return AccountsSelector {
                accounts: HashSet::default(),
                owners: HashSet::default(),
                select_all_accounts,
            };
        }

        let accounts = accounts
            .iter()
            .map(|key| Pubkey::from_str(key).unwrap_or_else(|_| panic!("Invalid account pubkey: {}", key)))
            .collect();

        let owners = owners
            .iter()
            .map(|key| Pubkey::from_str(key).unwrap_or_else(|_| panic!("Invalid owner pubkey: {}", key)))
            .collect();

        AccountsSelector {
            accounts,
            owners,
            select_all_accounts,
        }
    }

    pub fn is_account_selected(&self, account: &Pubkey, owner: &Pubkey) -> bool {
        self.select_all_accounts || self.accounts.contains(account) || self.owners.contains(owner)
    }

    /// Check if any account is of interest at all
    pub fn is_enabled(&self) -> bool {
        self.select_all_accounts || !self.accounts.is_empty() || !self.owners.is_empty()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_create_accounts_selector() {
        AccountsSelector::new(
            &["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string()],
            &[],
        );

        AccountsSelector::new(
            &[],
            &["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string()],
        );
    }
}