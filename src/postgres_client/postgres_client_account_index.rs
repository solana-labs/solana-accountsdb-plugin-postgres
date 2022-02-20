use {
    super::{
        DbAccountInfo, ReadableAccountInfo, SimplePostgresClient,
        DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE,
    },
    crate::{
        accountsdb_plugin_postgres::{
            AccountsDbPluginPostgresConfig, AccountsDbPluginPostgresError,
        },
        inline_spl_token::{self, GenericTokenAccount},
        inline_spl_token_2022,
    },
    log::*,
    postgres::{Client, Statement},
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::AccountsDbPluginError,
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::pubkey::Pubkey,
    tokio_postgres::types,
};

const TOKEN_IDX_COLUMN_COUNT: usize = 2;
/// Model the secondary index for token owner and mint
pub struct TokenSecondaryIndex {
    owner: Vec<u8>,
    inner_key: Vec<u8>,
}

impl SimplePostgresClient {
    pub fn build_bulk_token_owner_index_insert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        let mut stmt =
            String::from("INSERT INTO spl_token_owner_index AS idx (owner_key, inner_key) VALUES");
        for j in 0..batch_size {
            let row = j * TOKEN_IDX_COLUMN_COUNT;
            let val_str = format!("(${}, ${})", row + 1, row + 2,);

            if j == 0 {
                stmt = format!("{} {}", &stmt, val_str);
            } else {
                stmt = format!("{}, {}", &stmt, val_str);
            }
        }

        let handle_conflict = "ON CONFLICT DO NOTHING";

        stmt = format!("{} {}", stmt, handle_conflict);

        info!("{}", stmt);
        let bulk_stmt = client.prepare(&stmt);

        match bulk_stmt {
            Err(err) => {
                return Err(AccountsDbPluginError::Custom(Box::new(AccountsDbPluginPostgresError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the token owner index update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                        err, config.host, config.user, config
                    ),
                })));
            }
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    pub fn build_bulk_token_mint_index_insert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        let mut stmt =
            String::from("INSERT INTO spl_token_mint_index AS idx (mint_key, inner_key) VALUES");
        for j in 0..batch_size {
            let row = j * TOKEN_IDX_COLUMN_COUNT;
            let val_str = format!("(${}, ${})", row + 1, row + 2,);

            if j == 0 {
                stmt = format!("{} {}", &stmt, val_str);
            } else {
                stmt = format!("{}, {}", &stmt, val_str);
            }
        }

        let handle_conflict = "ON CONFLICT DO NOTHING";

        stmt = format!("{} {}", stmt, handle_conflict);

        info!("{}", stmt);
        let bulk_stmt = client.prepare(&stmt);

        match bulk_stmt {
            Err(err) => {
                return Err(AccountsDbPluginError::Custom(Box::new(AccountsDbPluginPostgresError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the token mint index update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                        err, config.host, config.user, config
                    ),
                })));
            }
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    pub fn bulk_insert_token_owner_idx(&mut self) -> Result<(), AccountsDbPluginError> {
        if self.pending_token_owner_idx.len() == self.batch_size {
            let mut measure = Measure::start("accountsdb-plugin-postgres-prepare-owner-idx-values");

            let mut values: Vec<&(dyn types::ToSql + Sync)> =
                Vec::with_capacity(self.batch_size * TOKEN_IDX_COLUMN_COUNT);
            for j in 0..self.batch_size {
                let index = &self.pending_token_owner_idx[j];
                values.push(&index.owner);
                values.push(&index.inner_key);
            }
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-prepare-owner-idx-values-us",
                measure.as_us() as usize,
                10000,
                10000
            );

            let mut measure = Measure::start("accountsdb-plugin-postgres-update-owner-idx-account");
            let client = self.client.get_mut().unwrap();
            let result = client.client.query(
                client.bulk_insert_token_owner_idx_stmt.as_ref().unwrap(),
                &values,
            );

            self.pending_token_owner_idx.clear();

            if let Err(err) = result {
                let msg = format!(
                    "Failed to persist the update of account to the PostgreSQL database. Error: {:?}",
                    err
                );
                error!("{}", msg);
                return Err(AccountsDbPluginError::AccountsUpdateError { msg });
            }

            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-update-owner-idx-us",
                measure.as_us() as usize,
                10000,
                10000
            );
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-update-owner-idx-count",
                self.batch_size,
                10000,
                10000
            );
        }
        Ok(())
    }

    pub fn bulk_insert_token_mint_idx(&mut self) -> Result<(), AccountsDbPluginError> {
        if self.pending_token_owner_idx.len() == self.batch_size {
            let mut measure = Measure::start("accountsdb-plugin-postgres-prepare-mint-idx-values");

            let mut values: Vec<&(dyn types::ToSql + Sync)> =
                Vec::with_capacity(self.batch_size * TOKEN_IDX_COLUMN_COUNT);
            for j in 0..self.batch_size {
                let index = &self.pending_token_mint_idx[j];
                values.push(&index.owner);
                values.push(&index.inner_key);
            }
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-prepare-mint-idx-values-us",
                measure.as_us() as usize,
                10000,
                10000
            );

            let mut measure = Measure::start("accountsdb-plugin-postgres-update-mint-idx-account");
            let client = self.client.get_mut().unwrap();
            let result = client.client.query(
                client.bulk_insert_token_mint_idx_stmt.as_ref().unwrap(),
                &values,
            );

            self.pending_token_mint_idx.clear();

            if let Err(err) = result {
                let msg = format!(
                    "Failed to persist the update of account to the PostgreSQL database. Error: {:?}",
                    err
                );
                error!("{}", msg);
                return Err(AccountsDbPluginError::AccountsUpdateError { msg });
            }

            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-update-mint-idx-us",
                measure.as_us() as usize,
                10000,
                10000
            );
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-update-mint-idx-count",
                self.batch_size,
                10000,
                10000
            );
        }
        Ok(())
    }

    fn queue_token_owner_idx_gen<G: GenericTokenAccount>(
        &mut self,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) {
        if account.owner() == token_id.to_bytes() {
            if let Some(owner_key) = G::unpack_account_owner(account.data()) {
                let owner_key = owner_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                self.pending_token_owner_idx.push(TokenSecondaryIndex {
                    owner: owner_key,
                    inner_key: pubkey.to_vec(),
                });
            }
        }
    }

    fn queue_token_mint_idx_gen<G: GenericTokenAccount>(
        &mut self,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) {
        if account.owner() == token_id.to_bytes() {
            if let Some(mint_key) = G::unpack_account_mint(account.data()) {
                let mint_key = mint_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                self.pending_token_mint_idx.push(TokenSecondaryIndex {
                    owner: mint_key,
                    inner_key: pubkey.to_vec(),
                })
            }
        }
    }

    /// Queue bulk insert secondary indexes
    pub fn queue_secondary_indexes(&mut self, account: &DbAccountInfo) {
        if self.index_token_owner {
            self.queue_token_owner_idx_gen::<inline_spl_token::Account>(
                &inline_spl_token::id(),
                account,
            );
            self.queue_token_owner_idx_gen::<inline_spl_token_2022::Account>(
                &inline_spl_token_2022::id(),
                account,
            );
        }

        if self.index_token_mint {
            self.queue_token_mint_idx_gen::<inline_spl_token::Account>(
                &inline_spl_token::id(),
                account,
            );
            self.queue_token_mint_idx_gen::<inline_spl_token_2022::Account>(
                &inline_spl_token_2022::id(),
                account,
            );
        }
    }

    fn update_token_owner_idx_gen<G: GenericTokenAccount>(
        client: &mut Client,
        statement: &Statement,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        if account.owner() == token_id.to_bytes() {
            if let Some(owner_key) = G::unpack_account_owner(account.data()) {
                let owner_key = owner_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                let result = client.execute(statement, &[&owner_key, &pubkey]);
                if let Err(err) = result {
                    let msg = format!(
                        "Failed to update the token owner index to the PostgreSQL database. Error: {:?}",
                        err
                    );
                    error!("{}", msg);
                    return Err(AccountsDbPluginError::AccountsUpdateError { msg });
                }
            }
        }

        Ok(())
    }

    fn update_token_mint_idx_gen<G: GenericTokenAccount>(
        client: &mut Client,
        statement: &Statement,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        if account.owner() == token_id.to_bytes() {
            if let Some(mint_key) = G::unpack_account_mint(account.data()) {
                let mint_key = mint_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                let result = client.execute(statement, &[&mint_key, &pubkey]);
                if let Err(err) = result {
                    let msg = format!(
                        "Failed to update the token mint index to the PostgreSQL database. Error: {:?}",
                        err
                    );
                    error!("{}", msg);
                    return Err(AccountsDbPluginError::AccountsUpdateError { msg });
                }
            }
        }

        Ok(())
    }

    pub fn update_token_owner_idx(
        client: &mut Client,
        statement: &Statement,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        Self::update_token_owner_idx_gen::<inline_spl_token::Account>(
            client,
            statement,
            &inline_spl_token::id(),
            account,
        )?;

        Self::update_token_owner_idx_gen::<inline_spl_token_2022::Account>(
            client,
            statement,
            &inline_spl_token_2022::id(),
            account,
        )
    }

    pub fn update_token_mint_idx(
        client: &mut Client,
        statement: &Statement,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        Self::update_token_mint_idx_gen::<inline_spl_token::Account>(
            client,
            statement,
            &inline_spl_token::id(),
            account,
        )?;

        Self::update_token_mint_idx_gen::<inline_spl_token_2022::Account>(
            client,
            statement,
            &inline_spl_token_2022::id(),
            account,
        )
    }
}
