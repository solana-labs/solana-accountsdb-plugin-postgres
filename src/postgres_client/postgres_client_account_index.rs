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

const TOKEN_INDEX_COLUMN_COUNT: usize = 3;
/// Model the secondary index for token owner and mint
pub struct TokenSecondaryIndex {
    owner: Vec<u8>,
    inner_key: Vec<u8>,
    slot: i64,
}

impl SimplePostgresClient {
    pub fn build_single_token_owner_index_upsert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let stmt = "INSERT INTO spl_token_owner_index AS owner_index (owner_key, inner_key, slot) \
        VALUES ($1, $2, $3) ON CONFLICT (owner_key, inner_key) DO UPDATE SET slot=excluded.slot where owner_index.slot < excluded.slot";

        Self::prepare_query_statement(client, config, stmt)
    }

    pub fn build_single_token_mint_index_upsert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let stmt = "INSERT INTO spl_token_mint_index AS mint_index (mint_key, inner_key, slot) \
        VALUES ($1, $2, $3) ON CONFLICT (mint_key, inner_key) DO UPDATE SET slot=excluded.slot where mint_index.slot < excluded.slot";

        Self::prepare_query_statement(client, config, stmt)
    }

    /// Build the token owner index bulk insert statement
    pub fn build_bulk_token_owner_index_insert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        let mut stmt = String::from(
            "INSERT INTO spl_token_owner_index AS index (owner_key, inner_key, slot) VALUES",
        );
        for j in 0..batch_size {
            let row = j * TOKEN_INDEX_COLUMN_COUNT;
            let val_str = format!("(${}, ${}, ${})", row + 1, row + 2, row + 3);

            if j == 0 {
                stmt = format!("{} {}", &stmt, val_str);
            } else {
                stmt = format!("{}, {}", &stmt, val_str);
            }
        }

        let handle_conflict =
            "ON CONFLICT (owner_key, inner_key) DO UPDATE SET slot=excluded.slot where index.slot < excluded.slot";

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

    /// Build the token mint index bulk insert statement.
    pub fn build_bulk_token_mint_index_insert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        let mut stmt = String::from(
            "INSERT INTO spl_token_mint_index AS index (mint_key, inner_key, slot) VALUES",
        );
        for j in 0..batch_size {
            let row = j * TOKEN_INDEX_COLUMN_COUNT;
            let val_str = format!("(${}, ${}, ${})", row + 1, row + 2, row + 3);

            if j == 0 {
                stmt = format!("{} {}", &stmt, val_str);
            } else {
                stmt = format!("{}, {}", &stmt, val_str);
            }
        }

        let handle_conflict =
            "ON CONFLICT (mint_key, inner_key) DO UPDATE SET slot=excluded.slot where index.slot < excluded.slot";

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

    /// Execute the token owner bulk insert query.
    pub fn bulk_insert_token_owner_index(&mut self) -> Result<(), AccountsDbPluginError> {
        if self.pending_token_owner_index.len() == self.batch_size {
            let mut measure =
                Measure::start("accountsdb-plugin-postgres-prepare-owner-index-values");

            let mut values: Vec<&(dyn types::ToSql + Sync)> =
                Vec::with_capacity(self.batch_size * TOKEN_INDEX_COLUMN_COUNT);
            for j in 0..self.batch_size {
                let index = &self.pending_token_owner_index[j];
                values.push(&index.owner);
                values.push(&index.inner_key);
                values.push(&index.slot);
            }
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-prepare-owner-index-values-us",
                measure.as_us() as usize,
                10000,
                10000
            );

            let mut measure =
                Measure::start("accountsdb-plugin-postgres-update-owner-index-account");
            let client = self.client.get_mut().unwrap();
            let result = client.client.query(
                client.bulk_insert_token_owner_index_stmt.as_ref().unwrap(),
                &values,
            );

            self.pending_token_owner_index.clear();

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
                "accountsdb-plugin-postgres-update-owner-index-us",
                measure.as_us() as usize,
                10000,
                10000
            );
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-update-owner-index-count",
                self.batch_size,
                10000,
                10000
            );
        }
        Ok(())
    }

    /// Execute the token mint index bulk insert query.
    pub fn bulk_insert_token_mint_index(&mut self) -> Result<(), AccountsDbPluginError> {
        if self.pending_token_mint_index.len() == self.batch_size {
            let mut measure =
                Measure::start("accountsdb-plugin-postgres-prepare-mint-index-values");

            let mut values: Vec<&(dyn types::ToSql + Sync)> =
                Vec::with_capacity(self.batch_size * TOKEN_INDEX_COLUMN_COUNT);
            for j in 0..self.batch_size {
                let index = &self.pending_token_mint_index[j];
                values.push(&index.owner);
                values.push(&index.inner_key);
                values.push(&index.slot);
            }
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-prepare-mint-index-values-us",
                measure.as_us() as usize,
                10000,
                10000
            );

            let mut measure =
                Measure::start("accountsdb-plugin-postgres-update-mint-index-account");
            let client = self.client.get_mut().unwrap();
            let result = client.client.query(
                client.bulk_insert_token_mint_index_stmt.as_ref().unwrap(),
                &values,
            );

            self.pending_token_mint_index.clear();

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
                "accountsdb-plugin-postgres-update-mint-index-us",
                measure.as_us() as usize,
                10000,
                10000
            );
            inc_new_counter_debug!(
                "accountsdb-plugin-postgres-update-mint-index-count",
                self.batch_size,
                10000,
                10000
            );
        }
        Ok(())
    }

    /// Generic function to queue the token owner index for bulk insert.
    fn queue_token_owner_index_gen<G: GenericTokenAccount>(
        &mut self,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) {
        if account.owner() == token_id.to_bytes() {
            if let Some(owner_key) = G::unpack_account_owner(account.data()) {
                let owner_key = owner_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                self.pending_token_owner_index.push(TokenSecondaryIndex {
                    owner: owner_key,
                    inner_key: pubkey.to_vec(),
                    slot: account.slot,
                });
            }
        }
    }

    /// Generic function to queue the token mint index for bulk insert.
    fn queue_token_mint_index_gen<G: GenericTokenAccount>(
        &mut self,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) {
        if account.owner() == token_id.to_bytes() {
            if let Some(mint_key) = G::unpack_account_mint(account.data()) {
                let mint_key = mint_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                self.pending_token_mint_index.push(TokenSecondaryIndex {
                    owner: mint_key,
                    inner_key: pubkey.to_vec(),
                    slot: account.slot,
                })
            }
        }
    }

    /// Queue bulk insert secondary indexes: token owner and token mint indexes.
    pub fn queue_secondary_indexes(&mut self, account: &DbAccountInfo) {
        if self.index_token_owner {
            self.queue_token_owner_index_gen::<inline_spl_token::Account>(
                &inline_spl_token::id(),
                account,
            );
            self.queue_token_owner_index_gen::<inline_spl_token_2022::Account>(
                &inline_spl_token_2022::id(),
                account,
            );
        }

        if self.index_token_mint {
            self.queue_token_mint_index_gen::<inline_spl_token::Account>(
                &inline_spl_token::id(),
                account,
            );
            self.queue_token_mint_index_gen::<inline_spl_token_2022::Account>(
                &inline_spl_token_2022::id(),
                account,
            );
        }
    }

    /// Generic function to update a single token owner index.
    fn update_token_owner_index_gen<G: GenericTokenAccount>(
        client: &mut Client,
        statement: &Statement,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        if account.owner() == token_id.to_bytes() {
            if let Some(owner_key) = G::unpack_account_owner(account.data()) {
                let owner_key = owner_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                let slot = account.slot;
                let result = client.execute(statement, &[&owner_key, &pubkey, &slot]);
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

    /// Generic function to update a single token mint index.
    fn update_token_mint_index_gen<G: GenericTokenAccount>(
        client: &mut Client,
        statement: &Statement,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        if account.owner() == token_id.to_bytes() {
            if let Some(mint_key) = G::unpack_account_mint(account.data()) {
                let mint_key = mint_key.to_bytes().to_vec();
                let pubkey = account.pubkey();
                let slot = account.slot;
                let result = client.execute(statement, &[&mint_key, &pubkey, &slot]);
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

    /// Function for updating a single token owner index.
    pub fn update_token_owner_index(
        client: &mut Client,
        statement: &Statement,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        Self::update_token_owner_index_gen::<inline_spl_token::Account>(
            client,
            statement,
            &inline_spl_token::id(),
            account,
        )?;

        Self::update_token_owner_index_gen::<inline_spl_token_2022::Account>(
            client,
            statement,
            &inline_spl_token_2022::id(),
            account,
        )
    }

    /// Function for updating a single token mint index.
    pub fn update_token_mint_index(
        client: &mut Client,
        statement: &Statement,
        account: &DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        Self::update_token_mint_index_gen::<inline_spl_token::Account>(
            client,
            statement,
            &inline_spl_token::id(),
            account,
        )?;

        Self::update_token_mint_index_gen::<inline_spl_token_2022::Account>(
            client,
            statement,
            &inline_spl_token_2022::id(),
            account,
        )
    }

    /// Clean up the buffered indexes -- we do not need to
    /// write them to disk individually as they have already been handled
    /// when the accounts were flushed out individually in `upsert_account_internal`.
    pub fn clear_buffered_indexes(&mut self) {
        self.pending_token_owner_index.clear();
        self.pending_token_mint_index.clear();
    }
}
