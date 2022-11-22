use {
    super::{
        DbAccountInfo, ReadableAccountInfo, SimplePostgresClient,
        DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE,
    },
    crate::{
        geyser_plugin_postgres::{GeyserPluginPostgresConfig, GeyserPluginPostgresError},
        inline_spl_token::{self, GenericTokenAccount},
        inline_spl_token_2022,
    },
    log::*,
    postgres::{Client, Statement},
    solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::pubkey::Pubkey,
    tokio_postgres::types,
};

const TOKEN_INDEX_COLUMN_COUNT: usize = 3;
/// Struct for the secondary index for both token account's owner and mint index,
pub struct TokenSecondaryIndexEntry {
    /// In case of token owner, the secondary key is the Pubkey of the owner and in case of
    /// token index the secondary_key is the Pubkey of mint.
    secondary_key: Vec<u8>,

    /// The Pubkey of the account
    account_key: Vec<u8>,

    /// Record the slot at which the index entry is created.
    slot: i64,
}

impl SimplePostgresClient {
    pub fn build_single_token_owner_index_upsert_statement(
        client: &mut Client,
        config: &GeyserPluginPostgresConfig,
    ) -> Result<Statement, GeyserPluginError> {
        const BULK_OWNER_INDEX_INSERT_STATEMENT: &str =
            "INSERT INTO spl_token_owner_index AS owner_index (owner_key, account_key, slot) \
        VALUES ($1, $2, $3) \
        ON CONFLICT (owner_key, account_key) \
        DO UPDATE SET slot=excluded.slot \
        WHERE owner_index.slot < excluded.slot";

        Self::prepare_query_statement(client, config, BULK_OWNER_INDEX_INSERT_STATEMENT)
    }

    pub fn build_single_token_mint_index_upsert_statement(
        client: &mut Client,
        config: &GeyserPluginPostgresConfig,
    ) -> Result<Statement, GeyserPluginError> {
        const BULK_MINT_INDEX_INSERT_STATEMENT: &str =
            "INSERT INTO spl_token_mint_index AS mint_index (mint_key, account_key, slot) \
        VALUES ($1, $2, $3) \
        ON CONFLICT (mint_key, account_key) \
        DO UPDATE SET slot=excluded.slot \
        WHERE mint_index.slot < excluded.slot";

        Self::prepare_query_statement(client, config, BULK_MINT_INDEX_INSERT_STATEMENT)
    }

    /// Common build the token mint index bulk insert statement.
    pub fn build_bulk_token_index_insert_statement_common(
        client: &mut Client,
        table: &str,
        source_key_name: &str,
        config: &GeyserPluginPostgresConfig,
    ) -> Result<Statement, GeyserPluginError> {
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        let mut stmt = format!(
            "INSERT INTO {} AS index ({}, account_key, slot) VALUES",
            table, source_key_name
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

        let handle_conflict = format!(
            "ON CONFLICT ({}, account_key) DO UPDATE SET slot=excluded.slot where index.slot < excluded.slot",
            source_key_name);

        stmt = format!("{} {}", stmt, handle_conflict);

        info!("{}", stmt);
        let bulk_stmt = client.prepare(&stmt);

        match bulk_stmt {
            Err(err) => {
                Err(GeyserPluginError::Custom(Box::new(GeyserPluginPostgresError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the {} index update PostgreSQL database: {} host: {:?} user: {:?} config: {:?}",
                        table, err, config.host, config.user, config
                    ),
                })))
            }
            Ok(statement) => Ok(statement),
        }
    }

    /// Build the token owner index bulk insert statement
    pub fn build_bulk_token_owner_index_insert_statement(
        client: &mut Client,
        config: &GeyserPluginPostgresConfig,
    ) -> Result<Statement, GeyserPluginError> {
        Self::build_bulk_token_index_insert_statement_common(
            client,
            "spl_token_owner_index",
            "owner_key",
            config,
        )
    }

    /// Build the token mint index bulk insert statement.
    pub fn build_bulk_token_mint_index_insert_statement(
        client: &mut Client,
        config: &GeyserPluginPostgresConfig,
    ) -> Result<Statement, GeyserPluginError> {
        Self::build_bulk_token_index_insert_statement_common(
            client,
            "spl_token_mint_index",
            "mint_key",
            config,
        )
    }

    /// Execute the common token bulk insert query.
    fn bulk_insert_token_index_common(
        batch_size: usize,
        client: &mut Client,
        index_entries: &mut Vec<TokenSecondaryIndexEntry>,
        query: &Statement,
    ) -> Result<(), GeyserPluginError> {
        if index_entries.len() == batch_size {
            let mut measure = Measure::start("geyser-plugin-postgres-prepare-index-values");

            let mut values: Vec<&(dyn types::ToSql + Sync)> =
                Vec::with_capacity(batch_size * TOKEN_INDEX_COLUMN_COUNT);
            for index in index_entries.iter().take(batch_size) {
                values.push(&index.secondary_key);
                values.push(&index.account_key);
                values.push(&index.slot);
            }
            measure.stop();
            inc_new_counter_debug!(
                "geyser-plugin-postgres-prepare-index-values-us",
                measure.as_us() as usize,
                10000,
                10000
            );

            let mut measure = Measure::start("geyser-plugin-postgres-update-index-account");
            let result = client.query(query, &values);

            index_entries.clear();

            if let Err(err) = result {
                let msg = format!(
                    "Failed to persist the update of account to the PostgreSQL database. Error: {:?}",
                    err
                );
                error!("{}", msg);
                return Err(GeyserPluginError::AccountsUpdateError { msg });
            }

            measure.stop();
            inc_new_counter_debug!(
                "geyser-plugin-postgres-update-index-us",
                measure.as_us() as usize,
                10000,
                10000
            );
            inc_new_counter_debug!(
                "geyser-plugin-postgres-update-index-count",
                batch_size,
                10000,
                10000
            );
        }
        Ok(())
    }

    /// Execute the token owner bulk insert query.
    pub fn bulk_insert_token_owner_index(&mut self) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
        if client.bulk_insert_token_owner_index_stmt.is_none() {
            return Ok(());
        }
        let query = client.bulk_insert_token_owner_index_stmt.as_ref().unwrap();
        Self::bulk_insert_token_index_common(
            self.batch_size,
            &mut client.client,
            &mut self.pending_token_owner_index,
            query,
        )
    }

    /// Execute the token mint index bulk insert query.
    pub fn bulk_insert_token_mint_index(&mut self) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
        if client.bulk_insert_token_mint_index_stmt.is_none() {
            return Ok(());
        }
        let query = client.bulk_insert_token_mint_index_stmt.as_ref().unwrap();
        Self::bulk_insert_token_index_common(
            self.batch_size,
            &mut client.client,
            &mut self.pending_token_mint_index,
            query,
        )
    }

    /// Generic function to queue the token owner index for bulk insert.
    fn queue_token_owner_index_generic<G: GenericTokenAccount>(
        &mut self,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) {
        if account.owner() == token_id.as_ref() {
            if let Some(owner_key) = G::unpack_account_owner(account.data()) {
                let owner_key = owner_key.as_ref().to_vec();
                let pubkey = account.pubkey();
                self.pending_token_owner_index
                    .push(TokenSecondaryIndexEntry {
                        secondary_key: owner_key,
                        account_key: pubkey.to_vec(),
                        slot: account.slot,
                    });
            }
        }
    }

    /// Generic function to queue the token mint index for bulk insert.
    fn queue_token_mint_index_generic<G: GenericTokenAccount>(
        &mut self,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) {
        if account.owner() == token_id.as_ref() {
            if let Some(mint_key) = G::unpack_account_mint(account.data()) {
                let mint_key = mint_key.as_ref().to_vec();
                let pubkey = account.pubkey();
                self.pending_token_mint_index
                    .push(TokenSecondaryIndexEntry {
                        secondary_key: mint_key,
                        account_key: pubkey.to_vec(),
                        slot: account.slot,
                    })
            }
        }
    }

    /// Queue bulk insert secondary indexes: token owner and token mint indexes.
    pub fn queue_secondary_indexes(&mut self, account: &DbAccountInfo) {
        if self.index_token_owner {
            self.queue_token_owner_index_generic::<inline_spl_token::Account>(
                &inline_spl_token::id(),
                account,
            );
            self.queue_token_owner_index_generic::<inline_spl_token_2022::Account>(
                &inline_spl_token_2022::id(),
                account,
            );
        }

        if self.index_token_mint {
            self.queue_token_mint_index_generic::<inline_spl_token::Account>(
                &inline_spl_token::id(),
                account,
            );
            self.queue_token_mint_index_generic::<inline_spl_token_2022::Account>(
                &inline_spl_token_2022::id(),
                account,
            );
        }
    }

    /// Generic function to update a single token owner index.
    fn update_token_owner_index_generic<G: GenericTokenAccount>(
        client: &mut Client,
        statement: &Statement,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) -> Result<(), GeyserPluginError> {
        if account.owner() == token_id.as_ref() {
            if let Some(owner_key) = G::unpack_account_owner(account.data()) {
                let owner_key = owner_key.as_ref().to_vec();
                let pubkey = account.pubkey();
                let slot = account.slot;
                let result = client.execute(statement, &[&owner_key, &pubkey, &slot]);
                if let Err(err) = result {
                    let msg = format!(
                        "Failed to update the token owner index to the PostgreSQL database. Error: {:?}",
                        err
                    );
                    error!("{}", msg);
                    return Err(GeyserPluginError::AccountsUpdateError { msg });
                }
            }
        }

        Ok(())
    }

    /// Generic function to update a single token mint index.
    fn update_token_mint_index_generic<G: GenericTokenAccount>(
        client: &mut Client,
        statement: &Statement,
        token_id: &Pubkey,
        account: &DbAccountInfo,
    ) -> Result<(), GeyserPluginError> {
        if account.owner() == token_id.as_ref() {
            if let Some(mint_key) = G::unpack_account_mint(account.data()) {
                let mint_key = mint_key.as_ref().to_vec();
                let pubkey = account.pubkey();
                let slot = account.slot;
                let result = client.execute(statement, &[&mint_key, &pubkey, &slot]);
                if let Err(err) = result {
                    let msg = format!(
                        "Failed to update the token mint index to the PostgreSQL database. Error: {:?}",
                        err
                    );
                    error!("{}", msg);
                    return Err(GeyserPluginError::AccountsUpdateError { msg });
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
    ) -> Result<(), GeyserPluginError> {
        Self::update_token_owner_index_generic::<inline_spl_token::Account>(
            client,
            statement,
            &inline_spl_token::id(),
            account,
        )?;

        Self::update_token_owner_index_generic::<inline_spl_token_2022::Account>(
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
    ) -> Result<(), GeyserPluginError> {
        Self::update_token_mint_index_generic::<inline_spl_token::Account>(
            client,
            statement,
            &inline_spl_token::id(),
            account,
        )?;

        Self::update_token_mint_index_generic::<inline_spl_token_2022::Account>(
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
