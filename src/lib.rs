extern crate diesel;
#[macro_use]
extern crate log;

use std::time::{Duration, Instant};

use diesel::backend::Backend;
use diesel::connection::{SimpleConnection, TransactionManager};
use diesel::debug_query;
use diesel::deserialize::FromSqlRow;
use diesel::expression::QueryMetadata;
use diesel::migration::MigrationConnection;
use diesel::prelude::*;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::query_dsl::CompatibleType;
use diesel::r2d2::R2D2Connection;

/// Wraps a diesel `Connection` to time and log each query using
/// the configured logger for the `log` crate.
///
/// Currently, this produces a `debug` log on every query,
/// an `info` on queries that take longer than 1 second,
/// and a `warn`ing on queries that take longer than 5 seconds.
/// These thresholds will be configurable in a future version.
pub struct LoggingConnection<C: Connection> {
    transaction_manager: LoggingTransactionManager<C>,
}

impl<C: Connection + Send> LoggingConnection<C> {
    pub fn new(connection: C) -> Self {
        Self {
            transaction_manager: LoggingTransactionManager::new(connection),
        }
    }
}

impl<C: Connection + Send> Connection for LoggingConnection<C>
where
    C: Connection + Send + 'static,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    type Backend = C::Backend;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        Ok(LoggingConnection::new(C::establish(database_url)?))
    }

    fn execute(&self, query: &str) -> QueryResult<usize> {
        let start_time = Instant::now();
        let result = self.transaction_manager.connection.execute(query);
        let duration = start_time.elapsed();
        log_query(query, duration);
        result
    }

    fn load<T, U, ST>(&self, source: T) -> QueryResult<Vec<U>>
    where
        Self: Sized,
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        T::SqlType: CompatibleType<U, Self::Backend, SqlType = ST>,
        U: FromSqlRow<ST, Self::Backend>,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let query = source.as_query();
        let debug_query = debug_query::<Self::Backend, _>(&query).to_string();
        let start_time = Instant::now();
        let result = self.transaction_manager.connection.load(query);
        let duration = start_time.elapsed();
        log_query(&debug_query, duration);
        result
    }

    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        Self: Sized,
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let debug_query = debug_query::<Self::Backend, _>(&source).to_string();
        let start_time = Instant::now();
        let result = self.transaction_manager.connection.execute_returning_count(source);
        let duration = start_time.elapsed();
        log_query(&debug_query, duration);
        result
    }

    fn transaction_manager(&self) -> &dyn TransactionManager<Self> {
        &self.transaction_manager
    }
}

impl<C> SimpleConnection for LoggingConnection<C>
where
    C: SimpleConnection + Connection + Send + 'static,
{
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        let start_time = Instant::now();
        let result = self.transaction_manager.connection.batch_execute(query);
        let duration = start_time.elapsed();
        log_query(query, duration);
        result
    }
}

impl<C: 'static + Connection> R2D2Connection for LoggingConnection<C>
where
    C: R2D2Connection + Connection + Send + 'static,
    <<C as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    fn ping(&self) -> QueryResult<()> {
        self.transaction_manager.connection.ping()
    }
}

impl<C: 'static + Connection + MigrationConnection> MigrationConnection for LoggingConnection<C>
where
    <<C as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    fn setup(&self) -> QueryResult<usize> {
        self.transaction_manager.connection.setup()
    }
}

struct LoggingTransactionManager<C: Connection> {
    connection: C,
}

impl<C: 'static + Connection> TransactionManager<LoggingConnection<C>>
    for LoggingTransactionManager<C>
where
    <C::Backend as Backend>::QueryBuilder: std::default::Default,
{
    fn begin_transaction(&self, _conn: &LoggingConnection<C>) -> QueryResult<()> {
        self.connection
            .transaction_manager()
            .begin_transaction(&self.connection)
    }

    fn rollback_transaction(&self, _conn: &LoggingConnection<C>) -> QueryResult<()> {
        self.connection
            .transaction_manager()
            .rollback_transaction(&self.connection)
    }

    fn commit_transaction(&self, _conn: &LoggingConnection<C>) -> QueryResult<()> {
        self.connection
            .transaction_manager()
            .commit_transaction(&self.connection)
    }

    fn get_transaction_depth(&self) -> u32 {
        self.connection
            .transaction_manager()
            .get_transaction_depth()
    }
}

impl<C: Connection> LoggingTransactionManager<C> {
    pub fn new(conn: C) -> Self {
        Self { connection: conn }
    }
}

fn log_query(query: &str, duration: Duration) {
    if duration.as_secs() >= 5 {
        warn!(
            "Slow query ran in {:.2} seconds: {}",
            duration_to_secs(duration),
            query
        );
    } else if duration.as_secs() >= 1 {
        info!(
            "Slow query ran in {:.2} seconds: {}",
            duration_to_secs(duration),
            query
        );
    } else {
        debug!("Query ran in {:.1} ms: {}", duration_to_ms(duration), query);
    }
}

const NANOS_PER_MILLI: u32 = 1_000_000;
const MILLIS_PER_SEC: u32 = 1_000;

fn duration_to_secs(duration: Duration) -> f32 {
    duration_to_ms(duration) / MILLIS_PER_SEC as f32
}

fn duration_to_ms(duration: Duration) -> f32 {
    (duration.as_secs() as u32 * 1000) as f32
        + (duration.subsec_nanos() as f32 / NANOS_PER_MILLI as f32)
}
