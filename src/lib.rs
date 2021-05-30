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
    connection: C,
    transaction_manager: LoggingTransactionManager,
}

impl<C: 'static + Connection> LoggingConnection<C> {
    fn bench_query<F, R>(query: &dyn QueryFragment<C::Backend>, func: F) -> R
    where
        F: FnMut() -> R,
        <C::Backend as Backend>::QueryBuilder: Default,
    {
        let debug_query =
            debug_query::<<LoggingConnection<C> as Connection>::Backend, _>(&query).to_string();
        Self::bench_query_str(&debug_query, func)
    }

    fn bench_query_str<F, R>(query: &str, mut func: F) -> R
    where
        F: FnMut() -> R,
        <C::Backend as Backend>::QueryBuilder: Default,
    {
        let start_time = Instant::now();
        let result = func();
        let duration = start_time.elapsed();
        log_query(&query, duration);
        result
    }
}

impl<C: Connection + Send> LoggingConnection<C> {
    pub fn new(connection: C) -> Self {
        Self {
            connection,
            transaction_manager: LoggingTransactionManager::new(),
        }
    }
}

impl<C: Connection + Send> Connection for LoggingConnection<C>
where
    C: Connection + Send + 'static,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    type Backend = C::Backend;
    type TransactionManager = LoggingTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        Ok(LoggingConnection::new(C::establish(database_url)?))
    }

    fn execute(&mut self, query: &str) -> QueryResult<usize> {
        Self::bench_query_str(query, || self.connection.execute(query))
    }

    fn load<T, U, ST>(&mut self, source: T) -> QueryResult<Vec<U>>
    where
        Self: Sized,
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        T::SqlType: CompatibleType<U, Self::Backend, SqlType = ST>,
        U: FromSqlRow<ST, Self::Backend>,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let query = source.as_query();
        Self::bench_query(&query, || self.connection.load(&query))
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        Self: Sized,
        T: QueryFragment<Self::Backend> + QueryId,
    {
        Self::bench_query(source, || {
            self                .connection
                .execute_returning_count(source)
        })
    }

    fn transaction_state(&mut self) -> &mut <Self::TransactionManager as TransactionManager<LoggingConnection<C>>>::TransactionStateData {
        &mut self.transaction_manager
    }
}

impl<C> SimpleConnection for LoggingConnection<C>
where
    C: SimpleConnection + Connection + Send + 'static,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        Self::bench_query_str(query, || {
            self.connection.batch_execute(query)
        })
    }
}

impl<C: 'static + Connection> R2D2Connection for LoggingConnection<C>
where
    C: R2D2Connection + Connection + Send + 'static,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    fn ping(&mut self) -> QueryResult<()> {
        self.connection.ping()
    }
}

impl<C: 'static + Connection + MigrationConnection> MigrationConnection for LoggingConnection<C>
where
    <C::Backend as Backend>::QueryBuilder: Default,
{
    fn setup(&mut self) -> QueryResult<usize> {
        self.connection.setup()
    }
}

pub struct LoggingTransactionManager {}

impl<C: 'static + Connection> TransactionManager<LoggingConnection<C>>
    for LoggingTransactionManager
where
    <C::Backend as Backend>::QueryBuilder: Default,
{
    type TransactionStateData = Self;

    fn begin_transaction(conn: &mut LoggingConnection<C>) -> QueryResult<()> {
        <<C as Connection>::TransactionManager as TransactionManager<C>>::begin_transaction(&mut conn.connection)
    }

    fn rollback_transaction(conn: &mut LoggingConnection<C>) -> QueryResult<()> {
        <<C as Connection>::TransactionManager as TransactionManager<C>>::rollback_transaction(&mut conn.connection)
    }

    fn commit_transaction(conn: &mut LoggingConnection<C>) -> QueryResult<()> {
        <<C as Connection>::TransactionManager as TransactionManager<C>>::commit_transaction(&mut conn.connection)
    }

    fn get_transaction_depth(conn: &mut LoggingConnection<C>) -> u32 {
        <<C as Connection>::TransactionManager as TransactionManager<C>>::get_transaction_depth(&mut conn.connection)
    }
}

impl LoggingTransactionManager {
    pub fn new() -> Self {
        Self { }
    }
}

fn log_query(query: &str, duration: Duration) {
    if duration.as_secs() >= 5 {
        warn!(
            "SLOW QUERY [{:.2} s]: {}",
            duration_to_secs(duration),
            query
        );
    } else if duration.as_secs() >= 1 {
        info!(
            "SLOW QUERY [{:.2} s]: {}",
            duration_to_secs(duration),
            query
        );
    } else {
        debug!("QUERY: [{:.1}ms]: {}", duration_to_ms(duration), query);
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
