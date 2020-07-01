extern crate diesel;
#[macro_use]
extern crate log;

use std::ops::Deref;
use std::time::{Duration, Instant};

use diesel::backend::{Backend, UsesAnsiSavepointSyntax};
use diesel::connection::{AnsiTransactionManager, SimpleConnection};
use diesel::debug_query;
use diesel::deserialize::QueryableByName;
use diesel::prelude::*;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::sql_types::HasSqlType;

/// Wraps a diesel `Connection` to time and log each query using
/// the configured logger for the `log` crate.
///
/// Currently, this produces a `debug` log on every query,
/// an `info` on queries that take longer than 1 second,
/// and a `warn`ing on queries that take longer than 5 seconds.
/// These thresholds will be configurable in a future version.
pub struct LoggingConnection<C: Connection>(C);

impl<C: Connection> LoggingConnection<C> {
    pub fn new(conn: C) -> Self {
        LoggingConnection(conn)
    }
}

impl<C: Connection> Deref for LoggingConnection<C> {
    type Target = C;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C> SimpleConnection for LoggingConnection<C>
where
    C: Connection + Send + 'static,
{
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        self.0.batch_execute(query)
    }
}

impl<C: Connection> Connection for LoggingConnection<C>
where
    C: Connection<TransactionManager = AnsiTransactionManager> + Send + 'static,
    C::Backend: UsesAnsiSavepointSyntax,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    type Backend = C::Backend;
    type TransactionManager = C::TransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        Ok(LoggingConnection(C::establish(database_url)?))
    }

    fn execute(&self, query: &str) -> QueryResult<usize> {
        let start_time = Instant::now();
        let result = self.0.execute(query);
        let duration = start_time.elapsed();
        log_query(query, duration);
        result
    }

    fn query_by_index<T, U>(&self, source: T) -> QueryResult<Vec<U>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        Self::Backend: HasSqlType<T::SqlType>,
        U: Queryable<T::SqlType, Self::Backend>,
    {
        let query = source.as_query();
        let debug_query = debug_query::<Self::Backend, _>(&query).to_string();
        let start_time = Instant::now();
        let result = self.0.query_by_index(query);
        let duration = start_time.elapsed();
        log_query(&debug_query, duration);
        result
    }

    fn query_by_name<T, U>(&self, source: &T) -> QueryResult<Vec<U>>
    where
        T: QueryFragment<Self::Backend> + QueryId,
        U: QueryableByName<Self::Backend>,
    {
        let debug_query = debug_query::<Self::Backend, _>(&source).to_string();
        let start_time = Instant::now();
        let result = self.0.query_by_name(source);
        let duration = start_time.elapsed();
        log_query(&debug_query, duration);
        result
    }

    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let debug_query = debug_query::<Self::Backend, _>(&source).to_string();
        let start_time = Instant::now();
        let result = self.0.execute_returning_count(source);
        let duration = start_time.elapsed();
        log_query(&debug_query, duration);
        result
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        self.0.transaction_manager()
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
