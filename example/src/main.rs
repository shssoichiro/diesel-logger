#[macro_use]
extern crate diesel;

pub mod models;
pub mod schema;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::SqliteConnection;
use diesel_logger::LoggingConnection;
use models::NewPost;
use schema::posts;

pub fn main() {
    simple_logger::SimpleLogger::new().env().init().unwrap();
    let conn = SqliteConnection::establish("example.sqlite").unwrap();
    let conn = LoggingConnection::new(conn);

    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS posts (id INTEGER, title TEXT, body TEXT, published BOOL);",
    )
    .unwrap();
    let new_post = NewPost {
        title: "mytitle",
        body: "mybody",
    };
    diesel::insert_into(posts::table)
        .values(&new_post)
        .execute(&conn)
        .expect("Error saving new post");
}
