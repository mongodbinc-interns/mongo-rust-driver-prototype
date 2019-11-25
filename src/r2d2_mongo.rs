use crate::{
    connstring::ConnectionString, db::ThreadedDatabase, Client, ClientOptions,
    ThreadedClient,
};

/// A basic r2d2 connection manager for this driver.
///
/// - returns a Database object matching the provided database name, not a Client
/// - takes a parsed connection string and client options
#[derive(Debug)]
pub struct MongoConnectionManager {
    conn_str: ConnectionString,
    db_name: String,
    client_options: Option<ClientOptions>,
}

impl MongoConnectionManager {
    pub fn new<S, CO>(connection_str: ConnectionString, db_name: S, client_options: CO) -> Self
        where
            S: Into<String>,
            CO: Into<Option<ClientOptions>>,
    {
        Self {
            conn_str: connection_str,
            db_name: db_name.into(),
            client_options: client_options.into(),
        }
    }
}

impl r2d2::ManageConnection for MongoConnectionManager {
    type Connection = crate::db::Database;
    type Error = crate::error::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let client =
            Client::with_config(self.conn_str.clone(), self.client_options.clone(), None)?;
        if let (Some(username), Some(password)) = (&self.conn_str.user, &self.conn_str.password)
        {
            client.db("admin").auth(username, password)?;
        }
        Ok(client.db(&self.db_name))
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.version()?;
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}