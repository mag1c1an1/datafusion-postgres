use std::sync::Arc;

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;

use crate::QueryHook;
use crate::hooks::HookClient;

/// Hook for processing transaction related statements
///
/// Note that this hook doesn't create actual transactions. It just responds
/// with reasonable return values.
#[derive(Debug)]
pub struct TransactionStatementHook;

impl QueryHook for TransactionStatementHook {
    /// called in simple query handler to return response directly
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        _session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            let resp = try_respond_transaction_statements(client, statement)
                .await
                .transpose();

            if let Some(result) = resp {
                return Some(result);
            }

            // Check if we're in a failed transaction and block non-transaction
            // commands
            if client.transaction_status() == TransactionStatus::Error {
                return Some(Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "25P01".to_string(),
                    "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                ),
            ))));
            }

            None
        })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        stmt: &'a Statement,
        _session_context: &'a SessionContext,
        _client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<LogicalPlan>>> {
        Box::pin(async move {
            // We don't generate logical plan for these statements
            if matches!(
                stmt,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
            ) {
                // Return a dummy plan for transaction commands - they'll be handled by transaction handler
                let dummy_schema = datafusion::common::DFSchema::empty();
                return Some(Ok(LogicalPlan::EmptyRelation(
                    datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(dummy_schema),
                    },
                )));
            }
            None
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        _logical_plan: &'a LogicalPlan,
        _params: &'a ParamValues,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            self.handle_simple_query(statement, session_context, client)
                .await
        })
    }
}

async fn try_respond_transaction_statements<C>(
    client: &C,
    stmt: &Statement,
) -> PgWireResult<Option<Response>>
where
    C: ClientInfo + Send + Sync + ?Sized,
{
    match stmt {
        Statement::StartTransaction { .. } => {
            match client.transaction_status() {
                TransactionStatus::Idle => Ok(Some(Response::TransactionStart(Tag::new("BEGIN")))),
                TransactionStatus::Transaction => {
                    // PostgreSQL behavior: ignore nested BEGIN, just return SUCCESS
                    // This matches PostgreSQL's handling of nested transaction blocks
                    log::warn!("BEGIN command ignored: already in transaction block");
                    Ok(Some(Response::Execution(Tag::new("BEGIN"))))
                }
                TransactionStatus::Error => {
                    // Can't start new transaction from failed state
                    Err(PgWireError::UserError(Box::new(
                            pgwire::error::ErrorInfo::new(
                                "ERROR".to_string(),
                                "25P01".to_string(),
                                "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                            ),
                        )))
                }
            }
        }
        Statement::Commit { .. } => match client.transaction_status() {
            TransactionStatus::Idle | TransactionStatus::Transaction => {
                Ok(Some(Response::TransactionEnd(Tag::new("COMMIT"))))
            }
            TransactionStatus::Error => Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK")))),
        },
        Statement::Rollback { .. } => Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK")))),
        _ => Ok(None),
    }
}
