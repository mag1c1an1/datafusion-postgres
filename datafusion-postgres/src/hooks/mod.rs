pub mod cursor;
pub mod permissions;
pub mod set_show;
pub mod transactions;

use async_trait::async_trait;

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use futures::Sink;
use futures::future::BoxFuture;
use pgwire::api::ClientInfo;
use pgwire::api::ClientPortalStore;
use pgwire::api::results::Response;
use pgwire::api::store::{MemPortalStore, PortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::hooks::cursor::DfStatement;

#[async_trait]
pub trait HookClient: ClientInfo + Send + Sync {
    fn portal_store(&self) -> &MemPortalStore<DfStatement>;

    async fn send_message(&mut self, item: PgWireBackendMessage) -> PgWireResult<()>;
}

#[async_trait]
impl<S> HookClient for S
where
    S: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Send + Sync + Unpin,
    PgWireError: From<<S as Sink<PgWireBackendMessage>>::Error>,
    S::PortalStore: PortalStore,
{
    fn portal_store(&self) -> &MemPortalStore<DfStatement> {
        self.portal_store()
            .as_any()
            .downcast_ref::<MemPortalStore<DfStatement>>()
            .expect("portal store is not MemPortalStore<DfStatement>")
    }

    async fn send_message(&mut self, item: PgWireBackendMessage) -> PgWireResult<()> {
        use futures::SinkExt;
        self.send(item).await.map_err(PgWireError::from)
    }
}

/// A statement hook run before the default handling.
///
/// The methods return boxed futures rather than using `#[async_trait]`, which
/// adds `'life0: 'async_trait`-style bounds to every method. Each impl then has
/// its own parameter environment, and rustc cannot reuse the proof that the
/// captured `&SessionContext`/`&LogicalPlan` are `Send` across impls: every
/// hook repeated a walk of about 1000 DataFusion types. With one late-bound
/// lifetime and no where-clauses, the proof is shared by all impls.
pub trait QueryHook: Send + Sync {
    /// called in simple query handler to return response directly
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> BoxFuture<'a, Option<PgWireResult<Response>>>;

    /// called at extended query parse phase, for generating `LogicalPlan`from statement
    fn handle_extended_parse_query<'a>(
        &'a self,
        sql: &'a Statement,
        session_context: &'a SessionContext,
        client: &'a (dyn ClientInfo + Send + Sync),
    ) -> BoxFuture<'a, Option<PgWireResult<LogicalPlan>>>;

    /// called at extended query execute phase, for query execution
    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        logical_plan: &'a LogicalPlan,
        params: &'a ParamValues,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> BoxFuture<'a, Option<PgWireResult<Response>>>;
}
