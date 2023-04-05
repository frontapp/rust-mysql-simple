use std::marker::PhantomData;

use crate::{
    conn::{query_result::Binary, ConnMut},
    prelude::*,
    Conn, Params, QueryResult, Result, Statement,
};

struct PipelineInner<'c> {
    conn: &'c mut Conn,
    queries: Vec<u8>,
}

impl<'c> PipelineInner<'c> {
    fn exec(&mut self, stmt: &Statement, params: Params) -> Result<()> {
        self.conn._execute_pipeline(stmt, params)?;
        self.queries
            .push(self.conn.stream_mut().codec().front_hack_get_seq_id());
        Ok(())
    }

    fn finish(self) -> PipelineResult<'c, Binary> {
        PipelineResult {
            conn: self.conn,
            queries: self.queries,
            protocol: PhantomData,
        }
    }
}

pub struct Pipeline<'c>(Option<PipelineInner<'c>>);

impl<'c> Pipeline<'c> {
    pub(crate) fn new(conn: &'c mut Conn) -> Self {
        Self(Some(PipelineInner {
            conn,
            queries: vec![],
        }))
    }

    pub fn exec<P: Into<Params>>(&mut self, stmt: &Statement, params: P) -> Result<()> {
        self.0.as_mut().unwrap().exec(stmt, params.into())
    }

    pub fn finish(mut self) -> PipelineResult<'c, Binary> {
        self.0.take().unwrap().finish()
    }
}

impl Drop for Pipeline<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            drop(inner.finish())
        }
    }
}

pub struct PipelineResult<'c, T: Protocol> {
    conn: &'c mut Conn,
    queries: Vec<u8>,
    protocol: PhantomData<T>,
}

impl<'c, T: Protocol> PipelineResult<'c, T> {
    pub fn next_query(&mut self) -> Result<Option<QueryResult<'_, '_, '_, T>>> {
        if self.queries.is_empty() {
            Ok(None)
        } else {
            println!(
                "[PipelineResult] in next_query calling handle_result_set q={}",
                self.queries.len()
            );
            self.conn
                .stream_mut()
                .codec_mut()
                .front_hack_set_seq_id(self.queries.remove(0));
            let meta = self.conn.handle_result_set()?;
            Ok(Some(QueryResult::new(ConnMut::Mut(self.conn), meta)))
        }
    }
}

impl<T: crate::prelude::Protocol> Drop for PipelineResult<'_, T> {
    fn drop(&mut self) {
        println!("[PipelineResult] in drop draining q={}", self.queries.len());
        while self.next_query().unwrap().is_some() {}
    }
}
