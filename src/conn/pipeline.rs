use mysql_common::proto::codec::PacketCodecState;
use std::{collections::VecDeque, marker::PhantomData};

use crate::{
    conn::{query_result::Binary, ConnMut},
    prelude::*,
    Conn, Params, QueryResult, Result, Statement,
};

struct PipelineInner<'c> {
    conn: &'c mut Conn,
    cmd_states: VecDeque<PacketCodecState>,
}

impl<'c> PipelineInner<'c> {
    fn exec(&mut self, stmt: &Statement, params: Params) -> Result<()> {
        self.conn._execute_pipeline(stmt, params)?;
        self.cmd_states
            .push_back(self.conn.stream_mut().codec().save_state());
        Ok(())
    }

    fn finish(self) -> PipelineResult<'c, Binary> {
        PipelineResult {
            conn: self.conn,
            cmd_states: self.cmd_states,
            protocol: PhantomData,
        }
    }
}

pub struct Pipeline<'c>(Option<PipelineInner<'c>>);

impl<'c> Pipeline<'c> {
    pub(crate) fn new(conn: &'c mut Conn) -> Self {
        Self(Some(PipelineInner {
            conn,
            cmd_states: VecDeque::new(),
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
    cmd_states: VecDeque<PacketCodecState>,
    protocol: PhantomData<T>,
}

impl<'c, T: Protocol> PipelineResult<'c, T> {
    pub fn iter(&mut self) -> Result<Option<QueryResult<'_, '_, '_, T>>> {
        let Some(cmd_state) = self.cmd_states.pop_front() else {
            return Ok(None);
        };
        self.conn.stream_mut().codec_mut().restore_state(cmd_state);
        let meta = self.conn.handle_result_set()?;
        Ok(Some(QueryResult::new(ConnMut::Mut(self.conn), meta)))
    }
}

impl<T: crate::prelude::Protocol> Drop for PipelineResult<'_, T> {
    fn drop(&mut self) {
        while self.iter().unwrap().is_some() {}
    }
}
