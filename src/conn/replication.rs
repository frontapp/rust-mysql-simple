use crate::conn::Conn;

pub struct BinlogEventIterator {
    conn: Conn,
}

impl BinlogEventIterator {
    pub fn new(conn: Conn) -> Self {
        BinlogEventIterator { conn }
    }

    pub fn reset(
        &mut self,
        filename_pos: Option<(&[u8], u32)>,
        blocking: bool,
        server_id: u32,
    ) -> crate::Result<()> {
        self.conn.reset()?;
        self.conn
            .request_binlog_front(filename_pos, blocking, server_id)
    }
}

impl Iterator for BinlogEventIterator {
    type Item = crate::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        // FIXME: Avoid copying here
        Some(
            self.conn
                .read_packet()
                .map(|mut data| data.as_mut().clone()),
        )
    }
}
