use crate::conn::Conn;

pub struct BinLogEventIterator {
    conn: Conn,
}

impl BinLogEventIterator {
    pub fn new(conn: Conn) -> Self {
        BinLogEventIterator { conn }
    }
}

impl Iterator for BinLogEventIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.conn.read_packet() {
            Ok(data) => Some(data),
            Err(_err) => None,
        }
    }
}
