use crate::conn::Conn;

pub struct BinlogEventIterator<'a> {
    conn: &'a mut Conn,
}

impl<'a> BinlogEventIterator<'a> {
    pub fn new(conn: &'a mut Conn) -> Self {
        BinlogEventIterator { conn }
    }
}

impl<'a> Iterator for BinlogEventIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.conn.read_packet() {
            // FIXME: Avoid copying here
            Ok(mut data) => Some(data.as_mut().clone()),
            Err(err) => {
                println!("{:?}", err);
                None
            }
        }
    }
}
