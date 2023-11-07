use crate::conn::Conn;

pub struct BinlogOptions {
    pub(crate) server_id: u32,
    pub(crate) filename: Vec<u8>,
    pub(crate) position: u32,
    pub(crate) blocking: bool,
    pub(crate) start_gtid: Option<String>,
    pub(crate) until_gtid: Option<String>,
    pub(crate) gtid_strict_mode: bool,
}

impl BinlogOptions {
    pub fn new(server_id: u32) -> Self {
        Self {
            server_id,
            filename: vec![],
            position: 4,
            blocking: false,
            start_gtid: None,
            until_gtid: None,
            gtid_strict_mode: false,
        }
    }

    pub fn filename(mut self, filename: &[u8]) -> Self {
        self.filename = filename.to_vec();
        self
    }

    pub fn position(mut self, position: u32) -> Self {
        self.position = position;
        self
    }

    pub fn blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }

    pub fn start_gtid(mut self, start_gtid: Option<&str>) -> Self {
        self.start_gtid = start_gtid.map(str::to_string);
        self
    }

    pub fn until_gtid(mut self, until_gtid: Option<&str>) -> Self {
        self.until_gtid = until_gtid.map(str::to_string);
        self
    }

    pub fn gtid_strict_mode(mut self, strict: bool) -> Self {
        self.gtid_strict_mode = strict;
        self
    }
}

pub struct BinlogEventIterator {
    conn: Conn,
    options: BinlogOptions,
}

impl BinlogEventIterator {
    pub fn new(conn: Conn, options: BinlogOptions) -> Self {
        BinlogEventIterator { conn, options }
    }

    pub fn reset(&mut self, filename_pos: Option<(&[u8], u32)>) -> crate::Result<()> {
        self.conn.reset()?;
        if let Some((filename, pos)) = filename_pos {
            self.options.start_gtid = None;
            self.options.filename = filename.to_vec();
            self.options.position = pos;
        }
        self.conn.request_binlog_front(&self.options)
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
