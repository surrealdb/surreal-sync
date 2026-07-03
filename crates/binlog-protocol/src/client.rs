use crate::error::Error;
use crate::event::{EventBody, EventParser, RawEvent};
use crate::flavor::mariadb::register;
use crate::flavor::mysql::dump_gtid;
use crate::flavor::Flavor;
use crate::options::{ReplicaOptions, ResumePosition};
use crate::shared::checksum::event_body;
use crate::shared::event_header::EventHeader;
use crate::shared::position::PositionTracker;
use crate::shared::stream::EventStream;
use crate::shared::wire::{
    authenticate, check_error_packet, encode_binlog_dump_async, encode_register_slave, handshake,
    PacketChannel,
};
use crate::types::BinlogPosition;

enum Tracker {
    MySql(crate::flavor::mysql::MySqlPositionTracker),
    MariaDb(crate::flavor::mariadb::MariaDbPositionTracker),
}

impl Tracker {
    fn on_event(&mut self, event: &RawEvent, file: &str) {
        match self {
            Tracker::MySql(t) => t.on_event(event, file),
            Tracker::MariaDb(t) => t.on_event(event, file),
        }
    }

    fn position(&self) -> BinlogPosition {
        match self {
            Tracker::MySql(t) => t.position(),
            Tracker::MariaDb(t) => t.position(),
        }
    }

    fn commit(&mut self, position: BinlogPosition) {
        match self {
            Tracker::MySql(t) => t.commit(position),
            Tracker::MariaDb(t) => t.commit(position),
        }
    }

    fn set_pending_mariadb_gtid(&mut self, gtid: crate::flavor::mariadb::MariaDbGtid) {
        if let Tracker::MariaDb(t) = self {
            t.set_pending_gtid(gtid);
        }
    }

    /// Record a MySQL `GTID_LOG_EVENT` marker so the tracker folds it into the
    /// executed set at the next commit boundary (analogous to
    /// `set_pending_mariadb_gtid`).
    fn set_pending_mysql_gtid(&mut self, gtid: crate::flavor::mysql::Gtid) {
        if let Tracker::MySql(t) = self {
            t.set_pending_gtid(gtid);
        }
    }

    /// Seed the MariaDB tracker with a GTID list so runtime positions are
    /// reported (and accumulated) as `BinlogPosition::MariaDbGtid`.
    fn seed_mariadb_gtid_list(&mut self, list: crate::flavor::mariadb::MariaDbGtidList) {
        if let Tracker::MariaDb(t) = self {
            t.seed_gtid_list(list);
        }
    }

    /// Seed the MySQL tracker with a GTID set so runtime positions are reported
    /// (and accumulated) as `BinlogPosition::MySqlGtid` after a GTID-based resume.
    fn seed_mysql_gtid_set(&mut self, set: crate::flavor::mysql::MySqlGtidSet) {
        if let Tracker::MySql(t) = self {
            t.seed_gtid_set(set);
        }
    }
}

pub struct BinlogClient {
    opts: ReplicaOptions,
    flavor: Flavor,
    channel: Option<PacketChannel>,
    stream: EventStream,
    parser: EventParser,
    tracker: Tracker,
    binlog_file: String,
    binlog_pos: u32,
    read_buffer: Vec<u8>,
    record_stream: bool,
    recorded: Vec<u8>,
}

impl BinlogClient {
    pub async fn connect(opts: ReplicaOptions) -> Result<Self, Error> {
        let mut channel = PacketChannel::connect(&opts.host, opts.port).await?;
        let hs = handshake(&mut channel).await?;
        authenticate(
            &mut channel,
            &hs,
            &opts.host,
            &opts.username,
            &opts.password,
            &opts.ssl,
        )
        .await?;

        let flavor = opts
            .flavor
            .unwrap_or_else(|| Flavor::detect(&hs.server_version));
        let stream = EventStream::new(flavor, "mysql-bin.000001");
        let parser = EventParser::new(flavor);
        let tracker = match flavor {
            Flavor::MySql => Tracker::MySql(crate::flavor::mysql::MySqlPositionTracker::new(
                "mysql-bin.000001",
                4,
            )),
            Flavor::MariaDb => Tracker::MariaDb(
                crate::flavor::mariadb::MariaDbPositionTracker::new("mysql-bin.000001", 4),
            ),
        };

        Ok(Self {
            opts,
            flavor,
            channel: Some(channel),
            stream,
            parser,
            tracker,
            binlog_file: "mysql-bin.000001".into(),
            binlog_pos: 4,
            read_buffer: Vec::new(),
            record_stream: false,
            recorded: Vec::new(),
        })
    }

    /// Append every raw event byte consumed from the replication stream (for fixture capture).
    pub fn enable_stream_recording(&mut self) {
        self.record_stream = true;
    }

    pub fn take_recorded_stream(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.recorded)
    }

    pub async fn start_stream(&mut self, resume: ResumePosition) -> Result<(), Error> {
        self.validate_resume(&resume)?;
        let channel = self
            .channel
            .as_mut()
            .ok_or_else(|| Error::Protocol("not connected".into()))?;

        channel.reset_seq();
        encode_register_slave(channel, self.opts.server_id, "", "", "").await?;

        if self.flavor == Flavor::MySql || self.flavor == Flavor::MariaDb {
            channel.reset_seq();
            channel
                .query("SET @master_binlog_checksum = @@global.binlog_checksum")
                .await?;
            self.parser.enable_checksum();
        }

        if self.flavor == Flavor::MariaDb {
            register::register_session_vars(channel, &self.opts.mariadb_flags).await?;
        }

        match resume {
            ResumePosition::Start => {
                self.binlog_file = "mysql-bin.000001".into();
                self.binlog_pos = 4;
                self.stream.set_file_and_offset(&self.binlog_file, 4);
            }
            ResumePosition::End => {
                self.binlog_file.clear();
                self.binlog_pos = 4;
                self.stream.set_file_and_offset("", 4);
            }
            ResumePosition::FilePos { file, pos } => {
                self.binlog_file = file.clone();
                self.binlog_pos = pos;
                self.stream.set_file_and_offset(&file, u64::from(pos));
                self.tracker
                    .commit(BinlogPosition::file_pos(file, u64::from(pos)));
            }
            ResumePosition::MySqlGtid(set) => {
                channel.reset_seq();
                dump_gtid::encode_dump_gtid(channel, self.opts.server_id, &set).await?;
                self.tracker.seed_mysql_gtid_set(set);
                return Ok(());
            }
            ResumePosition::MariaDbGtid(list) => {
                // MariaDB has no COM_BINLOG_DUMP_GTID. Resume by registering the
                // connect state and issuing COM_BINLOG_DUMP with an empty
                // filename / position 4 — the server streams from the GTID list.
                register::register_gtid_session_vars(
                    channel,
                    &list,
                    self.opts.mariadb_gtid_strict_mode,
                )
                .await?;
                self.tracker.seed_mariadb_gtid_list(list);
                self.binlog_file.clear();
                self.binlog_pos = 4;
                self.stream.set_file_and_offset("", 4);
                channel.reset_seq();
                encode_binlog_dump_async(channel, self.opts.server_id, "", 4).await?;
                return Ok(());
            }
        }

        channel.reset_seq();
        encode_binlog_dump_async(
            channel,
            self.opts.server_id,
            &self.binlog_file,
            self.binlog_pos,
        )
        .await
    }

    pub async fn next_events(&mut self, max: usize) -> Result<Vec<RawEvent>, Error> {
        let mut out = Vec::new();
        self.drain_parsed_events(max, &mut out)?;

        if !out.is_empty() {
            return Ok(out);
        }

        let packet = {
            let channel = self
                .channel
                .as_mut()
                .ok_or_else(|| Error::Protocol("not connected".into()))?;
            match tokio::time::timeout(self.opts.blocking_poll, channel.read_packet()).await {
                Ok(r) => r?,
                Err(_) => return Ok(Vec::new()),
            }
        };

        if !packet.is_empty() {
            let payload = if packet.first() == Some(&0x00) {
                &packet[1..]
            } else {
                check_error_packet(&packet, "binlog stream error")?;
                &packet[..]
            };
            self.read_buffer.extend_from_slice(payload);
            self.drain_parsed_events(max, &mut out)?;
        }

        Ok(out)
    }

    fn drain_parsed_events(&mut self, max: usize, out: &mut Vec<RawEvent>) -> Result<(), Error> {
        while out.len() < max {
            if self.read_buffer.len() < EventHeader::HEADER_LEN {
                break;
            }
            let header = EventHeader::parse(&self.read_buffer, self.stream.file_offset())?;
            let total = header.event_size as usize;
            if self.read_buffer.len() < total {
                break;
            }
            let event_bytes: Vec<u8> = self.read_buffer.drain(..total).collect();
            if self.record_stream {
                self.recorded.extend_from_slice(&event_bytes);
            }
            let body = event_body(&event_bytes, self.parser.checksum_enabled())?;
            let event = self.parser.parse(header.clone(), body).map_err(|e| {
                Error::Protocol(format!(
                    "parse {:?} size={} body_len={}: {e}",
                    header.event_type,
                    header.event_size,
                    body.len()
                ))
            })?;
            self.stream.advance(&event.header, &event.body);
            self.tracker.on_event(&event, self.stream.current_file());
            self.handle_side_effects(&event);
            if let EventBody::TransactionPayload(inner) = event.body {
                out.extend(inner);
            } else {
                out.push(event);
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, position: BinlogPosition) {
        if let BinlogPosition::FilePos { file, pos } = &position {
            self.binlog_file = file.clone();
            self.binlog_pos = *pos as u32;
        }
        self.tracker.commit(position);
    }

    pub fn flavor(&self) -> Flavor {
        self.flavor
    }

    pub fn current_position(&self) -> BinlogPosition {
        self.tracker.position()
    }

    fn validate_resume(&self, resume: &ResumePosition) -> Result<(), Error> {
        match (self.flavor, resume) {
            (Flavor::MySql, ResumePosition::MariaDbGtid(_)) => Err(Error::IncompatibleResume {
                expected: Flavor::MySql,
                got: "MariaDbGtid".into(),
            }),
            (Flavor::MariaDb, ResumePosition::MySqlGtid(_)) => Err(Error::IncompatibleResume {
                expected: Flavor::MariaDb,
                got: "MySqlGtid".into(),
            }),
            _ => Ok(()),
        }
    }

    fn handle_side_effects(&mut self, event: &RawEvent) {
        if let EventBody::Rotate(ref r) = event.body {
            if r.position > 0 {
                self.binlog_file = r.next_file.clone();
                self.binlog_pos = r.position as u32;
            }
        }
        match event.body {
            EventBody::Gtid(crate::types::GtidMarker::MariaDb {
                domain_id,
                server_id,
                sequence,
            }) => {
                self.tracker
                    .set_pending_mariadb_gtid(crate::flavor::mariadb::MariaDbGtid {
                        domain_id,
                        server_id,
                        sequence,
                    });
            }
            EventBody::Gtid(crate::types::GtidMarker::MySql { source_id, gno }) => {
                self.tracker
                    .set_pending_mysql_gtid(crate::flavor::mysql::Gtid { source_id, gno });
            }
            _ => {}
        }
    }
}
