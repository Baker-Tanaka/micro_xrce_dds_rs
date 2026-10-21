//! Host test harness: mock fragmented transport, fake Agent, manual polling.
#![allow(dead_code)]

use std::{
    collections::VecDeque,
    future::{poll_fn, Future},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

use embassy_time::{Duration, Instant, MockDriver};
use micro_xrce_dds_rs::{
    cdr::CdrWriter, cdr_reader::CdrReader, Error, Message, Timestamp, Transport,
};

pub static TEST_LOCK: Mutex<()> = Mutex::new(());

pub fn lock() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    MockDriver::get().reset();
    guard
}

pub fn advance(ms: u64) {
    MockDriver::get().advance(Duration::from_millis(ms));
}

pub fn poll<F: Future>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}

// ── Test message ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Cmd {
    pub v: i32,
}

impl Message for Cmd {
    const TYPE_NAME: &'static str = "test_msgs::msg::dds_::Cmd_";
    const MAX_SERIALIZED_SIZE: usize = 4;
    fn serialize(&self, w: &mut CdrWriter) {
        w.i32_val(self.v);
    }
    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        Ok(Self { v: r.i32_val()? })
    }
}

/// Serializes `n` bytes (used to provoke writer overflow).
#[derive(Debug, Clone, PartialEq)]
pub struct Blob {
    pub n: usize,
}

impl Message for Blob {
    const TYPE_NAME: &'static str = "test_msgs::msg::dds_::Blob_";
    const MAX_SERIALIZED_SIZE: usize = 4096;
    fn serialize(&self, w: &mut CdrWriter) {
        for _ in 0..self.n {
            w.u8_val(0xAB);
        }
    }
    fn deserialize(_: &mut CdrReader<'_>) -> Result<Self, Error> {
        Err(Error::Deserialization)
    }
}

// ── Mock transport ──────────────────────────────────────────────────────────

#[derive(Default)]
pub struct Shared {
    pub rx: VecDeque<u8>,
    pub tx: Vec<u8>,
    pub closed: bool,
    /// Max bytes returned per read (fragmentation).
    pub chunk: usize,
    pub resets: usize,
}

#[derive(Clone)]
pub struct Mock(pub Arc<Mutex<Shared>>);

impl Mock {
    pub fn new(chunk: usize) -> Self {
        Mock(Arc::new(Mutex::new(Shared {
            chunk,
            ..Default::default()
        })))
    }
    pub fn s(&self) -> MutexGuard<'_, Shared> {
        self.0.lock().unwrap()
    }
}

impl embedded_io_async::ErrorType for Mock {
    type Error = embedded_io_async::ErrorKind;
}

impl embedded_io_async::Read for Mock {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        poll_fn(|_| {
            let mut s = self.s();
            if !s.rx.is_empty() {
                let n = buf.len().min(s.chunk).min(s.rx.len());
                for b in buf.iter_mut().take(n) {
                    *b = s.rx.pop_front().unwrap();
                }
                Poll::Ready(Ok(n))
            } else if s.closed {
                Poll::Ready(Ok(0))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl embedded_io_async::Write for Mock {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.s().tx.extend_from_slice(buf);
        Ok(buf.len())
    }
}

impl Transport for Mock {
    fn reset(&mut self) {
        let mut s = self.s();
        s.resets += 1;
        s.rx.clear();
    }
}

// ── Fake Agent ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Created {
    pub seq: u16,
    pub kind: u8,
    pub oid: u16,
    pub xml: String,
}

pub const AGENT_EPOCH_SECS: i32 = 1_700_000_000;

pub struct FakeAgent {
    pub mock: Mock,
    parsed: usize,
    pub client_creates: usize,
    pub created: Vec<Created>,
    pub reads: Vec<u16>,
    pub writes: Vec<(u16, Vec<u8>)>,
    pub timestamps: usize,
    pub answer_timestamps: bool,
    pub silent: bool,
}

impl FakeAgent {
    pub fn new(mock: Mock) -> Self {
        Self {
            mock,
            parsed: 0,
            client_creates: 0,
            created: vec![],
            reads: vec![],
            writes: vec![],
            timestamps: 0,
            answer_timestamps: true,
            silent: false,
        }
    }

    pub fn agent_now() -> Timestamp {
        let local = Instant::now().as_micros() as i64 * 1000;
        Timestamp::from_nanos(AGENT_EPOCH_SECS as i64 * 1_000_000_000 + local)
    }

    pub fn push_frame(&self, frame: &[u8]) {
        let mut s = self.mock.s();
        s.rx.extend((frame.len() as u16).to_le_bytes());
        s.rx.extend(frame.iter().copied());
    }

    pub fn send_data(&self, dr_oid: u16, body: &[u8]) {
        let mut f = vec![0x81, 0x01, 0, 0, 9, 1];
        f.extend(((4 + body.len()) as u16).to_le_bytes());
        f.extend([0, 1]);
        f.extend(dr_oid.to_be_bytes());
        f.extend_from_slice(body);
        self.push_frame(&f);
    }

    /// Handle all complete frames the client wrote. Returns true if any.
    pub fn process(&mut self) -> bool {
        let mut any = false;
        loop {
            let frame = {
                let s = self.mock.s();
                let tx = &s.tx[self.parsed..];
                if tx.len() < 2 {
                    break;
                }
                let len = u16::from_le_bytes([tx[0], tx[1]]) as usize;
                if tx.len() < 2 + len {
                    break;
                }
                let f = tx[2..2 + len].to_vec();
                self.parsed += 2 + len;
                f
            };
            any = true;
            self.handle(&frame);
        }
        any
    }

    fn handle(&mut self, f: &[u8]) {
        let hdr = if f[0] >= 0x80 { 4 } else { 8 };
        let seq = u16::from_le_bytes([f[2], f[3]]);
        let id = f[hdr];
        let p = &f[hdr + 4..];
        match id {
            0 => {
                self.client_creates += 1;
                if !self.silent {
                    self.push_frame(&[0x81, 0, 0, 0, 4, 1, 2, 0, 0, 0]);
                }
            }
            1 => {
                let req = [p[0], p[1]];
                let oid = u16::from_be_bytes([p[2], p[3]]);
                let len = u32::from_le_bytes([p[8], p[9], p[10], p[11]]) as usize;
                let xml = String::from_utf8(p[12..12 + len - 1].to_vec()).unwrap();
                self.created.push(Created {
                    seq,
                    kind: p[4],
                    oid,
                    xml,
                });
                if !self.silent {
                    let o = oid.to_be_bytes();
                    self.push_frame(&[0x81, 0, 0, 0, 5, 1, 6, 0, req[0], req[1], o[0], o[1], 0, 0]);
                }
            }
            7 => self
                .writes
                .push((u16::from_be_bytes([p[2], p[3]]), p[4..].to_vec())),
            8 => self.reads.push(u16::from_be_bytes([p[2], p[3]])),
            14 => {
                self.timestamps += 1;
                if self.answer_timestamps && !self.silent {
                    let now = Self::agent_now();
                    let mut r = vec![0x81, 0, 0, 0, 15, 1, 24, 0];
                    for _ in 0..2 {
                        r.extend(now.sec.to_le_bytes());
                        r.extend(now.nanosec.to_le_bytes());
                    }
                    r.extend_from_slice(&p[0..8]);
                    self.push_frame(&r);
                }
            }
            _ => {}
        }
    }
}

/// Poll `fut` to completion while the fake Agent answers; advances mock time
/// by 10 ms whenever nothing happens. Panics after `max_ms` of mock time.
pub fn drive<F: Future>(mut fut: Pin<&mut F>, agent: &mut FakeAgent, max_ms: u64) -> F::Output {
    let start = Instant::now();
    loop {
        if let Poll::Ready(v) = poll(fut.as_mut()) {
            return v;
        }
        if !agent.process() {
            advance(10);
        }
        assert!(
            Instant::now() - start <= Duration::from_millis(max_ms),
            "future did not complete"
        );
    }
}

/// Poll `fut` a few times letting the Agent respond, without advancing time.
pub fn pump<F: Future>(mut fut: Pin<&mut F>, agent: &mut FakeAgent) -> Option<F::Output> {
    for _ in 0..200 {
        if let Poll::Ready(v) = poll(fut.as_mut()) {
            return Some(v);
        }
        agent.process();
    }
    None
}
