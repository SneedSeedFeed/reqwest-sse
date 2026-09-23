use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A Server-Sent Events message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The event data payload.
    pub data: String,
    /// The optional event name.
    pub event: Option<String>,
    /// The optional event id.
    pub id: Option<String>,
    /// The optional retry value, in milliseconds.
    pub retry: Option<u64>,
}

#[derive(Debug)]
struct EventState {
    data: String,
    has_data: bool,
    event: Option<String>,
    id: Option<String>,
    retry: Option<u64>,
}

impl EventState {
    fn new() -> Self {
        Self {
            data: String::new(),
            has_data: false,
            event: None,
            id: None,
            retry: None,
        }
    }

    fn reset(&mut self) {
        self.data.clear();
        self.has_data = false;
        self.event = None;
        // self.id = None;
        self.retry = None;
    }

    fn build_event(&mut self) -> Option<SseEvent> {
        if !self.has_data {
            self.reset();
            return None;
        }

        let event = SseEvent {
            data: std::mem::take(&mut self.data),
            event: self.event.take(),
            id: self.id.clone(),
            retry: self.retry.take(),
        };

        self.has_data = false;

        Some(event)
    }

    fn push_data(&mut self, value: &str) {
        if self.has_data {
            self.data.push('\n');
        }
        self.data.push_str(value);
        self.has_data = true;
    }
}

pin_project! {
    /// A stream adapter that parses Server-Sent Events from a byte stream.
    #[derive(Debug)]
    pub struct SseStream<S> {
        #[pin]
        inner: S,
        buffer: BytesMut,
        state: EventState,
        // where to scan for EOL from (to avoid re-scanning)
        scan_pos: usize,
        // have we seen and skipped past the byte order mark yet?
        bom_handled: bool,
        done: bool,
    }
}

impl<S> SseStream<S>
where
    S: Stream<Item = crate::Result<Bytes>>,
{
    /// Create a new SSE stream from a bytes stream.
    pub fn new(stream: S) -> Self {
        Self {
            inner: stream,
            buffer: BytesMut::new(),
            state: EventState::new(),
            scan_pos: 0,
            bom_handled: false,
            done: false,
        }
    }

    fn process_line(state: &mut EventState, line: &[u8]) -> crate::Result<Option<SseEvent>> {
        if line.is_empty() {
            return Ok(state.build_event());
        }

        if line[0] == b':' {
            return Ok(None);
        }

        let (field_bytes, value_bytes) = match line.iter().position(|&b| b == b':') {
            Some(idx) => {
                let mut value = &line[idx + 1..];
                if value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                (&line[..idx], value)
            }
            None => (line, &b""[..]),
        };

        //let field = std::str::from_utf8(field_bytes).map_err(crate::error::decode)?;
        let value = std::str::from_utf8(value_bytes).map_err(crate::error::decode)?;

        match field_bytes {
            b"data" => state.push_data(value),
            b"event" => state.event = Some(value.to_string()),
            b"id" => {
                if !value.contains('\0') {
                    state.id = Some(value.to_string());
                }
            }
            b"retry" => {
                if let Ok(retry) = value.parse::<u64>() {
                    state.retry = Some(retry);
                }
            }
            _ => {}
        }

        Ok(None)
    }

    fn poll_event_from_buffer(
        state: &mut EventState,
        buffer: &mut BytesMut,
        scan_pos: &mut usize,
        bom_handled: &mut bool,
    ) -> crate::Result<Option<SseEvent>> {
        if !*bom_handled {
            match buffer.as_ref() {
                [0xEF, 0xBB, 0xBF, ..] => {
                    buffer.split_to(3);
                    *bom_handled = true;
                }
                [0xEF] | [0xEF, 0xBB] => return Ok(None),
                _ => *bom_handled = true,
            }
        }

        loop {
            while *scan_pos < buffer.len() {
                let pos = *scan_pos;

                let line_end = match buffer[pos] {
                    b'\n' => Some(pos + 1),
                    b'\r' if pos + 1 == buffer.len() => {
                        // this could be CR or CRLF and we need more data to prove it
                        return Ok(None);
                    }
                    b'\r' if buffer[pos + 1] == b'\n' => Some(pos + 2),
                    b'\r' => Some(pos + 1),
                    _ => {
                        *scan_pos += 1;
                        None
                    }
                };

                let Some(line_end) = line_end else {
                    continue;
                };

                let line_bytes = buffer.split_to(line_end);
                *scan_pos = 0;

                let line = &line_bytes[..pos];

                if let Some(event) = Self::process_line(state, line)? {
                    return Ok(Some(event));
                }
            }

            return Ok(None);
        }
    }
}

impl<S> Stream for SseStream<S>
where
    S: Stream<Item = crate::Result<Bytes>>,
{
    type Item = crate::Result<SseEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        loop {
            match Self::poll_event_from_buffer(
                this.state,
                this.buffer,
                this.scan_pos,
                this.bom_handled,
            ) {
                Ok(Some(event)) => return Poll::Ready(Some(Ok(event))),
                Ok(None) => {}
                Err(err) => {
                    *this.done = true;
                    return Poll::Ready(Some(Err(err)));
                }
            }

            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    this.buffer.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Err(err))) => {
                    *this.done = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    *this.done = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}
