//! The child's side of its socket: blocking frame reads, the control thread
//! that owns them, and the writer thread that sends every child frame in
//! order.
//!
//! The control thread owns the socket reads, so the end of the parent shows
//! whatever the task code does: at EOF or a failed read the child exits at
//! once with the parent-gone code; nothing can save its result, and a run
//! that went on could overlap a retry of the same task.

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::thread;

use tokio::sync::{mpsc, oneshot};

use crate::worker::executor::protocol::{
    decode_parent_frame, encode_child_frame, ChildFrame, FrameOverBound, FrameReader, ParentFrame,
    RawFrame, BAD_PARENT_FRAME_EXIT_CODE, PARENT_GONE_EXIT_CODE,
};

const CONTROL_THREAD_NAME: &str = "horsies-child-control";
const WRITER_THREAD_NAME: &str = "horsies-child-writer";
/// A frame with a body up to this size goes to the socket in one write.
const JOINED_WRITE_BODY_BYTES: usize = 1 << 16;

/// Why a blocking frame read ended without a frame.
#[derive(Debug)]
pub(super) enum ReadEnd {
    Eof,
    /// The read failed. Every caller treats it as the end of the parent, so
    /// the I/O error is not kept.
    Failed,
    OverBound(FrameOverBound),
}

/// Read exactly the next frame from `stream`. Each pass reads at least one
/// byte or ends, so the loop ends within the frame's bytes.
pub(super) fn read_frame<R: Read>(
    stream: &mut R,
    reader: &mut FrameReader,
) -> Result<RawFrame, ReadEnd> {
    loop {
        match reader.next_frame() {
            Ok(Some(raw)) => return Ok(raw),
            Ok(None) => {}
            Err(over) => return Err(ReadEnd::OverBound(over)),
        }
        match stream.read(reader.read_space()) {
            Ok(0) => return Err(ReadEnd::Eof),
            Ok(count) => reader.arrived(count),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_error) => return Err(ReadEnd::Failed),
        }
    }
}

/// Write one child frame. A frame that cannot be encoded is a defect of the
/// child host, which bounds every result body before it builds the frame.
pub(super) fn write_frame(mut socket: &UnixStream, frame: &ChildFrame) -> io::Result<()> {
    let encoded =
        encode_child_frame(frame).expect("the child host bounds every frame before it is sent");
    match encoded.body().len() <= JOINED_WRITE_BODY_BYTES {
        true => socket.write_all(&encoded.to_bytes()),
        false => match socket.write_all(encoded.head()) {
            Ok(()) => socket.write_all(encoded.body()),
            Err(error) => Err(error),
        },
    }
}

/// Start the control thread: it reads the parent's frames and hands each to
/// the serve loop, at most `bound` ahead. At EOF or a failed read it exits
/// the process with the parent-gone code; at a bad frame with the bad-frame
/// code.
pub(super) fn start_control_thread(
    socket: UnixStream,
    mut reader: FrameReader,
    bound: usize,
) -> io::Result<mpsc::Receiver<ParentFrame>> {
    let (frames, receiver) = mpsc::channel(bound);
    thread::Builder::new()
        .name(CONTROL_THREAD_NAME.to_owned())
        .spawn(move || {
            let mut stream = &socket;
            loop {
                let raw = match read_frame(&mut stream, &mut reader) {
                    Ok(raw) => raw,
                    Err(ReadEnd::Eof | ReadEnd::Failed) => {
                        std::process::exit(PARENT_GONE_EXIT_CODE)
                    }
                    Err(ReadEnd::OverBound(over)) => {
                        tracing::error!(
                            error = %over,
                            "Parent sent a frame over the bounds: {over}",
                        );
                        std::process::exit(BAD_PARENT_FRAME_EXIT_CODE)
                    }
                };
                match decode_parent_frame(&raw.meta, raw.body) {
                    Ok(frame) => {
                        if frames.blocking_send(frame).is_err() {
                            // The serve loop ended; the process is exiting.
                            return;
                        }
                    }
                    Err(refusal) => {
                        tracing::error!(error = %refusal, "Parent sent a bad frame: {refusal}");
                        std::process::exit(BAD_PARENT_FRAME_EXIT_CODE)
                    }
                }
            }
        })?;
    Ok(receiver)
}

/// What the writer thread takes, in order.
enum Outgoing {
    /// A frame to write.
    Frame(ChildFrame),
    /// Answered when every frame queued before it is written.
    Flush(oneshot::Sender<()>),
}

/// How a flush of the frame writer ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Flushed {
    /// Every frame queued before the flush is written.
    Written,
    /// The writer thread is gone: it ended without the answer. The thread
    /// exits the process when a write fails, so this is the end of the
    /// process.
    WriterGone,
}

/// The sender of child frames. One writer thread writes them in order; a
/// failed write means the parent is gone, and the process exits with the
/// parent-gone code.
#[derive(Clone)]
pub(super) struct FrameWriter {
    frames: mpsc::Sender<Outgoing>,
}

impl FrameWriter {
    pub(super) fn start(socket: UnixStream, bound: usize) -> io::Result<Self> {
        let (frames, mut receiver) = mpsc::channel::<Outgoing>(bound);
        thread::Builder::new()
            .name(WRITER_THREAD_NAME.to_owned())
            .spawn(move || {
                while let Some(outgoing) = receiver.blocking_recv() {
                    match outgoing {
                        Outgoing::Frame(frame) => {
                            if write_frame(&socket, &frame).is_err() {
                                std::process::exit(PARENT_GONE_EXIT_CODE);
                            }
                        }
                        Outgoing::Flush(written) => {
                            // The flush caller may have stopped waiting.
                            let _answered = written.send(());
                        }
                    }
                }
            })?;
        Ok(Self { frames })
    }

    /// Queue a frame from async code.
    pub(super) async fn send(&self, frame: ChildFrame) {
        // The receiver ends only with the writer thread, which exits the
        // process when it fails; at a stop the frame is no longer needed.
        let _ = self.frames.send(Outgoing::Frame(frame)).await;
    }

    /// Queue a frame from a thread that is not a runtime thread.
    pub(super) fn send_blocking(&self, frame: ChildFrame) {
        let _ = self.frames.blocking_send(Outgoing::Frame(frame));
    }

    /// Wait until every frame queued before this call is written. Not
    /// bounded: the caller bounds the wait.
    pub(super) async fn flush(&self) -> Flushed {
        let (written, answer) = oneshot::channel();
        match self.frames.send(Outgoing::Flush(written)).await {
            Ok(()) => {}
            Err(_writer_gone) => return Flushed::WriterGone,
        }
        match answer.await {
            Ok(()) => Flushed::Written,
            Err(_writer_gone) => Flushed::WriterGone,
        }
    }

    /// A writer with no writer thread: its frames stay in the queue until
    /// the test takes them from [`QueuedFrames`]. A send waits while `bound`
    /// frames are queued.
    #[cfg(test)]
    pub(super) fn without_thread(bound: usize) -> (Self, QueuedFrames) {
        let (frames, queued) = mpsc::channel(bound);
        (Self { frames }, QueuedFrames { queued })
    }
}

/// The queue of a [`FrameWriter`] with no writer thread.
#[cfg(test)]
pub(super) struct QueuedFrames {
    queued: mpsc::Receiver<Outgoing>,
}

#[cfg(test)]
impl QueuedFrames {
    /// How many frames are queued now.
    pub(super) fn count(&self) -> usize {
        self.queued.len()
    }

    /// Take the next queued frame. A test of this queue sends no flush.
    pub(super) async fn take(&mut self) -> ChildFrame {
        match self.queued.recv().await {
            Some(Outgoing::Frame(frame)) => frame,
            Some(Outgoing::Flush(_)) => panic!("a test of the queued frames got a flush"),
            None => panic!("every sender of the queued frames is gone"),
        }
    }
}
