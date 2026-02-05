#![allow(dead_code)] // Remove this once the full surface is used

use std::{
    io::{stdout, Stdout},
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::errors::Result;
use crossterm::{
    cursor,
    event::{
        self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event as CrosstermEvent, KeyEvent, KeyEventKind, MouseEvent,
    },
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::backend::CrosstermBackend as Backend;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
    time::interval,
};

use crate::action::ListenerState;

/// Batch of NOTIFY events coalesced during debounce window.
#[derive(Clone, Debug, Default)]
pub struct NotifyBatch {
    pub task_status: bool,
    pub workflow_status: bool,
    pub worker_state: bool,
}

#[derive(Clone, Debug)]
pub enum Event {
    Init,
    Quit,
    Error,
    Closed,
    Tick,
    Render,
    FocusGained,
    FocusLost,
    Paste(String),
    Key(KeyEvent),
    Mouse(MouseEvent),
    Resize(u16, u16),
    /// Database NOTIFY events (debounced batch)
    DbNotify(NotifyBatch),
    /// Listener connection state changed
    ListenerStateChanged(ListenerState),
}

pub struct Tui {
    pub terminal: ratatui::Terminal<Backend<Stdout>>,
    pub task: JoinHandle<()>,
    pub event_rx: Receiver<Event>,
    pub event_tx: Sender<Event>,
    pub frame_rate: f64,
    pub tick_rate: f64,
    pub mouse: bool,
    pub paste: bool,
    stop_flag: Arc<AtomicBool>,
}

impl Tui {
    pub fn new() -> Result<Self> {
        let (event_tx, event_rx) = mpsc::channel(1024);
        Ok(Self {
            terminal: ratatui::Terminal::new(Backend::new(stdout()))?,
            task: tokio::spawn(async {}),
            event_rx,
            event_tx,
            frame_rate: 60.0,
            tick_rate: 4.0,
            mouse: false,
            paste: false,
            stop_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn tick_rate(mut self, tick_rate: f64) -> Self {
        self.tick_rate = tick_rate;
        self
    }

    pub fn frame_rate(mut self, frame_rate: f64) -> Self {
        self.frame_rate = frame_rate;
        self
    }

    pub fn mouse(mut self, mouse: bool) -> Self {
        self.mouse = mouse;
        self
    }

    pub fn paste(mut self, paste: bool) -> Self {
        self.paste = paste;
        self
    }

    pub fn start(&mut self) {
        self.cancel();
        self.stop_flag = Arc::new(AtomicBool::new(false));
        let event_tx = self.event_tx.clone();
        let stop_flag = Arc::clone(&self.stop_flag);
        let tick_rate = self.tick_rate;
        let frame_rate = self.frame_rate;

        let task = tokio::spawn(async move {
            let tick_handle = tokio::spawn(Self::tick_loop(
                Arc::clone(&stop_flag),
                event_tx.clone(),
                tick_rate,
            ));
            let render_handle = tokio::spawn(Self::render_loop(
                Arc::clone(&stop_flag),
                event_tx.clone(),
                frame_rate,
            ));
            let event_handle = tokio::task::spawn_blocking(move || {
                Self::event_reader(stop_flag, event_tx);
            });

            let _ = tokio::join!(tick_handle, render_handle);
            let _ = event_handle.await;
        });

        self.task = task;
        let _ = self.event_tx.try_send(Event::Init);
    }

    async fn tick_loop(
        stop_flag: Arc<AtomicBool>,
        event_tx: Sender<Event>,
        tick_rate: f64,
    ) {
        let mut tick_interval = interval(Duration::from_secs_f64(1.0 / tick_rate));
        loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            tick_interval.tick().await;
            let _ = event_tx.try_send(Event::Tick);
        }
    }

    async fn render_loop(
        stop_flag: Arc<AtomicBool>,
        event_tx: Sender<Event>,
        frame_rate: f64,
    ) {
        let mut render_interval = interval(Duration::from_secs_f64(1.0 / frame_rate));
        loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            render_interval.tick().await;
            let _ = event_tx.try_send(Event::Render);
        }
    }

    fn event_reader(stop_flag: Arc<AtomicBool>, event_tx: Sender<Event>) {
        while !stop_flag.load(Ordering::Relaxed) {
            match event::poll(Duration::from_millis(50)) {
                Ok(true) => match event::read() {
                    Ok(CrosstermEvent::Key(key)) if key.kind == KeyEventKind::Press => {
                        let _ = event_tx.try_send(Event::Key(key));
                    }
                    Ok(CrosstermEvent::Mouse(mouse)) => {
                        let _ = event_tx.try_send(Event::Mouse(mouse));
                    }
                    Ok(CrosstermEvent::Resize(x, y)) => {
                        let _ = event_tx.try_send(Event::Resize(x, y));
                    }
                    Ok(CrosstermEvent::FocusLost) => {
                        let _ = event_tx.try_send(Event::FocusLost);
                    }
                    Ok(CrosstermEvent::FocusGained) => {
                        let _ = event_tx.try_send(Event::FocusGained);
                    }
                    Ok(CrosstermEvent::Paste(content)) => {
                        let _ = event_tx.try_send(Event::Paste(content));
                    }
                    Ok(_) => {}
                    Err(_) => {
                        let _ = event_tx.try_send(Event::Error);
                        stop_flag.store(true, Ordering::Relaxed);
                        break;
                    }
                },
                Ok(false) => continue,
                Err(_) => {
                    let _ = event_tx.try_send(Event::Error);
                    stop_flag.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }
    }

    pub fn stop(&self) -> Result<()> {
        self.cancel();
        let mut counter = 0;
        while !self.task.is_finished() {
            std::thread::sleep(Duration::from_millis(1));
            counter += 1;
            if counter > 50 {
                self.task.abort();
            }
            if counter > 100 {
                eprintln!("Failed to abort task in 100 milliseconds for unknown reason");
                break;
            }
        }
        Ok(())
    }

    pub fn enter(&mut self) -> Result<()> {
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(stdout(), EnterAlternateScreen, cursor::Hide)?;
        if self.mouse {
            crossterm::execute!(stdout(), EnableMouseCapture)?;
        }
        if self.paste {
            crossterm::execute!(stdout(), EnableBracketedPaste)?;
        }
        self.start();
        Ok(())
    }

    pub fn exit(&mut self) -> Result<()> {
        self.stop()?;
        if crossterm::terminal::is_raw_mode_enabled()? {
            self.flush()?;
            if self.paste {
                crossterm::execute!(stdout(), DisableBracketedPaste)?;
            }
            if self.mouse {
                crossterm::execute!(stdout(), DisableMouseCapture)?;
            }
            crossterm::execute!(stdout(), LeaveAlternateScreen, cursor::Show)?;
            crossterm::terminal::disable_raw_mode()?;
        }
        Ok(())
    }

    pub fn cancel(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }

    pub fn suspend(&mut self) -> Result<()> {
        self.exit()?;
        Ok(())
    }

    pub fn resume(&mut self) -> Result<()> {
        self.enter()?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        self.event_rx.recv().await
    }
}

impl Deref for Tui {
    type Target = ratatui::Terminal<Backend<Stdout>>;

    fn deref(&self) -> &Self::Target {
        &self.terminal
    }
}

impl DerefMut for Tui {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terminal
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        let _ = self.exit();
    }
}
