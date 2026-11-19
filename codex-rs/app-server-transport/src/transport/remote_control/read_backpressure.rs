use super::protocol::ClientEnvelope;
use super::protocol::ClientEvent;
use super::protocol::ClientId;
use super::protocol::ServerEvent;
use super::protocol::StreamId;
use crate::outgoing_message::OutgoingMessage;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::RequestId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

type StreamKey = (ClientId, StreamId);
type ReadKey = (ClientId, StreamId, RequestId);

#[derive(Default)]
pub(super) struct ReadCancellation {
    enabled: HashSet<StreamKey>,
    reads: HashMap<ReadKey, CancellationToken>,
}

impl ReadCancellation {
    pub(super) fn incoming(&mut self, envelope: &ClientEnvelope) -> bool {
        let Some(stream_id) = envelope.stream_id.as_ref() else {
            return false;
        };
        if !self
            .enabled
            .contains(&(envelope.client_id.clone(), stream_id.clone()))
        {
            return false;
        }
        let ClientEvent::ClientMessage { message } = &envelope.event else {
            return false;
        };
        match message {
            JSONRPCMessage::Request(request) if request.method == "thread/items/list" => {
                self.reads.insert(
                    (
                        envelope.client_id.clone(),
                        stream_id.clone(),
                        request.id.clone(),
                    ),
                    CancellationToken::new(),
                );
                false
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "$/cancelRequest" =>
            {
                let Some(id) = notification
                    .params
                    .as_ref()
                    .and_then(|p| p.get("id"))
                    .and_then(|id| serde_json::from_value::<RequestId>(id.clone()).ok())
                else {
                    return false;
                };
                if let Some(token) =
                    self.reads
                        .get(&(envelope.client_id.clone(), stream_id.clone(), id))
                {
                    token.cancel();
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub(super) fn outgoing(
        &mut self,
        client: &ClientId,
        stream: &StreamId,
        event: &ServerEvent,
    ) -> Option<(RequestId, CancellationToken)> {
        let ServerEvent::ServerMessage { message } = event else {
            return None;
        };
        let id = match message.as_ref() {
            OutgoingMessage::Response(response) => {
                if let ClientResponsePayload::Initialize(result) = response.result.as_ref() {
                    let key = (client.clone(), stream.clone());
                    if result.remote_control_read_cancellation {
                        self.enabled.insert(key);
                    } else {
                        self.enabled.remove(&key);
                    }
                }
                &response.id
            }
            OutgoingMessage::Error(error) => &error.id,
            _ => return None,
        };
        self.reads
            .get(&(client.clone(), stream.clone(), id.clone()))
            .cloned()
            .map(|token| (id.clone(), token))
    }

    pub(super) fn finish(&mut self, client: &ClientId, stream: &StreamId, id: &RequestId) {
        self.reads
            .remove(&(client.clone(), stream.clone(), id.clone()));
    }

    pub(super) fn close(&mut self, client: &ClientId, stream: Option<&StreamId>) {
        self.enabled
            .retain(|(c, s)| c != client || stream.is_some_and(|expected| s != expected));
        self.reads.retain(|(c, s, _), token| {
            let retain = c != client || stream.is_some_and(|expected| s != expected);
            if !retain {
                token.cancel();
            }
            retain
        });
    }
}

#[derive(Default)]
pub(super) struct SocketSendQueue {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    socket: Option<std::os::fd::OwnedFd>,
}

impl SocketSendQueue {
    pub(super) fn new(connection: &codex_websocket_client::WebSocketConnection) -> Self {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            match connection.direct_tcp_socket() {
                Ok(socket) => Self { socket },
                Err(error) => {
                    tracing::warn!(%error, "could not inspect remote-control TCP send queue");
                    Self::default()
                }
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = connection;
            Self::default()
        }
    }

    pub(super) async fn admit(
        &mut self,
        bytes: usize,
        cancel: &CancellationToken,
        shutdown: &CancellationToken,
    ) -> bool {
        const CREDIT: usize = 64 * 1024;
        loop {
            if cancel.is_cancelled() || shutdown.is_cancelled() {
                return false;
            }
            let queued = match self.queued() {
                Ok(Some(queued)) => queued,
                Ok(None) => return true,
                Err(error) => {
                    tracing::warn!(%error, "remote-control TCP send-queue inspection failed");
                    #[cfg(any(target_os = "linux", target_os = "macos"))]
                    {
                        self.socket = None;
                    }
                    return true;
                }
            };
            if queued == 0 || queued.saturating_add(bytes) <= CREDIT {
                return true;
            }
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = shutdown.cancelled() => return false,
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
    }

    fn queued(&self) -> std::io::Result<Option<usize>> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::AsRawFd;
            let Some(socket) = &self.socket else {
                return Ok(None);
            };
            let mut queued: libc::c_int = 0;
            #[cfg(target_os = "linux")]
            let result = unsafe { libc::ioctl(socket.as_raw_fd(), libc::TIOCOUTQ, &mut queued) };
            #[cfg(target_os = "macos")]
            let result = {
                let mut len = std::mem::size_of_val(&queued) as libc::socklen_t;
                unsafe {
                    libc::getsockopt(
                        socket.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_NWRITE,
                        (&mut queued as *mut libc::c_int).cast(),
                        &mut len,
                    )
                }
            };
            if result == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Some(queued.max(0) as usize))
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Ok(None)
        }
    }
}
