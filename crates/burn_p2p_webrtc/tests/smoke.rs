// Copyright 2022 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

#![allow(clippy::unwrap_used)]

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use burn_p2p_webrtc as webrtc;
use futures::{
    FutureExt, future,
    future::{BoxFuture, Either},
    ready,
    stream::StreamExt,
};
use libp2p_core::{
    Endpoint, Multiaddr, Transport,
    muxing::StreamMuxerBox,
    transport::{Boxed, DialOpts, ListenerId, PortUse, TransportEvent},
};
use libp2p_identity::PeerId;
use rand::thread_rng;
use tracing_subscriber::EnvFilter;

#[tokio::test]
async fn smoke() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let (a_peer_id, mut a_transport) = create_transport();
    let (b_peer_id, mut b_transport) = create_transport();

    let addr = start_listening(&mut a_transport, "/ip4/127.0.0.1/udp/0/webrtc-direct").await;
    start_listening(&mut b_transport, "/ip4/127.0.0.1/udp/0/webrtc-direct").await;
    let ((a_connected, _, _), (b_connected, _)) =
        connect(&mut a_transport, &mut b_transport, addr).await;

    assert_eq!(a_connected, b_peer_id);
    assert_eq!(b_connected, a_peer_id);
}

fn generate_tls_keypair() -> libp2p_identity::Keypair {
    libp2p_identity::Keypair::generate_ed25519()
}

fn create_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
    let keypair = generate_tls_keypair();
    let peer_id = keypair.public().to_peer_id();

    let transport = webrtc::tokio::Transport::new(
        keypair,
        webrtc::tokio::Certificate::generate(&mut thread_rng()).unwrap(),
    )
    .map(|(p, c), _| (p, StreamMuxerBox::new(c)))
    .boxed();

    (peer_id, transport)
}

async fn start_listening(transport: &mut Boxed<(PeerId, StreamMuxerBox)>, addr: &str) -> Multiaddr {
    transport
        .listen_on(ListenerId::next(), addr.parse().unwrap())
        .unwrap();
    match transport.next().await {
        Some(TransportEvent::NewAddress { listen_addr, .. }) => listen_addr,
        e => panic!("{e:?}"),
    }
}

async fn connect(
    a_transport: &mut Boxed<(PeerId, StreamMuxerBox)>,
    b_transport: &mut Boxed<(PeerId, StreamMuxerBox)>,
    addr: Multiaddr,
) -> (
    (PeerId, Multiaddr, StreamMuxerBox),
    (PeerId, StreamMuxerBox),
) {
    match futures::future::select(
        ListenUpgrade::new(a_transport),
        Dial::new(b_transport, addr),
    )
    .await
    {
        Either::Left((listen_done, dial)) => {
            let mut pending_dial = dial;

            loop {
                match future::select(pending_dial, a_transport.next()).await {
                    Either::Left((dial_done, _)) => return (listen_done, dial_done),
                    Either::Right((_, dial)) => {
                        pending_dial = dial;
                    }
                }
            }
        }
        Either::Right((dial_done, listen)) => {
            let mut pending_listen = listen;

            loop {
                match future::select(pending_listen, b_transport.next()).await {
                    Either::Left((listen_done, _)) => return (listen_done, dial_done),
                    Either::Right((_, listen)) => {
                        pending_listen = listen;
                    }
                }
            }
        }
    }
}

struct ListenUpgrade<'a> {
    listener: &'a mut Boxed<(PeerId, StreamMuxerBox)>,
    listener_upgrade_task: Option<BoxFuture<'static, (PeerId, Multiaddr, StreamMuxerBox)>>,
}

impl<'a> ListenUpgrade<'a> {
    pub(crate) fn new(listener: &'a mut Boxed<(PeerId, StreamMuxerBox)>) -> Self {
        Self {
            listener,
            listener_upgrade_task: None,
        }
    }
}

struct Dial<'a> {
    dialer: &'a mut Boxed<(PeerId, StreamMuxerBox)>,
    dial_task: BoxFuture<'static, (PeerId, StreamMuxerBox)>,
}

impl<'a> Dial<'a> {
    fn new(dialer: &'a mut Boxed<(PeerId, StreamMuxerBox)>, addr: Multiaddr) -> Self {
        Self {
            dial_task: dialer
                .dial(
                    addr,
                    DialOpts {
                        role: Endpoint::Dialer,
                        port_use: PortUse::Reuse,
                    },
                )
                .unwrap()
                .map(|r| r.unwrap())
                .boxed(),
            dialer,
        }
    }
}

impl Future for Dial<'_> {
    type Output = (PeerId, StreamMuxerBox);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.dialer.poll_next_unpin(cx) {
                Poll::Ready(_) => {
                    continue;
                }
                Poll::Pending => {}
            }

            let conn = ready!(self.dial_task.poll_unpin(cx));
            return Poll::Ready(conn);
        }
    }
}

impl Future for ListenUpgrade<'_> {
    type Output = (PeerId, Multiaddr, StreamMuxerBox);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.listener.poll_next_unpin(cx) {
                Poll::Ready(Some(TransportEvent::Incoming {
                    upgrade,
                    send_back_addr,
                    ..
                })) => {
                    self.listener_upgrade_task = Some(
                        async move {
                            let (peer, conn) = upgrade.await.unwrap();

                            (peer, send_back_addr, conn)
                        }
                        .boxed(),
                    );
                    continue;
                }
                Poll::Ready(None) => unreachable!("stream never ends"),
                Poll::Ready(Some(_)) => continue,
                Poll::Pending => {}
            }

            let conn = match self.listener_upgrade_task.as_mut() {
                None => return Poll::Pending,
                Some(inner) => ready!(inner.poll_unpin(cx)),
            };

            return Poll::Ready(conn);
        }
    }
}
