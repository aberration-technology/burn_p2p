use super::*;

/// Represents a memory swarm shell.
pub struct MemorySwarmShell {
    local_peer_id: Libp2pPeerId,
    swarm: Swarm<dummy::Behaviour>,
}

impl MemorySwarmShell {
    /// Creates a new value.
    pub fn new() -> Self {
        Self::with_keypair(Keypair::generate_ed25519())
    }

    /// Returns a copy configured with the keypair.
    pub fn with_keypair(keypair: Keypair) -> Self {
        let local_peer_id = keypair.public().to_peer_id();
        let transport = MemoryTransport::default()
            .upgrade(upgrade::Version::V1)
            .authenticate(plaintext::Config::new(&keypair))
            .multiplex(yamux::Config::default())
            .boxed();
        let swarm = Swarm::new(
            transport,
            dummy::Behaviour,
            local_peer_id,
            Libp2pSwarmConfig::without_executor(),
        );

        Self {
            local_peer_id,
            swarm,
        }
    }

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    /// Performs the listen on operation.
    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .listen_on(address)
            .map(|_| ())
            .map_err(|error| SwarmError::Listen(error.to_string()))
    }

    /// Performs the dial operation.
    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .dial(address)
            .map_err(|error| SwarmError::Dial(error.to_string()))
    }

    /// Performs the connected peer count operation.
    pub fn connected_peer_count(&self) -> usize {
        self.swarm.network_info().num_peers()
    }

    /// Performs the poll event operation.
    pub fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<LiveSwarmEvent> {
        match Pin::new(&mut self.swarm).poll_next(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(LiveSwarmEvent::from(event)),
            Poll::Ready(None) => Poll::Ready(LiveSwarmEvent::Other {
                kind: "stream-closed".into(),
            }),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Performs the wait event operation.
    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveSwarmEvent> {
        let deadline = Instant::now() + timeout;
        let waker = futures::task::noop_waker_ref();

        loop {
            let mut cx = Context::from_waker(waker);
            match self.poll_event(&mut cx) {
                Poll::Ready(event) => return Some(event),
                Poll::Pending if Instant::now() >= deadline => return None,
                Poll::Pending => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

impl Default for MemorySwarmShell {
    fn default() -> Self {
        Self::new()
    }
}
