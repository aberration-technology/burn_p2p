use super::*;

pub(super) fn load_or_create_keypair(path: &Path) -> Result<Keypair, Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if path.exists() {
        let bytes = std::fs::read(path)?;
        return Ok(Keypair::from_protobuf_encoding(&bytes)?);
    }

    let keypair = Keypair::generate_ed25519();
    std::fs::write(path, keypair.to_protobuf_encoding()?)?;
    Ok(keypair)
}
