fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "src/protodefs/raftserver.proto",
            "src/protodefs/common.proto",
            "src/protodefs/shardserver.proto",
            "src/protodefs/manager.proto",
            "src/protodefs/client.proto",
            "src/protodefs/clustermanagement.proto",
            "src/protodefs/chainmanagement.proto",
        ],
        &["src/protodefs"],
    )?;
    Ok(())
}
