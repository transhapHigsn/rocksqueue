pub mod control_plane;

pub mod proto {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("controlplane_descriptor");

    tonic::include_proto!("controlplane");
}
