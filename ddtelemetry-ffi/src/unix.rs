// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use std::{fs::File, mem, os::unix::{net::UnixStream, prelude::FromRawFd}};

use ddtelemetry::ipc::{example_interface::ExampleTransport, platform::PlatformHandle, sidecar};

use crate::{try_c, MaybeError};

pub struct NativeFile {
    handle: Box<PlatformHandle<File>>
}

pub struct NativeUnixStream {
    handle: PlatformHandle<UnixStream>
}

/// This creates Rust PlatformHandle<File> from supplied C std FILE object.
/// This method takes the ownership of the underlying filedescriptor.
///
/// # Safety
/// Caller must ensure the file descriptor associated with FILE pointer is open, and valid
/// Caller must not close the FILE associated filedescriptor after calling this fuction
#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn ddog_ph_file_from(file: *mut libc::FILE) -> NativeFile {
    let handle = PlatformHandle::from_raw_fd(libc::fileno(file));

    NativeFile { handle: Box::from( handle) }
}

#[no_mangle]
pub extern "C" fn ddog_ph_file_clone(
    platform_handle: &NativeFile,
) -> Box<NativeFile> {
    Box::new(NativeFile { handle: platform_handle.handle.clone() })
}

#[no_mangle]
pub extern "C" fn ddog_ph_file_drop(ph: NativeFile) {
    drop(ph)
}

#[no_mangle]
pub extern "C" fn ddog_ph_unix_stream_drop(ph: Box<NativeUnixStream>) {
    drop(ph)
}

#[no_mangle]
pub extern "C" fn ddog_example_transport_drop(transport: Box<ExampleTransport>) {
    drop(transport)
}

#[no_mangle]
/// # Safety
/// Caller must ensure the process is safe to fork, at the time when this method is called
#[no_mangle]
pub unsafe extern "C" fn ddog_sidecar_connect(
    connection: &mut *mut ExampleTransport,
) -> MaybeError {
    let stream = Box::new(try_c!(sidecar::start_or_connect_to_sidecar()));
    *connection = Box::into_raw(stream);

    MaybeError::None
}

#[no_mangle]
pub extern "C" fn ddog_sidecar_ping(transport: &mut Box<ExampleTransport>) -> MaybeError {
    let rv = try_c!(
        transport.send(ddtelemetry::ipc::example_interface::ExampleInterfaceRequest::Ping {})
    );

    match rv {
        ddtelemetry::ipc::example_interface::ExampleInterfaceResponse::Ping(_) => {}
        _ => return MaybeError::Some("wrong response".as_bytes().to_vec().into()),
    }
    MaybeError::None
}

#[cfg(test)]
mod test_c_sidecar {

    use super::*;
    use std::{ffi::CString, io::Write, os::unix::prelude::AsRawFd};

    #[test]
    fn test_ddog_ph_file_handling() {
        let fname = CString::new(std::env::temp_dir().join("test_file").to_str().unwrap()).unwrap();
        let mode = CString::new("a+").unwrap();

        let file = unsafe { libc::fopen(fname.as_ptr(), mode.as_ptr()) };
        let file = unsafe { ddog_ph_file_from(file) };
        let fd = file.handle.as_raw_fd();
        {
            let mut file = &*file.handle.as_filelike_view().unwrap();
            writeln!(file, "test").unwrap();
        }
        ddog_ph_file_drop(file);

        let mut file = unsafe { File::from_raw_fd(fd) };
        writeln!(file, "test").unwrap_err(); // file is closed, so write returns an error
    }

    #[test]
    #[ignore] // run all tests that can fork in a separate run, to avoid any race conditions with default rust test harness
    fn test_ddog_sidecar_connection() {
        let mut transport = std::ptr::null_mut();
        assert_eq!(
            unsafe { ddog_sidecar_connect(&mut transport) },
            MaybeError::None
        );
        let mut transport = unsafe { Box::from_raw(transport) };
        ddog_sidecar_ping(&mut transport);
        ddog_example_transport_drop(transport);
    }
}
