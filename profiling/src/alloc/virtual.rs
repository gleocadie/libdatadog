// Copyright 2023-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use super::{pad_to, page_size, InBoundsPtr};
use core::ptr;
use std::io;

#[cfg(unix)]
mod os {
    use std::{io, ptr};

    /// Allocates virtual memory of the given size, which must be a
    /// multiple of a page boundary and may not be zero.
    pub fn virtual_alloc(size: usize) -> io::Result<ptr::NonNull<[u8]>> {
        let result = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };

        if result == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        if result.is_null() {
            unsafe { libc::munmap(result, size as libc::size_t) };
            Err(io::Error::new(
                io::ErrorKind::Other,
                "mmap returned a null pointer",
            ))
        } else {
            let slice = ptr::slice_from_raw_parts_mut(result.cast(), size);
            // SAFETY: checked that the ptr was not null above.
            Ok(unsafe { ptr::NonNull::new_unchecked(slice) })
        }
    }

    /// # Safety
    ///  1. The fatptr must have been previously allocated by [virtual_alloc].
    ///  2. The size should be the exact size it was returned with.
    pub unsafe fn virtual_free(fatptr: ptr::NonNull<[u8]>) -> io::Result<()> {
        let result = libc::munmap(fatptr.as_ptr().cast(), fatptr.len() as libc::size_t);
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
mod os {
    use std::{io, ptr};
    use windows_sys::Win32::System::Memory;

    pub fn virtual_alloc(size: usize) -> io::Result<ptr::NonNull<[u8]>> {
        let result = unsafe {
            Memory::VirtualAlloc(
                ptr::null(),
                size,
                Memory::MEM_COMMIT | Memory::MEM_RESERVE,
                Memory::PAGE_READWRITE,
            )
        };
        if result.is_null() {
            Err(io::Error::last_os_error())
        } else {
            let slice = ptr::slice_from_raw_parts_mut(result.cast(), size);
            // SAFETY: checked that the ptr was not null above.
            Ok(unsafe { ptr::NonNull::new_unchecked(slice) })
        }
    }

    /// # Safety
    ///  1. The fatptr must have been previously allocated by [virtual_alloc].
    ///  2. The size should be the exact size it was returned with.
    pub unsafe fn virtual_free(fatptr: ptr::NonNull<[u8]>) -> io::Result<()> {
        let result = Memory::VirtualFree(fatptr.as_ptr().cast(), 0, Memory::MEM_RELEASE);
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

pub use os::*;

#[repr(C)]
pub struct Mapping {
    fatptr: ptr::NonNull<[u8]>,
}

/// # Safety
/// A mapping can move to a new thread, no problem. It's not Sync, though.
unsafe impl Send for Mapping {}

impl Mapping {
    /// Pads `min_size` to the page size, and creates a new mapping of the
    /// padded size.
    pub fn new(min_size: usize) -> io::Result<Mapping> {
        let page_size = page_size();
        match pad_to(min_size, page_size) {
            Some(size) if size <= isize::MAX as usize => {
                let fatptr = virtual_alloc(size)?;
                Ok(Mapping { fatptr })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("requested virtual allocation of {min_size} bytes was too large (possibly after padding)"),
            )),
        }
    }

    #[inline]
    pub fn base_in_bounds_ptr(&self) -> InBoundsPtr {
        let base = self.fatptr;
        InBoundsPtr { base, offset: 0 }
    }

    #[inline]
    pub fn allocation_size(&self) -> usize {
        self.fatptr.len()
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: passing ptr and size exactly as received from alloc.
        let _result = unsafe { virtual_free(self.fatptr) };

        // If this fails, there's not much that can be done about it. It
        // could panic but panic in drops are generally frowned on.
        // Compromise: in debug builds, panic if it's invalid but in
        // release builds just move on.
        #[cfg(debug_assertions)]
        if let Err(err) = _result {
            panic!("failed to drop mapping: {err}");
        }
    }
}