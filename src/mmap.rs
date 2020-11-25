use std::ptr;
use std::ffi::c_void;
use std::os::unix::io::AsRawFd;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use nix::sys::mman;

#[derive(Debug)]
pub struct Handle {
    ptr: *mut c_void,
    len: usize,
}

unsafe impl Sync for Handle {}
unsafe impl Send for Handle {}

impl Handle {
    pub unsafe fn new<F>(as_fd: F, len: usize) -> nix::Result<Self>
    where
        F: AsRawFd,
    {
        if len == 0 {
            return Ok(Self { ptr: ptr::null_mut(), len: 0 });
        }
        let ptr = mman::mmap(
            ptr::null_mut(),
            len,
            mman::ProtFlags::PROT_READ | mman::ProtFlags::PROT_WRITE,
            mman::MapFlags::MAP_SHARED,
            as_fd.as_raw_fd(),
            0
        )?;
        Ok(Self { ptr, len })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if self.ptr == ptr::null_mut() {
            return;
        }
        unsafe {
            mman::munmap(self.ptr, self.len).expect("failed to munmap");
        }
    }
}

#[derive(Debug)]
pub struct Chunker {
    handle: Arc<Handle>,
    offset: usize,
}

impl Chunker {
    pub fn new(handle: Handle) -> Self {
        Chunker {
            handle: Arc::new(handle),
            offset: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.handle.len - self.offset
    }

    pub fn take_chunk(&mut self, len: usize) -> Chunk {
        assert!(self.size() >= len);
        let chunk = Chunk {
            handle: self.handle.clone(),
            offset: self.offset,
            len,
        };
        self.offset += len;
        return chunk;
    }
}

#[derive(Debug)]
pub struct Chunk {
    handle: Arc<Handle>,
    offset: usize,
    len: usize,
}

impl Deref for Chunk {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        if self.len == 0 {
            return &[];
        }
        unsafe {
            let ptr = (self.handle.ptr as *const u8).add(self.offset);
            std::slice::from_raw_parts(ptr, self.len)
        }
    }
}

impl DerefMut for Chunk {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if self.len == 0 {
            return &mut [];
        }
        unsafe {
            let ptr = (self.handle.ptr as *mut u8).add(self.offset);
            std::slice::from_raw_parts_mut(ptr, self.len)
        }
    }
}
