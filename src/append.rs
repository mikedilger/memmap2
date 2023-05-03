
use crate::advice::Advice;
use crate::os::MmapInner;
use crate::{MmapAsRawDesc, MmapOptions};

use std::fmt;
use std::io::{self, Result};
use std::ops::Deref;
use std::slice;

/// A handle to an append-only memory mapped buffer.
pub struct MmapAppend {
    pub(crate) inner: MmapInner,
}

impl MmapAppend {
    pub unsafe fn map_append<T: MmapAsRawDesc>(file: T) -> Result<MmapAppend> {
        let map = MmapOptions::new().map_append(file)?;

        // File must be long enough for 2 usize records at the front
        if file.len() < 2 * std::mem::size_of::<usize>() {
            return Err(io::Error::new(io::ErrorKind::Other, "File not large enough."));
        }

        // The first usize record is a lock. 0 means unlocked, anything else is locked.
        let lock: usize = 0;
        lock.write_to_buffer(&mut self.deref_mut()[0..])?;

        // FIXME put the lock and size in the front

        Ok(map)
    }

    pub fn append(&self, data: &[u8]) -> Result<()> {
        // FIXME add this functionality
        Ok(())
    }

    #[inline]
    fn get_append_lock(&self) {
        usize
    }

    pub fn flush(&self) -> Result<()> {
        let len = self.len();
        self.inner.flush(0, len)
    }

    pub fn flush_async(&self) -> Result<()> {
        let len = self.len();
        self.inner.flush_async(0, len)
    }

    pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        self.inner.flush(offset, len)
    }

    pub fn flush_async_range(&self, offset: usize, len: usize) -> Result<()> {
        self.inner.flush_async(offset, len)
    }

    pub fn advise(&self, advice: Advice) -> Result<()> {
        self.inner.advise(advice, 0, self.inner.len())
    }

    pub fn advise_range(&self, advice: Advice, offset: usize, len: usize) -> Result<()> {
        self.inner.advise(advice, offset, len)
    }

    pub fn lock(&mut self) -> Result<()> {
        self.inner.lock()
    }

    pub fn unlock(&mut self) -> Result<()> {
        self.inner.unlock()
    }
}

#[cfg(feature = "stable_deref_trait")]
unsafe impl stable_deref_trait::StableDeref for MmapAppend {}

impl Deref for MmapAppend {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.inner.ptr(), self.inner.len()) }
    }
}

impl AsRef<[u8]> for MmapAppend {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl fmt::Debug for MmapAppend {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MmapAppend")
            .field("ptr", &self.as_ptr())
            .field("len", &self.len())
            .finish()
    }
}
