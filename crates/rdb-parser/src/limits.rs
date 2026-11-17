//! Constructor-time aggregate admission for the strict native snapshot profile.
use crate::RdbError;

/// Explicit bounds. Allocation and element allowances apply per yielded entry
/// or chunk; callers must separately bound the memory held by outputs they have
/// already received. Input, metadata and work remain cumulative across the
/// entire stream.
///
/// The struct is `#[non_exhaustive]` so new limits can be added without a breaking
/// change: start from [`Limits::unlimited`] and use the `with_*` setters to set
/// every bound you need. All limits must be non-zero for
/// [`RdbReader::new_limited`](crate::RdbReader::new_limited) to accept them.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Limits {
    /// Maximum bytes read, including the checksum (the single trailing-byte probe after
    /// the checksum is not counted).
    pub input_bytes: u64,
    /// Maximum parser allocation for one yielded entry/chunk, excluding metadata.
    pub allocated_bytes: u64,
    /// Cumulative raw AUX storage.
    pub metadata_bytes: u64,
    /// Maximum single string or compact block, before allocation/decompression.
    pub string_bytes: u64,
    /// Maximum elements per yielded entry/chunk (compact blocks charge a safe upper bound).
    pub elements: u64,
    /// Cumulative work units (input bytes plus conservative expansion units).
    pub work: u64,
}

impl Limits {
    /// Limits that never trip (every bound is `u64::MAX`).
    ///
    /// This keeps the strict profile's format checks (exact snapshot, raw AUX
    /// bytes, unsupported-opcode refusal) but imposes no resource bound, so it is
    /// only appropriate for trusted input. Tighten individual bounds with the
    /// `with_*` setters.
    pub const fn unlimited() -> Self {
        Self {
            input_bytes: u64::MAX,
            allocated_bytes: u64::MAX,
            metadata_bytes: u64::MAX,
            string_bytes: u64::MAX,
            elements: u64::MAX,
            work: u64::MAX,
        }
    }

    /// Sets [`Limits::input_bytes`].
    #[must_use]
    pub const fn with_input_bytes(mut self, value: u64) -> Self {
        self.input_bytes = value;
        self
    }

    /// Sets [`Limits::allocated_bytes`].
    #[must_use]
    pub const fn with_allocated_bytes(mut self, value: u64) -> Self {
        self.allocated_bytes = value;
        self
    }

    /// Sets [`Limits::metadata_bytes`].
    #[must_use]
    pub const fn with_metadata_bytes(mut self, value: u64) -> Self {
        self.metadata_bytes = value;
        self
    }

    /// Sets [`Limits::string_bytes`].
    #[must_use]
    pub const fn with_string_bytes(mut self, value: u64) -> Self {
        self.string_bytes = value;
        self
    }

    /// Sets [`Limits::elements`].
    #[must_use]
    pub const fn with_elements(mut self, value: u64) -> Self {
        self.elements = value;
        self
    }

    /// Sets [`Limits::work`].
    #[must_use]
    pub const fn with_work(mut self, value: u64) -> Self {
        self.work = value;
        self
    }
}

pub(crate) fn refused() -> RdbError {
    RdbError::ResourceLimit
}
pub(crate) fn debit(left: &mut u64, count: u64) -> Result<(), RdbError> {
    *left = left.checked_sub(count).ok_or_else(refused)?;
    Ok(())
}
pub(crate) fn vector<T>(capacity: usize) -> Result<Vec<T>, RdbError> {
    let mut value = Vec::new();
    value.try_reserve_exact(capacity).map_err(|_| refused())?;
    Ok(value)
}
pub(crate) fn copy(bytes: &[u8]) -> Result<Vec<u8>, RdbError> {
    let mut out = vector(bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(out)
}
/// Fallible `Vec::push` with amortised growth.
///
/// `try_reserve` grows geometrically, so a run of pushes costs O(n) in total
/// (reserving exactly one slot per push would reallocate and copy every time once
/// the capacity hint is exceeded, making it O(n^2)). Callers debit the element or
/// allocation budget for the whole batch before pushing, and geometric growth can
/// overshoot that charge by at most a factor of two, which the conservative
/// per-element charge absorbs.
pub(crate) fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), RdbError> {
    values.try_reserve(1).map_err(|_| refused())?;
    values.push(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_grows_geometrically() {
        let mut values: Vec<u32> = Vec::new();
        let mut reallocations = 0;
        let mut capacity = values.capacity();
        for i in 0..100_000u32 {
            push(&mut values, i).unwrap();
            if values.capacity() != capacity {
                capacity = values.capacity();
                reallocations += 1;
            }
        }
        assert_eq!(values.len(), 100_000);
        assert_eq!(values[99_999], 99_999);
        // Reserving one slot per push would reallocate on (almost) every push.
        assert!(reallocations < 64, "{reallocations} reallocations");
    }
}
