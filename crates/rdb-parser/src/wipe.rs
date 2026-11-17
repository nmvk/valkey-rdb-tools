//! Optional zeroization.
//!
//! With the `zeroize` feature these are the real `zeroize` items. Without it they are
//! no-op stand-ins with the same call-site API, so the parser needs no `cfg` attributes
//! around every buffer it would otherwise wipe.

#[cfg(feature = "zeroize")]
pub(crate) use zeroize::{Zeroize, Zeroizing};

#[cfg(not(feature = "zeroize"))]
pub(crate) use noop::{Zeroize, Zeroizing};

#[cfg(not(feature = "zeroize"))]
mod noop {
    use std::ops::{Deref, DerefMut};

    pub(crate) trait Zeroize {
        fn zeroize(&mut self);
    }

    impl<T: ?Sized> Zeroize for T {
        #[inline]
        fn zeroize(&mut self) {}
    }

    pub(crate) struct Zeroizing<Z>(Z);

    impl<Z> Zeroizing<Z> {
        #[inline]
        pub(crate) fn new(value: Z) -> Self {
            Self(value)
        }
    }

    impl<Z> Deref for Zeroizing<Z> {
        type Target = Z;

        #[inline]
        fn deref(&self) -> &Z {
            &self.0
        }
    }

    impl<Z> DerefMut for Zeroizing<Z> {
        #[inline]
        fn deref_mut(&mut self) -> &mut Z {
            &mut self.0
        }
    }
}
