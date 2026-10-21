//! Internal logging shims: defmt when enabled, otherwise no-ops.
#![allow(unused_macros)]

#[cfg(feature = "defmt")]
macro_rules! debug { ($($t:tt)*) => { ::defmt::debug!($($t)*) }; }
#[cfg(feature = "defmt")]
macro_rules! info { ($($t:tt)*) => { ::defmt::info!($($t)*) }; }
#[cfg(feature = "defmt")]
macro_rules! warn { ($($t:tt)*) => { ::defmt::warn!($($t)*) }; }

#[cfg(not(feature = "defmt"))]
macro_rules! debug { ($fmt:literal $(, $arg:expr)* $(,)?) => {{ $( let _ = &$arg; )* }}; }
#[cfg(not(feature = "defmt"))]
macro_rules! info { ($fmt:literal $(, $arg:expr)* $(,)?) => {{ $( let _ = &$arg; )* }}; }
#[cfg(not(feature = "defmt"))]
macro_rules! warn { ($fmt:literal $(, $arg:expr)* $(,)?) => {{ $( let _ = &$arg; )* }}; }
