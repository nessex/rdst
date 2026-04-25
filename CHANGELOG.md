# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased: 0.21.0](https://github.com/nessex/rdst/compare/0.20.14...master) - 2026-02-04

### BREAKING

- `[u8; N]` Array sort order changed from little-endian to lexicographic (big-endian) matching the Rust standard library order
- Rust version (MSRV) increased to `1.87` to allow removal of `arbitrary-chunks` dependency
- Rayon upgraded to `1.12` (latest, MSRV `1.85`)

## Fixed

- Trait dependencies like `Copy` now directly added to `RadixKey` so errors are shown to implementers
- `Send` + `Sync` no longer required for `T` if you disable the `multi-threading` feature
- All GitHub Actions pinned by SHA
- Dev dependencies that only existed for maximizing benchmark performance have been removed
- Unsafe blocks for uninitialized buffers moved to point of use and wrapped with `MaybeUninit`

## Changed

- `arbitrary-chunks` dependency removed in favor of `split_off_mut`
- `profiling` and `timings` flags / cfgs for performance testing removed in favor of independent scripts
