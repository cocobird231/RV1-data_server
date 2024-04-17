# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased] - 2024-04-18

- Modifies timer and thread to `LiteTimer` and safe thread.
- Add `control.cpp` for simple data server parameter control.

### Added
- `control.cpp` for simple data server parameter control.

### Changed
- `header.h`
    - Delete `SpinExecutor` and `split`, use `vehicle_interfaces::SpinExecutor` and `vehicle_interfaces::split` instead.
    - Modify comments.
- `main.cpp`
    - Change timer and thread to `LiteTimer` and safe thread.
