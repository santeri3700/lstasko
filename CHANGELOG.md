# Changelog

## 0.2.0 - 2022-11-15
New features and refactoring. Tested with Uyuni Server 2022.10.
### Added
- Version information (`-V/--version` and `LSTasko.version`)
- Dependency `javaobj-py3==0.4.3`
- Task ID filter (`-i/--id`)
- New Exception class `LSTaskoChannelNotFoundException` (raised by `get_channel_*()` functions unless argument `ignore_missing` is set as `True`)
### Changed
- `get_reposync_details()` was refactored by replacing the hack `_hack_parse_reposync_data()` with new code using the `javaobj-py3` library.
- `get_channel_details()` was refactored to return all related database fields and have better error handling (`LSTaskoChannelNotFoundException`).
- Other `get_channel_*()` functions were refactored to use `get_channel_details`.
- Repo-data information now contains synchonization flags (no-errata, latest, sync-kickstart & fail)
  - no-errata = "Do not sync errata"
  - latest = "Sync only latest packages"
  - sync-kickstart = "Create kickstartable tree"
  - fail = "Terminate upon any error"
- `-F` flag was renamed as `-o` in preparation to the follow/continuous mode `-f/-F` similar to `tail -F`.
### Removed
- Internal hack `_hack_parse_reposync_data()`

## 0.1.0 - 2022-10-30
Initial release
### Added
- This project
### Changed
- Everything
### Deprecated
- Nothing yet
### Removed
- Nothing yet