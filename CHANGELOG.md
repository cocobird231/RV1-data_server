# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased] - 2024-03-19

Significant changes for `SubNode` and `SaveQueue` implementation. Separate `ScanTopicNode` parameters into new `Params` class, add callback function for set parameters callback.

The new `TopicRecordNode` and `TopicRecordSaveQueueNode` support callback function enable/disable control. While the subscription callback function called, the message will pass to `_setMsgContent()` function if the callback function flag is enabled.

### Added
- `CHANGELOG.md`

### Changed
- All `SubNode` rewrite to `TopicRecordNode<topicT, msgnodeT>` specialization and inheritance from `BaseTopicRecordNode`.
- All `SubSaveQueueNode` rewrite to `TopicRecordSaveQueueNode<topicT, msgnodeT, saveT>` specialization and inheritance from `BaseTopicRecordNode`.
- The `TopicRecordNode` and `TopicRecordSaveQueueNode` support callback function enable/disable control.
- `ScanTopicNode` parameters moved to `Params` class. Add `_paramsCbFunc()` function to set parameters callback.
- Rewrite `SaveQueue` thread function to be more efficient and customized friendly using `std::condition_variable` and `_saveCbFunc()` callback function.

### Fixed
- The `TopicRecordSaveQueueNode` (formerly `SubSaveQueueNode`) will not always push the data into queue, instead it will push the data into queue only if callback function is enabled.