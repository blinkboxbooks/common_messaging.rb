# Change log

## 0.4.1 ([#5](https://git.mobcastdev.com/Platform/common_messaging.rb/pull/5) 2014-11-06 16:31:05)

Refactor

### Improvements

- Just shift code between two files. No code alterations whatsoever.

## 0.4.0 ([#4](https://git.mobcastdev.com/Platform/common_messaging.rb/pull/4) 2014-10-24 10:11:02)

Validation of messages

### New feature

- Payloads can be validated or not, allowing 'catch all' queues.

## 0.3.0 ([#3](https://git.mobcastdev.com/Platform/common_messaging.rb/pull/3) 2014-10-09 10:46:10)

Prefetch

### New feature

- Add prefetch capability

## 0.2.0 ([#2](https://git.mobcastdev.com/Platform/common_messaging.rb/pull/2) 2014-10-03 13:30:51)

Remote uris

### New feature

- Any message sent with this library that contains a hash with the key `"type" => "remote"` will have its deep key placed into the `remote_uris` header for Marvin 2.0's resource fetcher to process.
- Consolidated `VERSION` retrieval.

## 0.1.0 ([#1](https://git.mobcastdev.com/Platform/common_messaging.rb/pull/1) 2014-09-01 08:37:37)

Basic needs of the messaging library

### New Features

- Allows blinkbox Books specific message message publishing and subscription
- Automatically validates incoming and outbound message structure against the json schema.

