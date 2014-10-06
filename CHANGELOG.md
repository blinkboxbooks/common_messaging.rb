# Change log

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

