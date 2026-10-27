# Parser source snapshot

Source: https://github.com/mytecor/parser
Revision: `7ea1fe7cce640a2aff60a11bdcda9acfdce3fe15`

This directory contains the unmodified `Cargo.toml` and `src/` files from that
revision. Keeping the library sources in this repository makes web-driver builds
independent of sibling checkouts. Third-party Rust dependencies remain pinned by
`drivers/web/Cargo.lock`.

To update the parser, copy these files from a reviewed upstream revision, update
this revision record, and run the web-driver unit and browser integration tests.
