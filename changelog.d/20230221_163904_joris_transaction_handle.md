<!--
A new scriv changelog fragment.

Uncomment the section that is right (remove the HTML comment wrapper).
-->

<!--
### Patch

- A bullet item for the Patch category.

-->
### Non-Breaking

- Add `TransactionHandle`s in new `Database.LMDB.Simple.TransactionHandle`
  module. These `TransactionHandle`s are a wrapper around internal LMDB
  transaction handles, which keep open a consistent view of the database as long
  as the handle is live.

<!--
### Breaking

- A bullet item for the Breaking category.

-->
