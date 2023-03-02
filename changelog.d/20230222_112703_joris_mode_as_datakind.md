<!--
A new scriv changelog fragment.

Uncomment the section that is right (remove the HTML comment wrapper).
-->

<!--
### Patch

- A bullet item for the Patch category.

-->
<!--
### Non-Breaking

- A bullet item for the Non-Breaking category.

-->
### Breaking

- Represent transaction/environment modes as a `Mode` data kind instead of as
  `Type`s. As a consequence, restrict types that are parameterised by modes
  (such as `Transaction`, `Environment`, `TransactionHandle` and `CursorM`) to
  `Mode`-kinded type arguments. e.g.,
  ```haskell
  type Transaction :: Mode -> Type -> Type
  ```
- Rename `Mode` type class to `IsMode`. Its kind is restricted to `Mode`-kinded
  types.

