Performance tests were run using both `show objects` and `show terse objects` at three scales.
With `2024_03` turned off, both methods are able to correctly identify a dynamic table.
However, when `2024_03` is turned on, only `show objects` is able to correctly identify
a dynamic table. This is done by inspecting the new column `is_dynamic` since both a table
and a dynamic table show up with a `kind` of table.
In order to properly compare the two methods, an additional scenario was added that does not
create dynamic tables, and instead splits those objects evenly between views and tables.

Let's take the small scale as an example. The small scale creates 30 objects.
There is a run that creates 10 of each object, resulting in 30 objects.
This is successful for `show objects` whether `2024_03` is turned on or off.
It is also successful for `show terse objects` when `2024_03` is turned off.
There is another scenario that creates 15 views and 15 table, but no dynamic tables.
This scenario still creates 30 objects, and both methods return the correct types
regardless of setting for `2024_03`.
These scenarios can be combined to compare `show terse objects` with `2024_03` off
to `show objects` with `2024_03` turned on.
This comparison represents the change that will happen when `2024_03` becomes a mandatory bundle.

### 30 Objects

| 2024_03 | method             | mean time | mean time - no DTs |
|:-------:|--------------------|----------:|-------------------:|
|   NO    | show terse objects |    1.02 s |                 -- |
|   YES   | show objects       |    0.91 s |             0.92 s |
|   YES   | show terse objects |        -- |             0.94 s |

- 11% improved run time of `list_relations_without_caching` when turning on `2024_03`
- similar performance of `show objects` and `show terse objects` in `2024_03`

### 300 Objects

| 2024_03 | method             | mean time | mean time - no DTs |
|:-------:|--------------------|----------:|-------------------:|
|   NO    | show terse objects |    0.96 s |                 -- |
|   YES   | show objects       |    1.19 s |             1.37 s |
|   YES   | show terse objects |        -- |             0.92 s |

- 24% longer run time of `list_relations_without_caching` when turning on `2024_03`
- 49% longer run time of `show objects` than `show terse objects` in `2024_03`

### 3000 Objects

| 2024_03 | method             | mean time | mean time - no DTs |
|:-------:|--------------------|----------:|-------------------:|
|   NO    | show terse objects |    2.00 s |                 -- |
|   YES   | show objects       |    3.05 s |             3.22 s |
|   YES   | show terse objects |        -- |             2.33 s |

- 53% longer run time of `list_relations_without_caching` when turning on `2024_03`
- 38% longer run time of `show objects` than `show terse objects` in `2024_03`
