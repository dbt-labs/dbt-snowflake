# Performance testing

These tests are not meant to run on a regular basis; instead, they are tools for measuring performance impacts of changes as needed.
We often get requests for reducing processing times, researching why a particular component is taking longer to run than expected, etc.
In the past we have performed one-off analyses to address these requests and documented the results in the relevant PR (when a change is made).
It is more useful to document those analyses in the form of performance tests so that we can easily rerun the analysis at a later date.
