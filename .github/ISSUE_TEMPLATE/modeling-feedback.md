---
name: Modeling feedback
about: Something in the models looks wrong, lossy, or questionable
title: ''
labels: 'modeling'
---

**Which model**
Path, e.g. `natality_names_kimball/dbt/models/intermediate/int_nn_natality.sql`

**What looks wrong**
Grain, crosswalk, denominator, dimension, test coverage - whatever it is.

**Why**
What would go wrong downstream if this stayed as it is.

---

The data itself is local and mostly not shareable (see the README), but happy
to provide schemas, row counts or profiling output if that would help settle it.
