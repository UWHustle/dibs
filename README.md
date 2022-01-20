# DIBS: Database Isolation By Scheduling

This repository accompanies our VLDB 2021 paper about modular transaction isolation:

[Kevin P. Gaffney, Robert Claus, and Jignesh M. Patel. Database Isolation By Scheduling. PVLDB, 14(9): 1467 - 1480, 2021.](http://vldb.org/pvldb/vol14/p1467-gaffney.pdf)

## Build instructions

DIBS is implemented in Rust. You can install Rust and its package manager, Cargo, by following the instructions [here](https://doc.rust-lang.org/cargo/getting-started/installation.html).

To build the project, run `cargo build` from the top-level directory. For a release build, run `cargo build --release`.

To run a specific experiment, run `cargo run --bin <name>`. Each experiment takes several parameters. You can examine the parameters by running `path/to/bin --help`.

## Project structure

**`/dibs`** contains the transaction isolation logic. The file `predicate.rs` includes the definition of the predicate data structure and some auxiliary functions. The file `solver.rs` implements the solver that determines whether two predicates conflict.

**`/experiments`** contains the code that was used to produce the results in the paper. Each executable in subdirectory `/bin` is a separate experiment.
