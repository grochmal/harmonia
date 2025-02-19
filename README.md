# Harmonia

Orchestrator framework.  Have you ever felt that every orchestrator needs
to be adapted to fit your codebase?  That no orchestrator comes ready out
of the box.  Harmonia extends that idea.  It is not a ready to use
orchestrator, instead it is a framework to build orchestrators.


## Distributed by nature

Another idea introduced by harmonia is distribution of orchestration.
One can build a typical orchestrator with a single source of truth,
e.g. an RDBMS that holds all state with full ACID.  Yet, harmonia
provides stubs to build pipelines that run completely independent
of each other - pipelines that can run on different systems
and only update state with eventual consistency.

The state of a pipeline is stored in either a file or a database,
and this state is only updated by the process orchestrating the pipeline.
All other interactions with the state are read-only.
This allows for tho different people run stats, or UIs against
the same pipeline (and state).
In order to modify state one must interact with the orchestrator
process, once the orchestrator process finishes the pipeline
state is written in stone and cannot be changed (only deleted).
These assumptions - albeit perplexing - allow for seamless
composition.
For example, one can run one pipeline after another,
by creting a third pipeline with two steps.
Each step running an harmonia process orchestrating
their own (sub) pipeline.

