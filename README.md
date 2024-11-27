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

