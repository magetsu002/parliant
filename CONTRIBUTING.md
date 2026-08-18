# Contributing

SIDECAR is still pre-implementation. Keep changes narrow and avoid speculative scaffolding.

Before changing architecture or implementation behavior, read [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

## Pull requests

A pull request should:

- solve one clearly stated problem;
- keep unrelated cleanup out of the diff;
- explain any privacy or security impact;
- include tests once executable code exists;
- state what was actually verified and what remains uncertain;
- never include real meeting audio, transcripts, credentials, private repository content, or customer data.

Do not create empty packages, adapters, or abstraction layers simply because they appear in the planned architecture. Add them when an implemented vertical slice needs them.

Large architectural changes should be discussed before implementation.

## Current licensing status

The repository is publicly viewable but does not yet grant an open-source license. External implementation contributions should wait until the project owner chooses a contribution/license policy.
