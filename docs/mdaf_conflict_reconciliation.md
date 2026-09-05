# Conflict-aware retained-artifact release

Recipe **1.5.0**, release file `mistral-ocr-4.1-wiki-v6.json`, normalization
**wiki-v5**, is an automatic compatible successor to earlier Mistral wiki
recipes. Routing policy revision 4 and the recipe-worker CLI default select it.
Frozen older recipes and their normalization behavior are unchanged.

When selected contents-page and body-title evidence disagree, retain a unique
body title rather than silently substituting an unrelated page boundary. A
separately numbered opener immediately preceding that title is retained when
its label is observed or bracketed by consecutive observed labels and it does
not belong to another selected chapter. Repeated body titles can use a unique
corroborated unoccupied contents page; otherwise the entry is reported unmatched.
These conditional choices stay visible in the hierarchy evidence report.
Bracketed/inferred labels never become observed citation targets.

On the nine-document regression corpus this repairs London's Bonus Scenes
boundary while retaining all thirteen Omnibus sections and their opening prose.
All other major-section counts remain stable. This is not proof of universal
semantic correctness: geometry-assisted tiers and uncertain evidence still
require review, and the corpus is not an independent holdout.

Registered workers automatically upgrade compatible retained outputs via
artifact input, without a provider probe or new extraction. Pending source jobs
follow compatible releases without changing quota backoff, retry history or
active purchase boundaries. Major/extraction changes remain explicit. Old
artifacts remain addressable; do not relabel them or overwrite edited wikis.
Validate all compatible retained sources have the target artifact after rollout,
not merely the nine samples. A worker pinned to an older release must be updated
before it can advertise and execute the new profile.

Vulcan imports normalized references and outlines; it must not parse this
producer-specific evidence to implement page or chapter logic. Its MDAF v1 SPEC
clarifies confidence versus byte alignment and independent import granularity.
