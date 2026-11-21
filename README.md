# Shopping Sessions: mission

A merge of two pipelines:

- **Fast path** (`src/`): based on Team 67's Express Lane
  (github.com/jason-scheffel/express-lane, commit f122b64), used under the MIT
  License. Team 67's copyright notice is kept in LICENSE. It infers a compact ID
  representation from the input and checks it as it goes.
- **General path** (`fallback/`): our own pipeline (joch44/shopping-sessions,
  v36). When the data breaks the fast path's assumptions (long or opaque IDs,
  unusual categories, ...), the fast path hands the whole job to it
  (`/fallback` in the image). On such data it is 2-4x faster than the fast
  path's own generic mode.

Our changes on top of Team 67's code are listed in the git history after
commit 92cf412. `tools/docker-impl.sh` runs the image as a gate adapter.
