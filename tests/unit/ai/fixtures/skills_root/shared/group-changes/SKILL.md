---
name: group-changes
description: Group a flat list of merged changes by feature, fix and breaking change.
---

Read each change and place it in exactly one group: feature, fix or breaking
change. A change that alters an existing contract is a breaking change even when
it also adds behaviour. Keep the original order inside every group and never
invent a group that the input does not support.
