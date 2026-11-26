// expect: TS2322 at 7, TS2322 at 9, TS2322 at 11
// A task limit is `{ kind: "taskLimit", tasks }` or `{ kind: "noTaskLimit" }`; a memory limit is
// `{ kind: "memoryLimit", mb }` or `{ kind: "noMemoryLimit" }`. The tags `tasks`, `mb` and `noLimit` are no limits.

import type { MemoryLimit, TaskLimit } from "../../../horsies/src/index.ts";

export const tasks: TaskLimit = { kind: "tasks", tasks: 100 };

export const memory: MemoryLimit = { kind: "mb", mb: 512 };

export const unlimited: TaskLimit = { kind: "noLimit" };
