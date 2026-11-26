// Where a child runs its bodies, from its start facts (the child's own start hook, hook 0):
//
// - A `SHARED_THREAD` child runs them in its seat isolates: hook 0 boots the seat pool, and each run, cancel and
//   operation of a seat goes through the pool. The main thread runs no body and no provider, so it sets no run mark:
//   the main-thread watch of the core names no culprit when this thread stalls.
// - Every other kind runs them on the main thread: the child's own post dispatcher takes each post.
//
// The core posts no run before its start hooks ended, so a post before hook 0 is a defect of the core or the package.
// A thread child has no spawned work: a `stopWork` post there is a defect.

import type { ChildPost, StartFactsRecord } from "../native/child/addon.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type SeatPool, type SeatPoolDeps, startSeatPool } from "./pool.ts";

/** Where the bodies of a child run. */
type BodyPlace = { readonly kind: "notStarted" } | { readonly kind: "mainThread" } | { readonly kind: "seats"; readonly pool: SeatPool };

/** The router of the posts of one child. */
export class SeatRouter {
  readonly #deps: SeatPoolDeps;
  #place: BodyPlace = { kind: "notStarted" };

  constructor(deps: SeatPoolDeps) {
    this.#deps = deps;
  }

  /**
   * The start facts of the child: a thread child boots its seat pool, and the promise ends with the boot (a failed
   * boot rejects: the hook fails, the child ends with 173); every other kind resolves at once. A second start is a
   * defect.
   */
  async start(facts: StartFactsRecord): Promise<void> {
    if (this.#place.kind !== "notStarted") {
      const defect = routerDefect("the start facts of a child come once");

      this.#deps.reportDefect(defect);

      throw defect;
    }

    switch (facts.kind) {
      case "sharedThread": {
        const started = startSeatPool(facts.seats, this.#deps);

        this.#place = { kind: "seats", pool: started.pool };

        return started.booted;
      }

      case "isolated":
      case "sharedAsync":
      case "handler":
        this.#place = { kind: "mainThread" };

        return;
      default:
        unreachable(facts, "SeatRouter.start");
    }
  }

  /** The post dispatcher of the child: the seat pool in a thread child, else `mainThread`. It never throws. */
  posts(mainThread: (post: ChildPost) => void): (post: ChildPost) => void {
    return (post) => {
      try {
        this.#route(post, mainThread);
      } catch (cause) {
        this.#deps.reportDefect(cause instanceof CoreDefect ? cause : routerDefect(`a post threw: ${String(cause)}`));
      }
    };
  }

  #route(post: ChildPost, mainThread: (post: ChildPost) => void): void {
    const place = this.#place;

    switch (place.kind) {
      case "notStarted":
        throw routerDefect(`the core posted ${post.kind} before the start facts of the child`);
      case "mainThread":
        mainThread(post);

        return;
      case "seats":
        routeToSeats(place.pool, post);

        return;
      default:
        unreachable(place, "SeatRouter.route");
    }
  }
}

function routeToSeats(pool: SeatPool, post: ChildPost): void {
  switch (post.kind) {
    case "runBody":
      pool.run(post);

      return;
    case "cancelRun":
      pool.cancel(post.runKey, post.cause);

      return;
    case "stopWork":
      throw routerDefect("the core posted stopWork to a thread child, whose bodies have no spawned work");
    default:
      unreachable(post, "routeToSeats");
  }
}

function routerDefect(rule: string): ClientDefect {
  return new ClientDefect({ subject: "seat router", rule });
}
