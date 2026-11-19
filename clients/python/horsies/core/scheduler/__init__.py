# clients/python/horsies/core/scheduler/__init__.py
"""
Scheduler module for executing scheduled tasks.

Main components:
- Scheduler: Main service for running scheduled tasks

Example usage:

```python
from horsies.core.scheduler import Scheduler

scheduler = Scheduler(app)
await scheduler.run_forever()
```
"""

from horsies.core.scheduler.service import Scheduler

__all__ = [
    'Scheduler',
]
