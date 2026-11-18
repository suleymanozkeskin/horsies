from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase



class Base(DeclarativeBase):
    """SQLAlchemy declarative base for PostgreSQL models"""

    pass


"""
NOTE:
- When a task returns a `TaskError`, the task is not necessarily failed.
  It could be that be that running the task itself was successful from library's perspective,
  but the task function itself wanted to return an error for a given condition.

    Scenario 1:
    ```python
    @app.task()
    def task_function(input: int) -> TaskResult[int, TaskError]:
        if input < 0:
            return TaskResult(err=TaskError(
            error_code="NEGATIVE_INPUT",
            message="Input cannot be negative",
        ))
        return TaskResult(ok=input * 2)

    # In this case, task did not fail, rather returns an error as expected.
    handle = task_function.send(-1)  # This will return a TaskError
    result = handle.get()
    ```

  Scenario 2:
  ```python
    class Dog:
        def __init__(self, name: str):
            self.name = name

    def return_dog(input: str) -> Dog:
        return Dog(input)


    @app.task()
    def example_task(input: str) -> TaskResult[Dog, TaskError]:
        try:
            unserializable_return = return_dog(input)
            return TaskResult(ok=unserializable_return)
        except Exception as e:
            return TaskResult(err=TaskError(
                error_code="YOUR_ERROR_CODE", # this is still the case you as the developer, catch in the task function
                message="Your error message",
                data={"error": str(e)},
            ))

    handle = example_task.send("Rex")
    result = handle.get()
    if result.is_err():
        print(f"✓ Task execution failed: {result.unwrap_err()}") # Since a python class of Dog is not serializable by the library, 
        # this will return a TaskError with error_code="WORKER_SERIALIZATION_ERROR" and message="Cannot serialize value of type Dog"
        # And the task will be marked as failed. 
    ```
  
  """
