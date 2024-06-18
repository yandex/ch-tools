from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from ch_tools.common import logging


@dataclass
class WorkerTask:
    indeifier: str
    function: Callable
    kwargs: Dict[str, Any]


def execute_tasks_in_parallel(
    tasks: List[WorkerTask], max_workers: int = 4, keep_going: bool = False
) -> Dict[str, Any]:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Can't use map function here. The map method returns a generator
        # and it is not possible to resume a generator after an exception occurs.
        # https://peps.python.org/pep-0255/#specification-generators-and-exception-propagation
        futures_to_indedifier = {
            executor.submit(
                task.function,
                **task.kwargs,
            ): task.indeifier
            for task in tasks
        }
        result: Dict[str, Any] = {}
        for future in as_completed(futures_to_indedifier):
            idf = futures_to_indedifier[future]
            try:
                result[idf] = future.result()
            except Exception as e:
                if keep_going:
                    logging.warning(
                        "Ignoring the exception due to while executing {} due to keep-going flag : {!r}",
                        id,
                        e,
                    )
                else:
                    raise
        return result
