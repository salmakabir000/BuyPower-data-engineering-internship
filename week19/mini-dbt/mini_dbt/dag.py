
from graphlib import TopologicalSorter, CycleError
from collections import defaultdict, deque


def build_graph(models):
    graph = {}

    for model in models:
        graph[model["name"]] = set(model["refs"])

    # Check for references to missing models
    for name, dependencies in graph.items():
        for dependency in dependencies:
            if dependency not in graph:
                raise ValueError(
                    f"Model '{name}' references unknown model "
                    f"'{dependency}'"
                )

    return graph


def execution_order(graph):
    try:
        sorter = TopologicalSorter(graph)
        return list(sorter.static_order())
    except CycleError as error:
        cycle = error.args[1]
        raise ValueError(
            f"Circular dependency detected: {' -> '.join(cycle)}"
        ) from error


def select_downstream(graph, model_name):
    if model_name not in graph:
        raise ValueError(f"Unknown model: {model_name}")

    downstream = defaultdict(set)

    for model, dependencies in graph.items():
        for dependency in dependencies:
            downstream[dependency].add(model)

    selected = {model_name}
    queue = deque([model_name])

    while queue:
        current = queue.popleft()

        for child in downstream[current]:
            if child not in selected:
                selected.add(child)
                queue.append(child)

    return selected


def select_upstream(graph, model_name):
    if model_name not in graph:
        raise ValueError(f"Unknown model: {model_name}")

    selected = set()
    queue = deque([model_name])

    while queue:
        current = queue.popleft()

        if current not in selected:
            selected.add(current)

            for dependency in graph[current]:
                queue.append(dependency)

    return selected
