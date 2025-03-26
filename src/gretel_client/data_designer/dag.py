import logging

import networkx as nx

from gretel_client.data_designer.log import get_logger
from gretel_client.data_designer.types import AIDDColumnT, WithDAGColumnMixin


def topologically_sort_columns(
    columns: AIDDColumnT,
    *,
    logger: logging.Logger | None = None,
    verbose_logging: bool = False,
) -> list[str]:
    """Construct DAG and topologically sort the columns based on their dependencies.

    Args:
        columns: List of AIDD column objects to sort.
        logger: Optional logger to use for logging.
        verbose_logging: If True, log the dependencies between columns.

    Returns:
        List of column names sorted in topological order.
    """
    if logger is None:
        logger = get_logger(__name__, level=logging.INFO)

    dag = nx.DiGraph()
    dag_column_names = [col.name for col in columns]

    if len(dag_column_names) > 1 and verbose_logging:
        logger.info("⛓️ Representing generation steps as a Directed Acyclic Graph")

    side_effect_dict = {c.name: list(c.side_effect_columns) for c in columns}

    for col in columns:

        if not issubclass(col.__class__, WithDAGColumnMixin):
            raise ValueError(
                f"Column `{col.name}` is of type `{type(col)}`, which is not "
                "supported for topological sorting. Please ensure all columns "
                "are subclasses of `WithDAGColumnMixin`."
            )

        dag.add_node(col.name)
        for req_col_name in col.required_columns:

            if req_col_name in dag_column_names:
                if verbose_logging:
                    logger.info(f"  |-- 🔗 `{col.name}` depends on `{req_col_name}`")
                dag.add_edge(req_col_name, col.name)

            # If the required column is a side effect of another column,
            # add an edge from the parent column to the current column.
            elif req_col_name in sum(side_effect_dict.values(), []):
                for parent, cols in side_effect_dict.items():
                    if req_col_name in cols:
                        if verbose_logging:
                            logger.info(
                                f"  |-- 🔗 `{col.name}` depends on `{parent}` via `{req_col_name}`"
                            )
                        dag.add_edge(parent, col.name)
                        break

    if not nx.is_directed_acyclic_graph(dag):
        raise ValueError(
            "🛑 The workflow generation steps contain cyclic dependencies. Please "
            "inspect the column configurations and ensure they can be sorted without "
            "circular references."
        )

    sorted_column_names = list(nx.topological_sort(dag))

    return sorted_column_names
