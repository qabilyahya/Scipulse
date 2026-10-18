"""Exécution des suites GE sur un DataFrame pandas + Data Docs + métriques vers Kibana."""
from __future__ import annotations

import pandas as pd

from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.quality.suites import SUITES

log = get_logger(__name__)
GX_ROOT = "/opt/airflow/great_expectations"


def validate(df: pd.DataFrame, source: str, layer: str = "silver", project_root: str = GX_ROOT,
             raise_on_failure: bool = False) -> dict:
    import great_expectations as gx  # import local : lourd à charger

    context = gx.get_context(mode="file", project_root_dir=project_root)
    name = f"{source}_{layer}"

    try:
        ds = context.data_sources.get("pandas")
    except Exception:
        ds = context.data_sources.add_pandas("pandas")
    try:
        asset = ds.get_asset(name)
    except Exception:
        asset = ds.add_dataframe_asset(name)
    try:
        batch_def = asset.get_batch_definition("whole")
    except Exception:
        batch_def = asset.add_batch_definition_whole_dataframe("whole")

    suite = gx.ExpectationSuite(name=name)
    for exp_name, kwargs in SUITES[source]:
        suite.add_expectation(getattr(gx.expectations, exp_name)(**kwargs))
    try:
        context.suites.delete(name)
    except Exception:
        pass
    suite = context.suites.add(suite)

    vd_name = f"vd_{name}"
    for collection, obj in ((context.checkpoints, f"cp_{name}"), (context.validation_definitions, vd_name)):
        try:
            collection.delete(obj)
        except Exception:
            pass
    vd = context.validation_definitions.add(gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite))

    # Un Checkpoint (et non vd.run) : il ajoute le run_id nécessaire aux Data Docs et les met à jour
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name=f"cp_{name}",
        validation_definitions=[vd],
        actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
    ))
    cp_result = checkpoint.run(batch_parameters={"dataframe": df})
    result = next(iter(cp_result.run_results.values()))

    stats = result.statistics
    summary = {
        "success": bool(result.success),
        "evaluated": stats["evaluated_expectations"],
        "successful": stats["successful_expectations"],
        "success_pct": stats["success_percent"],
        "rows": len(df),
    }
    log.info("GE %s : %s", name, summary)
    emit("quality", source=source, layer=layer, **summary)
    if raise_on_failure and not result.success:
        raise ValueError(f"Validation GE échouée pour {name}: {summary}")
    return summary
