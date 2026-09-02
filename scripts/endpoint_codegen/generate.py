import importlib
import inspect
from pathlib import Path
from types import ModuleType

from pydantic import BaseModel

from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper

from . import descriptions, job_config
from .code_generation import (
    GENERATED_FILE_BANNER,
    InlinedSource,
    Spec,
    generate_arrow_client_class,
    generate_client_class,
    inline_source,
)
from .embedding import config, encode, predict, train
from .job_config import BaseJobConfig, MutateNodePropertyConfig, WriteNodePropertyConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCEDURE_SURFACE_DIR = REPO_ROOT / "src" / "graphdatascience" / "procedure_surface"
API_DIR = PROCEDURE_SURFACE_DIR / "api"
API_EMBEDDING_DIR = PROCEDURE_SURFACE_DIR / "api" / "embedding"
ARROW_EMBEDDING_DIR = PROCEDURE_SURFACE_DIR / "arrow" / "embedding"
API_EMBEDDING_MODULE = "graphdatascience.procedure_surface.api.embedding"

# The encoder and decoder configs are shared by every spec, so they are copied into a module of their
# own rather than inlined into each set of endpoints, and imported from there by the generated code.
CONFIG_MODULE = f"{API_EMBEDDING_MODULE}.config"

# Not specific to embeddings, so these land outside the package, ready for the next surface. Every
# spec module relies on both: the field descriptions it shares with the others, and the parameters
# common to every job (`job_id`, `node_labels`, ...). Both are copied to the library the same way and
# resolved through this one mapping -- there is no separate mechanism for "just descriptions".
JOB_CONFIG_MODULE = "graphdatascience.procedure_surface.api.job_config"
DESCRIPTIONS_MODULE = "graphdatascience.procedure_surface.api.descriptions"
SHARED_SPEC_MODULES = {".config": CONFIG_MODULE, "..descriptions": DESCRIPTIONS_MODULE}

MODE_CONFIGS: dict[str, type[BaseModel]] = {"mutate": MutateNodePropertyConfig, "write": WriteNodePropertyConfig}

# Each spec module exports its own `SPEC`, right next to the classes it describes, instead of this
# file re-listing every procedure's config and result types by hand.
SPECS = [Spec(*module.SPEC) for module in (encode, train, predict)]


def main() -> None:
    for directory in (API_EMBEDDING_DIR, ARROW_EMBEDDING_DIR):
        directory.mkdir(parents=True, exist_ok=True)
        init_file = directory / "__init__.py"
        if not init_file.exists():
            init_file.touch()

    _copy_spec_module(config, API_EMBEDDING_DIR / "config.py")
    _copy_spec_module(job_config, API_DIR / "job_config.py")
    _copy_spec_module(descriptions, API_DIR / "descriptions.py")

    shared_modules = {config.__name__: CONFIG_MODULE, job_config.__name__: JOB_CONFIG_MODULE}

    for spec in SPECS:
        # `{Name}Endpoints` / `{Name}ArrowEndpoints`, matching every other hand-written surface
        # (e.g. `BetweennessEndpoints` / `BetweennessArrowEndpoints`) -- no embedding-specific naming.
        endpoint_class = f"{spec.name}Endpoints"
        arrow_class = f"{spec.name}ArrowEndpoints"
        entrypoints_module = f"{API_EMBEDDING_MODULE}.{spec.name.lower()}_endpoints"
        spec_module = spec.config_model.__module__

        # The models the spec is written against are inlined below the entrypoints class, matching
        # how the hand-written endpoints keep their result types, and imported from there elsewhere.
        code = generate_client_class(
            config_model=spec.config_model,
            class_name=endpoint_class,
            return_types=spec.return_types,
            module_overrides={spec_module: None, **shared_modules},
            inlined=_spec_definitions(spec_module),
            base_config=BaseJobConfig,
            mode_configs=MODE_CONFIGS,
        )
        _write(API_EMBEDDING_DIR / f"{spec.name.lower()}_endpoints.py", code)

        code = generate_arrow_client_class(
            config_model=spec.config_model,
            class_name=arrow_class,
            entrypoints_module=entrypoints_module,
            entrypoints_class_name=endpoint_class,
            endpoint=spec.endpoint,
            return_types=spec.return_types,
            endpoints_helper_class=NodePropertyEndpointsHelper,
            module_overrides={spec_module: entrypoints_module, **shared_modules},
            base_config=BaseJobConfig,
            mode_configs=MODE_CONFIGS,
        )
        _write(ARROW_EMBEDDING_DIR / f"{spec.name.lower()}_arrow_endpoints.py", code)


def _write(path: Path, code: str) -> None:
    path.write_text(code + "\n")


def _spec_definitions(module_name: str) -> InlinedSource:
    """Read the config and result models a spec is written against, to be inlined into the library.

    Carrying the source over rather than regenerating it from the model keeps the pydantic
    `Field(...)` metadata (validation aliases, discriminators, default factories) that the generated
    endpoints rely on when they build and dump a config.
    """
    source = inspect.getsource(importlib.import_module(module_name))
    return inline_source(source, SHARED_SPEC_MODULES)


def _copy_spec_module(module: ModuleType, target: Path) -> None:
    """Copy a spec module verbatim into the library, for definitions shared across specs."""
    target.write_text(f'"""{GENERATED_FILE_BANNER}"""\n\n{inspect.getsource(module)}')


if __name__ == "__main__":
    main()
