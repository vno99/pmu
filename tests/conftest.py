"""Fixtures et mocks globaux pour la suite de tests.

Injecte un faux module ``airflow.providers.postgres.hooks.postgres`` dans
``sys.modules`` lorsque le vrai provider Apache Airflow n'est pas installé
(c'est le cas dans l'environnement conda local et en CI). Seule l'existence de
la classe ``PostgresHook`` importe à l'import des DAGs ; ses méthodes
(``get_conn``, ``insert_rows``) sont entièrement mockées dans chaque test.
"""

import sys
import types


def _install_fake_postgres_provider() -> None:
    """Fournit un module minimal ``airflow.providers.postgres.hooks.postgres``."""
    pkg = types.ModuleType("airflow.providers.postgres")
    pkg.__path__ = []
    hooks = types.ModuleType("airflow.providers.postgres.hooks")
    hooks.__path__ = []

    class PostgresHook:
        """API minimale ; les tests mockent ces méthodes."""

        def __init__(self, *args, **kwargs):
            pass

        def get_conn(self):
            raise NotImplementedError("fake PostgresHook — à patcher dans les tests")

        def insert_rows(self, *args, **kwargs):
            raise NotImplementedError("fake PostgresHook — à patcher dans les tests")

    mod = types.ModuleType("airflow.providers.postgres.hooks.postgres")
    mod.PostgresHook = PostgresHook

    sys.modules["airflow.providers.postgres"] = pkg
    sys.modules["airflow.providers.postgres.hooks"] = hooks
    sys.modules["airflow.providers.postgres.hooks.postgres"] = mod


try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa: F401
except ModuleNotFoundError:
    _install_fake_postgres_provider()
