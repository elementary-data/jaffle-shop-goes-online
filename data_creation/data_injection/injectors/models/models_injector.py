import json
from typing import List, Optional
from elementary.clients.dbt.subprocess_dbt_runner import (
    SubprocessDbtRunner as DbtRunner,
)
from data_creation.data_injection.injectors.base_injector import BaseInjector


class ModelsInjector(BaseInjector):
    def __init__(
        self,
        dbt_runner: Optional[DbtRunner] = None,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
    ) -> None:
        super().__init__(dbt_runner, target, profiles_dir)

    def get_model_ids(self, select: Optional[str] = None) -> List[str]:
        print(f"🔍 DEBUG: Getting model IDs with filter: {select}")

        # First, let's see all available model IDs directly from Elementary
        debug_query = """
            select unique_id, alias, package_name 
            from {{ ref('elementary', 'dbt_models') }} 
            order by package_name, alias
        """
        all_models_debug = self.run_query(debug_query)
        print(
            f"📊 DEBUG: Elementary has {len(all_models_debug)} total models in dbt_models table:"
        )

        # Group by package
        by_package = {}
        for model in all_models_debug:
            pkg = model.get("package_name", "unknown")
            if pkg not in by_package:
                by_package[pkg] = []
            by_package[pkg].append(model)

        for pkg, models in by_package.items():
            print(f"  📦 Package '{pkg}': {len(models)} models")
            for model in models[:3]:  # Show first 3 per package
                print(
                    f"    - {model.get('alias', 'N/A')} ({model.get('unique_id', 'N/A')})"
                )
            if len(models) > 3:
                print(f"    ... and {len(models) - 3} more")

        # Now run the original macro

    def get_model_id_from_name(self, model_name: str) -> str:
        print(f"🔍 DEBUG: Searching for model: '{model_name}'")

        # First, let's see what models are actually available
        debug_query = """
            select unique_id, alias, name, package_name 
            from {{ ref('elementary', 'dbt_models') }} 
            where package_name <> 'elementary'
            order by alias
        """
        all_models = self.run_query(debug_query)
        print(f"📊 DEBUG: Found {len(all_models)} total models:")
        for model in all_models[:10]:  # Show first 10
            print(
                f"  - alias: '{model.get('alias', 'N/A')}', unique_id: '{model.get('unique_id', 'N/A')}', package: '{model.get('package_name', 'N/A')}'"
            )
        if len(all_models) > 10:
            print(f"  ... and {len(all_models) - 10} more models")

        # Also check sources
        sources_query = """
            select unique_id, name, package_name 
            from {{ ref('elementary', 'dbt_sources') }}
            order by name
        """
        all_sources = self.run_query(sources_query)
        print(f"📊 DEBUG: Found {len(all_sources)} total sources:")
        for source in all_sources[:5]:  # Show first 5
            print(
                f"  - name: '{source.get('name', 'N/A')}', unique_id: '{source.get('unique_id', 'N/A')}', package: '{source.get('package_name', 'N/A')}'"
            )

        # Now try the original query
        query = """
            (
                select unique_id as model_id
                from {{ ref('elementary', 'dbt_models') }}
                where alias = '%(model_name)s'                        
            )
            union all
            (
                select unique_id as model_id
                from {{ ref('elementary', 'dbt_sources') }}
                where name = '%(model_name)s'                        
            )
            """ % {
            "model_name": model_name
        }

        print(f"🔍 DEBUG: Running query for '{model_name}'")
        result = self.run_query(query)
        print(f"📋 DEBUG: Query returned {len(result)} results: {result}")

        if not result:
            print(f"❌ ERROR: No model found with name '{model_name}'")
            # Show exact matches we're looking for
            exact_matches = [m for m in all_models if m.get("alias") == model_name]
            source_matches = [s for s in all_sources if s.get("name") == model_name]
            print(f"🔍 DEBUG: Exact alias matches: {exact_matches}")
            print(f"🔍 DEBUG: Exact source name matches: {source_matches}")
            raise ValueError(f"No model found with name '{model_name}'")

        print(f"✅ DEBUG: Found model '{model_name}' with ID: {result[0]['model_id']}")
        return result[0]["model_id"]

    def get_nodes(self):
        return self.run_query(
            """
            select unique_id as model_id, alias as model_name from {{ ref('elementary', 'dbt_models') }} where package_name <> 'elementary'
            union all
            select unique_id as model_id, name as model_name from {{ ref('elementary', 'dbt_sources') }}
            """,
        )
