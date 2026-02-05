from pathlib import Path
import yaml

def load_config(dataset_name: str, layer: str, nb_path: str):
    folder_map = {
        "dq": "data_quality",
        "silver": "silver",
        "validation": "validation"
    }

    repo_root = Path("/Workspace" + nb_path).parents[1]
    base_path = repo_root / "configs" / folder_map[layer]

    # Case 1: Single YAML file
    single_file = base_path / f"{dataset_name}_{layer}.yaml"
    if single_file.exists():
        with open(single_file, "r") as f:
            return yaml.safe_load(f)

    # Case 2: Folder with multiple YAMLs
    dataset_folder = base_path / dataset_name
    if dataset_folder.exists() and dataset_folder.is_dir():
        merged_config = {}
        for file in sorted(dataset_folder.glob("*.yaml")):
            with open(file, "r") as f:
                content = yaml.safe_load(f) or {}
                merged_config.update(content)
        return merged_config

    raise FileNotFoundError(f"No config found for {dataset_name} in {layer}")
