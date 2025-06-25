import yaml
from pathlib import Path

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

project_root = str(Path(__file__).parent.parent.resolve())
def expand_path(path):
    return path.replace("$project_root", project_root)

symbols_file = expand_path(config["symbols_file"])
print(f"Symbols file: {symbols_file}")