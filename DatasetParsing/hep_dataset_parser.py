# hep_dataset_parser.py

import re
import pandas as pd

class HEPDatasetParser:
    """
    Parser for HEP dataset names.
    Provides methods for parsing dataset strings into structured components.
    """

    def __init__(self):
        # Regex pattern for dataset scope
        self.scope_pattern = re.compile(
            r"^(?P<dataset_origin>mc|data|valid)"
            r"(?P<year>\d{2})?"
            r"(?:_(?P<energy>[0-9p]+)(?P<b_unit>TeV|GeV))?"
        )

    def parse_scope(self, scope: str):
        """Parse the dataset scope into structured components."""
        parsed = {
            "dataset_origin": None,
            "year": None,
            "energy": None,
            "b_unit": None,
            "dataset_category": None
        }

        match = self.scope_pattern.match(scope)
        if not match:
            return None

        parsed.update(match.groupdict())

        # Normalize energy like "13p6" → "13.6"
        if parsed["energy"]:
            parsed["energy"] = parsed["energy"].replace("p", ".")

        # Identify dataset category
        if "hi" in scope:
            parsed["dataset_category"] = "heavy_ion"
        elif "cos" in scope:
            parsed["dataset_category"] = "cosmic"
        elif "pPb" in scope or "hip" in scope:
            parsed["dataset_category"] = "proton_lead"
        else:
            parsed["dataset_category"] = "standard"

        # For validation datasets, ignore year
        if parsed["dataset_origin"] == "valid":
            parsed["year"] = None

        return parsed

    def parse_full_dataset_name(self, dataset_name: str):
        """Parse full dataset name into structured components."""
        parts = dataset_name.split(":")
        scope = parts[0]

        if len(parts) != 2:
            scope = dataset_name.split(".")[0]

        dataset_info = self.parse_scope(scope)
        if dataset_info is None:
            return None

        # Second part of dataset name
        if len(parts) > 1:
            second_part = parts[1].split(".")
        else:
            second_part = dataset_name.split(".")

        if len(second_part) < 5:
            return None

        datasetid = second_part[1]
        physics_process = second_part[2]
        production_step = second_part[3]
        data_format = second_part[4]

        ami_tags = None
        taskid = None
        if len(second_part) > 5:
            ami_tags = re.sub(r'_tid.*', '', second_part[5])
            taskid_with_tid = second_part[-1]
            match = re.search(r'_tid(\d+)', taskid_with_tid)
            if match:
                taskid = match.group(1)

        parsed_data = {
            "scope": scope,
            "dataset_origin": dataset_info["dataset_origin"],
            "year": dataset_info["year"],
            "energy": dataset_info["energy"],
            "b_unit": dataset_info["b_unit"],
            "dataset_category": dataset_info["dataset_category"],
            "run|id": datasetid,
            "stream|physics": physics_process,
            "production_step": production_step,
            "data_format": data_format,
            "ami_tags": ami_tags,
            "root_taskID": taskid
        }

        return parsed_data

    def parse_dataset_column(self, df: pd.DataFrame, column_name='dataset') -> pd.DataFrame:
        parsed_df = df[column_name].apply(self.parse_full_dataset_name).apply(pd.Series)
        return pd.concat([df, parsed_df], axis=1)