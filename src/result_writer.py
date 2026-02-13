import json
import os
from typing import Any, Dict


class ResultWriter:
    def __init__(self, output_path: str):
        self.output_path = output_path
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    def append_template_result(self, result_obj: Dict[str, Any]) -> None:
        line = json.dumps(result_obj, ensure_ascii=False)
        with open(self.output_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
