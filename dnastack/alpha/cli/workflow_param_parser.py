import re
from typing import Dict, Any

from dnastack.common.parser import DotPropertiesParser


class WorkflowParamParser(DotPropertiesParser):
    def parse(self, content: str) -> Dict[str, Any]:
        params_list = []
        split_params = content.split(",")
        for param in split_params:
            if "@" in param:
                key, value = param.split("=")
                if re.search(r"^(.+)\/([^\/]+)$", value[1:]):
                    with open(value[1:]) as param_file:
                        file_content = re.sub(r'[\n\t\s]', "", param_file.read())
                        params_list.append("=".join([key, file_content]))
            else:
                params_list.append(param)
        return DotPropertiesParser.parse(self, "\n".join(params_list))



