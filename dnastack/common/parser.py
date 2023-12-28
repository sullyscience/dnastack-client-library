import logging
import re
from typing import Any, Dict, List

from dnastack.common.logger import get_logger


class DotPropertiesSyntaxError(RuntimeError):
    def __init__(self, given_path):
        super().__init__(f'Invalid property ({given_path}). The valid property path is "kd0[.kd1[.kd2[...]]]".')


class DotPropertiesDuplicatedPathError(RuntimeError):
    def __init__(self, given_path):
        super().__init__(f'Duplicated property ({given_path})')


class DotPropertiesAmbiguousStructureError(RuntimeError):
    def __init__(self, given_path, stopped_path):
        super().__init__(f'The given property "{given_path}" causes the structural changes at "{stopped_path}".')


class DotPropertiesParser:
    def __init__(self, allow_structural_change: bool = False, allow_value_overriding: bool = False):
        self.__logger = get_logger(type(self).__name__, logging.INFO)
        self.allow_structural_change = allow_structural_change
        self.allow_value_overriding = allow_value_overriding

    def parse(self, content: str) -> Dict[str, Any]:
        data: Dict[str, Any] = dict()

        for line in re.split(r'\r\n|\r|\n', content):
            truncated_line = line.strip()

            if not truncated_line:
                continue

            self.__logger.debug(f'LINE: {truncated_line} ({len(truncated_line)})')
            key, value = truncated_line.split(r'=')

            # Parse the key
            path = []
            path_segment: List[str] = []
            for ch in key:
                if ch == '.':
                    if not path_segment:
                        self.__logger.error(f'Detected that the previous path segment of "{key}" is empty.')
                        raise DotPropertiesSyntaxError(key)
                    # Handle the escaped dot.
                    if path_segment[-1] != '\\':
                        self.__logger.debug(f"ADDED/DOT: [{''.join(path_segment)}]")
                        path.append(''.join(path_segment))
                        path_segment.clear()
                        continue
                    else:
                        path_segment = path_segment[:-1]
                path_segment.append(ch)
            if path_segment:
                self.__logger.debug(f"ADDED/EOL: [{''.join(path_segment)}]")
                path.append(''.join(path_segment))
            else:
                # As the path cannot end empty (with just a dot), this is a syntax error.
                self.__logger.error(f'Detected that the last path segment of "{key}" is empty.')
                raise DotPropertiesSyntaxError(key)

            # Fill in the tree.
            node = data
            max_depth = len(path)
            last_depth = max_depth - 1
            for depth in range(max_depth):
                p_name = path[depth]
                is_array = re.search(r'\[.*?\]', p_name)
                p_name_without_array = re.sub(r'\[.*?\]', '', p_name)
                if depth == last_depth:
                    # The end of the path
                    if p_name in node:
                        if isinstance(value, type(node[p_name])):
                            error_message = f'Detected duplicated path at {key}'
                            self.__logger.error(error_message)
                            raise DotPropertiesDuplicatedPathError(key)
                        else:
                            error_message = f'Detected structural change from "{type(node[p_name]).__name__}" (intermediate node) to "{type(value).__name__}" (end of path) at "{key}"'
                            self.__logger.error(error_message)
                            raise DotPropertiesAmbiguousStructureError(key,
                                                                       '.'.join([path[i] for i in range(depth + 1)]))
                    # Now, set the value.
                    if is_array:
                        node[p_name_without_array] = [value]
                    else:
                        if isinstance(node, list):
                            node.append(value)
                        else:
                            node[p_name] = value
                else:
                    # Continue to traverse the tree.
                    if p_name in node and not isinstance(node.get(p_name), dict):
                        self.__logger.error(f'Prevented structural change from "{type(node[p_name]).__name__}" (end of path) to "dict" (intermediate node) at "{key}"')
                        raise DotPropertiesAmbiguousStructureError(key,
                                                                   '.'.join([path[i] for i in range(depth + 1)]))

                    if is_array:
                        node[p_name_without_array] = list()
                        node = node[p_name_without_array]
                    else:
                        if p_name not in node:
                            node[p_name] = dict()
                        node = node[p_name]

            self.__logger.debug(f'PARSED: {path} = {value}')

        self.__logger.debug(f'END: data = {data}')
        return data
