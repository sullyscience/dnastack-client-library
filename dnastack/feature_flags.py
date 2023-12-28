import sys
from dnastack.common.environments import flag

# For more details about environment variables, please check out dev-configuration.md.

dev_mode = flag('DNASTACK_DEV',
                description='Make all experimental/work-in-progress functionalities visible')
in_global_debug_mode = flag('DNASTACK_DEBUG',
                            description='Enable the debug mode')
in_interactive_shell = sys.__stdout__ and sys.__stdout__.isatty()
cli_show_list_item_index = flag('DNASTACK_SHOW_LIST_ITEM_INDEX',
                                description='The CLI output will show the index number of items in any list output')
detailed_error = flag('DNASTACK_DETAILED_ERROR', description='Provide more details on error')
show_distributed_trace_stack_on_error = flag('DNASTACK_DISPLAY_TRACE_ON_ERROR',
                                             description='Display distributed trace on error')
