from config_py import logging_constants

TRACE_ENABLED = logging_constants.TRACE_ENABLED
SUPER_DEBUG_ENABLED = logging_constants.SUPER_DEBUG_ENABLED
DIFF_DEBUG_ENABLED = logging_constants.DIFF_DEBUG_ENABLED
OP_GRAPH_DEBUG_ENABLED = logging_constants.OP_GRAPH_DEBUG_ENABLED

# do not modify this behavior
if TRACE_ENABLED:
    SUPER_DEBUG_ENABLED = True
