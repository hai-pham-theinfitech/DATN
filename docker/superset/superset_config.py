from datetime import timedelta
from cachelib.redis import RedisCache


RESULTS_BACKEND = RedisCache(
    host="redis",
    port=6379,
    key_prefix="superset_results",
    default_timeout=300
)


WTF_CSRF_ENABLED = False
WTF_CSRF_EXEMPT_LIST = [r"^/api/"]

FEATURE_FLAGS = {
    "EMBEDDED_REPORTS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "GENERIC_CHART_AXES": True,
    "SCHEDULED_QUERIES": True,
}

