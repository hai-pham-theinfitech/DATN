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
SESSION_LIFETIME = timedelta(minutes=120)  # Ví dụ: 2 tiếng
SESSION_LIFETIME__PERMANENT = True  # Giữ session vĩnh viễn
FEATURE_FLAGS = {
    "EMBEDDED_REPORTS": True,
    "DASHBOARD_NATIVE_FILTERS": False,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "GENERIC_CHART_AXES": True,
    "SCHEDULED_QUERIES": True,
}
SESSION_COOKIE_DURATION = timedelta(hours=1)


