import re
from datetime import datetime, timezone

# Deliberately tiny and safe: a fixed whitelist of template functions,
# matched with a simple regex - not a general expression evaluator (no
# eval()). Good enough for "stamp this row with the current time," which
# covers the common case, while staying safe for arbitrary YAML input.
_TEMPLATE_PATTERN = re.compile(r"^\{\{\s*(\w+)\(\)\s*\}\}$")

_TEMPLATE_FUNCS = {
    "now": lambda: datetime.now(timezone.utc).isoformat(),
    "today": lambda: datetime.now(timezone.utc).date().isoformat(),
}


def _resolve_value(value):
    if isinstance(value, str):
        match = _TEMPLATE_PATTERN.match(value.strip())
        if match:
            func_name = match.group(1)
            if func_name not in _TEMPLATE_FUNCS:
                raise ValueError(
                    f"add_column: unknown template function '{func_name}()'. "
                    f"Supported: {list(_TEMPLATE_FUNCS)}"
                )
            return _TEMPLATE_FUNCS[func_name]()
    return value  # literal constant - string, number, bool, etc.


class Transform:
    def __init__(self, config):
        self.name = config["name"]
        self.value = config["value"]

    def apply(self, df):
        df = df.copy()
        df[self.name] = _resolve_value(self.value)
        return df
