import re

_MDV2_SPECIAL = re.compile(r"([_*\[\]()~`>#+\-=|{}.!\\])")


def escape(text: str) -> str:
    return _MDV2_SPECIAL.sub(r"\\\1", text)
