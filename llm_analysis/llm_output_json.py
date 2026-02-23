import ast
import json
import re


CODE_BLOCK_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
JSON_CODE_BLOCK_RE = re.compile(r"```json\s*(.*?)```", re.DOTALL | re.IGNORECASE)
TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")
MISSING_COMMA_BEFORE_KEY_RE = re.compile(
    r'("|\}|\]|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*(?="[^"]+"\s*:)',
    re.IGNORECASE,
)


def _strip_fences(text):
    blocks = CODE_BLOCK_RE.findall(text)
    if blocks:
        return blocks
    return []


def _balanced_json_candidates(text):
    candidates = []
    start = None
    depth = 0
    for idx, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = idx
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    candidates.append(text[start:idx + 1])
                    start = None
    return candidates


def _sanitize_json_text(text):
    cleaned = text.strip()
    cleaned = cleaned.replace("\u201c", "\"").replace("\u201d", "\"")
    cleaned = cleaned.replace("\u2018", "'").replace("\u2019", "'")
    cleaned = TRAILING_COMMA_RE.sub(r"\1", cleaned)
    cleaned = MISSING_COMMA_BEFORE_KEY_RE.sub(r"\1, ", cleaned)
    return cleaned


def _close_unbalanced_json(text):
    stack = []
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "\"":
                in_string = False
            continue
        if ch == "\"":
            in_string = True
        elif ch == "{":
            stack.append("}")
        elif ch == "[":
            stack.append("]")
        elif ch in "}]":
            if stack and stack[-1] == ch:
                stack.pop()
    if not stack:
        return text
    return text + "".join(reversed(stack))


def _try_parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    sanitized = _sanitize_json_text(text)
    try:
        return json.loads(sanitized)
    except json.JSONDecodeError:
        pass

    auto_closed = _close_unbalanced_json(sanitized)
    try:
        return json.loads(auto_closed)
    except json.JSONDecodeError:
        pass

    try:
        parsed = ast.literal_eval(auto_closed)
    except (ValueError, SyntaxError):
        return None

    if isinstance(parsed, (dict, list)):
        return parsed
    return None


def normalize_output_schema(obj):
    if not isinstance(obj, dict):
        return obj
    sentiment = obj.get("sentiment_toward_israel")
    if not isinstance(sentiment, dict):
        return obj

    normalized = dict(obj)
    normalized_sentiment = dict(sentiment)
    for key in ("themes", "confidence_and_ambiguity"):
        if key not in normalized and key in normalized_sentiment:
            normalized[key] = normalized_sentiment.pop(key)
    normalized["sentiment_toward_israel"] = normalized_sentiment
    return normalized


def extract_json_objects(text):
    json_blocks = JSON_CODE_BLOCK_RE.findall(text)
    if json_blocks:
        parsed = _try_parse_json(json_blocks[-1])
        if parsed is not None:
            return [normalize_output_schema(parsed)]
        return []

    candidates = []
    candidates.extend(_strip_fences(text))
    candidates.extend(_balanced_json_candidates(text))

    seen = set()
    for cand in reversed(candidates):
        key = cand.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        parsed = _try_parse_json(cand)
        if parsed is not None:
            return [normalize_output_schema(parsed)]

    return []


def has_valid_json_output(text):
    return bool(extract_json_objects(text))
