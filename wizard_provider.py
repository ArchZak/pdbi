"""Replace draft_query with a provider that returns query fields or a clarification."""
import re


def draft_query(text):
    """Translate the example prompt. The shared API validates the returned fields."""
    # Keep this sample grammar narrow so unsupported requests stay editable.
    match = re.fullmatch(
        r"(?:find|show)(?: me)? (human|mouse) proteins (?:containing|with motif) ([a-z]+)[.!]?",
        " ".join(text.split()), re.IGNORECASE)
    if not match:
        return {"clarification": 'Try “Find human proteins containing ACD”, or edit the fields below.'}
    organism, motif = match.groups()
    return {"organism": {"human": 9606, "mouse": 10090}[organism.lower()], "motif": motif.upper()}
