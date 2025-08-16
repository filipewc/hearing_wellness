import re
from typing import Dict, Optional, List

CANONICALS = {
    "age": [
        r"\bage\b", r"\bidade\b",
        r"how\s*old", r"qual.*idade",
        r"age\s*\(years\)"
    ],
    "gender": [
        r"\bgender\b", r"\bsexo\b",
        r"\bidentit[y|y ]*gender\b", r"\bmale|female|non[- ]?binary|other\b"
    ],
    # campo booleano: se a pessoa relata problema/dificuldade auditiva
    "hearing_issues": [
        r"hearing.*(issue|loss|problem|difficulty)",
        r"difficulty.*hearing", r"problema.*auditiv", r"perda.*auditiv"
    ],
    # uso de aparelho auditivo (opcional, para KPIs)
    "hearing_aid_use": [
        r"hearing.*aid.*(use|usage|using)",
        r"usa.*aparelho", r"utiliza.*aparelho"
    ],
}

def _norm(s: str) -> str:
    return s.strip().lower()

def find_column(columns: List[str], patterns: List[str]) -> Optional[str]:
    lower_cols = {c: _norm(c) for c in columns}
    for col, low in lower_cols.items():
        for pat in patterns:
            if re.search(pat, low):
                return col
    return None

def build_mapping(columns: List[str]) -> Dict[str, Optional[str]]:
    mapping = {}
    for canonical, pats in CANONICALS.items():
        mapping[canonical] = find_column(columns, pats)
    return mapping
