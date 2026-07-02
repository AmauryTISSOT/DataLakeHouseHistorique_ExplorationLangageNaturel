"""
Choix du rendu (tableau / graphique) a partir de la FORME du resultat SQL.

Heuristique deterministe (aucun appel LLM) — cf. synthese, decision 8/option A :
  - 1 seule valeur scalaire                -> 'metric'
  - 1 colonne dimension + 1 colonne mesure :
        dimension temporelle (annee/edition/date) -> 'line'
        sinon                                      -> 'bar'
  - tout le reste (>2 colonnes, aucune mesure, 0 ligne) -> 'table'

Le repli 'table' garantit qu'on n'affiche jamais un graphe bancal : en cas de
doute, on montre les donnees brutes. Ce module ne DEPEND PAS de Streamlit ; il
renvoie une simple description que l'app traduira en widgets.

Testable en isolation :
    python nl2sql-app/render.py
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pandas.api.types import is_numeric_dtype

# Noms de colonnes consideres comme temporels -> favorise la courbe.
_TEMPORAL_HINTS = ("annee", "year", "edition", "date", "saison", "season")


@dataclass
class RenderSpec:
    """Description du rendu a produire. `kind` in {'metric','bar','line','table'}."""
    kind: str
    x: str | None = None
    y: str | None = None
    value: object | None = None
    reason: str = ""


def _is_temporal(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in _TEMPORAL_HINTS)


def choose_rendering(df: pd.DataFrame) -> RenderSpec:
    """Decide le rendu adapte a la forme de `df`."""
    if df is None or df.empty:
        return RenderSpec("table", reason="resultat vide")

    n_rows, n_cols = df.shape

    # Scalaire unique -> indicateur.
    if n_rows == 1 and n_cols == 1:
        return RenderSpec("metric", value=df.iat[0, 0], reason="valeur scalaire")

    # Deux colonnes : on tente dimension + mesure.
    if n_cols == 2:
        numeric = [c for c in df.columns if is_numeric_dtype(df[c])]
        if len(numeric) == 1:
            y = numeric[0]
            x = next(c for c in df.columns if c != y)
            kind = "line" if _is_temporal(x) else "bar"
            return RenderSpec(kind, x=x, y=y, reason=f"{x} (dim) + {y} (mesure)")
        if len(numeric) == 2:
            temporal = [c for c in df.columns if _is_temporal(c)]
            if temporal:
                x = temporal[0]
                y = next(c for c in df.columns if c != x)
                return RenderSpec("line", x=x, y=y, reason=f"axe temporel {x}")
            x, y = df.columns[0], df.columns[1]
            return RenderSpec("bar", x=x, y=y, reason="deux mesures, pas d'axe temporel")

    # Repli : trop de colonnes, aucune mesure, ou colonne unique multi-lignes.
    return RenderSpec("table", reason=f"forme {n_rows}x{n_cols} non graphable")


# --- Tests (sans Streamlit) ---------------------------------------------------

if __name__ == "__main__":
    cas = [
        ("scalaire", pd.DataFrame({"total": [15]}), "metric"),
        ("equipe+buts", pd.DataFrame({"equipe": ["FR", "AR"], "buts": [14, 15]}), "bar"),
        ("annee+nb", pd.DataFrame({"annee": [2018, 2022], "nb": [64, 64]}), "line"),
        ("liste 1 col", pd.DataFrame({"equipe": ["FR", "AR", "BR"]}), "table"),
        ("3 colonnes", pd.DataFrame({"a": [1], "b": ["x"], "c": [2.0]}), "table"),
        ("vide", pd.DataFrame({"x": []}), "table"),
    ]
    ok = True
    for label, df, attendu in cas:
        spec = choose_rendering(df)
        etat = "OK" if spec.kind == attendu else f"ECHEC (attendu {attendu})"
        ok &= spec.kind == attendu
        print(f"  {label:14} -> {spec.kind:6} ({spec.reason}) : {etat}")
    print("\nTous les controles sont passes." if ok else "\nDES CONTROLES ONT ECHOUE.")
    raise SystemExit(0 if ok else 1)
