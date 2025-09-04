import json
import logging
import re
from typing import Tuple, List

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    from rapidfuzz import fuzz, process
except ImportError as e:
    raise ImportError(
        "Please install rapidfuzz for best performance:\n"
        "    pip install rapidfuzz"
    ) from e


def normalize_text(s: str) -> str:
    """Lowercase, strip, normalize umlauts/ß, drop punctuation, collapse spaces."""
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = (s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"))
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_signature(s: str, length: int = 8) -> str:
    """Blocking key from normalized company name: first N alnum chars."""
    s = normalize_text(s)
    s = re.sub(r"\W+", "", s)
    return s[:length]


def first_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def combine_name_columns(row: pd.Series) -> str:
    """Combine likely name columns into one comparable string."""
    parts = []
    for col in ["Name", "name", "name2", "Name2", "Name3"]:
        if col in row and pd.notna(row[col]):
            parts.append(str(row[col]))
    if not parts:
        for c in row.index:
            if c.lower() == "name" and pd.notna(row[c]):
                parts.append(str(row[c]))
    return " ".join(parts).strip()


class FuzzyMatcher:
    """
    Duplicate detection across BW/ENIT:
      - exact matches on 'BVD Code' => score=1.0, reason='bvd_code_exact'
      - otherwise fuzzy on name+address with weights
    """

    def __init__(self, name_weight: float = 0.7, address_weight: float = 0.3):
        assert 0 <= name_weight <= 1 and 0 <= address_weight <= 1
        assert abs(name_weight + address_weight - 1.0) < 1e-9
        self.name_weight = name_weight
        self.address_weight = address_weight

        self.dfbw = pd.DataFrame()
        self.dfenit = pd.DataFrame()
        self.dfall = pd.DataFrame()

        self.addr_col = None
        self.bvd_col = None

    def load_data(self, bw_path: str, enit_path: str):
        # Read source files
        self.dfbw = pd.read_csv(bw_path, sep=";", encoding="latin1")
        self.dfenit = pd.read_csv(enit_path, sep=";", encoding="latin1")

        # Tag sources
        self.dfbw["__source__"] = "BW"
        self.dfenit["__source__"] = "ENIT"

        # Column mapping ENIT -> BW
        column_mapping_ENIT_to_BW = {
            "Adresse1": "Address1",
            "BvD Code": "BVD Code",
            "NACE Code": "NACE Codes Primär",
            "Kunde-Nummer": "ID",
        }
        logging.info("Renaming ENIT columns to match BW columns")
        logging.info("Column mapping:\n%s", json.dumps(column_mapping_ENIT_to_BW, indent=2))
        self.dfenit.rename(columns=column_mapping_ENIT_to_BW, inplace=True)

        # Sanity check columns
        for col in self.dfbw.columns:
            if col not in self.dfenit.columns:
                logging.warning("Column %s from BW not found in ENIT", col)
        for col in self.dfenit.columns:
            if col not in self.dfbw.columns:
                logging.warning("Column %s from ENIT not found in BW", col)

        # Combine for optional intra-source search
        self.dfall = pd.concat([self.dfbw, self.dfenit], ignore_index=True)

        # Prepare helpers
        self._prepare_helpers()

    def _prepare_helpers(self):
        # Key fields (best-effort)
        self.addr_col = first_col(self.dfall, ["Address1", "Adresse1"])
        self.bvd_col = first_col(self.dfall, ["BVD Code", "BvD Code"])

        if self.addr_col is None:
            logging.warning("No address column (Address1/Adresse1) found.")
        if self.bvd_col is None:
            logging.warning("No 'BVD Code' column found.")

        # Normalized fields and blocking key
        self.dfall["__full_name__"] = self.dfall.apply(combine_name_columns, axis=1)
        self.dfall["__name_norm__"] = self.dfall["__full_name__"].map(normalize_text)
        self.dfall["__addr_norm__"] = self.dfall[self.addr_col].map(normalize_text) if self.addr_col else ""
        self.dfall["__name_sig__"] = self.dfall["__name_norm__"].map(lambda s: name_signature(s, 8))

    def _exact_bvd_matches(self, left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        """Return exact matches by BVD Code with score=1.0, but only if non-empty and not '0'."""
        if not self.bvd_col:
            return pd.DataFrame(columns=[
                "left_idx", "right_idx", "reason", "name_score", "address_score", "total_score"
            ])

        l = left.reset_index().rename(columns={"index": "left_idx"})
        r = right.reset_index().rename(columns={"index": "right_idx"})

        # Normalize to string for join
        l["_bvd_str"] = l[self.bvd_col].astype(str).str.strip()
        r["_bvd_str"] = r[self.bvd_col].astype(str).str.strip()

        # Filter out empties and "0"
        l = l[~l["_bvd_str"].isin(["", "0", "nan", "NaN"])]
        r = r[~r["_bvd_str"].isin(["", "0", "nan", "NaN"])]

        if l.empty or r.empty:
            return pd.DataFrame(columns=[
                "left_idx", "right_idx", "reason", "name_score", "address_score", "total_score"
            ])

        exact = l.merge(r, on="_bvd_str", how="inner", suffixes=("_l", "_r"))
        if exact.empty:
            return pd.DataFrame(columns=[
                "left_idx", "right_idx", "reason", "name_score", "address_score", "total_score"
            ])

        return pd.DataFrame({
            "left_idx": exact["left_idx"],
            "right_idx": exact["right_idx"],
            "reason": "bvd_code_exact",
            "name_score": 1.0,
            "address_score": 1.0,
            "total_score": 1.0,
        })

    @staticmethod
    def _weighted_score(name_score: float, addr_score: float, w_name: float, w_addr: float) -> float:
        return w_name * name_score + w_addr * addr_score

    def _fuzzy_block_pairs(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        threshold: float
    ) -> pd.DataFrame:
        """
        Create candidate pairs within name-signature blocks and score via rapidfuzz.
        Uses process.cdist to compute full NxM score matrices; we then pick indices >= cutoff.
        """
        results = []

        # Group by blocking key on both sides
        left_blocks = left.groupby("__name_sig__", dropna=False)
        right_blocks = right.groupby("__name_sig__", dropna=False)

        common_keys = set(left_blocks.groups.keys()).intersection(set(right_blocks.groups.keys()))
        if not common_keys:
            return pd.DataFrame(columns=[
                "left_idx", "right_idx", "reason", "name_score", "address_score", "total_score"
            ])

        # Name cutoff slightly below total threshold (since address adds weight)
        # Example: if total threshold is 0.85 and weights=0.7/0.3, allow name >= ~0.60
        name_cutoff = int(100 * max(0.01, threshold - 0.25))

        for key in common_keys:
            gl = left_blocks.get_group(key).copy()
            gr = right_blocks.get_group(key).copy()

            l_idx = gl.index.to_numpy()
            r_idx = gr.index.to_numpy()
            l_names = gl["__name_norm__"].astype(str).tolist()
            r_names = gr["__name_norm__"].astype(str).tolist()
            l_addr  = gl["__addr_norm__"].astype(str).tolist()
            r_addr  = gr["__addr_norm__"].astype(str).tolist()

            # Compute name similarity matrix (ints 0..100); below cutoff becomes 0 if score_cutoff>0
            name_scores = process.cdist(
                l_names, r_names,
                scorer=fuzz.token_set_ratio,
                score_cutoff=name_cutoff  # ensures values < cutoff become 0
            )
            # Address similarity (no cutoff; we filter by total score later)
            addr_scores = process.cdist(
                l_addr, r_addr,
                scorer=fuzz.token_set_ratio,
                score_cutoff=0
            )

            # Iterate rows; pick columns where name score >= cutoff
            # name_scores is a 2D numpy array
            for i in range(name_scores.shape[0]):
                row = name_scores[i]
                # indices of candidates meeting the name cutoff
                cand_js = np.where(row >= name_cutoff)[0]
                if cand_js.size == 0:
                    continue
                for j in cand_js:
                    nscore = float(row[j]) / 100.0
                    ascore = float(addr_scores[i, j]) / 100.0 if addr_scores.ndim == 2 else 0.0
                    total  = self._weighted_score(nscore, ascore, self.name_weight, self.address_weight)
                    if total >= threshold:
                        results.append({
                            "left_idx": l_idx[i],
                            "right_idx": r_idx[j],
                            "reason": "fuzzy_name_address",
                            "name_score": round(nscore, 4),
                            "address_score": round(ascore, 4),
                            "total_score": round(total, 4),
                        })

        if not results:
            return pd.DataFrame(columns=[
                "left_idx", "right_idx", "reason", "name_score", "address_score", "total_score"
            ])
        return pd.DataFrame(results)

    def find_duplicates(
        self,
        cross_source_only: bool = True,
        threshold: float = 0.85,
        export_csv: str | None = None
    ) -> pd.DataFrame:
        """
        Find potential duplicates.
        - cross_source_only=True: only compare BW vs ENIT (typical use case)
        - threshold in [0,1]: minimal total_score to include fuzzy match
        """
        if cross_source_only:
            left = self.dfall[self.dfall["__source__"] == "BW"].copy()
            right = self.dfall[self.dfall["__source__"] == "ENIT"].copy()
        else:
            # Compare full df against itself by splitting on index parity (simple dedup trick)
            df = self.dfall.copy()
            left = df[df.index % 2 == 0]
            right = df[df.index % 2 == 1]

        # 1) Exact matches via BVD Code
        exact_df = self._exact_bvd_matches(left, right)

        # 2) Fuzzy matches via blocking + cdist
        fuzzy_df = self._fuzzy_block_pairs(left, right, threshold=threshold)

        # Merge and enrich with display columns
        out = pd.concat([exact_df, fuzzy_df], ignore_index=True)
        if out.empty:
            logging.info("No matches above threshold=%.2f.", threshold)
            return out

        # Deduplicate pairs (keep best total_score per (left,right))
        out = out.sort_values("total_score", ascending=False).drop_duplicates(subset=["left_idx", "right_idx"])

        # Add context columns
        def take(df_source, idx, col, default=""):
            try:
                return df_source.loc[idx, col] if col in df_source.columns else default
            except Exception:
                return default

        enrich_cols = [
            ("left", left), ("right", right)
        ]
        enriched = out.copy()
        for side, src in enrich_cols:
            enriched[f"{side}_source"] = [take(src, i, "__source__", "") for i in enriched[f"{side}_idx"]]
            enriched[f"{side}_ID"] = [take(src, i, "ID", np.nan) for i in enriched[f"{side}_idx"]]
            enriched[f"{side}_BVD"] = [take(src, i, self.bvd_col, np.nan) for i in enriched[f"{side}_idx"]]
            enriched[f"{side}_Name"] = [take(src, i, "__full_name__", "") for i in enriched[f"{side}_idx"]]
            enriched[f"{side}_Address1"] = [take(src, i, self.addr_col, "") for i in enriched[f"{side}_idx"]]

        # Optional export
        if export_csv:
            cols_order = [
                "left_source", "left_ID", "left_BVD", "left_Name", "left_Address1",
                "right_source", "right_ID", "right_BVD", "right_Name", "right_Address1",
                "reason", "name_score", "address_score", "total_score",
            ]
            # Keep only columns that exist
            cols_order = [c for c in cols_order if c in enriched.columns]
            enriched.to_excel(export_csv, index=False, columns=cols_order)
            logging.info("Exported matches to %s", export_csv)

        return enriched


if __name__ == "__main__":
    matcher = FuzzyMatcher(name_weight=0.7, address_weight=0.3)
    matcher.load_data(
        "./bwenit/2025_09_03_Kunden und Interessenten_BW.csv",
        "./bwenit/2025_09_03_Kunden und Interessenten_ENIT.csv",
    )
    matches = matcher.find_duplicates(
        cross_source_only=True,
        threshold=0.85,
        export_csv="./bwenit/duplicate_candidates.xlsx"
    )
    print(matches.head(20))
    