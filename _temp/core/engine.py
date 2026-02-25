from typing import List, Tuple

from .defs import CurrencyPair, Exchanges, Triangle


def calculate_price_abnormalities(trades: List[CurrencyPair]) -> float:
    """exploit price abnormalities"""
    if len(trades) > 1:
        ratio = trades[0].ratio
        for cur in trades[1:]:
            ratio *= cur.ratio
        return ratio
    else:
        return 1.0


def extract_triangles(ex: Exchanges, threshold: float = 0.0, currency: str = None) -> List[Triangle]:
    mapping = {pair.code: pair for pair in ex.pairs}
    curs_base, curs_quota = set([pair.base for pair in ex.pairs]), set([pair.quota for pair in ex.pairs])
    curs_all = curs_base.union(curs_quota)
    triangles = []
    #
    curs_iter = [currency] if currency else curs_quota
    #
    for cur in curs_iter:
        for cur1 in curs_base:
            for cur2 in curs_quota:
                if cur != cur1 and cur != cur2 and cur1 != cur2:
                    pair1 = mapping.get(
                        CurrencyPair.build_code(cur1, cur)
                    )
                    if not pair1:
                        continue
                    pair2 = mapping.get(
                        CurrencyPair.build_code(cur2, cur1)
                    )
                    if not pair2:
                        continue
                    pair3 = mapping.get(
                        CurrencyPair.build_code(cur, cur2)
                    )
                    if pair1 and pair2 and pair3:
                        abnormalities = calculate_price_abnormalities(trades=[pair1, pair2, pair3])
                        win = abs(1-abnormalities)
                        if win > threshold:
                            triangles.append(
                                Triangle(pair1, pair2, pair3)
                            )
    #
    return triangles
