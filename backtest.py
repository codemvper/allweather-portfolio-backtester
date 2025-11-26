from typing import Dict, Tuple
import logging

import numpy as np
import pandas as pd

from utils import get_logger, align_to_trading_days


def _prepare_price_frame(price_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    series = []
    names = []
    for code, df in price_map.items():
        df = align_to_trading_days(df)
        close_col = "close" if "close" in df.columns else "收盘价"
        s = df[close_col].astype(float).rename(code)
        series.append(s)
        names.append(code)
    prices = pd.concat(series, axis=1).sort_index()
    prices = prices.ffill().dropna(how="all")
    return prices


def _normalize_freq(freq: str):
    if freq is None:
        return None
    f = str(freq).strip().upper()
    return f if f in {"M", "Q", "A", "Y", "W", "D"} else None


def _simulate_rebalanced_portfolio(prices: pd.DataFrame, weights: Dict[str, float], freq: str = "M") -> Tuple[pd.Series, pd.Series, pd.DataFrame, pd.DataFrame]:
    start_val = 1.0
    norm_freq = _normalize_freq(freq)
    rebal_dates = prices.resample(norm_freq).first().index if norm_freq else pd.DatetimeIndex([])

    holding_value = {c: start_val * weights.get(c, 0.0) for c in prices.columns}

    portfolio_values = []
    per_asset_values = []
    events_rows = []
    prev_prices = prices.iloc[0]

    for dt, row in prices.iterrows():
        if norm_freq and dt in rebal_dates:
            total_val = sum(holding_value.values())
            for c in prices.columns:
                holding_value[c] = total_val * weights.get(c, 0.0)
            for c in prices.columns:
                events_rows.append({
                    "date": dt,
                    "event": "fixed_rebalance",
                    "asset": c,
                    "new_weight": float(weights.get(c, 0.0)),
                    "factor": 1.0,
                    "reason": str(norm_freq)
                })

        for c in prices.columns:
            if prev_prices[c] == 0 or np.isnan(prev_prices[c]):
                ret = 0.0
            else:
                ret = (row[c] - prev_prices[c]) / prev_prices[c]
            holding_value[c] *= (1.0 + (ret if not np.isnan(ret) else 0.0))

        total_val = sum(holding_value.values())
        portfolio_values.append((dt, total_val))
        per_asset_values.append((dt, {c: holding_value[c] for c in prices.columns}))
        prev_prices = row

    pf = pd.Series({dt: v for dt, v in portfolio_values}).sort_index()
    asset_df = pd.DataFrame({dt: vals for dt, vals in per_asset_values}).T
    asset_df.index = pf.index
    daily_ret = pf.pct_change().fillna(0.0)
    events_df = pd.DataFrame(events_rows)
    return pf, daily_ret, asset_df, events_df


def _simulate_tvalue_portfolio(prices: pd.DataFrame, weights: Dict[str, float], sma_short: int = 50, sma_mid: int = 100, sma_long: int = 200, confirm_days: int = 5, cooldown_days: int = 10) -> Tuple[pd.Series, pd.Series, pd.DataFrame, pd.DataFrame]:
    start_val = 1.0
    codes = list(prices.columns)
    cash_code = None
    bond_code = None
    for c in codes:
        if "511880" in c:
            cash_code = c
        if "511010" in c:
            bond_code = c
    equity_like = [c for c in codes if c not in {cash_code, bond_code}]

    holding_value = {c: start_val * weights.get(c, 0.0) for c in prices.columns}
    portfolio_values = []
    per_asset_values = []
    events_rows = []

    factors = {c: 1.0 for c in equity_like}
    last_change = {c: None for c in equity_like}

    sma50 = prices[equity_like].rolling(int(sma_short)).mean()
    sma100 = prices[equity_like].rolling(int(sma_mid)).mean()
    sma200 = prices[equity_like].rolling(int(sma_long)).mean()
    ret10 = prices[equity_like] / prices[equity_like].shift(10) - 1.0

    tier_map = {0: 0.0, 1: 0.5, 2: 1.0, 3: 2.0}

    def tier_from_factor(f):
        if f >= 1.5:
            return 3
        if f >= 0.75:
            return 2
        if f > 0.0:
            return 1
        return 0

    prev_prices = prices.iloc[0]

    for dt, row in prices.iterrows():
        changed = False
        asset_reason = {}
        asset_prev_tier = {}
        asset_new_tier = {}
        for c in equity_like:
            p = row[c]
            s50 = sma50.loc[dt, c]
            s100 = sma100.loc[dt, c]
            s200 = sma200.loc[dt, c]
            if np.isnan(s50) or np.isnan(s100) or np.isnan(s200):
                continue
            t_val = int((p > s50)) + int((p > s100)) + int((p > s200))
            target_tier = t_val
            conf = False
            pos = prices.index.get_loc(dt)
            if isinstance(pos, int) and pos >= int(confirm_days) - 1:
                idxs = prices.index[pos - (int(confirm_days) - 1) : pos + 1]
                vals = []
                for ix in idxs:
                    pv = int((prices.loc[ix, c] > sma50.loc[ix, c])) + int((prices.loc[ix, c] > sma100.loc[ix, c])) + int((prices.loc[ix, c] > sma200.loc[ix, c]))
                    vals.append(pv)
                if len(set(vals)) == 1 and vals[-1] == target_tier:
                    conf = True

            cooldown = False
            if last_change[c] is not None:
                cooldown = (dt - last_change[c]).days < int(cooldown_days)

            desired_factor = tier_map.get(target_tier, 1.0)
            cur_factor = factors[c]
            cur_tier = tier_from_factor(cur_factor)

            if not cooldown and (target_tier < cur_tier) and desired_factor != cur_factor:
                new_tier = max(target_tier, cur_tier - 1)
                new_factor = tier_map[new_tier]
                factors[c] = new_factor
                last_change[c] = dt
                changed = True
                asset_reason[c] = "down_cross"
                asset_prev_tier[c] = cur_tier
                asset_new_tier[c] = new_tier
            elif not cooldown and conf and desired_factor != cur_factor:
                factors[c] = desired_factor
                last_change[c] = dt
                changed = True
                asset_reason[c] = "confirm"
                asset_prev_tier[c] = cur_tier
                asset_new_tier[c] = target_tier
            else:
                r10 = ret10.loc[dt, c]
                if not cooldown and not np.isnan(r10):
                    if r10 >= 0.06 and cur_tier < 3:
                        new_tier = min(3, cur_tier + 1)
                        new_factor = tier_map[new_tier]
                        if new_factor != cur_factor:
                            factors[c] = new_factor
                            last_change[c] = dt
                            changed = True
                            asset_reason[c] = "fast_up"
                            asset_prev_tier[c] = cur_tier
                            asset_new_tier[c] = new_tier
                    elif r10 <= -0.06 and cur_tier > 0:
                        new_tier = max(0, cur_tier - 1)
                        new_factor = tier_map[new_tier]
                        if new_factor != cur_factor:
                            factors[c] = new_factor
                            last_change[c] = dt
                            changed = True
                            asset_reason[c] = "fast_down"
                            asset_prev_tier[c] = cur_tier
                            asset_new_tier[c] = new_tier

        if changed:
            total_val = sum(holding_value.values())
            eq_new = {c: weights.get(c, 0.0) * factors[c] for c in equity_like}
            sum_eq = float(sum(eq_new.values()))
            w_cash = weights.get(cash_code, 0.0) if cash_code else 0.0
            w_bond = weights.get(bond_code, 0.0) if bond_code else 0.0
            base_eq_sum = float(sum(weights.get(c, 0.0) for c in equity_like))
            delta = sum_eq - base_eq_sum
            target_cb = max(0.0, 1.0 - sum_eq)
            if delta >= 0.0:
                reduce_cash = min(w_cash, delta)
                cash_new = max(0.0, w_cash - reduce_cash)
                bond_new = max(0.0, target_cb - cash_new)
            else:
                release = -delta
                cash_new = min(target_cb, w_cash + release)
                bond_new = max(0.0, target_cb - cash_new)

            new_w = {c: 0.0 for c in prices.columns}
            for k, v in eq_new.items():
                new_w[k] = v
            if cash_code:
                new_w[cash_code] = cash_new
            if bond_code:
                new_w[bond_code] = bond_new
            sumb = float(sum(new_w.values()))
            if sumb > 0:
                for k in new_w:
                    new_w[k] = new_w[k] / sumb
            for c in prices.columns:
                holding_value[c] = total_val * new_w.get(c, 0.0)
            for c in prices.columns:
                s50 = sma50.loc[dt, c] if c in sma50.columns else np.nan
                s100 = sma100.loc[dt, c] if c in sma100.columns else np.nan
                s200 = sma200.loc[dt, c] if c in sma200.columns else np.nan
                r10 = ret10.loc[dt, c] if c in ret10.columns else np.nan
                events_rows.append({
                    "date": dt,
                    "event": "tvalue_rebalance",
                    "asset": c,
                    "new_weight": float(new_w.get(c, 0.0)),
                    "factor": float(factors.get(c, 1.0)) if c in equity_like else 1.0,
                    "reason": asset_reason.get(c, ""),
                    "prev_tier": int(asset_prev_tier.get(c, tier_from_factor(factors.get(c, 1.0)))),
                    "new_tier": int(asset_new_tier.get(c, tier_from_factor(factors.get(c, 1.0)))),
                    "price": float(row[c]),
                    "sma50": float(s50) if not np.isnan(s50) else None,
                    "sma100": float(s100) if not np.isnan(s100) else None,
                    "sma200": float(s200) if not np.isnan(s200) else None,
                    "ret10": float(r10) if not np.isnan(r10) else None,
                    "cooldown": bool(last_change.get(c) is not None and (dt - last_change[c]).days < 10)
                })

        for c in prices.columns:
            prev = prev_prices[c]
            if prev == 0 or np.isnan(prev):
                ret = 0.0
            else:
                ret = (row[c] - prev) / prev
            holding_value[c] *= (1.0 + (ret if not np.isnan(ret) else 0.0))

        total_val = sum(holding_value.values())
        portfolio_values.append((dt, total_val))
        per_asset_values.append((dt, {c: holding_value[c] for c in prices.columns}))
        prev_prices = row

    pf = pd.Series({dt: v for dt, v in portfolio_values}).sort_index()
    asset_df = pd.DataFrame({dt: vals for dt, vals in per_asset_values}).T
    asset_df.index = pf.index
    daily_ret = pf.pct_change().fillna(0.0)
    events_df = pd.DataFrame(events_rows)
    return pf, daily_ret, asset_df, events_df


def backtest(prices_map: Dict[str, pd.DataFrame], weights: Dict[str, float], start_date: str = None, end_date: str = None, freq: str = "M", strategy: str = "fixed", sma_short: int = 50, sma_mid: int = 100, sma_long: int = 200, confirm_days: int = 5, cooldown_days: int = 10):
    logger = get_logger("backtest")
    prices = _prepare_price_frame(prices_map)
    if start_date:
        prices = prices.loc[pd.to_datetime(start_date):]
    if end_date:
        prices = prices.loc[:pd.to_datetime(end_date)]
    prices = prices.dropna(how="all")
    logger.info(f"Price frame prepared: {prices.index.min()} -> {prices.index.max()} | {list(prices.columns)}")

    if str(strategy).lower() == "tvalue":
        pf, daily_ret, asset_val, events = _simulate_tvalue_portfolio(prices, weights, sma_short=sma_short, sma_mid=sma_mid, sma_long=sma_long, confirm_days=confirm_days, cooldown_days=cooldown_days)
    else:
        pf, daily_ret, asset_val, events = _simulate_rebalanced_portfolio(prices, weights, freq=freq)
    return pf, daily_ret, asset_val, prices, events


def max_drawdown(series: pd.Series) -> Tuple[float, pd.Timestamp, pd.Timestamp]:
    cummax = series.cummax()
    drawdown = series / cummax - 1.0
    mdd = drawdown.min()
    end = drawdown.idxmin()
    start = series.loc[:end].idxmax()
    return float(mdd), start, end