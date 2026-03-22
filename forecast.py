import pandas as pd
import numpy as np

FILE = "data/CFL_External_Data_Pack_Phase1.xlsx"

# ── SHEET 1: Actuals + Expert Forecasts ───────────────────────────────────────
raw = pd.read_excel(FILE, sheet_name="Data Pack - Actual Bookings", header=None)

ACTUAL_COLS = list(range(3, 15))
QUARTERS    = ["FY23Q2","FY23Q3","FY23Q4",
               "FY24Q1","FY24Q2","FY24Q3","FY24Q4",
               "FY25Q1","FY25Q2","FY25Q3","FY25Q4","FY26Q1"]

products = []
for i in range(3, 33):
    row = raw.iloc[i]
    rank      = row[0]
    name      = str(row[1]).strip()
    lifecycle = str(row[2]).strip()
    actuals   = []
    for c in ACTUAL_COLS:
        val = row[c]
        actuals.append(float(val) if pd.notna(val) else None)
    dp  = float(row[16]) if pd.notna(row[16]) else None
    mkt = float(row[17]) if pd.notna(row[17]) else None
    ds  = float(row[18]) if pd.notna(row[18]) else None
    products.append({"rank": int(rank), "name": name, "lifecycle": lifecycle,
                     "actuals": actuals, "dp": dp, "mkt": mkt, "ds": ds})

df_main = pd.DataFrame(products)

# ── SHEET 2: Accuracy table ───────────────────────────────────────────────────
acc_rows = []
for i in range(36, 66):
    row = raw.iloc[i]
    if pd.isna(row[0]): continue
    def pct(v):
        try: return float(str(v).replace("%","").strip())
        except: return None
    acc_rows.append({
        "rank":        int(row[0]),
        "dp_acc_q1":   pct(row[2]),   "dp_bias_q1":  pct(row[3]),
        "dp_acc_q4":   pct(row[4]),   "dp_bias_q4":  pct(row[5]),
        "dp_acc_q3":   pct(row[6]),   "dp_bias_q3":  pct(row[7]),
        "mkt_acc_q1":  pct(row[9]),   "mkt_bias_q1": pct(row[10]),
        "mkt_acc_q4":  pct(row[11]),  "mkt_bias_q4": pct(row[12]),
        "mkt_acc_q3":  pct(row[13]),  "mkt_bias_q3": pct(row[14]),
        "ds_acc_q1":   pct(row[16]),  "ds_bias_q1":  pct(row[17]),
        "ds_acc_q4":   pct(row[18]),  "ds_bias_q4":  pct(row[19]),
        "ds_acc_q3":   pct(row[20]),  "ds_bias_q3":  pct(row[21]),
    })
df_acc = pd.DataFrame(acc_rows)

# ── SHEET 3: Big Deal ─────────────────────────────────────────────────────────
bd_raw = pd.read_excel(FILE, sheet_name="Big Deal", header=None)
BD_QUARTERS = ["2024Q2","2024Q3","2024Q4","2025Q1","2025Q2","2025Q3","2025Q4","2026Q1"]

big_deals, avg_deals = [], []
for i in range(1, 31):
    row = bd_raw.iloc[i]
    if pd.isna(row[0]): continue
    rank = int(row[0])
    name = str(row[1]).strip()
    bd = {q: (float(row[10+j]) if pd.notna(row[10+j]) else 0.0)
          for j, q in enumerate(BD_QUARTERS)}
    ad = {q: (float(row[18+j]) if pd.notna(row[18+j]) else 0.0)
          for j, q in enumerate(BD_QUARTERS)}
    big_deals.append({"rank": rank, "name": name, **{f"bd_{q}": v for q,v in bd.items()}})
    avg_deals.append({"rank": rank, "name": name, **{f"ad_{q}": v for q,v in ad.items()}})

df_bd = pd.DataFrame(big_deals)
df_ad = pd.DataFrame(avg_deals)

# ── SHEET 4: SCMS ─────────────────────────────────────────────────────────────
scms_raw = pd.read_excel(FILE, sheet_name="SCMS", header=None)
SCMS_QTRS = ["2023Q1","2023Q2","2023Q3","2023Q4",
             "2024Q1","2024Q2","2024Q3","2024Q4",
             "2025Q1","2025Q2","2025Q3","2025Q4","2026Q1"]
scms_rows = []
for i in range(2, scms_raw.shape[0]):
    row = scms_raw.iloc[i]
    if pd.isna(row[0]): continue
    entry = {"rank": int(row[0]), "name": str(row[1]).strip(), "segment": str(row[2]).strip()}
    for j, q in enumerate(SCMS_QTRS):
        entry[q] = float(row[3+j]) if pd.notna(row[3+j]) else 0.0
    scms_rows.append(entry)
df_scms = pd.DataFrame(scms_rows)

# ── SHEET 5: VMS ──────────────────────────────────────────────────────────────
vms_raw = pd.read_excel(FILE, sheet_name="VMS", header=None)
vms_rows = []
for i in range(2, vms_raw.shape[0]):
    row = vms_raw.iloc[i]
    if pd.isna(row[0]): continue
    entry = {"rank": int(row[0]), "name": str(row[1]).strip(), "vertical": str(row[2]).strip()}
    for j, q in enumerate(SCMS_QTRS):
        entry[q] = float(row[3+j]) if pd.notna(row[3+j]) else 0.0
    vms_rows.append(entry)
df_vms = pd.DataFrame(vms_rows)

print("=== LOAD COMPLETE ===")
print(f"Products: {len(df_main)} | Accuracy rows: {len(df_acc)} | "
      f"Big deal: {len(df_bd)} | SCMS: {len(df_scms)} | VMS: {len(df_vms)}")

# ── STEP 3: BUILD MASTER DATAFRAME ───────────────────────────────────────────
master = []

for _, p in df_main.iterrows():
    rank = p['rank']

    acc_row = df_acc[df_acc['rank'] == rank]
    if len(acc_row) > 0:
        a = acc_row.iloc[0]
        def weighted_acc(q1, q4, q3):
            vals = [(q1,0.5),(q4,0.3),(q3,0.2)]
            total_w, total = 0, 0
            for v,w in vals:
                if v is not None and not (isinstance(v,float) and np.isnan(v)):
                    total += v*w; total_w += w
            return total/total_w if total_w > 0 else None
        dp_wacc  = weighted_acc(a['dp_acc_q1'],  a['dp_acc_q4'],  a['dp_acc_q3'])
        mkt_wacc = weighted_acc(a['mkt_acc_q1'], a['mkt_acc_q4'], a['mkt_acc_q3'])
        ds_wacc  = weighted_acc(a['ds_acc_q1'],  a['ds_acc_q4'],  a['ds_acc_q3'])
    else:
        dp_wacc = mkt_wacc = ds_wacc = None

    ad_row = df_ad[df_ad['rank'] == rank]
    if len(ad_row) > 0:
        ad = ad_row.iloc[0]
        avg_deals_series = {f'ad_{q}': ad.get(f'ad_{q}', None) for q in BD_QUARTERS}
    else:
        avg_deals_series = {f'ad_{q}': None for q in BD_QUARTERS}

    scms_prod = df_scms[df_scms['rank'] == rank]
    scms_q2_yoy = None
    if len(scms_prod) > 0:
        t24 = scms_prod['2024Q2'].sum()
        t25 = scms_prod['2025Q2'].sum()
        if t24 > 0: scms_q2_yoy = (t25 - t24) / t24

    vms_prod = df_vms[df_vms['rank'] == rank]
    top_vertical = top_vertical_growth = None
    if len(vms_prod) > 0:
        v_growth = []
        for _, vrow in vms_prod.iterrows():
            v24 = vrow.get('2024Q2', 0)
            v25 = vrow.get('2025Q2', 0)
            if v24 and v24 > 0:
                v_growth.append((vrow['vertical'], (v25-v24)/v24))
        if v_growth:
            v_growth.sort(key=lambda x: x[1], reverse=True)
            top_vertical, top_vertical_growth = v_growth[0]

    master.append({
        'rank': rank, 'name': p['name'], 'lifecycle': p['lifecycle'],
        'actuals': p['actuals'],
        'dp': p['dp'], 'mkt': p['mkt'], 'ds': p['ds'],
        'dp_wacc': dp_wacc, 'mkt_wacc': mkt_wacc, 'ds_wacc': ds_wacc,
        'scms_q2_yoy': scms_q2_yoy,
        'top_vertical': top_vertical, 'top_vertical_growth': top_vertical_growth,
        **avg_deals_series
    })

df_master = pd.DataFrame(master)
print(f"Master dataframe: {df_master.shape}")

# ── STEP 4: STATISTICAL METHODS + BACK-TEST ──────────────────────────────────

def get_valid(series, indices):
    return [series[i] for i in indices if series[i] is not None]

def four_qtr_avg(series, up_to_idx):
    vals = [series[i] for i in range(max(0, up_to_idx-4), up_to_idx)
            if series[i] is not None]
    return sum(vals)/len(vals) if vals else None

def exponential_decay_forecast(series, up_to_idx):
    vals = [(i, series[i]) for i in range(max(0, up_to_idx-6), up_to_idx)
            if series[i] is not None]
    if len(vals) < 2: return four_qtr_avg(series, up_to_idx)
    qoq = []
    for j in range(1, len(vals)):
        prev = vals[j-1][1]
        curr = vals[j][1]
        if prev > 0: qoq.append(curr/prev)
    if not qoq: return four_qtr_avg(series, up_to_idx)
    weights = [0.1*(1.5**i) for i in range(len(qoq))]
    total_w = sum(weights)
    weighted_ratio = sum(r*w for r,w in zip(qoq, weights)) / total_w
    latest = vals[-1][1]
    result = latest * weighted_ratio
    if result is None or np.isnan(result): return four_qtr_avg(series, up_to_idx)
    return result

def seasonality_forecast(series, base_avg):
    fy24 = get_valid(series, [3,4,5,6])
    fy25 = get_valid(series, [7,8,9,10])
    fy24_avg = sum(fy24)/len(fy24) if len(fy24) >= 2 else None
    fy25_avg = sum(fy25)/len(fy25) if len(fy25) >= 2 else None
    ratios = []
    if fy24_avg and series[4]: ratios.append(series[4]/fy24_avg)
    if fy25_avg and series[8]: ratios.append(series[8]/fy25_avg)
    if not ratios: return base_avg
    return base_avg * (sum(ratios)/len(ratios)) if base_avg else None

def yoy_q2_trend(series):
    q2_vals = [(i, series[i]) for i in [0,4,8] if series[i] is not None]
    if len(q2_vals) < 2: return None
    yoy_changes = []
    for j in range(1, len(q2_vals)):
        prev = q2_vals[j-1][1]; curr = q2_vals[j][1]
        if prev > 0: yoy_changes.append((curr-prev)/prev)
    if not yoy_changes: return None
    return q2_vals[-1][1] * (1 + sum(yoy_changes)/len(yoy_changes))

def sqly(series): return series[8]

def accuracy_score(forecast, actual):
    if actual is None or actual == 0 or forecast is None: return None
    return 1 - abs((forecast-actual)/actual)

def weighted_ensemble_stat(forecasts_with_acc):
    valid = [(f,a) for f,a in forecasts_with_acc
             if f is not None and a is not None and a > 0]
    if not valid: return None
    if len(valid) == 1: return valid[0][0]
    weights = [a**2 for _,a in valid]
    total_w = sum(weights)
    return sum(f*w for (f,_),w in zip(valid, weights)) / total_w

def apply_bias_correction(expert_val, bias):
    if expert_val is None or bias is None: return expert_val
    if abs(bias) > 0.05: return expert_val / (1 + bias)
    return expert_val

def safe_round(v, decimals=0):
    if v is None: return None
    try:
        if np.isnan(v): return None
    except: pass
    return round(v, decimals) if decimals > 0 else round(v)

def backtest_q1(series):
    actual_q1 = series[11]
    if actual_q1 is None: return {}
    bt_avg4    = four_qtr_avg(series, 11)
    bt_sqly    = series[7]
    fy24       = get_valid(series, [3,4,5,6])
    fy25       = get_valid(series, [7,8,9,10])
    fy24_avg   = sum(fy24)/len(fy24) if len(fy24)>=2 else None
    fy25_avg   = sum(fy25)/len(fy25) if len(fy25)>=2 else None
    q1_ratios  = []
    if fy24_avg and series[3]: q1_ratios.append(series[3]/fy24_avg)
    if fy25_avg and series[7]: q1_ratios.append(series[7]/fy25_avg)
    avg_q1_ratio = sum(q1_ratios)/len(q1_ratios) if q1_ratios else None
    bt_seasonal  = bt_avg4 * avg_q1_ratio if (bt_avg4 and avg_q1_ratio) else bt_avg4
    bt_decay     = exponential_decay_forecast(series, 11)
    q1_vals = [(i, series[i]) for i in [3,7] if series[i] is not None]
    if len(q1_vals) >= 2:
        yoy = (q1_vals[-1][1]-q1_vals[-2][1])/q1_vals[-2][1]
        bt_yoy = q1_vals[-1][1]*(1+yoy)
    else:
        bt_yoy = bt_sqly
    return {
        'avg4':     (bt_avg4,    accuracy_score(bt_avg4,    actual_q1)),
        'sqly':     (bt_sqly,    accuracy_score(bt_sqly,    actual_q1)),
        'seasonal': (bt_seasonal,accuracy_score(bt_seasonal,actual_q1)),
        'yoy':      (bt_yoy,     accuracy_score(bt_yoy,     actual_q1)),
        'decay':    (bt_decay,   accuracy_score(bt_decay,   actual_q1)),
    }

# ── RUN FOR ALL 30 PRODUCTS ───────────────────────────────────────────────────
results = []

for _, p in df_master.iterrows():
    series    = p['actuals']
    rank      = p['rank']
    lc        = p['lifecycle']
    actual_q1 = series[11]

    bt_scores = backtest_q1(series)

    valid_bt = {k: v for k,v in bt_scores.items() if v[1] is not None and v[1] > 0}
    best_stat_method = max(valid_bt, key=lambda k: valid_bt[k][1]) if valid_bt else 'avg4'
    best_stat_acc    = valid_bt[best_stat_method][1] if valid_bt else None

    base_avg         = four_qtr_avg(series, 12)
    f_avg4           = base_avg
    f_sqly           = sqly(series)
    f_seasonal       = seasonality_forecast(series, base_avg)
    f_yoy            = yoy_q2_trend(series)
    f_decay          = exponential_decay_forecast(series, 12)

    ad_last4 = [p.get(f'ad_{q}') for q in ['2025Q1','2025Q2','2025Q3','2025Q4']]
    ad_last4 = [v for v in ad_last4 if v is not None]
    ad_base  = sum(ad_last4)/len(ad_last4) if ad_last4 else base_avg
    fy24_ad  = [p.get(f'ad_{q}') for q in ['2024Q2','2024Q3','2024Q4']]
    fy24_ad  = [v for v in fy24_ad if v is not None]
    fy25_ad  = [p.get(f'ad_{q}') for q in ['2025Q1','2025Q2','2025Q3','2025Q4']]
    fy25_ad  = [v for v in fy25_ad if v is not None]
    fy24_ad_avg = sum(fy24_ad)/len(fy24_ad) if fy24_ad else None
    fy25_ad_avg = sum(fy25_ad)/len(fy25_ad) if fy25_ad else None
    ad_ratios = []
    if fy24_ad_avg and p.get('ad_2024Q2'): ad_ratios.append(p['ad_2024Q2']/fy24_ad_avg)
    if fy25_ad_avg and p.get('ad_2025Q2'): ad_ratios.append(p['ad_2025Q2']/fy25_ad_avg)
    ad_ratio = sum(ad_ratios)/len(ad_ratios) if ad_ratios else 1.0
    f_seasonal_clean = ad_base * ad_ratio

    if lc == 'Decline': f_avg4 = f_decay

    ensemble_inputs = [
        (f_avg4,           bt_scores.get('avg4',    (None,None))[1]),
        (f_sqly,           bt_scores.get('sqly',    (None,None))[1]),
        (f_seasonal_clean, bt_scores.get('seasonal',(None,None))[1]),
    ]
    # Include decay for ALL products if it had decent backtest
    decay_acc = bt_scores.get('decay',(None,None))[1]
    if decay_acc and decay_acc > 0.5:
        ensemble_inputs.append((f_decay, decay_acc))
    yoy_acc = bt_scores.get('yoy',(None,None))[1]
    if yoy_acc and yoy_acc > 0.5:
        ensemble_inputs.append((f_yoy, yoy_acc))

    f_ensemble = weighted_ensemble_stat(ensemble_inputs)
    if f_ensemble is None or (isinstance(f_ensemble, float) and np.isnan(f_ensemble)):
        f_ensemble = four_qtr_avg(series, 12)

    scms_yoy = p.get('scms_q2_yoy')
    scms_adj = 1.0
    if scms_yoy is not None and not np.isnan(scms_yoy):
        scms_adj = 1 + (scms_yoy * 0.15)
        scms_adj = max(0.80, min(1.20, scms_adj))
    f_ensemble_scms = f_ensemble * scms_adj if f_ensemble else f_ensemble

    # Expert-range clamping: if all 3 experts agree within ±30%,
    # don't let stat ensemble exceed 1.3x the max expert forecast
    expert_vals = [v for v in [p['dp'], p['mkt'], p['ds']] if v is not None]
    if len(expert_vals) >= 2:
        exp_max = max(expert_vals)
        exp_min = min(expert_vals)
        # Check if experts agree (max/min ratio < 1.8)
        if exp_min > 0 and exp_max / exp_min < 1.8:
            upper_clamp = exp_max * 1.3
            lower_clamp = exp_min * 0.7
            if f_ensemble_scms is not None and f_ensemble_scms > upper_clamp:
                f_ensemble_scms = upper_clamp
            elif f_ensemble_scms is not None and f_ensemble_scms < lower_clamp:
                f_ensemble_scms = lower_clamp

    acc_row = df_acc[df_acc['rank'] == rank]
    dp_bias = mkt_bias = ds_bias = None
    if len(acc_row) > 0:
        a = acc_row.iloc[0]
        def weighted_bias(b1, b4, b3):
            vals = [(b1,0.5),(b4,0.3),(b3,0.2)]
            total_w, total = 0, 0
            for v,w in vals:
                if v is not None and not (isinstance(v,float) and np.isnan(v)):
                    total += v*w; total_w += w
            return total/total_w if total_w > 0 else None
        dp_bias  = weighted_bias(a['dp_bias_q1'],  a['dp_bias_q4'],  a['dp_bias_q3'])
        mkt_bias = weighted_bias(a['mkt_bias_q1'], a['mkt_bias_q4'], a['mkt_bias_q3'])
        ds_bias  = weighted_bias(a['ds_bias_q1'],  a['ds_bias_q4'],  a['ds_bias_q3'])

    dp_corrected  = apply_bias_correction(p['dp'],  dp_bias)
    mkt_corrected = apply_bias_correction(p['mkt'], mkt_bias)
    ds_corrected  = apply_bias_correction(p['ds'],  ds_bias)

    results.append({
        'rank': rank, 'name': p['name'], 'lifecycle': lc,
        'actual_q1': actual_q1,
        'bt_avg4':       bt_scores.get('avg4',    (None,None))[1],
        'bt_sqly':       bt_scores.get('sqly',    (None,None))[1],
        'bt_seasonal':   bt_scores.get('seasonal',(None,None))[1],
        'bt_yoy':        bt_scores.get('yoy',     (None,None))[1],
        'bt_decay':      bt_scores.get('decay',   (None,None))[1],
        'bt_avg4_val':   bt_scores.get('avg4',    (None,None))[0],
        'bt_sqly_val':   bt_scores.get('sqly',    (None,None))[0],
        'bt_seasonal_val':bt_scores.get('seasonal',(None,None))[0],
        'bt_yoy_val':    bt_scores.get('yoy',     (None,None))[0],
        'bt_decay_val':  bt_scores.get('decay',   (None,None))[0],
        'best_stat_method': best_stat_method,
        'best_stat_acc':    best_stat_acc,
        'f_avg4': f_avg4, 'f_sqly': f_sqly,
        'f_seasonal': f_seasonal, 'f_seasonal_clean': f_seasonal_clean,
        'f_yoy': f_yoy, 'f_decay': f_decay,
        'f_ensemble': f_ensemble, 'f_ensemble_scms': f_ensemble_scms,
        'scms_adj': scms_adj,
        'dp': p['dp'], 'mkt': p['mkt'], 'ds': p['ds'],
        'dp_corrected': dp_corrected, 'mkt_corrected': mkt_corrected,
        'ds_corrected': ds_corrected,
        'dp_bias': dp_bias, 'mkt_bias': mkt_bias, 'ds_bias': ds_bias,
        'dp_wacc': p['dp_wacc'], 'mkt_wacc': p['mkt_wacc'], 'ds_wacc': p['ds_wacc'],
        'scms_q2_yoy': p.get('scms_q2_yoy'),
    })

df_results = pd.DataFrame(results)

print("\n=== STEP 4: BACK-TEST RESULTS ===")
print(f"{'Rk':<3} {'Product':<40} {'LC':<10} {'4QA':>6} {'SQLY':>6} "
      f"{'Seas':>6} {'YoY':>6} {'Decay':>6} {'Best':>8}")
print("-"*95)
for _, r in df_results.iterrows():
    def fmt(v): return f"{v:.2f}" if v is not None else " n/a"
    print(f"[{r['rank']:>2}] {r['name'][:38]:<38} {r['lifecycle'][:9]:<9} "
          f"{fmt(r['bt_avg4']):>6} {fmt(r['bt_sqly']):>6} "
          f"{fmt(r['bt_seasonal']):>6} {fmt(r['bt_yoy']):>6} "
          f"{fmt(r['bt_decay']):>6}  {r['best_stat_method']:<8}")

# ── STEP 5: BLENDING ─────────────────────────────────────────────────────────

def get_best_expert_corrected(row):
    options = []
    if row['dp_corrected']  is not None and row['dp_wacc']  is not None:
        options.append(('dp',  row['dp_corrected'],  row['dp_wacc']))
    if row['mkt_corrected'] is not None and row['mkt_wacc'] is not None:
        options.append(('mkt', row['mkt_corrected'], row['mkt_wacc']))
    if row['ds_corrected']  is not None and row['ds_wacc']  is not None:
        options.append(('ds',  row['ds_corrected'],  row['ds_wacc']))
    if not options: return None, None, None
    best = max(options, key=lambda x: x[2])
    return best[0], best[1], best[2]

def blend_improved(row):
    lc = row['lifecycle']
    best_exp_name, best_exp_val, best_exp_wacc = get_best_expert_corrected(row)
    stat_val = row['f_ensemble_scms']
    stat_acc = row['best_stat_acc']

    if best_exp_val is None: return stat_val, 1.0, 0.0, 'ensemble', 'none'
    if stat_val is None:     return best_exp_val, 0.0, 1.0, 'none', best_exp_name

    if lc == 'NPI-Ramp':
        w_stat, w_exp = 0.15, 0.85
    elif lc == 'Decline':
        w_stat, w_exp = 0.30, 0.70
    else:
        if best_exp_wacc >= 0.93:   w_stat, w_exp = 0.40, 0.60
        elif best_exp_wacc >= 0.85: w_stat, w_exp = 0.50, 0.50
        else:                        w_stat, w_exp = 0.60, 0.40

    if stat_acc is not None and stat_acc < 0.70:
        w_exp  = min(1.0, w_exp + 0.20)
        w_stat = 1.0 - w_exp

    if best_exp_wacc and best_exp_wacc >= 0.95 and lc != 'NPI-Ramp':
        w_exp  = min(1.0, w_exp + 0.10)
        w_stat = 1.0 - w_exp

    final = (w_stat * stat_val) + (w_exp * best_exp_val)
    return final, w_stat, w_exp, 'ensemble', best_exp_name

final_forecasts = []
for _, row in df_results.iterrows():
    forecast, w_stat, w_exp, stat_m, exp_name = blend_improved(row)
    forecast_rounded = safe_round(forecast)
    exp_orig = row[exp_name]           if exp_name and exp_name != 'none' else None
    exp_corr = row[f'{exp_name}_corrected'] if exp_name and exp_name != 'none' else None
    exp_wacc = row[f'{exp_name}_wacc'] if exp_name and exp_name != 'none' else None
    exp_bias = row[f'{exp_name}_bias'] if exp_name and exp_name != 'none' else None

    final_forecasts.append({
        'rank':             row['rank'],
        'name':             row['name'],
        'lifecycle':        row['lifecycle'],
        'stat_ensemble':    safe_round(row['f_ensemble_scms']),
        'scms_adj':         safe_round(row['scms_adj'], 3),
        'expert_used':      exp_name,
        'expert_orig':      safe_round(exp_orig),
        'expert_corrected': safe_round(exp_corr),
        'expert_bias':      safe_round(exp_bias, 3),
        'expert_wacc':      safe_round(exp_wacc, 3),
        'w_stat':           safe_round(w_stat, 2),
        'w_exp':            safe_round(w_exp, 2),
        'final_forecast':   forecast_rounded,
        'stat_method':      stat_m,
        'best_stat_acc':    row['best_stat_acc'],
    })

df_final = pd.DataFrame(final_forecasts)

print("\n=== STEP 5: FINAL FORECASTS ===")
print(f"{'Rk':<3} {'Product':<40} {'LC':<9} {'Ensemble':>9} {'SCMSadj':>7} "
      f"{'Exp':>4} {'OrigF':>7} {'CorrF':>7} {'Bias':>6} {'wS':>4} {'wE':>4} {'FINAL':>8}")
print("-"*115)
for _, r in df_final.iterrows():
    def f(v, fmt="{:.0f}"): return fmt.format(v) if v is not None else "  n/a"
    def fp(v): return f"{v:+.1%}" if v is not None else "  n/a"
    print(f"[{r['rank']:>2}] {r['name'][:38]:<38} {r['lifecycle'][:8]:<8} "
          f"{f(r['stat_ensemble']):>9} {f(r['scms_adj'],'{:.3f}'):>7} "
          f"{str(r['expert_used']):<4} {f(r['expert_orig']):>7} "
          f"{f(r['expert_corrected']):>7} {fp(r['expert_bias']):>6} "
          f"{r['w_stat']:>4.2f} {r['w_exp']:>4.2f} {f(r['final_forecast']):>8}")

# ── MANUAL OVERRIDES ──────────────────────────────────────────────────────────
overrides = {5: 36500, 6: 1041, 14: 6800, 19: 8200, 22: 9500, 25: 3872, 26: 1750, 29: 370, 30: 1350}
print("\n=== MANUAL OVERRIDES ===")
for rank, new_val in overrides.items():
    idx = df_final[df_final['rank'] == rank].index[0]
    old_val = df_final.at[idx, 'final_forecast']
    df_final.at[idx, 'final_forecast'] = new_val
    print(f"  Product {rank:>2}: {old_val} → {new_val}")

# ── CLEAN SUBMISSION LIST ─────────────────────────────────────────────────────
print("\n=== CLEAN SUBMISSION LIST ===")
print(f"{'Rank':<5} {'Final Forecast':>15}  Product")
print("-"*70)
for _, r in df_final.iterrows():
    print(f"[{r['rank']:>2}]  {str(r['final_forecast']):>15}  {r['name']}")
print(f"\nTotal: {df_final['final_forecast'].notna().sum()}/30")

# ── ACCURACY SIMULATION ON FY26Q1 ────────────────────────────────────────────
print("\n=== BLEND ACCURACY SIMULATION ON FY26Q1 ===")
print(f"{'Rk':<3} {'Product':<42} {'Actual Q1':>10} {'Stat Blend Q1':>14} {'Accuracy':>9} {'Bias':>7}")
print("-"*88)

total_acc = []
for _, row in df_results.iterrows():
    actual_q1 = row['actual_q1']
    if actual_q1 is None or actual_q1 == 0: continue

    bt_stat_map = {
        'avg4':     row['bt_avg4_val'],
        'sqly':     row['bt_sqly_val'],
        'seasonal': row['bt_seasonal_val'],
        'yoy':      row['bt_yoy_val'],
        'decay':    row['bt_decay_val'],
    }
    best_stat_bt = bt_stat_map.get(row['best_stat_method'])
    # Fallback chain — never skip a product
    if best_stat_bt is None: best_stat_bt = row['bt_avg4_val']
    if best_stat_bt is None: best_stat_bt = row['bt_sqly_val']
    if best_stat_bt is None: best_stat_bt = row['bt_seasonal_val']
    if best_stat_bt is None: best_stat_bt = row['bt_decay_val']
    # If still None, use last known actual as naive forecast
    if best_stat_bt is None:
        series = df_master[df_master['rank'] == row['rank']].iloc[0]['actuals']
        valid = [v for v in series if v is not None]
        best_stat_bt = valid[-1] if valid else None
    if best_stat_bt is None: continue

    acc  = 1 - abs((best_stat_bt - actual_q1) / actual_q1)
    bias = (best_stat_bt - actual_q1) / actual_q1
    total_acc.append(acc)

    flag = " <-- review" if acc < 0.80 else ""
    print(f"[{row['rank']:>2}] {row['name'][:40]:<40} "
          f"{actual_q1:>10,.0f} {best_stat_bt:>14,.0f} "
          f"{acc:>8.1%} {bias:>+7.1%}{flag}")

print(f"\nAverage stat accuracy on Q1 back-test: {sum(total_acc)/len(total_acc):.1%}")
print(f"Products shown:          {len(total_acc)}/30")
print(f"Products above 90%:      {sum(1 for a in total_acc if a >= 0.90)}/{len(total_acc)}")
print(f"Products below 80%:      {sum(1 for a in total_acc if a < 0.80)}/{len(total_acc)}")