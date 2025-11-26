import sys
import pandas as pd
import traceback
try:
    import tushare as ts
except Exception as e:
    print('tushare import error:', e); sys.exit(1)
try:
    import config
except Exception as e:
    print('config import error:', e); sys.exit(1)
ts.set_token(config.TUSHARE_TOKEN)
pro = ts.pro_api()
print('has fund_adj:', hasattr(pro, 'fund_adj'))
try:
    df = pro.fund_adj(ts_code='513100.SH', start_date='20140101', end_date='20250101')
    print('fund_adj rows:', len(df))
    print('fund_adj cols:', list(df.columns))
    print(df.head(10).to_string(index=False))
except Exception as e:
    traceback.print_exc()
