def gen_bret_T(sdate,
               edate,
               T_0_time='09:30:00',
               bret_path='/gyqk/gyqk_res/bret'):
    import datetime as dt
    from gyqktools import files
    import pandas as pd
    try:
        from . import tools
    except:
        import tools
    print(f'{dt.datetime.now()}: start gen_bret_T from {sdate} to {edate}')
    # T = int(bret_type.split('_')[-1].replace('T', ''))
    T = 20
    _edate = str(
        tools.shift_n_tdate(tools.get_closest_tdate(edate), n=-(T + 1)).date())
    print(f'Please chech that bret exists at least up to {_edate}!')
    bret_table = files.read_parquet(sdate, _edate, path=bret_path)
    T_0_time = dt.time.fromisoformat(T_0_time)
    rack = bret_table[bret_table.time == T_0_time][['datetime', 'code']]
    bret_pivot = bret_table.pivot(index='datetime',
                                  columns='code',
                                  values='bret').fillna(0)

    bret_daily = bret_pivot.rolling(9, 9).sum()
    bret_oto_pivot = bret_daily[bret_daily.index.time == T_0_time].shift(-1)
    for i in [1, 2, 5, 20]:
        rack = rack.merge(bret_oto_pivot.rolling(
            i, i).sum().shift(-(i - 1)).stack().rename(f'bret_T{i}'),
                          how='left',
                          left_on=['datetime', 'code'],
                          right_index=True)
    rack.eval('bret_T1_2 = bret_T2 - bret_T1', inplace=True)
    rack.eval('bret_T3_5 = bret_T5 - bret_T2', inplace=True)
    rack.eval('bret_T6_20 = bret_T20 - bret_T5', inplace=True)
    print(f'{dt.datetime.now()}: finish gen_bret_T from {sdate} to {edate}')
    return rack[rack.datetime <= pd.to_datetime(edate)][[
        'datetime', 'code', 'bret_T1', 'bret_T1_2', 'bret_T3_5', 'bret_T6_20'
    ]]
