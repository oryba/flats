from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from flatfy import conn


def get_discounts():
    query = f"""
        select distinct
            today.flat_id,
            s.title,
            s.id as sid,
            yesterday.price as prev, today.price as now,
            today.price - yesterday.price as diff,
            round((1.0 * today.price / yesterday.price - 1) * 100) as diff_pct,
            round(today.price / today.area) as sqm,
            today.renovation
        from offer today join offer yesterday on today.flat_id = yesterday.flat_id
            and today.scan_date >= date('now')
            and yesterday.scan_date >= date('now','-1 days')
            and yesterday.scan_date < date('now')
        left join selection s on s.id = today.selection_id
        where today.price != yesterday.price;
    """
    data = conn.execute(text(query)).mappings().all()
    return data


def get_daily_stats():
    query = f"""
        select date(o.scan_date) as dt, o.price,
            case
                when area < 45 then 'S'
                when area >= 45 and area < 65 then 'M'
                when area >= 65 and area < 85 then 'L'
                else 'XL'
                end as type,
            price / area as m2_price
        from offer o
        where price > 12000 and price < 10000000
    """
    data = conn.execute(text(query)).mappings().all()
    df = pd.DataFrame(data)
    med = df.groupby('dt').agg(
        q1=('price', lambda x: x.quantile(0.2)),
        q2=('price', lambda x: x.quantile(0.5)),
        q3=('price', lambda x: x.quantile(0.8))
    )
    return med


def get_new_offers():
    query = f"""
        select
            distinct
            today.flat_id,
            s.title,
            s.id as sid,
            round(today.price) as price,
            round(today.price / today.area) as sqm,
            today.scan_date,
            today.insert_date,
            today.renovation
        from
            offer today
            left join selection s on s.id = today.selection_id
            where today.insert_date is not null and today.insert_date >= date('now', '-1 days')
        group by 
            today.flat_id;
    """
    return conn.execute(text(query)).mappings().all()


def get_recent_stats(rf=None, last_days=None):
    ren_filter = f"and renovation = {rf}" if rf is not None else ''
    if last_days:
        ld_filter = f"and insert_date >= date('now','-{last_days} days')"
    else:
        ld_filter = ""
    query = f"""
        select s.title, type, round(avg(m2_price)) as m2, count(*) as flats
        from (
                 select *,
                        case
                            when area < 45 then 'S'
                            when area >= 45 and area < 65 then 'M'
                            when area >= 65 and area < 85 then 'L'
                            else 'XL'
                            end as type,
                        price / area as m2_price
                 from offer
                 where scan_date >= (select date(max(scan_date)) from offer)
                     and price > 12000 and price < 10000000
                     {ren_filter}
                     {ld_filter}
             ) o left join selection s on o.selection_id = s.id
        group by s.title, type
        order by s.title, case type
            when 'S' then 0
            when 'M' then 1
            when 'L' then 2
            when 'XL' then 3
        end
    """
    data = conn.execute(text(query)).mappings().all()
    return pd.DataFrame({
        "Вибірка": [d['title'] for d in data],
        "Розмір": [d['type'] for d in data],
        "Ціна м2": [d['m2'] for d in data]
    })
