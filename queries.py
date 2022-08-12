import datetime

import sqlalchemy.exc
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import os
import pandas as pd
from models.tables import *
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, List, Literal


load_dotenv(".env")


def get_gateway_inventory(etl_engine: Engine) -> pd.DataFrame:
    sql = """select
    g.address,
    g.name,
    g.location,
    g.owner,
    g.payer,
    m.name as maker,
    l.long_country,
    l.long_state,
    l.long_city,
    g.first_block
    
    from gateway_inventory g 
    left join makers m on m.address = g.payer
    left join locations l on l.location = g.location;    
    """
    return pd.read_sql(sql, con=etl_engine, index_col="address")


def insert_records(denylist_engine: Engine, issues: list, entries: list):
    with Session(denylist_engine) as session:
        if len(issues) > 0:
            session.execute(insert(Issues).values(issues).on_conflict_do_nothing())
            session.commit()

        if len(entries) > 0:
            session.execute(insert(Entries).values(entries).on_conflict_do_nothing())
            session.commit()


def upsert_entries(denylist_engine: Engine, entries: list):
    with Session(denylist_engine) as session:
        for entry in entries:
            session.execute(insert(Entries).values(entry)
                            .on_conflict_do_update(constraint="entries_pkey", set_=entry))
        session.commit()


def upsert_issues(denylist_engine: Engine, issues: list):
    with Session(denylist_engine) as session:
        for issue in issues:
            session.execute(insert(Issues).values(issue)
                            .on_conflict_do_update(constraint="issues_pkey", set_=issue))
        session.commit()



def mark_entry_report_as_complete(denylist_engine: Engine, address: str, issue_number: int):
    sql = f"""update entries set reports_generated = true where address = '{address}' and issue_number = {issue_number};"""
    with Session(denylist_engine) as session:
        session.execute(sql)
        session.commit()


def mark_issue_report_as_complete(denylist_engine: Engine, issue_number: int):
    sql = f"""update issues set reports_generated = true where number = {issue_number};"""
    with Session(denylist_engine) as session:
        session.execute(sql)
        session.commit()


def get_entries_for_issue(denylist_engine: Engine, issue_number: int) -> List[str]:
    sql = f"""select address from entries where issue_number = {issue_number};"""
    with Session(denylist_engine) as session:
        res = session.execute(sql).fetchall()

    return [r[0] for r in res]


def get_entries_table(denylist_engine: Engine, issue_number: Optional[int] = None) -> List[dict]:
    sql = f"""select
    e.address,
    e.issue_number,
    e.reports_generated,
    e.review_status,
    e.name,
    e.location,
    e.owner,
    e.payer,
    e.maker,
    e.long_country,
    e.long_state,
    e.long_city,
    e.first_block,
	(select array_agg(e2.issue_number) from entries e2 where e2.issue_number != e.issue_number and e2.address = e.address) as other_mentioned_issues,
	(select array_agg(p.number) from pulls p join pull_issues pi on pi.pull = p.number join entries e2 on e2.issue_number = pi.issue where e2.address = e.address and p.state = 'closed') as closed_pulls,
	(select array_agg(p.number) from pulls p join pull_issues pi on pi.pull = p.number join entries e2 on e2.issue_number = pi.issue where e2.address = e.address and p.state = 'open') as open_pulls

    
    from entries e {f'where e.issue_number = {issue_number}' if issue_number else ''};"""
    with Session(denylist_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = [
        {
            "address": r[0],
            "issue_number": r[1],
            "reports_generated": r[2],
            "review_status": r[3],
            "name": r[4],
            "location": r[5],
            "owner": r[6],
            "payer": r[7],
            "maker": r[8],
            "long_country": r[9],
            "long_state": r[10],
            "long_city": r[11],
            "first_block": r[12],
            "other_mentioned_issues": str(r[13]),
            "closed_pulls": str(r[14]),
            "open_pulls": str(r[15])
        } for r in res
    ]
    return result_dict


def get_issue_details(denylist_engine: Engine, issue_number: int, with_body: bool = True, serializable: bool = True):
    sql = f"""select
     i.number,
     i.title,
     i.user,
     i.labels,
     i.issue_type,
     i.state,
     i.created_at,
     i.updated_at,
     i.closed_at,
     i.comments,
     {'i.body,' if with_body else 'NULL,'}
     i.reactions,
     (select array_agg(pi.pull) from pull_issues pi join pulls p on p.number = pi.pull where p.state = 'open' and pi.issue = i.number) as open_pulls,
     (select array_agg(pi.pull) from pull_issues pi join pulls p on p.number = pi.pull where p.state = 'closed' and pi.issue = i.number) as closed_pulls
     from issues i where number = {issue_number};"""

    with Session(denylist_engine) as session:
        res = session.execute(sql).one()

    result_dict = {
        "number": res[0],
        "title": res[1],
        "user": res[2],
        "labels": res[3],
        "issue_type": res[4],
        "state": res[5],
        "created_at": res[6].isoformat() if serializable and res[6] else res[6],
        "updated_at": res[7].isoformat() if serializable and res[7] else res[7],
        "closed_at": res[8].isoformat() if serializable and res[8] else res[8],
        "comments": res[9] if res[8] else "",
        "body": res[10],
        "reactions": res[11] if res[11] else "",
        "open_pulls": str(res[12]),
        "closed_pulls": str(res[13])
    }

    return result_dict


def get_issues_without_reports(denylist_engine: Engine, since: Optional[str] = None) -> List[int]:
    sql = f"""select number from issues 
    where reports_generated is not true {f"and created_at > '{since}'" if since else ""} order by number asc;"""
    with Session(denylist_engine) as session:
        res = session.execute(sql).fetchall()

    return [r[0] for r in res]


def update_entry_status(denylist_engine: Engine, issue_number: int, address: str, new_status: Literal["not_reviewed", "valid", "invalid", "unknown"]):
    sql = f"""update entries set review_status = '{new_status}' where address = '{address}' and issue_number = {issue_number};"""
    with Session(denylist_engine) as session:
        session.execute(sql)
        session.commit()


def get_user(denylist_engine: Engine, user_id: str) -> dict:
    sql = f"""select 
    u.user,
    u.last_issue,
    u.last_created_at::text,
    u.first_issue,
    u.first_created_at::text,
    u.n_issues,
    u.n_closed_issues,
    u.n_additions_submitted,
    u.n_additions_closed,
    u.n_removals_submitted,
    u.n_removals_closed
    
    from users u where u.user = '{user_id}';
    """
    with Session(denylist_engine) as session:
        res = session.execute(sql).one()

    result_dict = {
        "user": res[0],
        "last_issue": res[1],
        "last_created_at": res[2],
        "first_issue": res[3],
        "first_created_at": res[4],
        "n_issues": res[5],
        "n_closed_issues": res[6],
        "n_additions_submitted": res[7],
        "n_additions_closed": res[8],
        "n_removals_submitted": res[9],
        "n_removals_closed": res[10]
    }

    return result_dict


def get_issues_summary(denylist_engine: Engine, limit: Optional[int] = 100) -> List[dict]:
    sql = f"""select
    i.number,
    i.title,
    i.user,
    i.created_at::text,
    i.issue_type,
    (select count(*) from entries e where e.issue_number = i.number) as n_entries,
    (select array_agg(pi.pull) from pull_issues pi join pulls p on p.number = pi.pull where p.state = 'open' and pi.issue = i.number) as open_pulls,
    (select array_agg(pi.pull) from pull_issues pi join pulls p on p.number = pi.pull where p.state = 'closed' and pi.issue = i.number) as closed_pulls
    
    from issues i where reports_generated = True order by i.number desc {'limit ' + str(limit) if limit else ''};"""

    with Session(denylist_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = [
        {
            "number": r[0],
            "title": r[1],
            "user": r[2],
            "created_at": r[3],
            "issue_type": r[4],
            "n_entries": r[5],
            "open_pulls": str(r[6]),
            "closed_pulls": str(r[7])
        } for r in res
    ]
    return result_dict


def get_max_issue_timestamp(denylist_engine: Engine) -> datetime.datetime:
    sql = """select max(created_at) from issues;"""

    with Session(denylist_engine) as session:
        res = session.execute(sql).one()
    return res[0]


def get_unparsed_issues(denylist_engine: Engine):
    sql = """with entries_per_issue as (
    select 
        i.number as issue,
        count(e.address) as n_entries
    from issues i left join entries e on i.number = e.issue_number 
    group by issue)
    
    select epi.issue, i2.body from entries_per_issue epi join issues i2 on i2.number = epi.issue where n_entries = 0;"""

    with Session(denylist_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = [
        {
            "number": r[0],
            "body": r[1]
        } for r in res
    ]
    return result_dict


def upsert_pulls(denylist_engine: Engine, pulls: list[dict], issue_joins: list[dict]):
    with Session(denylist_engine) as session:
        for pull in pulls:
            upsert_stmt = insert(Pulls).values(pull)\
                .on_conflict_do_update(constraint="pulls_pkey", set_=pull)
            session.execute(upsert_stmt)
            session.commit()

        for issue_join in issue_joins:
            try:
                upsert_stmt = insert(PullIssues).values(issue_join)\
                    .on_conflict_do_update(constraint="pull_issues_pkey", set_=issue_join)
                session.execute(upsert_stmt)
                session.commit()
            except sqlalchemy.exc.IntegrityError:
                # edge case where issue was deleted
                session.rollback()
                continue



def get_height_for_timestamp(etl_engine: Engine, timestamp: str) -> int:
    sql = f"""select max(height) from blocks where timestamp < '{timestamp}';"""

    with Session(etl_engine) as session:
        res = session.execute(sql).one()
    return res[0]


def get_distance_vs_rssi(etl_engine: Engine, address: str, n_blocks: int = 43200, max_block: Optional[int] = None) -> dict:
    max_block = max_block if max_block else 'max(height)'

    sql = f"""with hashes as 
    
    (select transaction_hash, actor from transaction_actors where 
    actor = '{address}' 
    and actor_role = 'witness' 
    and block > (select {max_block} - {n_blocks} from blocks limit 1)),
    
    target_transactions as 
    (select fields, hash from transactions where 
    (type = 'poc_receipts_v2' or type = 'poc_receipts_v1') 
    and transactions.hash in (select transaction_hash from hashes)),
    
    metadata as 
    (select 
    actor as witness,
    fields->'path'->0->>'challengee' as transmitter,
    fields->'path'->0->'witnesses' as w, 
    fields->'path'->0->>'challengee_location' as location_tx from hashes 
    left join target_transactions on hashes.transaction_hash = target_transactions.hash),
    
    pairs as
    (select 
    
    witness,
    transmitter,
    location_tx,
    
    (select t -> 'signal' from jsonb_array_elements(w) as x(t) where t->>'gateway' = witness)::int as rssi,
    (select t ->> 'location' from jsonb_array_elements(w) as x(t) where t->>'gateway' = witness) as location_rx
    
    from metadata),
    
    results as
    (select
    
    witness,
    transmitter,
    rssi,
    
    ST_DistanceSphere(ST_Centroid(tx.geometry), ST_Centroid(rx.geometry)) as distance_m
    
    from pairs 
    
    join locations tx on tx.location = location_tx
    join locations rx on rx.location = location_rx)
    
    select distance_m, rssi from results where distance_m < 100e3;"""

    with Session(etl_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = {
        "distance_m": [r[0] for r in res],
        "rssi": [r[1] for r in res]
    }
    return result_dict


def get_witnessed_makers(etl_engine: Engine, address: str, n_blocks: int = 43200, max_block: Optional[int] = None) -> dict:
    max_block = max_block if max_block else 'max(height)'

    sql = f"""with hashes as 
        
    (select transaction_hash, actor from transaction_actors where 
    actor = '{address}' 
    and actor_role = 'witness' 
    and block > (select {max_block} - {n_blocks} from blocks limit 1)),
    
    target_transactions as 
    (select fields, hash from transactions where transactions.hash in (select transaction_hash from hashes)),
    
    metadata as 
    (select 
    distinct fields->'path'->0->>'challengee' as transmitter
    from hashes
    left join target_transactions on hashes.transaction_hash = target_transactions.hash)
    
    select
    
    m.name as maker, 
    count(*) as n_witnessed
    
    from metadata mt join gateway_inventory g on g.address = mt.transmitter join makers m on m.address = g.payer group by maker;"""

    with Session(etl_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = {
        "maker": [r[0] for r in res],
        "n_witnessed": [r[1] for r in res],
        "as_of_block": max_block
    }
    return result_dict


def get_hotspot_details(etl_engine: Engine, address: str) -> dict:
    sql = f"""select 
    
    g.name as name,
    g.owner as owner,
    g.first_block as first_block,
    g.last_block as last_block,
    g.reward_scale as reward_scale,
    g.elevation as elevation,
    g.gain as gain,
    g.nonce as nonce,
    m.name as maker,
    l.long_country as country,
    l.long_state as state,
    l.long_city as city,
    g.location as location,
    (select max(height) from blocks) as as_of_block
    
    from gateway_inventory g 
    join makers m on g.payer = m.address
    join locations l on l.location = g.location
    where g.address = '{address}';"""

    with Session(etl_engine) as session:
        res = session.execute(sql).one()

    result_dict = {
        "name": res[0],
        "owner": res[1],
        "first_block": res[2],
        "last_block": res[3],
        "reward_scale": res[4],
        "elevation": res[5],
        "gain": res[6],
        "nonce": res[7],
        "maker": res[8],
        "country": res[9],
        "state": res[10],
        "city": res[11],
        "location": res[12],
        "as_of_block": res[13]
    }
    return result_dict


def get_witness_graph(etl_engine: Engine, address: str, n_blocks: int = 43200, max_block: Optional[int] = None) -> dict:
    max_block = max_block if max_block else 'max(height)'

    sql = f"""with first_hop as (
    select distinct on (witness_address) transmitter_address, witness_address, 1 as hop 
    from challenge_receipts_parsed 
    where transmitter_address = '{address}' and block > (select {max_block} - {n_blocks} from blocks limit 1)
    ),
    
    second_hop as (
    select distinct on (witness_address) transmitter_address, witness_address, 2 as hop 
    from challenge_receipts_parsed 
    where transmitter_address in (select witness_address from first_hop) and block > (select {max_block} - {n_blocks} from blocks limit 1)
    ),
    
    combined as (
    select * from first_hop union select * from second_hop
    )
    
    select 
    
    c.transmitter_address,
    c.witness_address,
    c.hop,
    m.name as maker,
    g.owner as owner,
    g.location as location,
    g.first_block as first_block
    
    from combined c
    join gateway_inventory g on c.witness_address = g.address 
    join makers m on m.address = g.payer;"""

    with Session(etl_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = {
        "transmitter_address": [r[0] for r in res],
        "witness_address": [r[1] for r in res],
        "hop": [r[2] for r in res],
        "maker": [r[3] for r in res],
        "owner": [r[4] for r in res],
        "location": [r[5] for r in res],
        "first_block": [r[6] for r in res]
    }
    return result_dict


def get_rssi_vs_snr(etl_engine: Engine, address: str, n_blocks: int = 43200, max_block: Optional[int] = None) -> dict:
    max_block = max_block if max_block else 'max(height)'

    sql = f"""with hashes as 
        
    (select transaction_hash, actor from transaction_actors where 
    actor = '{address}' 
    and actor_role = 'witness' 
    and block > (select {max_block} - {n_blocks} from blocks limit 1)),
    
    target_transactions as 
    (select fields, hash from transactions where 
    (type = 'poc_receipts_v2' or type = 'poc_receipts_v1') 
    and transactions.hash in (select transaction_hash from hashes)),
    
    metadata as 
    (select 
    
    actor as witness,
    fields->'path'->0->'witnesses' as w
    from hashes
    left join target_transactions on hashes.transaction_hash = target_transactions.hash)
    
    select 
    
    (select t -> 'signal' from jsonb_array_elements(w) as x(t) where t->>'gateway' = witness)::int as rssi,
    (select t -> 'snr' from jsonb_array_elements(w) as x(t) where t->>'gateway' = witness)::float as snr
    
    from metadata;"""

    with Session(etl_engine) as session:
        res = session.execute(sql).fetchall()

    result_dict = {
        "rssi": [r[0] for r in res],
        "snr": [r[1] for r in res]
    }
    return result_dict