#!/usr/bin/env python3
"""
Claim Freshness Refresh Script

Scans all claims and updates their freshness_status based on data_date vs current date.
Also flags claims that need re-validation based on age thresholds.

Usage:
    python3 scripts/refresh_claim_freshness.py [--dry-run]
"""
import sqlite3
import argparse
from datetime import datetime, timedelta

FRESHNESS_THRESHOLDS = {
    'fresh': 90,       # <90 days old
    'aging': 180,      # 90-180 days old
    'stale': 365,      # 180-365 days old
    'expired': 99999,  # >365 days old
}

def compute_freshness(data_date_str: str, now: datetime) -> str:
    """Compute freshness status based on data_date age."""
    try:
        data_date = datetime.fromisoformat(data_date_str.replace('Z', '+00:00'))
        data_date = data_date.replace(tzinfo=None)  # normalize to naive
    except (ValueError, TypeError):
        return 'stale'  # unknown date = stale
    
    age_days = (now - data_date).days
    if age_days < 0:
        return 'fresh'  # future date = fresh
    
    for status, threshold in sorted(FRESHNESS_THRESHOLDS.items(), key=lambda x: x[1]):
        if age_days < threshold:
            return status
    return 'expired'

def main():
    parser = argparse.ArgumentParser(description='Refresh claim freshness')
    parser.add_argument('--dry-run', action='store_true', help='Preview without updating')
    parser.add_argument('--db', default='state/finagent.sqlite', help='Database path')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=5)
    now = datetime.utcnow()
    
    claims = conn.execute(
        'SELECT claim_id, data_date, freshness_status FROM claims WHERE data_date IS NOT NULL'
    ).fetchall()

    updates = {'fresh': 0, 'aging': 0, 'stale': 0, 'expired': 0}
    changed = 0

    for claim_id, data_date, old_status in claims:
        new_status = compute_freshness(data_date, now)
        updates[new_status] = updates.get(new_status, 0) + 1
        
        if new_status != old_status:
            changed += 1
            if not args.dry_run:
                conn.execute(
                    'UPDATE claims SET freshness_status = ? WHERE claim_id = ?',
                    (new_status, claim_id)
                )

    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f'Scanned {len(claims)} claims')
    print(f'Changed {changed} freshness statuses {"(dry-run)" if args.dry_run else ""}')
    for status, count in sorted(updates.items()):
        print(f'  {status}: {count}')

if __name__ == '__main__':
    main()
