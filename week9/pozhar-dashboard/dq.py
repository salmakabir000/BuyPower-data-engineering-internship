import yaml
import pandas as pd
import sqlite3
import re
import sys
import os

def load_dataset(source):
    if source['type'] == 'sqlite':
        path = os.path.expanduser(source['path'])
        conn = sqlite3.connect(path)
        df = pd.read_sql(f"SELECT * FROM {source['table']}", conn)
        conn.close()
    elif source['type'] == 'parquet':
        df = pd.read_parquet(os.path.expanduser(source['path']))
    return df

def run_checks(df, checks):
    results = []
    
    for check in checks:
        check_type = check['type']
        severity = check.get('severity', 'warning')
        
        if check_type == 'not_null':
            col = check['column']
            failures = df[col].isnull().sum()
            results.append({
                'column': col,
                'type': check_type,
                'failures': failures,
                'severity': severity,
                'passed': failures == 0
            })
        
        elif check_type == 'unique':
            col = check['column']
            failures = df[col].duplicated().sum()
            results.append({
                'column': col,
                'type': check_type,
                'failures': failures,
                'severity': severity,
                'passed': failures == 0
            })
        
        elif check_type == 'in_set':
            col = check['column']
            valid = set(check['values'])
            failures = (~df[col].isin(valid)).sum()
            results.append({
                'column': col,
                'type': check_type,
                'failures': failures,
                'severity': severity,
                'passed': failures == 0
            })
        
        elif check_type == 'range':
            col = check['column']
            failures = ((df[col] < check['min']) | (df[col] > check['max'])).sum()
            results.append({
                'column': col,
                'type': check_type,
                'failures': failures,
                'severity': severity,
                'passed': failures == 0
            })
        
        elif check_type == 'regex_match':
            col = check['column']
            pattern = check['pattern']
            failures_mask = ~df[col].astype(str).str.match(pattern)
            failures = failures_mask.sum()
            sample = df[col][failures_mask].head(5).tolist()
            results.append({
                'column': col,
                'type': check_type,
                'failures': failures,
                'severity': severity,
                'passed': failures == 0,
                'sample': sample
            })
        
        elif check_type == 'row_count_min':
            value = check['value']
            passed = len(df) >= value
            results.append({
                'column': 'row count',
                'type': check_type,
                'failures': 0 if passed else 1,
                'severity': severity,
                'passed': passed,
                'detail': f"{len(df)} >= {value}" if passed else f"{len(df)} < {value}"
            })
    
    return results

def print_report(dataset_name, df, results):
    print(f"\nData Quality Report — {dataset_name}")
    print(f"Total rows checked: {len(df):,}")
    print()
    
    warnings = 0
    criticals = 0
    
    for r in results:
        status = "✓" if r['passed'] else "✗"
        if not r['passed']:
            if r['severity'] == 'critical':
                criticals += 1
            else:
                warnings += 1
            msg = f"{status} {r['column']} {r['type']} {r['failures']} failures ({r['severity']})"
            if 'sample' in r and r['sample']:
                msg += f"\n  Sample failing values: {', '.join(str(s) for s in r['sample'])}"
        else:
            detail = r.get('detail', '0 failures')
            msg = f"{status} {r['column']} {r['type']} {detail if 'detail' in r else '0 failures'}"
        print(msg)
    
    print(f"\nResult: {warnings} warning(s), {criticals} critical failure(s)")
    return criticals

def main():
    if len(sys.argv) < 2:
        print("Usage: python dq.py checks/coin_prices.yml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    df = load_dataset(config['source'])
    results = run_checks(df, config['checks'])
    criticals = print_report(config['dataset'], df, results)
    
    sys.exit(1 if criticals > 0 else 0)

if __name__ == '__main__':
    main()
