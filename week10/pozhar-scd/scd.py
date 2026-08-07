import psycopg2
from datetime import datetime, timezone

def apply_scd1(conn, target_table, source_df, business_key, change_cols):
    """Type 1: overwrite in place, no history kept."""
    cur = conn.cursor()

    for _, row in source_df.iterrows():
        bk_value = row[business_key]

        cur.execute(f"SELECT customer_sk FROM {target_table} WHERE customer_id = %s", (bk_value,))
        existing = cur.fetchone()

        if existing:
            set_clause = ", ".join([f"{col} = %s" for col in change_cols])
            values = [row[col] for col in change_cols] + [bk_value]
            cur.execute(f"UPDATE {target_table} SET {set_clause} WHERE customer_id = %s", values)
        else:
            cols = [business_key] + change_cols + ['valid_from', 'is_current']
            placeholders = ", ".join(["%s"] * len(cols))
            values = [row[business_key]] + [row[col] for col in change_cols] + [datetime.now(timezone.utc), True]
            cur.execute(f"INSERT INTO {target_table} ({', '.join(cols)}) VALUES ({placeholders})", values)

    conn.commit()
    print(f"SCD Type 1 applied: {len(source_df)} rows processed")


def apply_scd2(conn, target_table, source_df, business_key, change_cols, run_ts):
    """Type 2: preserve history with valid_from, valid_to, is_current."""
    cur = conn.cursor()

    for _, row in source_df.iterrows():
        bk_value = row[business_key]

        cur.execute(f"""
            SELECT customer_sk, {', '.join(change_cols)}
            FROM {target_table}
            WHERE customer_id = %s AND is_current = TRUE
        """, (bk_value,))
        existing = cur.fetchone()

        if not existing:
            cols = [business_key] + change_cols + ['valid_from', 'valid_to', 'is_current']
            placeholders = ", ".join(["%s"] * len(cols))
            values = [row[business_key]] + [row[col] for col in change_cols] + [run_ts, None, True]
            cur.execute(f"INSERT INTO {target_table} ({', '.join(cols)}) VALUES ({placeholders})", values)
        else:
            existing_sk = existing[0]
            existing_values = existing[1:]
            new_values = [row[col] for col in change_cols]

            changed = any(str(existing_values[i]) != str(new_values[i]) for i in range(len(change_cols)))

            if changed:
                cur.execute(f"""
                    UPDATE {target_table}
                    SET valid_to = %s, is_current = FALSE
                    WHERE customer_sk = %s
                """, (run_ts, existing_sk))

                cols = [business_key] + change_cols + ['valid_from', 'valid_to', 'is_current']
                placeholders = ", ".join(["%s"] * len(cols))
                values = [row[business_key]] + [row[col] for col in change_cols] + [run_ts, None, True]
                cur.execute(f"INSERT INTO {target_table} ({', '.join(cols)}) VALUES ({placeholders})", values)

    conn.commit()
    print(f"SCD Type 2 applied: {len(source_df)} rows processed")
