chargebee.py line 270 — the strongest one:

The watermark comes from get_bronze_watermark() as a timestamp string, but Chargebee's updated_at[after] expects a Unix epoch. I don't see a conversion anywhere between the notebook and here, so full loads will work and incrementals will break on the second run. Should we convert inside fetch_raw() so the Chargebee-specific format stays with the Chargebee connector?

purge_chargebee_staging.py lines 63-64:

This reads the table into retained_df and then overwrites that same table. It also rewrites every retained row and does two full counts, so it's three passes to delete a few days of data. Could we use DELETE FROM {table} WHERE to_date(batch_date) < date_sub(current_date(), 6) instead? Atomic, and no self-reference.
