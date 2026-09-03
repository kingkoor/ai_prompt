    Returns:
        For a datetime-typed column: an ISO 8601 string
        (e.g. '2026-06-10T15:58:31.000Z'). For a numeric-typed column
        (e.g. an epoch-second bigint): the value as a plain numeric string.
        None if the table does not exist or contains no non-null watermark
        values.
    """
        val = result.first()["max_watermark"]
        if val is None:
            return None
        if hasattr(val, "strftime"):
            return val.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        # Numeric watermark column (e.g. Chargebee's epoch-second updated_at,
        # stored as bigint) - no datetime to format, just pass the value
        # through as a string. The caller's API filter is responsible for
        # knowing whether that string is an ISO timestamp or an epoch.
        return str(val)
    except Exception:
        return None
