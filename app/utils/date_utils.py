import arrow

def get_formatted_current_time():
    """
    Gets the current UTC time and formats it.

    Returns:
        str: The formatted current time.
    """
    return arrow.utcnow().format('YYYY-MM-DD HH:mm:ss ZZ')
