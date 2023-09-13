from functools import wraps


def fdb_transaction(status):
    def decorator(func):
        def wrapper(*args, **kwargs):
            conn = args[0]
            with conn.trans() as trans:
                try:
                    result = func(*args, **kwargs)
                    if status == 0:
                        trans.commit()
                    elif status == -1:
                        trans.rollback()
                    else:
                        raise ValueError("Invalid status value. Use 0 for commit or -1 for rollback.")
                    return result
                except Exception as e:
                    trans.rollback()
                    raise e
        return wrapper
    return decorator
