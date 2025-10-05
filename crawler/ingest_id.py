from datetime import datetime


def generate_ingest_id():
    return datetime.utcnow().strftime("%Y%m%dT")



