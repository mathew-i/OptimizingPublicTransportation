"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

# As I was asking in the knowledge, turnstile stream has more sense to me
# but it's not the preffered form, that's why I commented it out and use TABLE.
#KSQL_STATEMENT = """
#CREATE STREAM turnstile (
#    station_id INTEGER,
#    station_name VARCHAR,
#    line VARCHAR
#) WITH (KAFKA_TOPIC='org.chicago.cta.turnstiles',
#    VALUE_FORMAT='AVRO'
#);

#CREATE TABLE turnstile_summary
#WITH (VALUE_FORMAT='JSON') AS
#    select station_id, count()
#      from turnstile
#     group by station_id;
#"""

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (KAFKA_TOPIC='org.chicago.cta.turnstiles',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON') AS
    select station_id, count()
      from turnstile
     group by station_id;
"""

def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
        logging.debug("KSQL statement executed successfully")
    except:
        logger.error(f"Failed to execute KSQL statement {json.dumps(resp.json(), indent=2)}")


if __name__ == "__main__":
    execute_statement()
