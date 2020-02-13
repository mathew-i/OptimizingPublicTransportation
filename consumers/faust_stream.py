"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table", partitions=1)

table = app.Table(
    "TransformedStation",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def sendTransformedStation(stations):
    async for st in stations:
        transformedStation = TransformedStation(
            station_id = st.station_id, 
            station_name = st.station_name,
            order = st.order,
            line = "red" if st.red else ("blue" if st.blue else "green")
        )
        table[st.stop_id] = transformedStation
        print(f"{st.stop_id} - {st.station_name} , {transformedStation.line}")


if __name__ == "__main__":
    app.main()
