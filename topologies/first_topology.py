from streamparse import Spout, Bolt, Stream, Grouping, Topology
from kafka import KafkaConsumer
import os
from src.spouts.Kafkaspout import KafkaSpout
from src.bolts.second_bolt import Put
from src.bolts.First_bolt import Emit

class MyTopology(Topology):
    kafka_spout = KafkaSpout.spec()
    filter_bolt = Emit.spec(inputs={kafka_spout: Grouping.fields('message')})
    # hdfs_bolt = Put.spec(inputs={filter_bolt: Grouping.fields('filtered_user')})

if __name__ == '__main__':
    MyTopology().submit('my_topology')