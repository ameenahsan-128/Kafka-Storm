from src.spouts.Kafkaspout import KafkaSpout
from streamparse import Topology



class MyTopology(Topology):
    kafka_spout = KafkaSpout.spec()

    def configure(self):
        pass

if __name__ == '__main__':
    MyTopology().run()