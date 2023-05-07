from streamparse import Bolt
from hdfs import InsecureClient
from queue import Queue
import os



class Put(Bolt):
        
    def initialize(self, storm_conf, context):
        # Connect to HDFS

        self.hdfs_client = InsecureClient('http://localhost:50070', user='hdfs')

        # Specify the HDFS directory path
        self.hdfs_directory = '/aian/'

        def process(self, tup):
            # Get the list of tuples from the input
            tuples = tup.values
            self.log(tuples)

            for tuple_data in tuples:
                user_name = tuple_data
                self.log(user_name)

                # Define the data to be written
                file_name = str(tup.id)  # Modify as needed
                file_path = os.path.join(self.hdfs_directory, file_name + '.txt')
                data = user_name

                # Write the data to HDFS
                with self.hdfs_client.write(file_path, overwrite=False) as writer:
                    writer.write(data)

                    self.log('Data added to HDFS')

