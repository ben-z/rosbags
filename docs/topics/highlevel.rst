Highlevel APIs
==============
The :py:mod:`rosbags.highlevel` package provides classes that abstract the complexity of ROS types, serialization and message access into single easy-to-use interfaces.

All in one reader
-----------------
Instances of the :py:class:`AnyReader <rosbags.highlevel.AnyReader>` class give unified access to ROS1 and ROS2 bag files. If a bag file includes message definitions the reader auto-registers all messages into a blank type store, otherwise it falls back to the default type store. It also exposes appropriate deserialization methods on the reader instance itself. 

.. code-block:: python

   from pathlib import Path

   from rosbags.highlevel import AnyReader

   # create reader instance and open for reading
   with AnyReader([Path('/home/ros/rosbag_2020_03_24')]) as reader:
       connections = [x for x in reader.connections if x.topic == '/imu_raw/Imu']
       for connection, timestamp, rawdata in reader.messages(connections=connections):
            msg = reader.deserialize(rawdata, connection.msgtype)
            print(msg.header.frame_id)

AnyReader takes a list of ``pathlib.Path`` instances as arguments. It can take either one ROS2 bag file or one or more ROS1 bag files belonging to a split bag. The reader will replay ROS1 split bags in correct timestamp order.
