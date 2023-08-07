Edit message definition
=======================

Some ROS messages definitons differ between ROS1 and ROS2, most notably ``std_msgs/msg/Header``. Even withing the same version of ROS, message definitions can evolve over time. As it is such a common case, rosbags handles ``std_msgs/msg/Header`` automatically when converting between rosbag1 and rosbag2. For all other types it is up to the end user to convert between message definitions afterwards.


Update CameraInfo
-----------------

.. literalinclude:: ./edit_message_definition.py
