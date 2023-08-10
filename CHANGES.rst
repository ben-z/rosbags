.. _changes:

Changes
=======

0.9.16 - 2023-08-11
-------------------

- Support rosbag2 up to version 8 `#59`_
- Implement RIHS01 hashing of types
- Fix chunk size bug in rosbag1 writer `#58`_
- Fix handling of empty messages `#56`_
- Allow empty bags, chunks, and connections
- Avoid field name collisions with python keywords `#51`_
- Improve msg/idl type matching
- Improve some examples

.. _#51: https://gitlab.com/ternaris/rosbags/issues/51
.. _#56: https://gitlab.com/ternaris/rosbags/issues/56
.. _#58: https://gitlab.com/ternaris/rosbags/issues/58
.. _#59: https://gitlab.com/ternaris/rosbags/issues/59


0.9.15 - 2023-03-02
-------------------
- Refactor rosbag2 Reader for multipe storage backends
- Improve parsing of IDL files
- Handle bags contaning only connection records
- Add AnyReader to documentation
- Add initial MCAP reader for rosbag2 `#33`_

.. _#33: https://gitlab.com/ternaris/rosbags/issues/33


0.9.14 - 2023-01-12
-------------------
- Fix reader example in README `#40`_
- Flush decompressed files rosbag2.Reader
- Advertise Python 3.11 compatibility

.. _#40: https://gitlab.com/ternaris/rosbags/issues/40


0.9.13 - 2022-09-23
-------------------
- Fix parsing of comments in message definitions `#31`_
- Fix parsing of members starting with ``string`` in message definitions `#35`_
- Change lz4 compression level to 0 `#36`_
- Add include filters to rosbag conversion `#38`_
- Implement direct ros1 (de)serialization

.. _#31: https://gitlab.com/ternaris/rosbags/issues/31
.. _#35: https://gitlab.com/ternaris/rosbags/issues/35
.. _#36: https://gitlab.com/ternaris/rosbags/issues/36
.. _#38: https://gitlab.com/ternaris/rosbags/issues/38


0.9.12 - 2022-07-27
-------------------
- Add support for rosbag2 version 6 metadata `#30`_
- Enable rosbags-convert to exclude topics `#25`_

.. _#30: https://gitlab.com/ternaris/rosbags/issues/30
.. _#25: https://gitlab.com/ternaris/rosbags/issues/25


0.9.11 - 2022-05-17
-------------------
- Report start_time and end_time on empty bags


0.9.10 - 2022-05-04
-------------------
- Add support for multiple type stores
- Document which types are supported out of the box `#21`_
- Unify Connection and TopicInfo objects across rosbag1 and rosbag2
- Add experimental all-in-one reader for rosbag1, split rosbag1, and rosbag2
- Convert reader and writer .connection attribute from dict to list
- Add support for rosbag2 version 5 metadata `#18`_
- Speed up opening of rosbag1 files
- Fix serialization of empty message sequences `#23`_

.. _#18: https://gitlab.com/ternaris/rosbags/issues/18
.. _#21: https://gitlab.com/ternaris/rosbags/issues/21
.. _#23: https://gitlab.com/ternaris/rosbags/issues/23


0.9.9 - 2022-01-10
------------------
- Fix documentation code samples `#15`_
- Fix handling of padding after empty sequences `#14`_
- Support conversion from rosbag2 to rosbag1 `#11`_

.. _#11: https://gitlab.com/ternaris/rosbags/issues/11
.. _#14: https://gitlab.com/ternaris/rosbags/issues/14
.. _#15: https://gitlab.com/ternaris/rosbags/issues/15


0.9.8 - 2021-11-25
------------------
- Support bool and float constants in msg files


0.9.7 - 2021-11-09
------------------
- Fix parsing of const fields with string value `#9`_
- Parse empty msg definitions
- Make packages PEP561 compliant
- Parse msg bounded fields and default values `#12`_

.. _#9: https://gitlab.com/ternaris/rosbags/issues/9
.. _#12: https://gitlab.com/ternaris/rosbags/issues/12

0.9.6 - 2021-10-04
------------------
- Do not match msg separator as constant value


0.9.5 - 2021-10-04
------------------
- Add string constant support to msg parser


0.9.4 - 2021-09-15
------------------
- Make reader1 API match reader2
- Fix connection mapping for reader2 messages `#1`_, `#8`_

.. _#1: https://gitlab.com/ternaris/rosbags/issues/1
.. _#8: https://gitlab.com/ternaris/rosbags/issues/8

0.9.3 - 2021-08-06
------------------

- Add const fields to type classes
- Add CDR to ROS1 bytestream conversion
- Add ROS1 message definiton generator
- Use connection oriented APIs in readers and writers
- Add rosbag1 writer


0.9.2 - 2021-07-08
------------------

- Support relative type references in msg files


0.9.1 - 2021-07-05
------------------

- Use half-open intervals for time ranges
- Create appropriate QoS profiles for latched topics in converted bags
- Fix return value tuple order of messages() in documentation `#2`_
- Add type hints to message classes
- Remove non-default ROS2 message types
- Support multi-line comments in idl files
- Fix parsing of msg files on non-POSIX platforms `#4`_

.. _#2: https://gitlab.com/ternaris/rosbags/issues/2
.. _#4: https://gitlab.com/ternaris/rosbags/issues/4


0.9.0 - 2021-05-16
------------------

- Initial Release
