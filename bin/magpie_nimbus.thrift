#!/usr/local/bin/thrift --gen java:beans,nocamel,hashcode
#  Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

namespace java com.jd.magpie.generated

service Nimbus {

  string submitTopology(1: string id, 2: string jar, 3: string klass)
  string killTopology(1: string id)
  string pauseTopology(1: string id)
  string activeTopology(1: string id)
  string reloadTopology(1: string id)

  string submitTask(1: string id, 2: string jar, 3: string klass, 4: string group, 5: string type)
  string operateTask(1: string id, 2: string command)
}

